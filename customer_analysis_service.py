from flask import current_app
from db_manager import DBManager, safe_float
from datetime import datetime
import config

class CustomerAnalysisService:
    
    def __init__(self, db_manager: DBManager, redis_client=None):
        self.db = db_manager
        self.redis = redis_client

    # --- HÀM KIỂM TRA QUYỀN TRUY CẬP DỮ LIỆU (Yêu cầu 1 & 3) ---
    def check_data_access_permission(self, user_code, user_role, object_id):
        """
        Kiểm tra user có được xem khách hàng này không.
        - ADMIN/GM: Xem hết.
        - SALES: Chỉ xem nếu được phân công trong bảng DTCL năm nay.
        """
        # 1. Admin/Lãnh đạo được xem tất cả
        if user_role in [config.ROLE_ADMIN]:
            return True, "Authorized"

        # 2. Sales: Check bảng DTCL
        current_year = datetime.now().year
        # Lưu ý: Cột [PHU TRACH DS] là mã nhân viên, [Ma Doi Tuong] là mã KH
        query = f"""
            SELECT 1 
            FROM {config.CRM_DTCL}
            WHERE [Ma KH] = ? 
            AND [PHU TRACH DS] = ?
            AND [Nam] = ?
        """
        is_assigned = self.db.get_data(query, (object_id, user_code, current_year))
        
        if is_assigned:
            return True, "Authorized"
        
        return False, f"Bạn không phụ trách khách hàng {object_id} trong năm {current_year}."

    # --- HÀM KIỂM TRA GIỚI HẠN TRUY CẬP (Yêu cầu 2 & 3) ---
    def check_daily_view_limit(self, user_code, user_role):
        """
        Kiểm tra giới hạn xem 7 lần/ngày.
        Sử dụng Redis để đếm. Key: C360_VIEW:{user_code}:{YYYYMMDD}
        """
        # 1. Admin thoải mái
        if user_role in [config.ROLE_ADMIN, config.ROLE_GM]:
            return True, "Unlimited"

        if not self.redis:
            return True, "Redis not connected" # Fallback nếu không có Redis

        today_str = datetime.now().strftime('%Y%m%d')
        key = f"C360_VIEW:{user_code}:{today_str}"
        limit = config.CUSTOMER_360_VIEW_LIMIT

        try:
            # Tăng biến đếm
            current_count = self.redis.incr(key)
            
            # Nếu là lần đầu tiên trong ngày, set thời gian hết hạn (24h)
            if current_count == 1:
                self.redis.expire(key, 86400)
            
            if current_count > limit:
                return False, f"Bạn đã vượt quá giới hạn xem {limit} lần/ngày."
            
            return True, f"Lần xem thứ {current_count}/{limit}"
        except Exception as e:
            current_app.logger.error(f"Redis Error: {e}")
            return True, "Error checking limit" # Cho qua nếu lỗi hệ thống    
    # =========================================================================
    # 1. THÔNG TIN CƠ BẢN
    # =========================================================================
    def get_customer_info(self, object_id):
        """Lấy thông tin cơ bản khách hàng."""
        query = f"""
            SELECT TOP 1
                ObjectID, ShortObjectName, ObjectName, 
                ISNULL(Tel, '') as Tel,
                ISNULL(Address, '') as ObjectAddress,
                ISNULL(VATNo, '') as TaxCode
            FROM {config.ERP_IT1202}
            WHERE ObjectID = ?
        """
        data = self.db.get_data(query, (object_id,))
        return data[0] if data else None

    # =========================================================================
    # 2. HEADER METRICS (6 KHỐI KPI)
    # =========================================================================
    def get_header_metrics(self, object_id):
        """
        Lấy 6 chỉ số KPI quan trọng cho Header (Đồng bộ logic CEO Cockpit).
        1. Báo cáo, 2. Báo giá/ĐH, 3. Thanh toán, 4. Doanh số vs Target, 5. Công nợ, 6. OTIF
        """
        current_year = datetime.now().year
        
        # --- 1. SỐ LƯỢNG BÁO CÁO (Task/Note) ---
        try:
            q_rep = f"SELECT COUNT(*) as Cnt FROM {config.TASK_TABLE} WHERE ObjectID = ?"
            rep_data = self.db.get_data(q_rep, (object_id,))
            report_count = rep_data[0]['Cnt'] if rep_data else 0
        except: report_count = 0

        # --- 2. BÁO GIÁ & ĐƠN HÀNG YTD ---
        q_counts = f"""
            SELECT 
                (SELECT COUNT(*) FROM {config.ERP_QUOTES} WHERE ObjectID = ? AND YEAR(QuotationDate) = ?) as QuoteCount,
                (SELECT COUNT(*) FROM {config.ERP_OT2001} WHERE ObjectID = ? AND YEAR(OrderDate) = ? AND OrderStatus = 1) as OrderCount
        """
        res_counts = self.db.get_data(q_counts, (object_id, current_year, object_id, current_year))
        quote_ytd = res_counts[0]['QuoteCount'] if res_counts else 0
        order_ytd = res_counts[0]['OrderCount'] if res_counts else 0

        # --- 3. THỜI GIAN THANH TOÁN TB ---
        avg_payment_days = 30 # Logic placeholder (hoặc query thực tế nếu có)

        # --- 4. DOANH SỐ YTD VS TARGET ---
        q_sales = f"""
            SELECT SUM(ConvertedAmount) as SalesYTD
            FROM {config.ERP_GIAO_DICH}
            WHERE ObjectID = ? AND TranYear = ? 
            AND CreditAccountID LIKE '{config.ACC_DOANH_THU}'
            AND OTransactionID IS NOT NULL
        """
        sales_data = self.db.get_data(q_sales, (object_id, current_year))
        sales_ytd = safe_float(sales_data[0]['SalesYTD']) if sales_data else 0
        
        # Target (DTCL)
        try:
            q_target = f"SELECT SUM(DK) as Target FROM {config.CRM_DTCL} WHERE [Ma KH] = ? AND [Nam] = ?"
            target_data = self.db.get_data(q_target, (object_id, current_year))
            target_year = safe_float(target_data[0]['Target']) if target_data else 0
        except: target_year = 0

        # --- 5. CÔNG NỢ (Hiện tại vs Quá hạn) ---
        try:
            q_debt = f"SELECT TotalDebt, TotalOverdueDebt FROM {config.CRM_AR_AGING_SUMMARY} WHERE ObjectID = ?"
            debt_data = self.db.get_data(q_debt, (object_id,))
            curr_debt = safe_float(debt_data[0]['TotalDebt']) if debt_data else 0
            over_debt = safe_float(debt_data[0]['TotalOverdueDebt']) if debt_data else 0
        except:
            curr_debt = 0
            over_debt = 0

        # --- 6. OTIF (Giao hàng đúng hạn) ---
        q_otif = f"""
            SELECT COUNT(*) as Total, 
                   SUM(CASE WHEN ActualDeliveryDate <= DATEADD(day, 7, ISNULL(EarliestRequestDate, ActualDeliveryDate)) THEN 1 ELSE 0 END) as OnTime
            FROM {config.DELIVERY_WEEKLY_VIEW}
            WHERE ObjectID = ? AND DeliveryStatus = '{config.DELIVERY_STATUS_DONE}' AND YEAR(ActualDeliveryDate) = ?
        """
        otif_data = self.db.get_data(q_otif, (object_id, current_year))
        otif_score = 0
        if otif_data and safe_float(otif_data[0]['Total']) > 0:
            otif_score = (safe_float(otif_data[0]['OnTime']) / safe_float(otif_data[0]['Total'])) * 100

        return {
            'ReportCount': report_count,
            'QuoteYTD': quote_ytd, 'OrderYTD': order_ytd,
            'AvgPaymentDays': avg_payment_days,
            'SalesYTD': sales_ytd, 'TargetYear': target_year,
            'DebtCurrent': curr_debt, 'DebtOverdue': over_debt,
            'OTIF': round(otif_score, 1)
        }

    # =========================================================================
    # 3. BIỂU ĐỒ CƠ CẤU DOANH SỐ (STACKED BAR 5 NĂM - FIX LOGIC)
    # =========================================================================
    def get_sales_structure_stock_vs_order(self, object_id):
        """
        Biểu đồ Stacked Bar 5 năm: Hàng có sẵn vs Hàng đặt.
        - Join: GT9000.OrderID = OT2001.SOrderID
        - Phân loại: Dựa trên config.ORDER_TYPE_STOCK / ORDER
        """
        current_year = datetime.now().year
        start_year = current_year - 4
        
        # Chuẩn bị danh sách mã cho câu query SQL
        stock_types = "'" + "','".join(config.ORDER_TYPE_STOCK) + "'"
        order_types = "'" + "','".join(config.ORDER_TYPE_ORDER) + "'"
        
        query = f"""
            SELECT 
                T1.TranYear,
                CASE 
                    WHEN T2.VoucherTypeID IN ({stock_types}) THEN 'STOCK'
                    WHEN T2.VoucherTypeID IN ({order_types}) THEN 'ORDER'
                    ELSE 'OTHER'
                END as GroupType,
                SUM(T1.ConvertedAmount) as Revenue
            FROM {config.ERP_GIAO_DICH} T1
            LEFT JOIN {config.ERP_OT2001} T2 ON T1.OrderID = T2.SOrderID -- [REQ]: Join GT9000.OrderID = OT2001.SOrderID
            WHERE T1.ObjectID = ? 
            AND T1.TranYear >= ?
            AND T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}'
            AND T1.OTransactionID IS NOT NULL
            GROUP BY T1.TranYear, 
                     CASE 
                        WHEN T2.VoucherTypeID IN ({stock_types}) THEN 'STOCK'
                        WHEN T2.VoucherTypeID IN ({order_types}) THEN 'ORDER'
                        ELSE 'OTHER'
                     END
            ORDER BY T1.TranYear ASC
        """
        
        data = self.db.get_data(query, (object_id, start_year))
        
        # Pivot Data cho Chart
        years = list(range(start_year, current_year + 1))
        pivot_data = {y: {'STOCK': 0, 'ORDER': 0, 'OTHER': 0} for y in years}
        
        for row in data:
            y = row['TranYear']
            g = row['GroupType']
            val = safe_float(row['Revenue'])
            if y in pivot_data:
                pivot_data[y][g] = val
                
        series_stock = [pivot_data[y]['STOCK'] for y in years]
        series_order = [pivot_data[y]['ORDER'] for y in years]
        series_other = [pivot_data[y]['OTHER'] for y in years]
        
        return {
            'years': years,
            'series': [
                {'name': 'Hàng có sẵn', 'data': series_stock},
                {'name': 'Hàng đặt', 'data': series_order},
                {'name': 'Khác', 'data': series_other}
            ]
        }

    # =========================================================================
    # 4. TOP 30 SẢN PHẨM (TÁCH SL Y-1 vs YTD)
    # =========================================================================
    def get_top_products(self, object_id):
        """
        Top 30 SP bán chạy 2 năm gần nhất.
        """
        current_year = datetime.now().year
        last_year = current_year - 1
        
        query = f"""
            SELECT TOP 30
                T1.InventoryID, 
                ISNULL(T2.InventoryName, T1.InventoryID) as InventoryName,
                
                -- [REQ]: Tách cột SL Năm trước
                SUM(CASE WHEN T1.TranYear = {last_year} THEN T1.Quantity ELSE 0 END) as Qty_Prev,
                
                -- [REQ]: Tách cột SL Năm nay
                SUM(CASE WHEN T1.TranYear = {current_year} THEN T1.Quantity ELSE 0 END) as Qty_YTD,
                
                -- Tổng tiền (Cả 2 năm)
                SUM(T1.ConvertedAmount) as TotalRevenue
                
            FROM {config.ERP_GIAO_DICH} T1
            LEFT JOIN {config.ERP_IT1302} T2 ON T1.InventoryID = T2.InventoryID
            WHERE T1.ObjectID = ? 
            AND T1.TranYear IN ({last_year}, {current_year})
            AND T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}'
            GROUP BY T1.InventoryID, T2.InventoryName
            ORDER BY TotalRevenue DESC
        """
        return self.db.get_data(query, (object_id,))

    # =========================================================================
    # 5. CƠ HỘI BỎ LỠ (LOGIC TÊN MỚI & COUNT QUOTES)
    # =========================================================================
    def get_missed_opportunities_quotes(self, object_id):
        """
        Top 30 SP báo giá trượt (5 năm).
        Logic tên: Ưu tiên tên trong chi tiết báo giá (OT2002/OT2102) -> rồi mới đến tên danh mục.
        """
        current_year = datetime.now().year
        start_year_5y = current_year - 4 

        # Lưu ý: Bảng chi tiết báo giá thường là OT2102 hoặc OT2002 tùy version. 
        # Code gốc đang dùng config.ERP_QUOTE_DETAILS (thường là OT2102).
        query = f"""
            SELECT TOP 30
                T2.InventoryID, 
                
                -- [REQ]: Logic lấy tên ưu tiên
                ISNULL(NULLIF(T2.InventoryCommonName, ''), T3.InventoryName) as InventoryName,
                
                COUNT(T1.QuotationID) as QuoteCount, -- [REQ]: Tổng số lần báo
                SUM(T2.ConvertedAmount) as MissedValue
                
            FROM {config.ERP_QUOTES} T1
            INNER JOIN {config.ERP_QUOTE_DETAILS} T2 ON T1.QuotationID = T2.QuotationID
            LEFT JOIN {config.ERP_IT1302} T3 ON T2.InventoryID = T3.InventoryID
            WHERE T1.ObjectID = ? 
            AND T1.OrderStatus = 0 -- Chưa chốt
            AND YEAR(T1.QuotationDate) >= ?
            
            -- Group by theo logic tên mới
            GROUP BY T2.InventoryID, ISNULL(NULLIF(T2.InventoryCommonName, ''), T3.InventoryName)
            ORDER BY MissedValue DESC
        """
        return self.db.get_data(query, (object_id, start_year_5y))

    # =========================================================================
    # 6. PHÂN TÍCH GIÁ (CANDLESTICK - SORTED BY REVENUE)
    # =========================================================================
    def get_price_analysis_candlestick(self, object_id):
        """
        Phân tích giá Top 50 sản phẩm (Doanh thu 2 năm gần nhất).
        - Đã sort theo TotalRevenue DESC để đảm bảo mã quan trọng nhất nằm bên trái biểu đồ.
        """
        current_year = datetime.now().year
        last_year = current_year - 1
        start_history_year = current_year - 4 
        
        query = f"""
            WITH TopProducts AS (
                SELECT TOP 30 
                    T1.InventoryID, 
                    SUM(T1.ConvertedAmount) as TotalRevenue
                FROM {config.ERP_GIAO_DICH} T1
                WHERE T1.ObjectID = ? 
                AND T1.TranYear >= ? 
                AND T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}'
                GROUP BY T1.InventoryID
                -- Sort ngay từ đây để CTE giữ thứ tự (quan trọng)
                ORDER BY SUM(T1.ConvertedAmount) DESC 
            ),
            StatsHistory AS (
                SELECT T1.InventoryID,
                       MAX(T1.UnitPrice) as MaxPrice,
                       MIN(T1.UnitPrice) as MinPrice,
                       AVG(T1.UnitPrice) as AvgPriceHistory
                FROM {config.ERP_GIAO_DICH} T1
                WHERE T1.ObjectID = ? AND T1.TranYear >= ?
                AND T1.InventoryID IN (SELECT InventoryID FROM TopProducts)
                AND T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}'
                GROUP BY T1.InventoryID
            ),
            StatsRecent AS (
                SELECT T1.InventoryID, AVG(T1.UnitPrice) as AvgPriceRecent
                FROM {config.ERP_GIAO_DICH} T1
                WHERE T1.ObjectID = ? AND T1.TranYear >= ?
                AND T1.InventoryID IN (SELECT InventoryID FROM TopProducts)
                AND T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}'
                GROUP BY T1.InventoryID
            )
            SELECT 
                T.InventoryID, T.TotalRevenue,
                ISNULL(P.InventoryName, T.InventoryID) as InventoryName,
                ISNULL(Pricing.SalePrice01, 0) as StdPrice,
                H.MaxPrice, H.MinPrice, H.AvgPriceHistory,
                R.AvgPriceRecent
            FROM TopProducts T
            LEFT JOIN StatsHistory H ON T.InventoryID = H.InventoryID
            LEFT JOIN StatsRecent R ON T.InventoryID = R.InventoryID
            LEFT JOIN {config.ERP_IT1302} P ON T.InventoryID = P.InventoryID
            LEFT JOIN {config.ERP_ITEM_PRICING} Pricing ON T.InventoryID = Pricing.InventoryID
            ORDER BY T.TotalRevenue DESC -- [FIX]: Sort cuối cùng để ApexCharts nhận đúng thứ tự
        """
        
        params = (object_id, last_year, object_id, start_history_year, object_id, last_year)
        data = self.db.get_data(query, params)
        
        result = []
        for row in data:
            std = safe_float(row['StdPrice'])
            if std == 0: continue
            
            def calc_pct(val):
                return int(round(((safe_float(val) - std) / std * 100), 0)) if safe_float(val) > 0 else 0

            pct_open = calc_pct(row['AvgPriceHistory'])
            pct_high = calc_pct(row['MaxPrice'])
            pct_low = calc_pct(row['MinPrice'])
            pct_close = calc_pct(row['AvgPriceRecent'])

            fill_color = '#05CD99' if pct_close >= 0 else '#E31A1A'
            
            # Tính % chênh lệch của giá hiện tại để hiển thị cạnh tên mã
            current_pct_diff = pct_close 

            item = {
                'x': row['InventoryID'], 
                'y': [pct_open, pct_high, pct_low, pct_close],
                'fillColor': fill_color,
                'info': {
                    'name': row['InventoryName'],
                    'std': std,
                    'curr': safe_float(row['AvgPriceRecent']),
                    'max': safe_float(row['MaxPrice']),
                    'min': safe_float(row['MinPrice']),
                    'revenue': safe_float(row['TotalRevenue']),
                    'pct_diff': current_pct_diff # [NEW] Truyền % diff xuống Frontend
                }
            }
            result.append(item)
            
        return result

    # =========================================================================
    # 7. CÁC HÀM HỖ TRỢ KHÁC (PIE CHART, DRILL DOWN)
    # =========================================================================
    
    def get_category_analysis(self, object_id):
        """Cơ cấu nhóm hàng (Cho biểu đồ Pie)."""
        current_year = datetime.now().year
        query = f"""
            SELECT TOP 10 
                T2.I04ID, 
                ISNULL(T3.TEN, T2.I04ID) as CategoryName,
                SUM(T1.ConvertedAmount) as Revenue
            FROM {config.ERP_GIAO_DICH} T1
            INNER JOIN {config.ERP_IT1302} T2 ON T1.InventoryID = T2.InventoryID
            LEFT JOIN {config.TEN_BANG_NOI_DUNG_HD} T3 ON T2.I04ID = T3.LOAI
            WHERE T1.ObjectID = ?
            AND T1.TranYear = {current_year}
            AND T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}'
            GROUP BY T2.I04ID, T3.TEN
            ORDER BY Revenue DESC
        """
        data = self.db.get_data(query, (object_id,))
        return {
            'labels': [d['CategoryName'] for d in data],
            'series': [safe_float(d['Revenue']) for d in data],
            'ids': [d['I04ID'] for d in data]
        }

    # API Drill-down
    def get_drilldown_details(self, object_id, drill_type, filter_value):
        details = []
        if drill_type == 'CATEGORY':
            current_year = datetime.now().year
            query = f"""
                SELECT TOP 20 
                    T1.InventoryID, T2.InventoryName, 
                    SUM(T1.Quantity) as Qty, 
                    SUM(T1.ConvertedAmount) as Amount
                FROM {config.ERP_GIAO_DICH} T1
                INNER JOIN {config.ERP_IT1302} T2 ON T1.InventoryID = T2.InventoryID
                WHERE T1.ObjectID = ? 
                AND T2.I04ID = ?
                AND T1.TranYear = {current_year}
                AND T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}'
                GROUP BY T1.InventoryID, T2.InventoryName
                ORDER BY Amount DESC
            """
            details = self.db.get_data(query, (object_id, filter_value))
        
        # YEAR_SALES logic (cho biểu đồ cột stacked nếu cần drill)
        elif drill_type == 'YEAR_SALES':
             query = f"""
                SELECT VoucherDate, VoucherNo, VDescription, ConvertedAmount
                FROM {config.ERP_GIAO_DICH}
                WHERE ObjectID = ? AND TranYear = ?
                AND CreditAccountID LIKE '{config.ACC_DOANH_THU}'
                AND OTransactionID IS NOT NULL
                ORDER BY VoucherDate DESC
            """
             details = self.db.get_data(query, (object_id, filter_value))

        formatted = []
        for row in details:
            # Format generic
            val_str = f"{safe_float(row.get('Amount', row.get('ConvertedAmount', 0))):,.0f}"
            col1 = row.get('InventoryID', row.get('VoucherDate', ''))
            if hasattr(col1, 'strftime'): col1 = col1.strftime('%d/%m/%Y')
            
            col2 = row.get('InventoryName', row.get('VoucherNo', ''))
            
            col3 = ""
            if 'Qty' in row: col3 = f"SL: {safe_float(row['Qty']):,.0f}"
            elif 'VDescription' in row: col3 = row['VDescription']

            formatted.append({
                'Col1': col1, 'Col2': col2, 'Col3': col3, 'Value': val_str
            })
        return formatted

    # (Hàm sales_trend cũ đã được thay bằng get_sales_structure_stock_vs_order, xóa bỏ hoặc giữ làm legacy)
    # (Hàm cross_sell nếu không dùng cũng có thể bỏ)
    def get_sales_trend_5y(self, object_id): return {} # Placeholder nếu API cũ còn gọi

    def get_category_analysis(self, object_id):
        """
        Cơ cấu nhóm hàng & Lãi biên (Sử dụng SP đồng bộ với CEO Cockpit).
        """
        current_year = datetime.now().year
        
        # Gọi Stored Procedure
        try:
            data = self.db.execute_sp("sp_GetCustomerCategoryAnalysis", (object_id, current_year))
        except Exception as e:
            # Fallback nếu lỗi SP (hoặc chưa chạy script SQL)
            print(f"Error calling SP: {e}")
            return {'labels': [], 'series': [], 'details': []}

        # Xử lý dữ liệu trả về
        labels = []
        series = [] # Doanh thu cho biểu đồ Pie
        details = [] # Chi tiết cho Chatbot đọc (bao gồm Lãi biên)

        if data:
            for row in data:
                rev = safe_float(row['Revenue'])
                cost = safe_float(row['Cost'])
                profit = safe_float(row['GrossProfit'])
                
                # Tính % Margin
                margin_pct = (profit / rev * 100) if rev > 0 else 0
                
                cat_name = row['CategoryName']
                
                labels.append(cat_name)
                series.append(rev)
                
                details.append({
                    'id': row['CategoryID'],
                    'name': cat_name,
                    'revenue': rev,
                    'cost': cost,
                    'profit': profit,
                    'margin_pct': round(margin_pct, 1)
                })

        return {
            'labels': labels,
            'series': series,
            'details': details # [NEW] Trường này chứa full thông tin lãi lỗ
        }