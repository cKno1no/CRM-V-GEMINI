# services/executive_service.py

from db_manager import DBManager, safe_float
from datetime import datetime
import config

class ExecutiveService:
    """
    Service chuyên biệt cho CEO Cockpit.
    Tổng hợp dữ liệu từ Tài chính, Kinh doanh, Kho vận và Vận hành.
    """
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_kpi_scorecards(self, current_year, current_month):
        """
        Lấy dữ liệu cho 4 thẻ bài (Scorecards) trên cùng:
        1. Doanh số (Thực vs Mục tiêu)
        2. Lợi nhuận gộp (Ước tính từ GT9000)
        3. Nợ quá hạn (Rủi ro)
        4. Tồn kho Rủi ro (CLC)
        """
        kpi_data = {
            'Sales_CurrentMonth': 0, 'TargetMonth': 0, 'Percent': 0,
            'GrossProfit': 0, 'AvgMargin': 0,
            'TotalOverdueDebt': 0, 'Debt_Over_180': 0,
            'Inventory_Over_2Y': 0
        }

        try:
            # 1. DOANH SỐ & MỤC TIÊU (Reused logic from SalesService)
            # Mục tiêu tháng (Lấy tổng năm chia 12 cho đơn giản, hoặc logic theo tháng nếu có)
            query_target = f"SELECT SUM([DK]) FROM {config.CRM_DTCL} WHERE [Nam] = ?"
            target_data = self.db.get_data(query_target, (current_year,))
            
            # FIX: Xử lý key rỗng nếu query trả về SUM không có tên cột
            yearly_target = 0
            if target_data and len(target_data) > 0:
                # Lấy giá trị đầu tiên của row đầu tiên bất kể key là gì
                yearly_target = safe_float(list(target_data[0].values())[0])
                
            kpi_data['TargetMonth'] = yearly_target / 12

            # Doanh số thực tế tháng này (TK 511)
            query_sales = f"""
                SELECT SUM(ConvertedAmount) 
                FROM {config.ERP_GIAO_DICH} 
                WHERE TranMonth = ? AND TranYear = ? 
                AND CreditAccountID LIKE '511%' -- Doanh thu bán hàng
            """
            sales_data = self.db.get_data(query_sales, (current_month, current_year))
            
            # FIX: Xử lý key rỗng tương tự
            if sales_data and len(sales_data) > 0:
                 kpi_data['Sales_CurrentMonth'] = safe_float(list(sales_data[0].values())[0])
            
            if kpi_data['TargetMonth'] > 0:
                kpi_data['Percent'] = round((kpi_data['Sales_CurrentMonth'] / kpi_data['TargetMonth']) * 100, 1)

            # 2. LỢI NHUẬN GỘP (Logic nhanh từ GT9000 theo yêu cầu)
            # Lấy Doanh thu (511) - Giá vốn (632) trong tháng
            query_profit = f"""
                SELECT 
                    SUM(CASE WHEN CreditAccountID LIKE '511%' THEN ConvertedAmount ELSE 0 END) as Revenue,
                    SUM(CASE WHEN DebitAccountID LIKE '632%' THEN ConvertedAmount ELSE 0 END) as COGS
                FROM {config.ERP_GIAO_DICH}
                WHERE TranMonth = ? AND TranYear = ?
            """
            profit_data = self.db.get_data(query_profit, (current_month, current_year))
            if profit_data:
                rev = safe_float(profit_data[0]['Revenue'])
                cogs = safe_float(profit_data[0]['COGS'])
                kpi_data['GrossProfit'] = rev - cogs
                kpi_data['AvgMargin'] = ((rev - cogs) / rev * 100) if rev > 0 else 0

            # 3. NỢ QUÁ HẠN (Lấy từ View Summary AR)
            query_debt = """
                SELECT SUM(TotalOverdueDebt) as TotalOverdue, SUM(Debt_Over_180) as RiskDebt
                FROM dbo.CRM_AR_AGING_SUMMARY
            """
            debt_data = self.db.get_data(query_debt)
            if debt_data:
                kpi_data['TotalOverdueDebt'] = safe_float(debt_data[0]['TotalOverdue'])
                kpi_data['Debt_Over_180'] = safe_float(debt_data[0]['RiskDebt'])

            # 4. TỒN KHO RỦI RO (Gọi SP Inventory Aging nhưng chỉ lấy aggregate)
            # Lưu ý: Đây là tác vụ nặng, có thể cân nhắc cache hoặc chạy job định kỳ
            sp_inventory = "{CALL dbo.sp_GetInventoryAging (?)}"
            inv_data = self.db.get_data(sp_inventory, (None,))
            
            if inv_data:
                # Tính tổng giá trị các dòng có Range_Over_720_V > 0
                risk_val = sum(safe_float(row['Range_Over_720_V']) for row in inv_data)
                kpi_data['Inventory_Over_2Y'] = risk_val

        except Exception as e:
            print(f"Lỗi tính toán KPI Scorecards: {e}")
        
        return kpi_data

    def get_profit_trend_chart(self):
        """
        Lấy dữ liệu biểu đồ Xu hướng Lợi nhuận & Doanh số (12 tháng gần nhất).
        Truy vấn trực tiếp sổ cái GT9000.
        
        FIXED: Thay TranDate bằng VoucherDate (hoặc InvoiceDate tùy DB thực tế)
        """
        # Logic: Group by Year, Month và Pivot Revenue/COGS
        # CẬP NHẬT QUAN TRỌNG: Dùng VoucherDate thay vì TranDate
        query = f"""
            SELECT TOP 12
                TranYear, TranMonth,
                SUM(CASE WHEN CreditAccountID LIKE '511%' THEN ConvertedAmount ELSE 0 END) as Revenue,
                SUM(CASE WHEN DebitAccountID LIKE '632%' THEN ConvertedAmount ELSE 0 END) as COGS
            FROM {config.ERP_GIAO_DICH}
            WHERE VoucherDate >= DATEADD(month, -11, GETDATE())  -- SỬA Ở ĐÂY
            GROUP BY TranYear, TranMonth
            ORDER BY TranYear ASC, TranMonth ASC
        """
        
        try:
            data = self.db.get_data(query)
            chart_data = {
                'categories': [], # Nhãn trục X (Tháng)
                'revenue': [],    # Data Series 1
                'profit': []      # Data Series 2
            }
            
            if data:
                for row in data:
                    m = row['TranMonth']
                    y = row['TranYear']
                    rev = safe_float(row['Revenue'])
                    cogs = safe_float(row['COGS'])
                    profit = rev - cogs
                    
                    # Chuyển đổi đơn vị sang Tỷ VNĐ cho gọn biểu đồ
                    chart_data['categories'].append(f"T{m}/{y}")
                    chart_data['revenue'].append(round(rev / 1000000000.0, 2))
                    chart_data['profit'].append(round(profit / 1000000000.0, 2))
                    
            return chart_data
            
        except Exception as e:
            print(f"Lỗi lấy dữ liệu biểu đồ lợi nhuận: {e}")
            # Trả về dữ liệu rỗng để không crash trang
            return {'categories': [], 'revenue': [], 'profit': []}

    def get_pending_actions_count(self):
        """
        Đếm số lượng các item cần hành động gấp (Action Center).
        """
        counts = {'Quotes': 0, 'Budgets': 0, 'Orders': 0, 'UrgentTasks': 0}
        
        try:
            # 1. Báo giá chờ duyệt (Status = 0)
            quotes_data = self.db.get_data(f"SELECT COUNT(*) FROM {config.ERP_QUOTES} WHERE OrderStatus = 0")
            counts['Quotes'] = safe_float(list(quotes_data[0].values())[0]) if quotes_data else 0
            
            # 2. Đề nghị thanh toán chờ duyệt
            budgets_data = self.db.get_data("SELECT COUNT(*) FROM dbo.EXPENSE_REQUEST WHERE Status = 'PENDING'")
            counts['Budgets'] = safe_float(list(budgets_data[0].values())[0]) if budgets_data else 0
            
            # 3. Đơn hàng bán chờ duyệt
            orders_data = self.db.get_data(f"SELECT COUNT(*) FROM {config.ERP_OT2001} WHERE OrderStatus = 0")
            counts['Orders'] = safe_float(list(orders_data[0].values())[0]) if orders_data else 0
            
            # 4. Task Khẩn cấp
            query_task = f"""
                SELECT COUNT(*) 
                FROM {config.TASK_TABLE} 
                WHERE Status IN ('BLOCKED', 'HELP_NEEDED') 
                OR (Priority = 'HIGH' AND Status NOT IN ('COMPLETED', 'CANCELLED'))
            """
            tasks_data = self.db.get_data(query_task)
            counts['UrgentTasks'] = safe_float(list(tasks_data[0].values())[0]) if tasks_data else 0
            
            # Tổng cộng
            counts['Total'] = int(counts['Quotes'] + counts['Budgets'] + counts['Orders'] + counts['UrgentTasks'])
            
        except Exception as e:
            print(f"Lỗi đếm Pending Actions: {e}")
            
        return counts

    def get_top_sales_leaderboard(self, current_year):
        """Lấy bảng xếp hạng Sales (Top 5) dựa trên % KPI."""
        # 1. Lấy Doanh số thực
        query_actual = f"""
            SELECT SalesManID, SUM(ConvertedAmount) as ActualSales
            FROM {config.ERP_GIAO_DICH}
            WHERE TranYear = ? AND CreditAccountID LIKE '511%'
            GROUP BY SalesManID
        """
        actual_data = self.db.get_data(query_actual, (current_year,))
        actual_map = {row['SalesManID']: safe_float(row['ActualSales']) for row in actual_data} if actual_data else {}
        
        # 2. Lấy Mục tiêu & Thông tin User
        query_target = f"""
            SELECT T1.[PHU TRACH DS] as UserCode, SUM(T1.DK) as Target, T2.SHORTNAME
            FROM {config.CRM_DTCL} T1
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} T2 ON T1.[PHU TRACH DS] = T2.USERCODE
            WHERE T1.[Nam] = ?
            GROUP BY T1.[PHU TRACH DS], T2.SHORTNAME
        """
        target_data = self.db.get_data(query_target, (current_year,))
        
        leaderboard = []
        if target_data:
            for row in target_data:
                u_code = row['UserCode']
                target = safe_float(row['Target'])
                actual = actual_map.get(u_code, 0)
                
                if target > 0:
                    percent = (actual / target) * 100
                    leaderboard.append({
                        'UserCode': u_code,
                        'ShortName': row['SHORTNAME'] or u_code,
                        'TotalSalesAmount': actual,
                        'Percent': round(percent, 1)
                    })
        
        # Sort giảm dần theo % KPI và lấy Top 5
        leaderboard.sort(key=lambda x: x['Percent'], reverse=True)
        return leaderboard[:5]
