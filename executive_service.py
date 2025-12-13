# services/executive_service.py

from db_manager import DBManager, safe_float
from datetime import datetime, timedelta
import config

class ExecutiveService:
    """
    Service chuyên biệt cho CEO Cockpit.
    Tổng hợp dữ liệu từ Tài chính, Kinh doanh, Kho vận và Vận hành.
    """
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_kpi_scorecards(self, current_year, current_month):
        """Lấy KPI tổng hợp (Đã cập nhật logic Gross Profit)."""
        kpi_data = {
            'Sales_YTD': 0, 'TargetYear': 0, 'Percent': 0,
            'GrossProfit_YTD': 0, 'AvgMargin_YTD': 0,
            'TotalExpenses_YTD': 0, 'BudgetPlan_YTD': 0, 'OverBudgetCount': 0,
            'CrossSellProfit_YTD': 0, 'CrossSellCustCount': 0,
            'OTIF_Month': 0, 'OTIF_YTD': 0,
            
            # [REMOVED] NewCust_Count, NewCust_Sales
            # [KEPT] Risk
            'TotalOverdueDebt': 0, 'Debt_Over_180': 0,
            'Inventory_Over_2Y': 0
        }

        try:
            # 1. DOANH SỐ & LỢI NHUẬN (YTD) - [FIX: OTransactionID IS NOT NULL]
            query_sales_profit = f"""
                SELECT 
                    SUM(CASE WHEN CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN ConvertedAmount ELSE 0 END) as Revenue,
                    SUM(CASE WHEN DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN ConvertedAmount ELSE 0 END) as COGS
                FROM {config.ERP_GIAO_DICH} 
                WHERE TranYear = ? AND TranMonth <= ?
                AND OTransactionID IS NOT NULL -- [QUAN TRỌNG] Chỉ lấy giao dịch bán hàng chính thức
            """
            sp_data = self.db.get_data(query_sales_profit, (current_year, current_month))
            if sp_data:
                rev = safe_float(sp_data[0]['Revenue'])
                cogs = safe_float(sp_data[0]['COGS'])
                kpi_data['Sales_YTD'] = rev
                kpi_data['GrossProfit_YTD'] = rev - cogs
                kpi_data['AvgMargin_YTD'] = ((rev - cogs) / rev * 100) if rev > 0 else 0
            
            if kpi_data['TargetYear'] > 0:
                kpi_data['Percent'] = round((kpi_data['Sales_YTD'] / kpi_data['TargetYear']) * 100, 1)

            # --- B. TÀI CHÍNH: CHI PHÍ & HIỆU QUẢ BÁN CHÉO ---
            
            # 1. Chi phí YTD (Thực tế)
            # [FIXED]: Thêm điều kiện lọc tài khoản đầu 6 và 8 để khớp với Budget Report
            query_exp_actual = f"""
                SELECT SUM(ConvertedAmount) 
                FROM {config.ERP_GIAO_DICH}
                WHERE TranYear = ? AND TranMonth <= ?
                AND Ana03ID IS NOT NULL AND Ana03ID <> '{config.EXCLUDE_ANA03_CP2014}'
                AND (DebitAccountID LIKE '6%' OR DebitAccountID LIKE '8%') -- <--- THÊM DÒNG NÀY
            """
            act_data = self.db.get_data(query_exp_actual, (current_year, current_month))
            kpi_data['TotalExpenses_YTD'] = safe_float(act_data[0].get('') or list(act_data[0].values())[0]) if act_data else 0

            # 2. Chi phí YTD (Kế hoạch - Ngân sách)
            query_exp_plan = f"""
                SELECT SUM(BudgetAmount) 
                FROM {config.TABLE_BUDGET_PLAN} 
                WHERE FiscalYear = ? AND [Month] <= ?
            """
            plan_data = self.db.get_data(query_exp_plan, (current_year, current_month))
            kpi_data['BudgetPlan_YTD'] = safe_float(plan_data[0].get('') or list(plan_data[0].values())[0]) if plan_data else 0

            # 3. Đếm số khoản vượt ngân sách
            # [FIXED]: Cũng cần thêm điều kiện lọc tài khoản ở đây
            query_over_budget = f"""
                SELECT COUNT(*) as OverCount FROM (
                    SELECT T.Ana03ID, SUM(T.ConvertedAmount) as Actual, ISNULL(P.PlanAmount, 0) as PlanAmount
                    FROM {config.ERP_GIAO_DICH} T
                    LEFT JOIN (
                        SELECT BudgetCode, SUM(BudgetAmount) as PlanAmount 
                        FROM {config.TABLE_BUDGET_PLAN} 
                        WHERE FiscalYear = ? AND [Month] <= ? 
                        GROUP BY BudgetCode
                    ) P ON T.Ana03ID = P.BudgetCode
                    WHERE T.TranYear = ? AND T.TranMonth <= ? 
                    AND T.Ana03ID IS NOT NULL AND T.Ana03ID <> '{config.EXCLUDE_ANA03_CP2014}'
                    AND (T.DebitAccountID LIKE '6%' OR T.DebitAccountID LIKE '8%') -- <--- THÊM DÒNG NÀY
                    GROUP BY T.Ana03ID, P.PlanAmount
                ) AS Comparison
                WHERE Actual > PlanAmount
            """
            over_data = self.db.get_data(query_over_budget, (current_year, current_month, current_year, current_month))
            kpi_data['OverBudgetCount'] = over_data[0]['OverCount'] if over_data else 0

            # 4. Hiệu quả Bán chéo (VIP Profit YTD)
            # Logic: Tính Lợi nhuận gộp của KH mua >= 10 nhóm hàng (Rolling 12 tháng)
            query_vip_cust = f"""
                SELECT ObjectID
                FROM {config.ERP_GIAO_DICH} T1
                INNER JOIN {config.ERP_IT1302} T2 ON T1.InventoryID = T2.InventoryID
                WHERE T1.VoucherDate >= DATEADD(day, -365, GETDATE())
                AND T2.I04ID IS NOT NULL AND T2.I04ID <> ''
                AND (T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}' OR T1.DebitAccountID LIKE '{config.ACC_GIA_VON}')
                GROUP BY T1.ObjectID
                HAVING COUNT(DISTINCT T2.I04ID) >= 10
            """
            
            # Tính lợi nhuận YTD của danh sách VIP này
            query_cross_sell_profit = f"""
                SELECT 
                    SUM(CASE WHEN T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN T1.ConvertedAmount ELSE 0 END) -
                    SUM(CASE WHEN T1.DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN T1.ConvertedAmount ELSE 0 END) as VipProfit
                FROM {config.ERP_GIAO_DICH} T1
                WHERE T1.TranYear = ? AND T1.TranMonth <= ?
                AND T1.ObjectID IN ({query_vip_cust})
            """
            vip_data = self.db.get_data(query_cross_sell_profit, (current_year, current_month))
            if vip_data:
                kpi_data['CrossSellProfit_YTD'] = safe_float(vip_data[0]['VipProfit'])
                
            # Đếm số lượng KH VIP
            vip_count_data = self.db.get_data(f"SELECT COUNT(*) as Cnt FROM ({query_vip_cust}) as Sub")
            kpi_data['CrossSellCustCount'] = vip_count_data[0]['Cnt'] if vip_count_data else 0

            # --- C. VẬN HÀNH: GIAO HÀNG (OTIF) & NEW BUSINESS ---
            
            # 1. OTIF (Giao hàng đúng hạn)
            # [UPDATED]: Áp dụng độ trễ 7 ngày (DATEADD(day, 7, ...))
            query_otif = f"""
                SELECT 
                    SUM(CASE WHEN MONTH(ActualDeliveryDate) = ? AND YEAR(ActualDeliveryDate) = ? THEN 1 ELSE 0 END) as Delivered_Month,
                    
                    SUM(CASE WHEN MONTH(ActualDeliveryDate) = ? AND YEAR(ActualDeliveryDate) = ? 
                             AND ActualDeliveryDate <= DATEADD(day, 7, ISNULL(EarliestRequestDate, ActualDeliveryDate)) 
                        THEN 1 ELSE 0 END) as OnTime_Month,
                    
                    COUNT(*) as Delivered_YTD,
                    
                    SUM(CASE WHEN ActualDeliveryDate <= DATEADD(day, 7, ISNULL(EarliestRequestDate, ActualDeliveryDate)) 
                        THEN 1 ELSE 0 END) as OnTime_YTD
                FROM {config.DELIVERY_WEEKLY_VIEW}
                WHERE DeliveryStatus = '{config.DELIVERY_STATUS_DONE}' AND YEAR(ActualDeliveryDate) = ?
            """
            otif_data = self.db.get_data(query_otif, (current_month, current_year, current_month, current_year, current_year))
            if otif_data:
                row = otif_data[0]
                del_m = safe_float(row['Delivered_Month'])
                ont_m = safe_float(row['OnTime_Month'])
                del_y = safe_float(row['Delivered_YTD'])
                ont_y = safe_float(row['OnTime_YTD'])
                
                kpi_data['OTIF_Month'] = (ont_m / del_m * 100) if del_m > 0 else 100
                kpi_data['OTIF_YTD'] = (ont_y / del_y * 100) if del_y > 0 else 100

            # 2. New Business
            # KH tạo trong 360 ngày qua VÀ có doanh số > 10 triệu
            cutoff_date = datetime.now() - timedelta(days=config.NEW_BUSINESS_DAYS)
            query_new_biz = f"""
                SELECT COUNT(Sub.ObjectID) as NewCount, SUM(Sub.TotalSales) as NewSales
                FROM (
                    SELECT T1.ObjectID, SUM(T2.ConvertedAmount) as TotalSales
                    FROM {config.ERP_IT1202} T1
                    INNER JOIN {config.ERP_GIAO_DICH} T2 ON T1.ObjectID = T2.ObjectID
                    WHERE T1.CreateDate >= ? 
                    AND T2.VoucherDate >= ? 
                    AND T2.CreditAccountID LIKE '{config.ACC_DOANH_THU}'
                    GROUP BY T1.ObjectID
                    HAVING SUM(T2.ConvertedAmount) > {config.NEW_BUSINESS_MIN_SALES}
                ) AS Sub
            """
            try:
                nb_data = self.db.get_data(query_new_biz, (cutoff_date, cutoff_date))
                if nb_data:
                    kpi_data['NewCust_Count'] = safe_float(nb_data[0]['NewCount'])
                    kpi_data['NewCust_Sales'] = safe_float(nb_data[0]['NewSales'])
            except Exception:
                pass # Bỏ qua nếu lỗi (VD: thiếu cột CreateDate)

            # --- D. RỦI RO: NỢ & TỒN KHO (UPDATED) ---
            
            # 1. NỢ PHẢI THU (AR) - Từ view AR Aging
            query_ar = f"""
                SELECT 
                    SUM(TotalOverdueDebt) as TotalOverdue, 
                    SUM(Debt_Over_180) as RiskDebt 
                FROM {config.CRM_AR_AGING_SUMMARY}
            """
            ar_data = self.db.get_data(query_ar)
            if ar_data:
                kpi_data['AR_TotalOverdueDebt'] = safe_float(ar_data[0]['TotalOverdue'])
                kpi_data['AR_Debt_Over_180'] = safe_float(ar_data[0]['RiskDebt'])

            # 2. NỢ PHẢI TRẢ (AP) - [UPDATED: Chỉ lấy NCC (SUPPLIER)]
            query_ap = f"""
                SELECT 
                    SUM(TotalOverdueDebt) as TotalOverdue, 
                    SUM(Debt_Over_180) as RiskDebt 
                FROM {config.CRM_AP_AGING_SUMMARY}
                WHERE DebtType = 'SUPPLIER'  -- <-- ĐIỀU KIỆN LỌC MỚI
            """
            ap_data = self.db.get_data(query_ap)
            if ap_data:
                kpi_data['AP_TotalOverdueDebt'] = safe_float(ap_data[0]['TotalOverdue'])
                kpi_data['AP_Debt_Over_180'] = safe_float(ap_data[0]['RiskDebt'])
            
            # 2. Tồn kho (Gọi SP)
            sp_inventory = f"{{CALL {config.SP_GET_INVENTORY_AGING} (?)}}"
            inv_data = self.db.get_data(sp_inventory, (None,))
            if inv_data:
                kpi_data['Inventory_Over_2Y'] = sum(safe_float(row['Range_Over_720_V']) for row in inv_data)

        except Exception as e:
            print(f"Lỗi tính toán KPI Scorecards: {e}")
        
        return kpi_data

    def get_profit_trend_chart(self):
        """Biểu đồ xu hướng (Đã cập nhật logic Gross Profit)."""
        query = f"""
            SELECT TOP 12 TranYear, TranMonth,
                SUM(CASE WHEN CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN ConvertedAmount ELSE 0 END) as Revenue,
                SUM(CASE WHEN DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN ConvertedAmount ELSE 0 END) as COGS
            FROM {config.ERP_GIAO_DICH}
            WHERE VoucherDate >= DATEADD(month, -11, GETDATE())
            AND OTransactionID IS NOT NULL -- [FIX]
            GROUP BY TranYear, TranMonth
            ORDER BY TranYear ASC, TranMonth ASC
        """
        try:
            data = self.db.get_data(query)
            chart_data = {'categories': [], 'revenue': [], 'profit': []}
            if data:
                for row in data:
                    rev = safe_float(row['Revenue'])
                    profit = rev - safe_float(row['COGS'])
                    # Format tháng/năm
                    chart_data['categories'].append(f"T{row['TranMonth']}/{row['TranYear']}")
                    # Chia đơn vị (Tỷ)
                    chart_data['revenue'].append(round(rev / config.DIVISOR_VIEW, 2))
                    chart_data['profit'].append(round(profit / config.DIVISOR_VIEW, 2))
            return chart_data
        except Exception as e:
            print(f"Lỗi biểu đồ: {e}")
            return {'categories': [], 'revenue': [], 'profit': []}

    def get_pending_actions_count(self):
        """
        Đếm số lượng Action cần xử lý.
        """
        counts = {'Quotes': 0, 'Budgets': 0, 'Orders': 0, 'UrgentTasks': 0, 'Total': 0}
        try:
            # Báo giá
            c_q = self.db.get_data(f"SELECT COUNT(*) FROM {config.ERP_QUOTES} WHERE OrderStatus = 0")
            counts['Quotes'] = safe_float(list(c_q[0].values())[0]) if c_q else 0
            
            # Ngân sách
            c_b = self.db.get_data(f"SELECT COUNT(*) FROM {config.TABLE_EXPENSE_REQUEST} WHERE Status = 'PENDING'")
            counts['Budgets'] = safe_float(list(c_b[0].values())[0]) if c_b else 0
            
            # Đơn hàng
            c_o = self.db.get_data(f"SELECT COUNT(*) FROM {config.ERP_OT2001} WHERE OrderStatus = 0")
            counts['Orders'] = safe_float(list(c_o[0].values())[0]) if c_o else 0
            
            # Task
            q_task = f"""
                SELECT COUNT(*) FROM {config.TASK_TABLE} 
                WHERE Status IN ('{config.TASK_STATUS_BLOCKED}', '{config.TASK_STATUS_HELP}') 
                OR (Priority = 'HIGH' AND Status NOT IN ('{config.TASK_STATUS_COMPLETED}', 'CANCELLED'))
            """
            c_t = self.db.get_data(q_task)
            counts['UrgentTasks'] = safe_float(list(c_t[0].values())[0]) if c_t else 0
            
            counts['Total'] = int(counts['Quotes'] + counts['Budgets'] + counts['Orders'] + counts['UrgentTasks'])
        except Exception: 
            pass
        return counts

    def get_top_sales_leaderboard(self, current_year):
        """
        Lấy BXH Sales Top 5.
        """
        query = f"""
            SELECT T1.[PHU TRACH DS] as UserCode, SUM(T1.DK) as Target, T2.SHORTNAME,
                   ISNULL(Actual.Sale, 0) as ActualSales
            FROM {config.CRM_DTCL} T1
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} T2 ON T1.[PHU TRACH DS] = T2.USERCODE
            LEFT JOIN (
                SELECT SalesManID, SUM(ConvertedAmount) as Sale 
                FROM {config.ERP_GIAO_DICH} 
                WHERE TranYear = ? AND CreditAccountID LIKE '{config.ACC_DOANH_THU}' 
                GROUP BY SalesManID
            ) Actual ON T1.[PHU TRACH DS] = Actual.SalesManID
            WHERE T1.[Nam] = ?
            GROUP BY T1.[PHU TRACH DS], T2.SHORTNAME, Actual.Sale
        """
        data = self.db.get_data(query, (current_year, current_year))
        board = []
        if data:
            for row in data:
                tgt = safe_float(row['Target'])
                act = safe_float(row['ActualSales'])
                pct = (act / tgt * 100) if tgt > 0 else 0
                board.append({
                    'UserCode': row['UserCode'], 
                    'ShortName': row['SHORTNAME'], 
                    'TotalSalesAmount': act, 
                    'Percent': round(pct, 1)
                })
        board.sort(key=lambda x: x['Percent'], reverse=True)
        return board[:5]
    
    def get_inventory_aging_chart_data(self):
        """
        [NEW] Tổng hợp Tuổi hàng tồn kho cho biểu đồ Donut.
        Phân loại: An toàn (<6 tháng), Trung bình (6-12T), Chậm luân chuyển (1-2 năm), Rủi ro (>2 năm).
        """
        try:
            # Gọi SP lấy dữ liệu tồn kho chi tiết
            sp_query = f"{{CALL {config.SP_GET_INVENTORY_AGING} (?)}}"
            data = self.db.get_data(sp_query, (None,))
            
            if not data: return {'labels': [], 'series': []}

            # Tổng hợp theo các bucket
            summary = {
                'An toàn (< 6 Tháng)': 0.0,
                'Ổn định (6-12 Tháng)': 0.0,
                'Chậm (1-2 Năm)': 0.0,
                'RỦI RO (> 2 Năm)': 0.0
            }
            
            for row in data:
                # Range_0_180_V
                summary['An toàn (< 6 Tháng)'] += safe_float(row.get('Range_0_180_V'))
                # Range_181_360_V
                summary['Ổn định (6-12 Tháng)'] += safe_float(row.get('Range_181_360_V'))
                # Range_361_540_V + Range_541_720_V
                summary['Chậm (1-2 Năm)'] += (safe_float(row.get('Range_361_540_V')) + safe_float(row.get('Range_541_720_V')))
                # Range_Over_720_V
                summary['RỦI RO (> 2 Năm)'] += safe_float(row.get('Range_Over_720_V'))
            
            return {
                'labels': list(summary.keys()),
                'series': list(summary.values())
            }
        except Exception as e:
            print(f"Lỗi chart tồn kho: {e}")
            return {'labels': [], 'series': []}

    def get_top_categories_performance(self, current_year):
        """Top nhóm hàng (Đã cập nhật logic Gross Profit)."""
        query = f"""
            SELECT TOP 10
                ISNULL(T3.TEN, T2.I04ID) as CategoryName,
                SUM(CASE WHEN T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN T1.ConvertedAmount ELSE 0 END) as Revenue,
                (SUM(CASE WHEN T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN T1.ConvertedAmount ELSE 0 END) -
                 SUM(CASE WHEN T1.DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN T1.ConvertedAmount ELSE 0 END)) as GrossProfit
            FROM {config.ERP_GIAO_DICH} T1
            INNER JOIN {config.ERP_IT1302} T2 ON T1.InventoryID = T2.InventoryID
            LEFT JOIN {config.TEN_BANG_NOI_DUNG_HD} T3 ON T2.I04ID = T3.LOAI 
            WHERE T1.TranYear = ?
            AND T1.OTransactionID IS NOT NULL -- [FIX]
            GROUP BY ISNULL(T3.TEN, T2.I04ID)
            ORDER BY Revenue DESC
        """
        data = self.db.get_data(query, (current_year,))
        
        result = {'categories': [], 'revenue': [], 'profit': [], 'margin': []}
        if data:
            for row in data:
                rev = safe_float(row['Revenue'])
                prof = safe_float(row['GrossProfit'])
                margin = (prof / rev * 100) if rev > 0 else 0
                
                result['categories'].append(row['CategoryName'])
                result['revenue'].append(rev)
                result['profit'].append(prof)
                result['margin'].append(round(margin, 1))
                
        return result
    
    # [UPDATED] 1. Cập nhật hàm Inventory: Tách CLC & Chuẩn bị dữ liệu Drill-down
    def get_inventory_aging_chart_data(self):
        """
        [UPDATED] Tổng hợp Tuổi hàng + Drill-down chi tiết theo I04ID.
        """
        try:
            # Gọi SP lấy dữ liệu thô
            sp_query = f"{{CALL {config.SP_GET_INVENTORY_AGING} (?)}}"
            data = self.db.get_data(sp_query, (None,))
            
            if not data: return {'labels': [], 'series': [], 'drilldown': {}}

            # Cấu trúc dữ liệu tổng hợp
            # buckets chứa: 'val' (tổng tiền), 'items' (dict gom nhóm I04: { 'NSK': 100, 'JST': 50... })
            buckets = {
                'An toàn (< 6 Tháng)': {'val': 0.0, 'items': {}},
                'Ổn định (6-12 Tháng)': {'val': 0.0, 'items': {}},
                'Chậm (1-2 Năm)': {'val': 0.0, 'items': {}},
                'Tồn Lâu (> 2 Năm)': {'val': 0.0, 'items': {}}, # >2 năm nhưng ko phải CLC
                'Hàng CLC (Rủi ro cao)': {'val': 0.0, 'items': {}} # Hàng CLC riêng
            }
            
            for row in data:
                # Giả định: I04ID là 3 ký tự đầu của InventoryID (hoặc logic mapping của bạn)
                # Nếu có cột I04ID trong SP thì dùng row['I04ID'], nếu chưa có thì cắt chuỗi
                group_id = str(row.get('InventoryID', 'KHAC'))[:3].upper()
                
                # Hàm helper để cộng dồn vào bucket
                def add_detail(bucket_key, val):
                    if val > 0:
                        buckets[bucket_key]['val'] += val
                        current_val = buckets[bucket_key]['items'].get(group_id, 0)
                        buckets[bucket_key]['items'][group_id] = current_val + val

                # 1. Phân loại An toàn
                add_detail('An toàn (< 6 Tháng)', safe_float(row.get('Range_0_180_V')))
                
                # 2. Phân loại Ổn định
                add_detail('Ổn định (6-12 Tháng)', safe_float(row.get('Range_181_360_V')))
                
                # 3. Phân loại Chậm
                val_1_2 = safe_float(row.get('Range_361_540_V')) + safe_float(row.get('Range_541_720_V'))
                add_detail('Chậm (1-2 Năm)', val_1_2)
                
                # 4. Phân loại Rủi ro (>2 năm) & CLC
                val_over_2 = safe_float(row.get('Range_Over_720_V'))
                
                # Logic xác định CLC (như yêu cầu: >2 năm và rủi ro)
                # Nếu SP chưa tính Risk_CLC_Value, ta tính lại logic:
                risk_clc = safe_float(row.get('Risk_CLC_Value', 0)) 
                if 'Risk_CLC_Value' not in row:
                    stock_class = str(row.get('StockClass', '')).strip().upper()
                    # > 5 triệu và không phải loại D
                    if stock_class != 'D' and val_over_2 > config.RISK_INVENTORY_VALUE:
                        risk_clc = val_over_2
                    else:
                        risk_clc = 0

                val_normal_over_2 = val_over_2 - risk_clc
                
                add_detail('Hàng CLC (Rủi ro cao)', risk_clc)
                add_detail('Tồn Lâu (> 2 Năm)', val_normal_over_2)

            # Format dữ liệu trả về cho Frontend
            final_labels = []
            final_series = []
            final_drilldown = {}

            for label, content in buckets.items():
                # Chỉ thêm vào biểu đồ nếu có giá trị
                # if content['val'] > 0: (Có thể bỏ comment nếu muốn ẩn phần = 0)
                final_labels.append(label)
                final_series.append(content['val'])
                
                # Sắp xếp Top 10 nhóm I04 chiếm tỷ trọng cao nhất trong phần đó
                sorted_items = sorted(content['items'].items(), key=lambda x: x[1], reverse=True)[:15] 
                
                final_drilldown[label] = [{'name': k, 'value': v} for k, v in sorted_items]

            return {
                'labels': final_labels,
                'series': final_series,
                'drilldown': final_drilldown
            }
        except Exception as e:
            print(f"Lỗi chart tồn kho: {e}")
            return {'labels': [], 'series': [], 'drilldown': {}}

    # [UPDATED] 2. Cập nhật biểu đồ Xu hướng: Thêm Chi phí
    def get_profit_trend_chart(self):
        """
        Lấy Doanh thu, Chi phí, Lợi nhuận ròng (Net) theo tháng (12 tháng gần nhất).
        """
        query = f"""
            SELECT TOP 12 TranYear, TranMonth,
                SUM(CASE WHEN CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN ConvertedAmount ELSE 0 END) as Revenue,
                SUM(CASE WHEN DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN ConvertedAmount ELSE 0 END) as COGS,
                
                -- Tính Chi phí (Các tài khoản đầu 641, 642, 811...)
                (SELECT SUM(ConvertedAmount) 
                 FROM {config.ERP_GIAO_DICH} Sub 
                 WHERE Sub.TranMonth = Main.TranMonth AND Sub.TranYear = Main.TranYear
                 AND Sub.Ana03ID IS NOT NULL AND Sub.Ana03ID <> '{config.EXCLUDE_ANA03_CP2014}'
                 AND (Sub.DebitAccountID LIKE '64%' OR Sub.DebitAccountID LIKE '811%')
                ) as Expenses

            FROM {config.ERP_GIAO_DICH} Main
            WHERE VoucherDate >= DATEADD(month, -11, GETDATE())
            GROUP BY TranYear, TranMonth
            ORDER BY TranYear ASC, TranMonth ASC
        """
        try:
            data = self.db.get_data(query)
            chart_data = {'categories': [], 'revenue': [], 'expenses': [], 'net_profit': []}
            
            if data:
                for row in data:
                    rev = safe_float(row['Revenue'])
                    cogs = safe_float(row['COGS'])
                    exp = safe_float(row['Expenses'])
                    
                    # Lợi nhuận ròng = Doanh thu - Giá vốn - Chi phí
                    net = rev - cogs - exp
                    
                    chart_data['categories'].append(f"T{row['TranMonth']}/{row['TranYear']}")
                    chart_data['revenue'].append(round(rev / config.DIVISOR_VIEW, 1))
                    chart_data['expenses'].append(round(exp / config.DIVISOR_VIEW, 1))
                    chart_data['net_profit'].append(round(net / config.DIVISOR_VIEW, 1))
                    
            return chart_data
        except Exception as e:
            print(f"Lỗi biểu đồ trend: {e}")
            return {'categories': [], 'revenue': [], 'expenses': [], 'net_profit': []}

    # [NEW] 3. Biểu đồ Phễu Kinh doanh (Quote -> Order -> Revenue)
    def get_sales_funnel_data(self):
        """
        So sánh Số lượng Chào giá vs Số lượng Đơn hàng thành công vs Doanh số thực tế (6 tháng).
        """
        query = f"""
            SELECT 
                MONTH(T.DateRef) as Month, YEAR(T.DateRef) as Year,
                
                -- 1. Số lượng Chào giá
                SUM(CASE WHEN T.Type = 'QUOTE' THEN 1 ELSE 0 END) as QuoteCount,
                
                -- 2. Số lượng Đơn hàng (SOrderID)
                SUM(CASE WHEN T.Type = 'ORDER' THEN 1 ELSE 0 END) as OrderCount,
                
                -- 3. Doanh số thực tế (Hóa đơn/PXK)
                SUM(CASE WHEN T.Type = 'SALES' THEN T.Amount ELSE 0 END) as Revenue
                
            FROM (
                -- Lấy Quotes
                SELECT QuotationDate as DateRef, 'QUOTE' as Type, 0 as Amount 
                FROM {config.ERP_QUOTES} WHERE QuotationDate >= DATEADD(month, -5, GETDATE())
                
                UNION ALL
                
                -- Lấy Orders (Đã duyệt)
                SELECT OrderDate as DateRef, 'ORDER' as Type, 0 as Amount
                FROM {config.ERP_OT2001} WHERE OrderDate >= DATEADD(month, -5, GETDATE()) AND OrderStatus = 1
                
                UNION ALL
                
                -- Lấy Doanh thu
                SELECT VoucherDate as DateRef, 'SALES' as Type, ConvertedAmount as Amount
                FROM {config.ERP_GIAO_DICH} 
                WHERE VoucherDate >= DATEADD(month, -5, GETDATE()) 
                AND CreditAccountID LIKE '{config.ACC_DOANH_THU}'
            ) T
            GROUP BY YEAR(T.DateRef), MONTH(T.DateRef)
            ORDER BY YEAR(T.DateRef), MONTH(T.DateRef)
        """
        try:
            data = self.db.get_data(query)
            result = {'categories': [], 'quotes': [], 'orders': [], 'revenue': []}
            
            for row in data:
                result['categories'].append(f"T{row['Month']}")
                result['quotes'].append(row['QuoteCount'])
                result['orders'].append(row['OrderCount'])
                result['revenue'].append(round(safe_float(row['Revenue']) / config.DIVISOR_VIEW, 1))
                
            return result
        except Exception as e:
            print(f"Lỗi funnel chart: {e}")
            return {'categories': [], 'quotes': [], 'orders': [], 'revenue': []}
    
    def get_comparison_data(self, year1, year2):
        """
        Lấy dữ liệu so sánh chỉ số quản trị giữa 2 năm bất kỳ.
        [UPDATED]: Logic GrossProfit (OTransactionID), Expense (Ana03), Debt (Snapshot).
        """
        def get_year_metrics(y):
            # 1. TÀI CHÍNH (Flow)
            
            # A. Doanh thu & Giá vốn ([NEW] OTransactionID IS NOT NULL)
            query_profit = f"""
                SELECT 
                    SUM(CASE WHEN CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN ConvertedAmount ELSE 0 END) as Revenue,
                    SUM(CASE WHEN DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN ConvertedAmount ELSE 0 END) as COGS
                FROM {config.ERP_GIAO_DICH}
                WHERE TranYear = ? AND OTransactionID IS NOT NULL
            """
            prof = self.db.get_data(query_profit, (y,))[0]
            revenue = safe_float(prof['Revenue'])
            cogs = safe_float(prof['COGS'])
            gross_profit = revenue - cogs

            # B. Chi phí ([NEW] Chỉ dựa vào Ana03ID, bỏ AccountID)
            query_exp = f"""
                SELECT SUM(ConvertedAmount) as Expenses
                FROM {config.ERP_GIAO_DICH}
                WHERE TranYear = ? 
                  AND Ana03ID IS NOT NULL 
                  AND Ana03ID <> ''
                  AND Ana03ID <> '{config.EXCLUDE_ANA03_CP2014}'
            """
            exp_data = self.db.get_data(query_exp, (y,))
            expenses = safe_float(exp_data[0]['Expenses']) if exp_data else 0
            
            net_profit = gross_profit - expenses

            # 2. KHÁCH HÀNG VIP (Cross-sell: Mua >= 10 nhóm hàng trong năm)
            query_vip = f"""
                SELECT SUM(T1.ConvertedAmount) as VIP_Sales
                FROM {config.ERP_GIAO_DICH} T1
                WHERE T1.TranYear = ? 
                AND T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}'
                AND T1.ObjectID IN (
                    SELECT G.ObjectID
                    FROM {config.ERP_GIAO_DICH} G
                    INNER JOIN {config.ERP_IT1302} I ON G.InventoryID = I.InventoryID
                    WHERE G.TranYear = ? 
                    AND I.I04ID IS NOT NULL AND I.I04ID <> ''
                    AND (G.CreditAccountID LIKE '{config.ACC_DOANH_THU}' OR G.DebitAccountID LIKE '{config.ACC_GIA_VON}')
                    GROUP BY G.ObjectID
                    HAVING COUNT(DISTINCT I.I04ID) >= 10
                )
            """
            vip_data = self.db.get_data(query_vip, (y, y))
            vip_sales = safe_float(vip_data[0]['VIP_Sales']) if vip_data else 0
            # Margin VIP ước tính
            avg_margin_rate = (gross_profit / revenue) if revenue > 0 else 0
            vip_profit = vip_sales * avg_margin_rate

            # 3. VẬN HÀNH (OTIF)
            otif_score = 0
            try:
                query_otif = f"""
                    SELECT 
                        COUNT(*) as Total,
                        SUM(CASE WHEN ActualDeliveryDate <= DATEADD(day, 7, ISNULL(EarliestRequestDate, ActualDeliveryDate)) 
                            THEN 1 ELSE 0 END) as OnTime
                    FROM {config.DELIVERY_WEEKLY_VIEW}
                    WHERE DeliveryStatus = '{config.DELIVERY_STATUS_DONE}' 
                    AND YEAR(ActualDeliveryDate) = ?
                """
                otif_data = self.db.get_data(query_otif, (y,))
                if otif_data and safe_float(otif_data[0]['Total']) > 0:
                    otif_score = (safe_float(otif_data[0]['OnTime']) / safe_float(otif_data[0]['Total'])) * 100
            except: pass

            # 4. TÀI SẢN & CÔNG NỢ (Snapshot cuối năm)
            # [NEW] Logic tính Dư nợ và Tuổi nợ tại thời điểm 31/12/Y
            end_date = f"{y}-12-31"
            
            # Query tính Dư nợ Phải thu (131) & Phải trả (331) cuối kỳ
            query_balance = f"""
                SELECT 
                    SUM(CASE WHEN AccountID LIKE '131%' THEN (Debit - Credit) ELSE 0 END) as AR_Total,
                    SUM(CASE WHEN AccountID LIKE '331%' THEN (Credit - Debit) ELSE 0 END) as AP_Total
                FROM (
                    SELECT CreditAccountID as AccountID, ConvertedAmount as Credit, 0 as Debit 
                    FROM {config.ERP_GIAO_DICH} WHERE VoucherDate <= ?
                    UNION ALL
                    SELECT DebitAccountID as AccountID, 0 as Credit, ConvertedAmount as Debit 
                    FROM {config.ERP_GIAO_DICH} WHERE VoucherDate <= ?
                ) Bal
            """
            bal_data = self.db.get_data(query_balance, (end_date, end_date))
            ar_total = safe_float(bal_data[0]['AR_Total']) if bal_data else 0
            ap_total = safe_float(bal_data[0]['AP_Total']) if bal_data else 0

            # Tính AR Rủi ro (>180 ngày):
            # Logic ước tính: Rủi ro = Tổng Dư Nợ - (Phát sinh Nợ trong 180 ngày cuối năm)
            # Tức là: Những khoản nợ còn lại mà không được tạo ra trong 6 tháng gần nhất -> ắt hẳn là nợ cũ > 6 tháng.
            date_180_ago = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=180)).strftime("%Y-%m-%d")
            
            query_ar_recent = f"""
                SELECT SUM(ConvertedAmount) as RecentDebt
                FROM {config.ERP_GIAO_DICH}
                WHERE DebitAccountID LIKE '131%' 
                AND VoucherDate > ? AND VoucherDate <= ?
            """
            ar_recent_data = self.db.get_data(query_ar_recent, (date_180_ago, end_date))
            ar_recent = safe_float(ar_recent_data[0]['RecentDebt']) if ar_recent_data else 0
            
            ar_risk = max(0, ar_total - ar_recent)

            # Tính AP Quá hạn (Tương tự logic trên hoặc lấy tổng dư nợ nếu coi tất cả dư cuối năm là nợ phải trả)
            # Ở đây ta lấy Tổng AP
            
            # Tồn kho cuối kỳ
            query_inv = f"""
                SELECT SUM(Debit - Credit) as Inventory_EndYear
                FROM (
                    SELECT DebitAccountID as AccountID, ConvertedAmount as Debit, 0 as Credit 
                    FROM {config.ERP_GIAO_DICH} WHERE DebitAccountID LIKE '15%' AND VoucherDate <= ?
                    UNION ALL
                    SELECT CreditAccountID as AccountID, 0 as Debit, ConvertedAmount as Credit 
                    FROM {config.ERP_GIAO_DICH} WHERE CreditAccountID LIKE '15%' AND VoucherDate <= ?
                ) Inv
            """
            inv_data = self.db.get_data(query_inv, (end_date, end_date))
            inv_balance = safe_float(inv_data[0]['Inventory_EndYear']) if inv_data else 0

            # Tính CLC (>2 năm) tại thời điểm đó (Ước tính: Tồn kho - Nhập kho 2 năm gần nhất)
            date_2y_ago = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=730)).strftime("%Y-%m-%d")
            query_inv_recent = f"""
                SELECT SUM(ConvertedAmount) as RecentImport
                FROM {config.ERP_GIAO_DICH}
                WHERE DebitAccountID LIKE '15%' AND VoucherDate > ? AND VoucherDate <= ?
            """
            inv_rec_data = self.db.get_data(query_inv_recent, (date_2y_ago, end_date))
            inv_recent = safe_float(inv_rec_data[0]['RecentImport']) if inv_rec_data else 0
            inv_risk = max(0, inv_balance - inv_recent)

            return {
                'Revenue': revenue,
                'GrossProfit': gross_profit,
                'Expenses': expenses,
                'NetProfit': net_profit,
                'VIPProfit': vip_profit,
                'OTIF': otif_score,
                'AR_Total': ar_total,
                'AR_Risk': ar_risk,
                'AP_Total': ap_total,
                'Inv_Total': inv_balance,
                'Inv_Risk': inv_risk
            }

        # --- EXECUTE ---
        m1 = get_year_metrics(year1)
        m2 = get_year_metrics(year2)

        # Chart Data (Giữ nguyên)
        query_chart = f"""
            SELECT TranYear, TranMonth, SUM(ConvertedAmount) as Rev
            FROM {config.ERP_GIAO_DICH}
            WHERE TranYear IN (?, ?) AND CreditAccountID LIKE '{config.ACC_DOANH_THU}'
            AND OTransactionID IS NOT NULL
            GROUP BY TranYear, TranMonth
            ORDER BY TranYear, TranMonth
        """
        chart_raw = self.db.get_data(query_chart, (year1, year2))
        series_y1 = [0]*12; series_y2 = [0]*12
        if chart_raw:
            for row in chart_raw:
                idx = int(row['TranMonth']) - 1
                if row['TranYear'] == int(year1): series_y1[idx] = safe_float(row['Rev'])
                elif row['TranYear'] == int(year2): series_y2[idx] = safe_float(row['Rev'])

        return { 'metrics': {'y1': m1, 'y2': m2}, 'chart': {'y1': series_y1, 'y2': series_y2} }

    def get_drilldown_data(self, metric_type, year):
        """
        API Drill-down nâng cấp: Trả về dữ liệu chuẩn hóa cho Modal.
        Format trả về: { 'Label', 'Value', 'SubValue', 'SubLabel', 'Percent' }
        Value luôn là giá trị chính dùng để vẽ thanh Bar.
        """
        data = []
        
        if metric_type == 'GROSS_PROFIT': # [REQ 2] Top 30 Khách hàng
            raw = self.db.execute_sp_multi('sp_GetGrossProfit_By_Customer', (year,))[0]
            for row in raw:
                # Value = Profit, SubValue = Revenue
                prof = safe_float(row['Value'])
                rev = safe_float(row['Revenue'])
                margin = (prof / rev * 100) if rev > 0 else 0
                data.append({
                    'Label': row['Label'], 
                    'Value': prof, 
                    'SubValue': rev,
                    'SubLabel': 'Doanh số',
                    'Info': f"Biên LNG: {margin:.1f}%"
                })

        elif metric_type == 'VIP_PROFIT': # [REQ 3] VIP Performance
            raw = self.db.execute_sp_multi('sp_GetVIP_Performance', (year,))[0]
            for row in raw:
                data.append({
                    'Label': f"Nhóm {row['Label']}", 
                    'Value': safe_float(row['Value']), # LNG
                    'SubValue': safe_float(row['Revenue']), # Doanh số
                    'SubLabel': 'Doanh số',
                    'Info': f"{row['CustomerCount']} Khách hàng"
                })

        elif metric_type == 'EXPENSE': # [REQ 1] Chi phí theo ReportGroup
            raw = self.db.execute_sp_multi('sp_GetExpenses_By_Group', (year, config.EXCLUDE_ANA03_CP2014))[0]
            for row in raw:
                data.append({
                    'Label': row['Label'], 
                    'Value': safe_float(row['Value']),
                    'SubValue': 0, 'SubLabel': '', 'Info': ''
                })

        elif metric_type == 'INVENTORY': # [REQ 5] Tồn kho theo I04ID
            raw = self.db.execute_sp_multi('sp_GetInventory_By_I04', ())[0]
            for row in raw:
                stock = safe_float(row['TotalStock'])
                risk = safe_float(row['Value']) # Tồn > 2 năm
                risk_pct = (risk / stock * 100) if stock > 0 else 0
                data.append({
                    'Label': row['Label'],
                    'Value': stock, # Tổng tồn kho
                    'SubValue': risk,
                    'SubLabel': 'Tồn > 2 năm',
                    'Info': f"Rủi ro: {risk_pct:.1f}%",
                    'IsRisk': risk_pct > 30 # Cờ cảnh báo màu đỏ
                })

        elif metric_type == 'AR': # Công nợ (Giữ nguyên logic cũ hoặc gọi SP mới nếu cần)
            raw = self.db.execute_sp_multi('sp_GetDebt_Breakdown', ('AR',))[0]
            for row in raw:
                data.append({'Label': row['Label'], 'Value': safe_float(row['Amount'])})

        # --- Xử lý tính % cho thanh Bar ---
        if data:
            # Tìm giá trị lớn nhất để làm mẫu số cho thanh Progress (Max = 100%)
            max_val = max([d['Value'] for d in data]) if data else 1
            for item in data:
                item['BarPercent'] = (item['Value'] / max_val * 100) if max_val > 0 else 0

        return data