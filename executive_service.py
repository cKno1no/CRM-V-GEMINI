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
        """
        Lấy dữ liệu KPI tổng hợp cho CEO Cockpit.
        """
        kpi_data = {
            # 1. Sales & Profit (YTD)
            'Sales_YTD': 0, 'TargetYear': 0, 'Percent': 0,
            'GrossProfit_YTD': 0, 'AvgMargin_YTD': 0,
            
            # 2. Finance (Expenses & Cross-Sell Profit)
            'TotalExpenses_YTD': 0, 'BudgetPlan_YTD': 0, 'OverBudgetCount': 0,
            'CrossSellProfit_YTD': 0, 'CrossSellCustCount': 0,
            
            # 3. Operations (Delivery OTIF & New Biz)
            'OTIF_Month': 0, 'OTIF_YTD': 0,
            'NewCust_Count': 0, 'NewCust_Sales': 0,
            
            # 4. Risk (Debt & Inventory)
            'TotalOverdueDebt': 0, 'Debt_Over_180': 0,
            'Inventory_Over_2Y': 0
        }

        try:
            # --- A. DOANH SỐ & LỢI NHUẬN (YTD) ---
            # 1. Mục tiêu Năm
            query_target = f"SELECT SUM([DK]) FROM {config.CRM_DTCL} WHERE [Nam] = ?"
            target_data = self.db.get_data(query_target, (current_year,))
            if target_data and len(target_data) > 0:
                # Lấy giá trị đầu tiên bất kể tên cột
                kpi_data['TargetYear'] = safe_float(list(target_data[0].values())[0])

            # 2. Thực tế YTD
            query_sales_profit = f"""
                SELECT 
                    SUM(CASE WHEN CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN ConvertedAmount ELSE 0 END) as Revenue,
                    SUM(CASE WHEN DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN ConvertedAmount ELSE 0 END) as COGS
                FROM {config.ERP_GIAO_DICH} 
                WHERE TranYear = ? AND TranMonth <= ?
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
            # [UPDATED]: Lấy toàn bộ Ana03ID hợp lệ (trừ mã kết chuyển), BỎ lọc tài khoản đầu 6/8
            query_exp_actual = f"""
                SELECT SUM(ConvertedAmount) 
                FROM {config.ERP_GIAO_DICH}
                WHERE TranYear = ? AND TranMonth <= ?
                AND Ana03ID IS NOT NULL AND Ana03ID <> '{config.EXCLUDE_ANA03_CP2014}'
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

            # 3. Đếm số khoản vượt ngân sách (So sánh theo từng Ana03ID)
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

            # --- D. RỦI RO: NỢ & TỒN KHO ---
            # 1. Nợ
            query_debt = f"""
                SELECT SUM(TotalOverdueDebt) as TotalOverdue, SUM(Debt_Over_180) as RiskDebt 
                FROM {config.CRM_AR_AGING_SUMMARY}
            """
            debt_data = self.db.get_data(query_debt)
            if debt_data:
                kpi_data['TotalOverdueDebt'] = safe_float(debt_data[0]['TotalOverdue'])
                kpi_data['Debt_Over_180'] = safe_float(debt_data[0]['RiskDebt'])

            # 2. Tồn kho (Gọi SP)
            sp_inventory = f"{{CALL {config.SP_GET_INVENTORY_AGING} (?)}}"
            inv_data = self.db.get_data(sp_inventory, (None,))
            if inv_data:
                kpi_data['Inventory_Over_2Y'] = sum(safe_float(row['Range_Over_720_V']) for row in inv_data)

        except Exception as e:
            print(f"Lỗi tính toán KPI Scorecards: {e}")
        
        return kpi_data

    def get_profit_trend_chart(self):
        """
        Lấy biểu đồ xu hướng (12 tháng gần nhất).
        """
        query = f"""
            SELECT TOP 12 TranYear, TranMonth,
                SUM(CASE WHEN CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN ConvertedAmount ELSE 0 END) as Revenue,
                SUM(CASE WHEN DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN ConvertedAmount ELSE 0 END) as COGS
            FROM {config.ERP_GIAO_DICH}
            WHERE VoucherDate >= DATEADD(month, -11, GETDATE())
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