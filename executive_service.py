# services/executive_service.py

from flask import current_app
from db_manager import DBManager, safe_float
from datetime import datetime, timedelta
import config

class ExecutiveService:
    """
    Service chuyên biệt cho CEO Cockpit (Version 3.2 - Fix Conflict).
    """
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_dashboard_data_cached(self, year, month):
        cache_key = f"ceo_cockpit_data_{year}_{month}"
        try:
            cached_data = current_app.cache.get(cache_key)
            if cached_data:
                current_app.logger.info(f"CEO Cockpit: HIT Global Cache ({cache_key})")
                return cached_data
        except Exception as e:
            current_app.logger.warning(f"Cache Warning: {e}")

        current_app.logger.info(f"CEO Cockpit: MISS Cache -> Calculating DB...")
        data = self._calculate_dashboard_data(year, month)
        
        try:
            current_app.cache.set(cache_key, data, timeout=600)
        except Exception:
            pass
            
        return data

    def _calculate_dashboard_data(self, current_year, current_month):
        kpi_data = self.get_kpi_scorecards(current_year, current_month)
        
        charts = {
            'inventory': self.get_inventory_aging_chart_data(),
            'category': self.get_top_categories_performance(current_year),
            'financial': self.get_profit_trend_chart(),
            'funnel': self.get_sales_funnel_data()
        }
        
        lists = {
            'top_sales': self.get_top_sales_leaderboard(current_year),
            'actions': self.get_pending_actions_count()
        }

        profit_summary = {
            'GrossProfit': kpi_data.get('GrossProfit_YTD', 0),
            'AvgMargin': kpi_data.get('AvgMargin_YTD', 0)
        }
        finance_summary = {
            'TotalExpenses': kpi_data.get('TotalExpenses_YTD', 0),
            'CrossSellProfit': kpi_data.get('CrossSellProfit_YTD', 0)
        }
        risk_summary = {
            'AR_Debt_Over_180': kpi_data.get('AR_Debt_Over_180', 0),
            'AR_TotalOverdueDebt': kpi_data.get('AR_TotalOverdueDebt', 0),
            'AP_Debt_Over_180': kpi_data.get('AP_Debt_Over_180', 0),
            'AP_TotalOverdueDebt': kpi_data.get('AP_TotalOverdueDebt', 0),
            'Inventory_Over_2Y': kpi_data.get('Inventory_Over_2Y', 0)
        }

        return { 
            'kpi': kpi_data, 
            'charts': charts, 
            'lists': lists,
            'profit_summary': profit_summary,
            'finance_summary': finance_summary,
            'risk_summary': risk_summary
        }

    def get_kpi_scorecards(self, current_year, current_month):
        kpi_data = {
            'Sales_YTD': 0, 'TargetYear': 0, 'Percent': 0,
            'GrossProfit_YTD': 0, 'AvgMargin_YTD': 0,
            'TotalExpenses_YTD': 0, 'BudgetPlan_YTD': 0,
            'CrossSellProfit_YTD': 0, 'CrossSellCustCount': 0,
            'AR_TotalOverdueDebt': 0, 'AR_Debt_Over_180': 0,
            'AP_TotalOverdueDebt': 0, 'AP_Debt_Over_180': 0,
            'Inventory_Over_2Y': 0,
            'OTIF_Month': 0, 'OTIF_YTD': 0
        }

        try:
            result = self.db.execute_sp_multi('sp_GetExecutiveKPI', (current_year, current_month))
            if result and result[0]:
                row = result[0][0]
                kpi_data['Sales_YTD'] = safe_float(row.get('Sales_YTD', 0))
                kpi_data['GrossProfit_YTD'] = safe_float(row.get('GrossProfit_YTD', 0))
                kpi_data['TotalExpenses_YTD'] = safe_float(row.get('TotalExpenses_YTD', 0))
                kpi_data['BudgetPlan_YTD'] = safe_float(row.get('BudgetPlan_YTD', 0))
                kpi_data['CrossSellProfit_YTD'] = safe_float(row.get('CrossSellProfit_YTD', 0))
                kpi_data['CrossSellCustCount'] = int(row.get('CrossSellCustCount', 0))
                kpi_data['AR_TotalOverdueDebt'] = safe_float(row.get('AR_Overdue', 0))
                kpi_data['AR_Debt_Over_180'] = safe_float(row.get('AR_Risk', 0))
                kpi_data['AP_TotalOverdueDebt'] = safe_float(row.get('AP_Overdue', 0))
                kpi_data['AP_Debt_Over_180'] = safe_float(row.get('AP_Risk', 0))

                sales = kpi_data['Sales_YTD']
                profit = kpi_data['GrossProfit_YTD']
                if sales > 0:
                    kpi_data['AvgMargin_YTD'] = (profit / sales) * 100

                kpi_data['TargetYear'] = 200000000000 
                if kpi_data['TargetYear'] > 0:
                    kpi_data['Percent'] = round((sales / kpi_data['TargetYear']) * 100, 1)

            # 2. Inventory KPI (Dùng SP Summary mới cho nhanh)
            # [FIX] Dùng SP Summary thay vì SP Detail
            sp_inv = f"{{CALL {config.SP_GET_INVENTORY_AGING_SUMMARY} (?)}}"
            
            # SP Summary trả về: Bảng 1 (Tổng), Bảng 2 (Nhóm)
            inv_results = self.db.execute_sp_multi(config.SP_GET_INVENTORY_AGING_SUMMARY, (None,))
            
            if inv_results and inv_results[0]:
                 # Bảng 1, dòng 1, cột LongTerm hoặc tính tổng
                 summary_row = inv_results[0][0]
                 kpi_data['Inventory_Over_2Y'] = safe_float(summary_row.get('LongTerm', 0)) + safe_float(summary_row.get('Risk', 0))
            
            # 3. OTIF (View)
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

        except Exception as e:
            current_app.logger.error(f"Lỗi tính KPI: {e}")
        
        return kpi_data

    def get_inventory_aging_chart_data(self):
        """
        [FIXED] Sử dụng SP Summary (đã cộng gộp) để vẽ biểu đồ nhanh.
        """
        try:
            # [FIX] Dùng biến config MỚI: SP_GET_INVENTORY_AGING_SUMMARY
            sp_query = f"{{CALL {config.SP_GET_INVENTORY_AGING_SUMMARY} (?)}}"
            results = self.db.execute_sp_multi(config.SP_GET_INVENTORY_AGING_SUMMARY, (None,))
            
            if not results or not results[0]: 
                return {'labels': [], 'series': [], 'drilldown': {}}

            # 1. Xử lý biểu đồ tổng (Donut Chart)
            summary_row = results[0][0]
            labels = ['An toàn (< 6 Tháng)', 'Ổn định (6-12 Tháng)', 'Chậm (1-2 Năm)', 'Tồn Lâu (> 2 Năm)', 'Hàng CLC (Rủi ro cao)']
            series = [
                safe_float(summary_row.get('Safe', 0)),
                safe_float(summary_row.get('Stable', 0)),
                safe_float(summary_row.get('Slow', 0)),
                safe_float(summary_row.get('LongTerm', 0)),
                safe_float(summary_row.get('Risk', 0))
            ]

            # 2. Xử lý Drill-down (Chi tiết nhóm)
            drilldown = {label: [] for label in labels}
            
            if len(results) > 1:
                detail_rows = results[1]
                for row in detail_rows:
                    group_name = row.get('GroupID', 'UNK')
                    # Map dữ liệu vào từng nhóm drilldown
                    drilldown['An toàn (< 6 Tháng)'].append({'name': group_name, 'value': safe_float(row['Safe'])})
                    drilldown['Ổn định (6-12 Tháng)'].append({'name': group_name, 'value': safe_float(row['Stable'])})
                    drilldown['Chậm (1-2 Năm)'].append({'name': group_name, 'value': safe_float(row['Slow'])})
                    drilldown['Tồn Lâu (> 2 Năm)'].append({'name': group_name, 'value': safe_float(row['LongTerm'])})
                    drilldown['Hàng CLC (Rủi ro cao)'].append({'name': group_name, 'value': safe_float(row['Risk'])})

            return {'labels': labels, 'series': series, 'drilldown': drilldown}

        except Exception as e:
            current_app.logger.error(f"Lỗi chart tồn kho (Optimized): {e}")
            return {'labels': [], 'series': [], 'drilldown': {}}

    # ... (Giữ nguyên các hàm khác: get_profit_trend_chart, get_pending_actions_count...)
    def get_profit_trend_chart(self):
        query = f"""
            SELECT TOP 12 TranYear, TranMonth,
                SUM(CASE WHEN CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN ConvertedAmount ELSE 0 END) as Revenue,
                SUM(CASE WHEN DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN ConvertedAmount ELSE 0 END) as COGS
            FROM {config.ERP_GIAO_DICH}
            WHERE VoucherDate >= DATEADD(month, -11, GETDATE())
            AND OTransactionID IS NOT NULL
            GROUP BY TranYear, TranMonth
            ORDER BY TranYear ASC, TranMonth ASC
        """
        try:
            data = self.db.get_data(query)
            chart_data = {'categories': [], 'revenue': [], 'profit': [], 'expenses': [], 'net_profit': []}
            if data:
                for row in data:
                    rev = safe_float(row['Revenue'])
                    profit = rev - safe_float(row['COGS'])
                    chart_data['categories'].append(f"T{row['TranMonth']}/{row['TranYear']}")
                    chart_data['revenue'].append(round(rev / config.DIVISOR_VIEW, 2))
                    chart_data['profit'].append(round(profit / config.DIVISOR_VIEW, 2))
                    chart_data['expenses'].append(0) 
                    chart_data['net_profit'].append(round(profit / config.DIVISOR_VIEW, 2)) 
            return chart_data
        except Exception:
            return {'categories': [], 'revenue': [], 'profit': [], 'expenses': [], 'net_profit': []}

    def get_pending_actions_count(self):
        counts = {'Quotes': 0, 'Budgets': 0, 'Orders': 0, 'UrgentTasks': 0, 'Total': 0}
        try:
            c_q = self.db.get_data(f"SELECT COUNT(*) FROM {config.ERP_QUOTES} WHERE OrderStatus = 0")
            counts['Quotes'] = safe_float(list(c_q[0].values())[0]) if c_q else 0
            c_b = self.db.get_data(f"SELECT COUNT(*) FROM {config.TABLE_EXPENSE_REQUEST} WHERE Status = 'PENDING'")
            counts['Budgets'] = safe_float(list(c_b[0].values())[0]) if c_b else 0
            c_o = self.db.get_data(f"SELECT COUNT(*) FROM {config.ERP_OT2001} WHERE OrderStatus = 0")
            counts['Orders'] = safe_float(list(c_o[0].values())[0]) if c_o else 0
            q_task = f"""
                SELECT COUNT(*) FROM {config.TASK_TABLE} 
                WHERE Status IN ('{config.TASK_STATUS_BLOCKED}', '{config.TASK_STATUS_HELP}') 
                OR (Priority = 'HIGH' AND Status NOT IN ('{config.TASK_STATUS_COMPLETED}', 'CANCELLED'))
            """
            c_t = self.db.get_data(q_task)
            counts['UrgentTasks'] = safe_float(list(c_t[0].values())[0]) if c_t else 0
            counts['Total'] = int(counts['Quotes'] + counts['Budgets'] + counts['Orders'] + counts['UrgentTasks'])
        except Exception: pass
        return counts

    def get_top_sales_leaderboard(self, current_year):
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
                board.append({'UserCode': row['UserCode'], 'ShortName': row['SHORTNAME'], 'TotalSalesAmount': act, 'Percent': round(pct, 1)})
        board.sort(key=lambda x: x['Percent'], reverse=True)
        return board[:5]

    def get_top_categories_performance(self, current_year):
        query = f"""
            SELECT TOP 10
                ISNULL(T3.TEN, T2.I04ID) as CategoryName,
                SUM(CASE WHEN T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN T1.ConvertedAmount ELSE 0 END) as Revenue,
                (SUM(CASE WHEN T1.CreditAccountID LIKE '{config.ACC_DOANH_THU}' THEN T1.ConvertedAmount ELSE 0 END) -
                 SUM(CASE WHEN T1.DebitAccountID LIKE '{config.ACC_GIA_VON}' THEN T1.ConvertedAmount ELSE 0 END)) as GrossProfit
            FROM {config.ERP_GIAO_DICH} T1
            INNER JOIN {config.ERP_IT1302} T2 ON T1.InventoryID = T2.InventoryID
            LEFT JOIN {config.TEN_BANG_NOI_DUNG_HD} T3 ON T2.I04ID = T3.LOAI 
            WHERE T1.TranYear = ? AND T1.OTransactionID IS NOT NULL
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
    
    def get_sales_funnel_data(self):
        try:
            today = datetime.now()
            start_date = f"{today.year}-01-01"
            end_date = today.strftime('%Y-%m-%d')
            result = self.db.execute_sp_multi('sp_GetSalesFunnel', (start_date, end_date))
            data = {'categories': ['Chào giá', 'Đơn hàng', 'Doanh số (Tỷ)'], 'quotes': [], 'orders': [], 'revenue': []}
            if result and result[0]:
                rows = result[0]
                val_quotes = next((r['Value'] for r in rows if r['Stage'] == 'Quotes'), 0)
                val_orders = next((r['Value'] for r in rows if r['Stage'] == 'Orders'), 0)
                val_revenue = next((r['Value'] for r in rows if r['Stage'] == 'Revenue'), 0)
                data['quotes'] = [val_quotes, 0, 0]
                data['orders'] = [0, val_orders, 0]
                data['revenue'] = [0, 0, val_revenue]
            return data
        except Exception: return {}

    
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