# services/portal_service.py
import pyodbc
from db_manager import DBManager, safe_float
import config
from datetime import datetime, timedelta

class PortalService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def _fix_date(self, date_val):
        """Helper để xử lý ngày tháng tránh lỗi view"""
        if not date_val:
            return None
        if isinstance(date_val, datetime):
            return date_val
        if isinstance(date_val, str):
            try:
                return datetime.strptime(date_val[:10], '%Y-%m-%d')
            except:
                return date_val
        return date_val

    def _group_by_customer(self, items, name_key='CustomerName', id_key='ObjectID'):
        """
        Gom nhóm danh sách items theo Khách hàng.
        """
        groups = {}
        ordered_groups = []

        for item in items:
            # Lấy tên KH làm key gom nhóm
            c_name = item.get(name_key) or item.get('ObjectName') or 'Khách lẻ / Khác'
            c_id = item.get(id_key) or 'UNK'

            group_key = c_name.strip().upper()

            if group_key not in groups:
                new_group = {
                    'name': c_name,
                    'id': c_id,
                    'count': 0,
                    'details': [] 
                }
                groups[group_key] = new_group
                ordered_groups.append(new_group)
            
            groups[group_key]['details'].append(item)
            groups[group_key]['count'] += 1
            
        return ordered_groups

    def get_all_dashboard_data(self, user_code, bo_phan, role):
        data = {
            'sales_kpi': {'actual': 0, 'target': 0, 'percent': 0},
            'tasks': [], 
            'overdue_debt': [], 
            'orders_stat': 0, 
            'active_quotes': [], 
            'pending_deliveries': [],
            'orders_flow': [], 
            'urgent_replenish': [], 
            'recent_reports': [],
            'errors': {} 
        }

        current_year = datetime.now().year
        current_month = datetime.now().month
        is_thu_ky = "THU KY" in bo_phan or "THUKY" in bo_phan
        col_filter_erp = "EmployeeID" if is_thu_ky else "SalesManID"
        TABLE_CUSTOMER_ERP = config.ERP_IT1202

        try:
            conn = pyodbc.connect(self.db.conn_str)
            cursor = conn.cursor()
        except Exception as e:
            data['errors']['connection'] = str(e)
            return data

        # --- 1. KPI: DOANH SỐ ---
        try:
            cursor.execute(f"SELECT SUM([DK]) FROM {config.CRM_DTCL} WHERE [Nam]=? AND [PHU TRACH DS]=?", (current_year, user_code))
            row = cursor.fetchone()
            monthly_target = (safe_float(row[0]) / 12) if row and row[0] else 0
            
            cursor.execute(f"""
                SELECT SUM(ConvertedAmount) FROM {config.ERP_GIAO_DICH} 
                WHERE SalesManID=? AND TranMonth=? AND TranYear=? 
                AND DebitAccountID='13111' AND CreditAccountID LIKE '5%'
            """, (user_code, current_month, current_year))
            row = cursor.fetchone()
            actual_sales = safe_float(row[0]) if row and row[0] else 0
            
            percent = (actual_sales / monthly_target * 100) if monthly_target > 0 else 0
            data['sales_kpi'] = {'actual': actual_sales, 'target': monthly_target, 'percent': round(percent, 1)}
        except Exception as e:
            data['errors']['kpi'] = str(e)

        # --- 2. TASK ---
        try:
            cursor.execute(f"""
                SELECT TOP 20 M.TaskID, M.Title, M.Status, M.Priority, M.LastUpdated, M.ObjectID,
                (SELECT COUNT(*) FROM {config.TASK_LOG_TABLE} L WHERE L.TaskID = M.TaskID) as UpdateCount,
                CASE WHEN M.LastUpdated >= DATEADD(hour, -24, GETDATE()) THEN 1 ELSE 0 END as IsNewUpdate
                FROM {config.TASK_TABLE} M
                WHERE (M.UserCode=? OR M.CapTren=?) AND M.Status IN ('OPEN','PENDING','HELP_NEEDED','BLOCKED')
                ORDER BY CASE WHEN M.Priority='HIGH' THEN 0 ELSE 1 END, M.LastUpdated DESC
            """, (user_code, user_code))
            if cursor.description:
                cols = [c[0] for c in cursor.description]
                data['tasks'] = [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            data['errors']['tasks'] = str(e)

        # --- 3. CÔNG NỢ (Sửa ObjectID và ShortObjectName) ---
        try:
            cursor.execute(f"""
                SELECT TOP 20 
                    T1.ObjectID, 
                    ISNULL(C.ShortObjectName, T1.ObjectName) as ObjectName, 
                    T1.TotalOverdueDebt, 
                    T1.ReDueDays
                FROM dbo.CRM_AR_AGING_SUMMARY AS T1
                LEFT JOIN {TABLE_CUSTOMER_ERP} C ON T1.ObjectID = C.ObjectID 
                INNER JOIN {config.CRM_DTCL} AS T2 ON T1.ObjectID = T2.[MA KH]
                WHERE T2.[Nam]=? AND T2.[PHU TRACH DS]=? AND T1.TotalOverdueDebt > 1000
                ORDER BY T1.TotalOverdueDebt DESC
            """, (current_year, user_code))
            if cursor.description:
                cols = [c[0] for c in cursor.description]
                debt = [dict(zip(cols, r)) for r in cursor.fetchall()]
                for d in debt: 
                    d['TotalOverdueDebtFmt'] = "{:,.0f}".format(safe_float(d['TotalOverdueDebt']))
                data['overdue_debt'] = debt
        except Exception as e:
            data['errors']['debt'] = str(e)

        # --- 4. THỐNG KÊ ĐƠN TRONG THÁNG ---
        try:
            query_stat = f"""
                SELECT COUNT(DISTINCT T1.SOrderID)
                FROM {config.ERP_SALES_DETAIL} T2
                INNER JOIN {config.ERP_OT2001} T1 ON T2.SOrderID = T1.SOrderID
                WHERE T1.SalesManID = ?
                AND MONTH(T2.Date01) = MONTH(GETDATE()) AND YEAR(T2.Date01) = YEAR(GETDATE())
                AND T1.OrderStatus = 1 
                AND NOT EXISTS (
                    SELECT 1 FROM {config.ERP_GOODS_RECEIPT_DETAIL} W2
                    INNER JOIN {config.ERP_GOODS_RECEIPT_MASTER} W1 ON W2.VoucherID = W1.VoucherID
                    WHERE W2.OTransactionID = T2.TransactionID AND W1.VoucherTypeID = 'PX'
                )
            """
            cursor.execute(query_stat, (user_code,))
            row = cursor.fetchone()
            data['orders_stat'] = row[0] if row else 0
        except Exception as e:
            data['errors']['orders_stat'] = str(e)

        # --- 5. BÁO GIÁ (Sửa ObjectID và ShortObjectName) ---
        try:
            query = f"""
                SELECT TOP 40 
                    T1.QuotationNo as VoucherNo, 
                    T1.QuotationDate, 
                    T1.ObjectID, 
                    ISNULL(C.ShortObjectName, T1.ObjectName) as CustomerName, 
                    T1.SaleAmount as TotalAmount
                FROM {config.ERP_QUOTES} T1
                LEFT JOIN {TABLE_CUSTOMER_ERP} C ON T1.ObjectID = C.ObjectID
                WHERE T1.{col_filter_erp}=? 
                AND T1.QuotationDate > DATEADD(day, -30, GETDATE())
                AND NOT EXISTS (
                    SELECT 1 FROM {config.ERP_QUOTE_DETAILS} D1 
                    JOIN {config.ERP_SALES_DETAIL} D2 ON D1.TransactionID = D2.RetransactionID 
                    WHERE D1.QuotationID = T1.QuotationID
                )
                ORDER BY T1.QuotationDate DESC
            """
            cursor.execute(query, (user_code,))
            if cursor.description:
                cols = [c[0] for c in cursor.description]
                raw_quotes = [dict(zip(cols, r)) for r in cursor.fetchall()]
                for q in raw_quotes: q['TotalAmount'] = safe_float(q.get('TotalAmount', 0))
                
                data['active_quotes'] = self._group_by_customer(raw_quotes, name_key='CustomerName', id_key='ObjectID')
        except Exception as e:
            data['errors']['quotes'] = str(e)

        # --- 6. LXH - PENDING DELIVERIES (Sửa ObjectID và ShortObjectName) ---
        try:
            query = f"""
                SELECT DISTINCT TOP 40 
                    DW.VoucherNo, 
                    DW.VoucherDate as Request_Day, 
                    DW.Planned_Day,
                    DW.ObjectID,
                    ISNULL(C.ShortObjectName, DW.ObjectName) as ObjectName,
                    DATEDIFF(day, DW.VoucherDate, GETDATE()) as DaysPending,
                    DW.DeliveryStatus
                FROM {config.DELIVERY_WEEKLY_VIEW} DW
                LEFT JOIN {TABLE_CUSTOMER_ERP} C ON DW.ObjectID = C.ObjectID
                INNER JOIN {config.ERP_DELIVERY_DETAIL} T2 ON DW.VoucherID = T2.VoucherID
                INNER JOIN {config.ERP_OT2001} T3 ON T2.RespVoucherID = T3.SOrderID
                WHERE DW.DeliveryStatus <> N'DA GIAO' 
                AND { "T3.EmployeeID" if is_thu_ky else "T3.SalesManID" } = ?
                ORDER BY DW.VoucherDate ASC
            """
            cursor.execute(query, (user_code,))
            if cursor.description:
                cols = [c[0] for c in cursor.description]
                raw_dels = [dict(zip(cols, r)) for r in cursor.fetchall()]
                for d in raw_dels: 
                    d['IsOverdue'] = (d['DaysPending'] or 0) > 3
                    d['Planned_Day'] = self._fix_date(d.get('Planned_Day'))
                    d['Request_Day'] = self._fix_date(d.get('Request_Day'))
                
                data['pending_deliveries'] = self._group_by_customer(raw_dels, name_key='ObjectName', id_key='ObjectID')

        except Exception as e:
            data['errors']['delivery'] = str(e)

        # --- 7. LỊCH GIAO HÀNG (Sửa ObjectID và ShortObjectName) ---
        try:
            query = f"""
                SELECT TOP 40 
                    T1.VoucherNo, 
                    MIN(T2.Date01) as DeliveryDate, 
                    T1.ObjectID,
                    ISNULL(C.ShortObjectName, T1.ObjectName) as CustomerName,
                    SUM(T2.ConvertedAmount) as SaleAmount
                FROM {config.ERP_SALES_DETAIL} T2
                INNER JOIN {config.ERP_OT2001} T1 ON T2.SOrderID = T1.SOrderID
                LEFT JOIN {TABLE_CUSTOMER_ERP} C ON T1.ObjectID = C.ObjectID
                WHERE 
                    T1.SalesManID = ?
                    AND T2.Date01 BETWEEN DATEADD(day, -30, GETDATE()) AND DATEADD(day, 30, GETDATE())
                    AND T1.VoucherTypeID <> 'DTK' 
                    AND T1.OrderStatus = 1
                    AND NOT EXISTS (
                        SELECT 1
                        FROM {config.ERP_GOODS_RECEIPT_DETAIL} W2
                        INNER JOIN {config.ERP_GOODS_RECEIPT_MASTER} W1 ON W2.VoucherID = W1.VoucherID
                        WHERE W2.OTransactionID = T2.TransactionID
                        AND W1.VoucherTypeID = 'PX'
                    )
                GROUP BY T1.VoucherNo, T1.ObjectID, T1.ObjectName, C.ShortObjectName
                ORDER BY MIN(T2.Date01) ASC
            """
            cursor.execute(query, (user_code,))
            if cursor.description:
                cols = [c[0] for c in cursor.description]
                raw_orders = [dict(zip(cols, r)) for r in cursor.fetchall()]
                for o in raw_orders: 
                    o['IsOverdue'] = o['DeliveryDate'] < datetime.now()
                
                data['orders_flow'] = self._group_by_customer(raw_orders, name_key='CustomerName', id_key='ObjectID')

        except Exception as e:
            data['errors']['orders'] = str(e)

        # --- 8. DỰ PHÒNG (Giữ nguyên logic) ---
        try:
            cursor.execute("{CALL dbo.sp_GetPortalReplenishment (?, ?)}", (user_code, current_year))
            if cursor.description:
                cols = [c[0] for c in cursor.description]
                replenish_items = [dict(zip(cols, r)) for r in cursor.fetchall()]
                
                if replenish_items:
                    replenish_items = replenish_items[:40]
                    for item in replenish_items:
                        item['QuantitySuggestion'] = "{:,.0f}".format(safe_float(item.get('QuantitySuggestion', 0)))
                    
                    data['urgent_replenish'] = self._group_by_customer(replenish_items, name_key='CustomerName', id_key='ItemID')

        except Exception as e:
            data['errors']['replenish'] = str(e)

        # --- 9. BÁO CÁO (Sửa tên cột MA KH thành KHACH HANG) ---
        try:
            cursor.execute(f"SELECT TOP 20 STT, NGAY, [KHACH HANG] as [TEN DOI TUONG], [NOI DUNG 4] as MucDich FROM {config.TEN_BANG_BAO_CAO} WHERE NGUOI=? AND NGAY >= DATEADD(day, -7, GETDATE()) ORDER BY NGAY DESC", (user_code,))
            if cursor.description:
                cols = [c[0] for c in cursor.description]
                data['recent_reports'] = [dict(zip(cols, r)) for r in cursor.fetchall()]
        except Exception as e:
            data['errors']['reports'] = str(e)

        conn.close()
        return data