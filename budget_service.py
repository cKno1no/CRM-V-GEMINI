# services/budget_service.py

from db_manager import DBManager, safe_float
from datetime import datetime
import config

class BudgetService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def check_budget_company_wide(self, budget_code, amount_request):
        """
        Kiểm tra ngân sách TOÀN CÔNG TY.
        """
        amount_request = safe_float(amount_request)
        now = datetime.now()
        month = now.month
        year = now.year
        
        # 1. Lấy thông tin mã chi phí
        master_query = f"""
            SELECT BudgetCode, ParentCode, BudgetName, ERP_Ana03ID, ControlLevel 
            FROM {config.TABLE_BUDGET_MASTER} WHERE BudgetCode = ?
        """
        item_info = self.db.get_data(master_query, (budget_code,))
        if not item_info:
            return {'status': 'ERROR', 'message': 'Mã chi phí không tồn tại', 'group_remaining': 0}
            
        item = item_info[0]
        parent_code = item['ParentCode']
        erp_id = item['ERP_Ana03ID']
        
        # 2. Tính toán SỨC KHỎE CỦA CẢ NHÓM
        
        # A. Tổng Ngân sách Kế hoạch
        query_plan_group = f"""
            SELECT SUM(P.BudgetAmount) as TotalPlan
            FROM {config.TABLE_BUDGET_PLAN} P
            INNER JOIN {config.TABLE_BUDGET_MASTER} M ON P.BudgetCode = M.BudgetCode
            WHERE M.ParentCode = ? AND P.[Month] = ? AND P.FiscalYear = ?
        """
        plan_data = self.db.get_data(query_plan_group, (parent_code, month, year))
        group_plan = safe_float(plan_data[0]['TotalPlan']) if plan_data else 0

        # B. Tổng Thực chi từ ERP
        # [CONFIG]: Dùng ACC_CHI_PHI_QL thay vì '642%'
        query_actual_erp = f"""
            SELECT SUM(ConvertedAmount) as TotalActual
            FROM {config.ERP_GIAO_DICH}
            WHERE Ana03ID = ? AND TranMonth = ? AND TranYear = ? 
            AND DebitAccountID LIKE '{config.ACC_CHI_PHI_QL}'
        """
        actual_data = self.db.get_data(query_actual_erp, (erp_id, month, year))
        group_actual = safe_float(actual_data[0]['TotalActual']) if actual_data else 0

        # C. Tổng Đang chờ duyệt trên App
        query_pending_group = f"""
            SELECT SUM(R.Amount) as TotalPending
            FROM {config.TABLE_EXPENSE_REQUEST} R
            INNER JOIN {config.TABLE_BUDGET_MASTER} M ON R.BudgetCode = M.BudgetCode
            WHERE M.ParentCode = ? 
            AND MONTH(R.RequestDate) = ? AND YEAR(R.RequestDate) = ?
            AND R.[Status] = 'PENDING'
        """
        pending_data = self.db.get_data(query_pending_group, (parent_code, month, year))
        group_pending = safe_float(pending_data[0]['TotalPending']) if pending_data else 0

        # D. Tính dư
        group_remaining = group_plan - group_actual - group_pending
        
        # E. Logic Quyết định
        if amount_request <= group_remaining:
            return {
                'status': 'PASS', 
                'message': 'Ngân sách hợp lệ.',
                'group_plan': group_plan,
                'group_actual': group_actual,
                'group_pending': group_pending,
                'group_remaining': group_remaining,
                'is_warning': False
            }
        else:
            shortage = amount_request - group_remaining
            msg = f"Nhóm chi phí '{parent_code}' ({erp_id}) đã vượt ngân sách chung {shortage:,.0f} VNĐ."
            
            if item['ControlLevel'] == 'HARD':
                return {
                    'status': 'BLOCK', 
                    'message': msg,
                    'group_plan': group_plan,
                    'group_remaining': group_remaining
                }
            else:
                return {
                    'status': 'WARN', 
                    'message': msg, 
                    'is_warning': True,
                    'group_plan': group_plan,
                    'group_actual': group_actual,
                    'group_pending': group_pending,
                    'group_remaining': group_remaining
                }

    def get_budget_status(self, budget_code, department_code, month, year):
        """
        Tính toán tình hình ngân sách của 1 mã chi phí.
        """
        # 1. Plan
        query_plan = f"""
            SELECT BudgetAmount FROM {config.TABLE_BUDGET_PLAN} 
            WHERE BudgetCode = ? AND DepartmentCode = ? AND [Month] = ? AND FiscalYear = ?
        """
        plan_data = self.db.get_data(query_plan, (budget_code, department_code, month, year))
        budget_amount = safe_float(plan_data[0]['BudgetAmount']) if plan_data else 0

        # 2. Actual
        # [CONFIG]: Dùng ACC_CHI_PHI_QL
        query_actual = f"""
            SELECT SUM(ConvertedAmount) as Actual 
            FROM {config.ERP_GIAO_DICH} 
            WHERE Ana03ID = ? AND TranMonth = ? AND TranYear = ? 
            AND DebitAccountID LIKE '{config.ACC_CHI_PHI_QL}'
        """
        actual_data = self.db.get_data(query_actual, (budget_code, month, year))
        actual_amount = safe_float(actual_data[0]['Actual']) if actual_data and actual_data[0]['Actual'] else 0

        # 3. Pending
        query_pending = f"""
            SELECT SUM(Amount) as Pending 
            FROM {config.TABLE_EXPENSE_REQUEST} 
            WHERE BudgetCode = ? AND DepartmentCode = ? 
            AND MONTH(RequestDate) = ? AND YEAR(RequestDate) = ?
            AND [Status] = 'PENDING'
        """
        pending_data = self.db.get_data(query_pending, (budget_code, department_code, month, year))
        pending_amount = safe_float(pending_data[0]['Pending']) if pending_data and pending_data[0]['Pending'] else 0

        # 4. Remaining
        remaining = budget_amount - actual_amount - pending_amount
        
        return {
            'BudgetCode': budget_code,
            'Planned': budget_amount,
            'Actual_ERP': actual_amount,
            'Pending_App': pending_amount,
            'Remaining': remaining,
            'UsagePercent': ((actual_amount + pending_amount) / budget_amount * 100) if budget_amount > 0 else 0
        }

    def create_expense_request(self, user_code, dept_code, budget_code, amount, reason, object_id=None):
        """
        Tạo đề nghị thanh toán mới.
        """
        now = datetime.now()
        
        # 1. Lấy thông tin Control Level & Approver
        master_query = f"SELECT ControlLevel, DefaultApprover FROM {config.TABLE_BUDGET_MASTER} WHERE BudgetCode = ?"
        master_data = self.db.get_data(master_query, (budget_code,))
        if not master_data:
            return {'success': False, 'message': 'Mã ngân sách không tồn tại.'}
        
        control_level = master_data[0]['ControlLevel']
        default_approver = master_data[0]['DefaultApprover']

        # 2. Kiểm tra số dư ngân sách
        status = self.get_budget_status(budget_code, dept_code, now.month, now.year)
        
        if amount > status['Remaining']:
            if control_level == 'HARD':
                return {'success': False, 'message': f"Bị chặn: Vượt ngân sách khả dụng ({status['Remaining']:,.0f})."}
            else:
                reason = f"[CẢNH BÁO VƯỢT NS] {reason}"

        # 3. Xác định người duyệt (Logic nâng cao)
        approver = default_approver
        
        # Nếu chưa có người duyệt HOẶC Người duyệt trùng với Người tạo (tránh tự duyệt)
        if not approver or approver == user_code:
            # Lấy Cấp trên trực tiếp
            user_query = f"SELECT [CAP TREN] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
            user_data = self.db.get_data(user_query, (user_code,))
            parent_approver = user_data[0]['CAP TREN'] if user_data else None
            
            # Nếu Cấp trên cũng là mình (VD: CEO) -> Gán ADMIN
            if parent_approver == user_code:
                approver = config.ROLE_ADMIN
            else:
                approver = parent_approver or config.ROLE_ADMIN

        # 4. Lưu vào DB
        req_id = f"REQ-{now.strftime('%y%m')}-{int(datetime.now().timestamp())}"
        
        insert_query = f"""
            INSERT INTO {config.TABLE_EXPENSE_REQUEST} 
            (RequestID, UserCode, DepartmentCode, BudgetCode, Amount, Reason, CurrentApprover, Status, ObjectID)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
        """
        
        if self.db.execute_non_query(insert_query, (req_id, user_code, dept_code, budget_code, amount, reason, approver, object_id)):
            return {'success': True, 'message': 'Đã gửi đề nghị thành công.', 'request_id': req_id}
            
        return {'success': False, 'message': 'Lỗi CSDL khi lưu đề nghị.'}

    def get_requests_for_approval(self, approver_code, user_role=''):
        """
        Lấy danh sách phiếu chờ duyệt.
        """
        query_params = []
        role_check = str(user_role).strip().upper()
        
        # [CONFIG]: Dùng ROLE_ADMIN
        if role_check in [config.ROLE_ADMIN, config.ROLE_GM]:
            where_clause = "R.Status = 'PENDING'"
        else:
            where_clause = "R.CurrentApprover = ? AND R.Status = 'PENDING'"
            query_params.append(approver_code)

        # [CONFIG]: Thay thế tên bảng cứng bằng config
        query = f"""
            SELECT 
                R.*, 
                M.BudgetName, 
                U.SHORTNAME as RequesterName,
                U2.SHORTNAME as CurrentApproverName
            FROM {config.TABLE_EXPENSE_REQUEST} R
            LEFT JOIN {config.TABLE_BUDGET_MASTER} M ON R.BudgetCode = M.BudgetCode
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} U ON R.UserCode = U.USERCODE
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} U2 ON R.CurrentApprover = U2.USERCODE
            WHERE {where_clause}
            ORDER BY R.RequestDate DESC
        """
        
        requests = self.db.get_data(query, tuple(query_params))
        
        for req in requests:
            req['Amount'] = safe_float(req.get('Amount'))
            check = self.check_budget_company_wide(req['BudgetCode'], req['Amount'])
            req['GroupRemaining'] = check.get('group_remaining', 0)
            req['IsWarning'] = req['Amount'] > check.get('group_remaining', 0)
            
        return requests

    def approve_request(self, request_id, approver_code, action, note):
        """Xử lý Duyệt hoặc Từ chối."""
        new_status = 'APPROVED' if action == 'APPROVE' else 'REJECTED'
        
        query = f"""
            UPDATE {config.TABLE_EXPENSE_REQUEST}
            SET Status = ?, 
                ApprovalDate = GETDATE(), 
                ApprovalNote = ?,
                CurrentApprover = ?
            WHERE RequestID = ? AND Status = 'PENDING'
        """
        return self.db.execute_non_query(query, (new_status, note, approver_code, request_id))

    def get_request_detail_for_print(self, request_id):
        """Lấy chi tiết phiếu để in."""
        # [CONFIG]: Thay tên bảng cứng
        query = f"""
            SELECT R.*, M.BudgetName, 
                   U1.SHORTNAME AS RequesterName, U1.[BO PHAN] AS RequesterDept,
                   U2.SHORTNAME AS ApproverName
            FROM {config.TABLE_EXPENSE_REQUEST} R
            LEFT JOIN {config.TABLE_BUDGET_MASTER} M ON R.BudgetCode = M.BudgetCode
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} U1 ON R.UserCode = U1.USERCODE
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} U2 ON R.CurrentApprover = U2.USERCODE
            WHERE R.RequestID = ?
        """
        data = self.db.get_data(query, (request_id,))
        return data[0] if data else None

    def get_approved_requests_for_payment(self):
        """Lấy danh sách phiếu ĐÃ DUYỆT chờ thanh toán."""
        # [CONFIG]: Thay tên bảng cứng
        query = f"""
            SELECT R.*, U.SHORTNAME as RequesterName 
            FROM {config.TABLE_EXPENSE_REQUEST} R
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} U ON R.UserCode = U.USERCODE
            WHERE R.Status = 'APPROVED'
            ORDER BY R.ApprovalDate ASC
        """
        return self.db.get_data(query)

    def process_payment(self, request_id, user_code, payment_ref, payment_date):
        """Xác nhận ĐÃ CHI."""
        query = f"""
            UPDATE {config.TABLE_EXPENSE_REQUEST}
            SET Status = 'PAID', 
                PaymentRef = ?, 
                PaymentDate = ?,
                PayerCode = ?
            WHERE RequestID = ? AND Status = 'APPROVED'
        """
        return self.db.execute_non_query(query, (payment_ref, payment_date, user_code, request_id))

    def get_ytd_budget_report(self, department_code, year):
        """
        Lấy báo cáo YTD gom nhóm theo ReportGroup.
        """
        # 1. Plan
        query_plan = f"""
            SELECT 
                M.ReportGroup, PL.[Month], SUM(PL.BudgetAmount) as PlanAmount
            FROM {config.TABLE_BUDGET_PLAN} PL
            JOIN {config.TABLE_BUDGET_MASTER} M ON PL.BudgetCode = M.BudgetCode
            WHERE PL.FiscalYear = ? 
            GROUP BY M.ReportGroup, PL.[Month]
        """
        plan_raw = self.db.get_data(query_plan, (year,))

        # 2. Actual - [CONFIG]: Dùng EXCLUDE_ANA03_CP2014
        query_actual = f"""
            SELECT Ana03ID, TranMonth, SUM(ConvertedAmount) as ActualAmount
            FROM {config.ERP_GIAO_DICH}
            WHERE TranYear = ? 
              AND Ana03ID IS NOT NULL 
              AND Ana03ID <> '{config.EXCLUDE_ANA03_CP2014}'
            GROUP BY Ana03ID, TranMonth
        """
        actual_raw = self.db.get_data(query_actual, (year,))
        
        # 3. Mapping
        query_map = f"SELECT DISTINCT ERP_Ana03ID, ReportGroup FROM {config.TABLE_BUDGET_MASTER} WHERE ERP_Ana03ID IS NOT NULL"
        mapping_raw = self.db.get_data(query_map)
        ana03_to_group = {row['ERP_Ana03ID']: row['ReportGroup'] for row in mapping_raw if row['ERP_Ana03ID']} if mapping_raw else {}

        # 4. Aggregate
        groups_data = {}
        def get_entry(g):
            if g not in groups_data: groups_data[g] = {'GroupName': g, 'Plan_Month': {}, 'Actual_Month': {}}
            return groups_data[g]

        if plan_raw:
            for p in plan_raw:
                g = p['ReportGroup'] or 'Khác'
                get_entry(g)['Plan_Month'][p['Month']] = get_entry(g)['Plan_Month'].get(p['Month'], 0) + safe_float(p['PlanAmount'])

        if actual_raw:
            for a in actual_raw:
                g = ana03_to_group.get(a['Ana03ID'], 'Khác')
                get_entry(g)['Actual_Month'][a['TranMonth']] = get_entry(g)['Actual_Month'].get(a['TranMonth'], 0) + safe_float(a['ActualAmount'])

        # 5. Calculate
        current_month = datetime.now().month
        ytd_limit = 12 if year < datetime.now().year else current_month
        
        final_report = []
        for g_name, data in groups_data.items():
            row = {'GroupName': g_name, 'Month_Plan': 0, 'Month_Actual': 0, 'Month_Diff': 0, 'YTD_Plan': 0, 'YTD_Actual': 0, 'YTD_Diff': 0, 'Year_Plan': 0, 'UsagePercent': 0}
            for m in range(1, 13):
                p = data['Plan_Month'].get(m, 0); a = data['Actual_Month'].get(m, 0)
                row['Year_Plan'] += p
                if m <= ytd_limit: row['YTD_Plan'] += p; row['YTD_Actual'] += a
                if m == current_month: row['Month_Plan'] = p; row['Month_Actual'] = a
            
            row['Month_Diff'] = row['Month_Plan'] - row['Month_Actual']
            row['YTD_Diff'] = row['YTD_Plan'] - row['YTD_Actual']
            row['UsagePercent'] = (row['YTD_Actual'] / row['YTD_Plan'] * 100) if row['YTD_Plan'] > 0 else (0 if row['YTD_Actual'] == 0 else 100)
            final_report.append(row)

        final_report.sort(key=lambda x: x['Year_Plan'], reverse=True)
        return final_report
    
    def get_expense_details_by_group(self, report_group, year):
        """Lấy chi tiết phiếu chi theo ReportGroup."""
        ana_query = f"SELECT ERP_Ana03ID FROM {config.TABLE_BUDGET_MASTER} WHERE ReportGroup = ?"
        ana_data = self.db.get_data(ana_query, (report_group,))
        
        if not ana_data: return []
        ana_codes = [row['ERP_Ana03ID'] for row in ana_data if row['ERP_Ana03ID']]
        if not ana_codes: return []
        ana_str = "', '".join(ana_codes)
        
        query = f"""
            SELECT TOP 100 T1.VoucherNo, T1.VoucherDate, T1.VDescription, T1.ObjectID, 
                   ISNULL(T2.ShortObjectName, T2.ObjectName) as ObjectName, T1.Ana03ID, SUM(T1.ConvertedAmount) as TotalAmount
            FROM {config.ERP_GIAO_DICH} T1
            LEFT JOIN {config.ERP_IT1202} T2 ON T1.ObjectID = T2.ObjectID
            WHERE T1.TranYear = ? AND T1.Ana03ID IN ('{ana_str}')
            GROUP BY T1.VoucherNo, T1.VoucherDate, T1.VDescription, T1.ObjectID, T2.ShortObjectName, T2.ObjectName, T1.Ana03ID
            ORDER BY TotalAmount DESC
        """
        details = self.db.get_data(query, (year,))
        if details:
            for row in details:
                row['TotalAmount'] = safe_float(row['TotalAmount'])
                # --- SỬA LỖI SYNTAX Ở ĐÂY ---
                if row['VoucherDate']:
                    try:
                        row['VoucherDate'] = row['VoucherDate'].strftime('%d/%m/%Y')
                    except:
                        pass
        return details or []