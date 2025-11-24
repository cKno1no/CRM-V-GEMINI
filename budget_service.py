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
        # FIX LỖI: Đảm bảo amount_request luôn là số thực (float) trước khi so sánh
        amount_request = safe_float(amount_request)

        now = datetime.now()
        month = now.month
        year = now.year
        
        # 1. Lấy thông tin mã chi phí & Mã ERP tương ứng
        master_query = """
            SELECT BudgetCode, ParentCode, BudgetName, ERP_Ana03ID, ControlLevel 
            FROM dbo.BUDGET_MASTER WHERE BudgetCode = ?
        """
        item_info = self.db.get_data(master_query, (budget_code,))
        if not item_info:
            return {'status': 'ERROR', 'message': 'Mã chi phí không tồn tại', 'group_remaining': 0}
            
        item = item_info[0]
        parent_code = item['ParentCode']
        erp_id = item['ERP_Ana03ID']
        
        # 2. Tính toán SỨC KHỎE CỦA CẢ NHÓM
        
        # A. Tổng Ngân sách Kế hoạch
        query_plan_group = """
            SELECT SUM(P.BudgetAmount) as TotalPlan
            FROM dbo.BUDGET_PLAN P
            INNER JOIN dbo.BUDGET_MASTER M ON P.BudgetCode = M.BudgetCode
            WHERE M.ParentCode = ? AND P.[Month] = ? AND P.FiscalYear = ?
        """
        plan_data = self.db.get_data(query_plan_group, (parent_code, month, year))
        group_plan = safe_float(plan_data[0]['TotalPlan']) if plan_data else 0

        # B. Tổng Thực chi từ ERP
        query_actual_erp = f"""
            SELECT SUM(ConvertedAmount) as TotalActual
            FROM {config.ERP_GIAO_DICH}
            WHERE Ana03ID = ? AND TranMonth = ? AND TranYear = ? AND DebitAccountID LIKE '642%'
        """
        actual_data = self.db.get_data(query_actual_erp, (erp_id, month, year))
        group_actual = safe_float(actual_data[0]['TotalActual']) if actual_data else 0

        # C. Tổng Đang chờ duyệt trên App
        query_pending_group = """
            SELECT SUM(R.Amount) as TotalPending
            FROM dbo.EXPENSE_REQUEST R
            INNER JOIN dbo.BUDGET_MASTER M ON R.BudgetCode = M.BudgetCode
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
        [FIX]: Đã thêm tham số department_code để khớp với lời gọi hàm.
        """
        # 1. Lấy Ngân sách Kế hoạch (Plan)
        query_plan = f"""
            SELECT BudgetAmount FROM dbo.BUDGET_PLAN 
            WHERE BudgetCode = ? AND DepartmentCode = ? AND [Month] = ? AND FiscalYear = ?
        """
        plan_data = self.db.get_data(query_plan, (budget_code, department_code, month, year))
        budget_amount = safe_float(plan_data[0]['BudgetAmount']) if plan_data else 0

        # 2. Lấy Thực chi từ ERP (GT9000)
        # Lưu ý: Đảm bảo Ana03ID là mã ngân sách trong GT9000
        query_actual = f"""
            SELECT SUM(ConvertedAmount) as Actual 
            FROM {config.ERP_GIAO_DICH} 
            WHERE Ana03ID = ? AND TranMonth = ? AND TranYear = ? AND DebitAccountID LIKE '642%'
        """
        actual_data = self.db.get_data(query_actual, (budget_code, month, year))
        actual_amount = safe_float(actual_data[0]['Actual']) if actual_data and actual_data[0]['Actual'] else 0

        # 3. Lấy số tiền Đang chờ duyệt (Pending) trên App
        query_pending = """
            SELECT SUM(Amount) as Pending 
            FROM dbo.EXPENSE_REQUEST 
            WHERE BudgetCode = ? AND DepartmentCode = ? 
            AND MONTH(RequestDate) = ? AND YEAR(RequestDate) = ?
            AND [Status] = 'PENDING'
        """
        pending_data = self.db.get_data(query_pending, (budget_code, department_code, month, year))
        pending_amount = safe_float(pending_data[0]['Pending']) if pending_data and pending_data[0]['Pending'] else 0

        # 4. Tính còn lại
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
        [UPDATE]: Đã thêm tham số object_id để lưu Đối tượng thụ hưởng.
        """
        now = datetime.now()
        
        # 1. Kiểm tra Master để lấy thông tin Control Level & Approver
        master_query = "SELECT ControlLevel, DefaultApprover FROM dbo.BUDGET_MASTER WHERE BudgetCode = ?"
        master_data = self.db.get_data(master_query, (budget_code,))
        if not master_data:
            return {'success': False, 'message': 'Mã ngân sách không tồn tại.'}
        
        control_level = master_data[0]['ControlLevel']
        default_approver = master_data[0]['DefaultApprover']

        # 2. Kiểm tra số dư ngân sách (Đã sửa lỗi thiếu tham số tại đây)
        status = self.get_budget_status(budget_code, dept_code, now.month, now.year)
        
        if amount > status['Remaining']:
            if control_level == 'HARD':
                return {'success': False, 'message': f"Bị chặn: Vượt ngân sách khả dụng ({status['Remaining']:,.0f})."}
            else:
                # Soft block: Chỉ cảnh báo
                reason = f"[CẢNH BÁO VƯỢT NS] {reason}"

        # 3. Xác định người duyệt
        approver = default_approver
        if not approver:
            user_query = f"SELECT [CAP TREN] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
            user_data = self.db.get_data(user_query, (user_code,))
            approver = user_data[0]['CAP TREN'] if user_data else 'ADMIN'

        # 4. Lưu vào DB (Đã thêm cột ObjectID)
        req_id = f"REQ-{now.strftime('%y%m')}-{int(datetime.now().timestamp())}"
        
        insert_query = """
            INSERT INTO dbo.EXPENSE_REQUEST 
            (RequestID, UserCode, DepartmentCode, BudgetCode, Amount, Reason, CurrentApprover, Status, ObjectID)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'PENDING', ?)
        """
        
        if self.db.execute_non_query(insert_query, (req_id, user_code, dept_code, budget_code, amount, reason, approver, object_id)):
            return {'success': True, 'message': 'Đã gửi đề nghị thành công.', 'request_id': req_id}
            
        return {'success': False, 'message': 'Lỗi CSDL khi lưu đề nghị.'}

    # -------------------------------------------------------------------------
    # [QUAN TRỌNG] HÀM ĐÃ SỬA LOGIC PHÂN QUYỀN DUYỆT
    # -------------------------------------------------------------------------
    def get_requests_for_approval(self, approver_code, user_role=''):
        """
        Lấy danh sách phiếu chờ duyệt.
        - ADMIN/GM: Xem tất cả phiếu PENDING.
        - User thường: Chỉ xem phiếu gán cho mình (CurrentApprover).
        """
        
        query_params = []
        
        # 1. Xác định điều kiện lọc dựa trên Role
        role_check = str(user_role).strip().upper()
        
        if role_check in ['ADMIN', 'GM']:
            # Admin thấy hết
            where_clause = "R.Status = 'PENDING'"
        else:
            # User thường chỉ thấy phiếu của mình
            where_clause = "R.CurrentApprover = ? AND R.Status = 'PENDING'"
            query_params.append(approver_code)

        # 2. Câu truy vấn
        query = f"""
            SELECT 
                R.*, 
                M.BudgetName, 
                U.SHORTNAME as RequesterName,
                U2.SHORTNAME as CurrentApproverName -- Lấy thêm tên người đang giữ phiếu
            FROM dbo.EXPENSE_REQUEST R
            LEFT JOIN dbo.BUDGET_MASTER M ON R.BudgetCode = M.BudgetCode
            LEFT JOIN [GD - NGUOI DUNG] U ON R.UserCode = U.USERCODE
            LEFT JOIN [GD - NGUOI DUNG] U2 ON R.CurrentApprover = U2.USERCODE
            WHERE {where_clause}
            ORDER BY R.RequestDate DESC
        """
        
        requests = self.db.get_data(query, tuple(query_params))
        
        # 3. Tính toán thông tin bổ sung (Cảnh báo ngân sách)
        for req in requests:
            req['Amount'] = safe_float(req.get('Amount'))
            # Kiểm tra ngân sách toàn công ty để cảnh báo sếp
            check = self.check_budget_company_wide(req['BudgetCode'], req['Amount'])
            
            req['GroupRemaining'] = check.get('group_remaining', 0)
            # Nếu số tiền xin > số dư còn lại -> Cảnh báo
            req['IsWarning'] = req['Amount'] > check.get('group_remaining', 0)
            
        return requests

    def approve_request(self, request_id, approver_code, action, note):
        """Xử lý Duyệt (APPROVED) hoặc Từ chối (REJECTED)."""
        new_status = 'APPROVED' if action == 'APPROVE' else 'REJECTED'
        
        # [FIX QUAN TRỌNG]:
        # Bỏ điều kiện "AND CurrentApprover = ?" để Admin có thể duyệt thay người khác.
        # Cập nhật luôn cột CurrentApprover thành người thực sự đã bấm nút duyệt (để lưu vết).
        
        query = """
            UPDATE dbo.EXPENSE_REQUEST
            SET Status = ?, 
                ApprovalDate = GETDATE(), 
                ApprovalNote = ?,
                CurrentApprover = ? -- Ghi đè người duyệt thực tế vào đây
            WHERE RequestID = ? AND Status = 'PENDING'
        """
        
        # Thứ tự tham số: new_status, note, approver_code (người duyệt thực tế), request_id
        return self.db.execute_non_query(query, (new_status, note, approver_code, request_id))
    def get_request_detail_for_print(self, request_id):
        """Lấy chi tiết phiếu để in."""
        query = """
            SELECT R.*, M.BudgetName, 
                   U1.SHORTNAME AS RequesterName, U1.[BO PHAN] AS RequesterDept,
                   U2.SHORTNAME AS ApproverName
            FROM dbo.EXPENSE_REQUEST R
            LEFT JOIN dbo.BUDGET_MASTER M ON R.BudgetCode = M.BudgetCode
            LEFT JOIN [GD - NGUOI DUNG] U1 ON R.UserCode = U1.USERCODE
            LEFT JOIN [GD - NGUOI DUNG] U2 ON R.CurrentApprover = U2.USERCODE
            WHERE R.RequestID = ?
        """
        data = self.db.get_data(query, (request_id,))
        return data[0] if data else None

    def get_approved_requests_for_payment(self):
        """Lấy danh sách phiếu ĐÃ DUYỆT chờ thanh toán."""
        query = """
            SELECT R.*, U.SHORTNAME as RequesterName 
            FROM dbo.EXPENSE_REQUEST R
            LEFT JOIN [GD - NGUOI DUNG] U ON R.UserCode = U.USERCODE
            WHERE R.Status = 'APPROVED' -- Chỉ lấy phiếu đã duyệt
            ORDER BY R.ApprovalDate ASC
        """
        return self.db.get_data(query)

    def process_payment(self, request_id, user_code, payment_ref, payment_date):
        """Xác nhận ĐÃ CHI (Chuyển trạng thái sang PAID)."""
        query = """
            UPDATE dbo.EXPENSE_REQUEST
            SET Status = 'PAID', 
                PaymentRef = ?, 
                PaymentDate = ?,
                PayerCode = ? -- Người thực hiện chi (Kế toán)
            WHERE RequestID = ? AND Status = 'APPROVED'
        """
        # Cần thêm cột PaymentRef, PaymentDate, PayerCode vào bảng nếu chưa có
        return self.db.execute_non_query(query, (payment_ref, payment_date, user_code, request_id))