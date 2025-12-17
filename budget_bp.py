# blueprints/budget_bp.py

from flask import current_app
from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, current_app
from utils import login_required, permission_required # Import thêm
from datetime import datetime, timedelta
import os
from werkzeug.utils import secure_filename
import config
budget_bp = Blueprint('budget_bp', __name__)

# Helper lưu file (Tái sử dụng logic từ app.py hoặc định nghĩa tại đây)

def get_user_ip():
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

def save_budget_files(files):
    if not files: return None
    saved_filenames = []
    upload_path = config.UPLOAD_FOLDER_PATH
    if not os.path.exists(upload_path): os.makedirs(upload_path)
    
    now_str = datetime.now().strftime("%Y%m%d%H%M%S")
    for file in files:
        if file and file.filename:
            filename = secure_filename(file.filename)
            unique_filename = f"BUD_{now_str}_{filename}"
            try:
                file.save(os.path.join(upload_path, unique_filename))
                saved_filenames.append(unique_filename)
            except Exception as e:
                current_app.logger.error(f"Save file error: {e}")
    return ";".join(saved_filenames) if saved_filenames else None

@budget_bp.route('/budget/dashboard', methods=['GET'])
@login_required
@permission_required('VIEW_BUDGET') # Áp dụng quyền mới
def budget_dashboard():
    """Giao diện chính: Xem ngân sách & Tạo đề nghị."""
    budget_service  = current_app.budget_service # Import cục bộ tránh vòng lặp
    db_manager = current_app.db_manager

    user_code = session.get('user_code')
    dept_code = session.get('bo_phan', 'KD') 
    
    # Lấy danh sách mã chi phí (Đã sắp xếp theo Tên để dễ tìm)
    budget_codes = db_manager.get_data("SELECT BudgetCode, BudgetName FROM dbo.BUDGET_MASTER WHERE IsActive=1 ORDER BY BudgetName")
    
    # Lấy lịch sử đề nghị của user này
    import config # Đảm bảo đã import config
    
    query_history = f"""
        SELECT 
            R.*,
            B.BudgetName,
            ISNULL(O.ShortObjectName, O.ObjectName) AS ObjectName
        FROM dbo.EXPENSE_REQUEST R
        LEFT JOIN dbo.BUDGET_MASTER B ON R.BudgetCode = B.BudgetCode
        LEFT JOIN {config.ERP_IT1202} O ON R.ObjectID = O.ObjectID
        WHERE R.UserCode = ? 
        ORDER BY R.RequestDate DESC
    """
    my_requests = db_manager.get_data(query_history, (user_code,))
    # [MỚI] Ghi log truy cập
    try:
        current_app.db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='VIEW_BUDGET_DASHBOARD',
            severity='INFO', # Mức độ thông tin thường
            details='Truy cập màn hình Ngân sách & Lập đề nghị',
            ip_address=get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Log Error: {e}")

    return render_template('budget_dashboard.html', 
                           budget_codes=budget_codes, 
                           my_requests=my_requests,
                           dept_code=dept_code)

@budget_bp.route('/budget/approval', methods=['GET'])
@login_required
@permission_required('APPROVE_BUDGET') # Áp dụng quyền mới
def budget_approval():
    """Giao diện Duyệt cho Quản lý."""
    budget_service  = current_app.budget_service 
    user_code = session.get('user_code')
    
    # Lấy thêm Role
    user_role = session.get('user_role', '').strip().upper()
    
    # Truyền thêm user_role vào hàm
    pending_list = budget_service.get_requests_for_approval(user_code, user_role)
    
    return render_template('budget_approval.html', pending_list=pending_list)

# --- APIs ---

@budget_bp.route('/api/budget/objects/<string:search_term>', methods=['GET'])
@login_required
def api_search_objects(search_term):
    """API: Tra cứu Đối tượng (IT1202) cho đề nghị thanh toán."""
    db_manager = current_app.db_manager
    import config
    
    # Tìm kiếm theo Mã, Tên, hoặc Tên tắt
    query = f"""
        SELECT TOP 10 ObjectID, ObjectName, ShortObjectName 
        FROM {config.ERP_IT1202} 
        WHERE ObjectID LIKE ? OR ObjectName LIKE ? OR ShortObjectName LIKE ?
        ORDER BY ShortObjectName
    """
    search_pattern = f"%{search_term}%"
    data = db_manager.get_data(query, (search_pattern, search_pattern, search_pattern))
    
    results = []
    if data:
        for row in data:
            results.append({
                'id': row['ObjectID'],
                'name': row['ObjectName'],
                'short_name': row['ShortObjectName'] or row['ObjectName']
            })
    return jsonify(results)

@budget_bp.route('/api/budget/check_balance', methods=['POST'])
@login_required
def api_check_balance():
    """API: Kiểm tra số dư (Logic THÁNG) khi user tạo phiếu."""
    budget_service  = current_app.budget_service 
    data = request.json
    
    status = budget_service.get_budget_status(
        data.get('budget_code'), 
        session.get('bo_phan', 'KD'), 
        datetime.now().month, 
        datetime.now().year
    )
    return jsonify(status)

@budget_bp.route('/api/budget/submit_request', methods=['POST'])
@login_required
@permission_required('VIEW_BUDGET') # Áp dụng quyền mới
def api_submit_request():
    """
    [UPDATED] API: Gửi đề nghị thanh toán (Hỗ trợ File Upload).
    Lưu ý: Client phải gửi FormData thay vì JSON.
    """
    budget_service  = current_app.budget_service 
    
    # Lấy dữ liệu từ Form (do gửi kèm file nên không dùng request.json)
    budget_code = request.form.get('budget_code')
    amount = float(request.form.get('amount', 0))
    reason = request.form.get('reason')
    object_id = request.form.get('object_id')
    
    # Xử lý File
    files = request.files.getlist('attachments')
    attachments_str = save_budget_files(files)
    
    result = budget_service.create_expense_request(
        user_code=session.get('user_code'),
        dept_code=session.get('bo_phan', 'KD'),
        budget_code=budget_code,
        amount=amount,
        reason=reason,
        object_id=object_id,
        attachments=attachments_str # Truyền chuỗi tên file
    )
    # [BỔ SUNG LOG]
    if result.get('success'):
        try:
            current_app.db_manager.write_audit_log(
                user_code=session.get('user_code'),
                action_type='CREATE_EXPENSE_REQUEST',
                severity='INFO',
                details=f"Tạo đề nghị chi: {budget_code} - {amount:,.0f} - {result['request_id']}",
                ip_address=get_user_ip()
            )
        except Exception as e:
            current_app.logger.error(f"Log Error: {e}")

    return jsonify(result)

@budget_bp.route('/api/budget/approve', methods=['POST'])
@login_required
def api_approve_request():
    """API: Duyệt/Từ chối."""
    budget_service  = current_app.budget_service 
    data = request.json
    # --- [SỬA] KHAI BÁO BIẾN action RÕ RÀNG Ở ĐÂY ---
    request_id = data.get('request_id')
    action = data.get('action')  # <--- Dòng này quan trọng để fix lỗi NameError
    note = data.get('note')
    approver_code = session.get('user_code')

    # Gọi Service để xử lý logic duyệt
    success = current_app.budget_service.approve_request(request_id, approver_code, action, note)
    # [BỔ SUNG LOG]
    if success:
        log_action = 'APPROVE_EXPENSE' if action == 'APPROVE' else 'REJECT_EXPENSE'
        log_severity = 'CRITICAL' if action == 'APPROVE' else 'WARNING'
        try:
            current_app.db_manager.write_audit_log(
                user_code=session.get('user_code'),
                action_type=log_action,
                severity=log_severity,
                details=f"{action} đề nghị chi: {request_id}",
                ip_address=get_user_ip()
            )
        except Exception as e:
            current_app.logger.error(f"Log Error: {e}")

    return jsonify({'success': success})

@budget_bp.route('/budget/print/<string:request_id>', methods=['GET'])
@login_required
def print_request_voucher(request_id):
    """Trang in phiếu."""
    budget_service  = current_app.budget_service 
    req = budget_service.get_request_detail_for_print(request_id)
    if not req: return "Không tìm thấy", 404
    # Chỉ cho in nếu đã duyệt (Bảo mật quy trình)
    if req['Status'] != 'APPROVED': return "Phiếu chưa được duyệt, không thể in.", 403
    
    return render_template('print_expense_voucher.html', req=req)

@budget_bp.route('/budget/payment', methods=['GET'])
@login_required
@permission_required('EXECUTE_PAYMENT') # Áp dụng quyền mới
def payment_queue():
    """Giao diện Hàng đợi Thanh toán (Payment Queue) cho Kế toán."""
    budget_service  = current_app.budget_service 
    
    # 1. Xử lý khoảng thời gian (Mặc định 2 tuần)
    today = datetime.now().date()
    default_from = today - timedelta(days=14)
    
    from_date_str = request.args.get('from_date', default_from.strftime('%Y-%m-%d'))
    to_date_str = request.args.get('to_date', today.strftime('%Y-%m-%d'))
    
    try:
        from_date = datetime.strptime(from_date_str, '%Y-%m-%d').date()
        to_date = datetime.strptime(to_date_str, '%Y-%m-%d').date()
        
        # 2. Validate khoảng cách tối đa 60 ngày
        delta = to_date - from_date
        if delta.days > 60:
            flash("Khoảng thời gian tra cứu tối đa là 60 ngày. Đã tự động điều chỉnh.", "warning")
            to_date = from_date + timedelta(days=60)
            to_date_str = to_date.strftime('%Y-%m-%d')
            
        if from_date > to_date:
            flash("Ngày bắt đầu không thể lớn hơn ngày kết thúc.", "danger")
            from_date = default_from
            to_date = today
            from_date_str = from_date.strftime('%Y-%m-%d')
            to_date_str = to_date.strftime('%Y-%m-%d')

    except ValueError:
        from_date = default_from
        to_date = today
    
    # 3. Gọi Service
    # Lưu ý: Đổi tên hàm gọi nếu bạn đã đổi trong service (get_payment_queue)
    approved_list = budget_service.get_payment_queue(from_date, to_date)
    
    try:
        current_app.db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='VIEW_PAYMENT_QUEUE',
            severity='WARNING', # Mức WARNING để dễ lọc ra ai hay vào xem tiền
            details='Truy cập màn hình Thực chi (Payment Queue)',
            ip_address=get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Log Error: {e}")

    return render_template(
        'budget_payment_queue.html',
        approved_list=approved_list,
        from_date=from_date_str,
        to_date=to_date_str,
        current_date=today.strftime('%Y-%m-%d')
    )

@budget_bp.route('/api/budget/pay', methods=['POST'])
@login_required
def api_confirm_payment():
    """API: Xác nhận đã chi tiền."""
    budget_service  = current_app.budget_service 
    data = request.json
    success = budget_service.process_payment(
        data.get('request_id'),
        session.get('user_code'),
        data.get('payment_ref'),
        data.get('payment_date')
    )
    # [BỔ SUNG LOG]
    if success:
        try:
            current_app.db_manager.write_audit_log(
                user_code=session.get('user_code'),
                action_type='EXECUTE_PAYMENT',
                severity='CRITICAL',
                details=f"Xác nhận chi tiền {request_id} (Ref: {payment_ref})",
                ip_address=get_user_ip()
            )
        except Exception as e:
            current_app.logger.error(f"Log Error: {e}")

    return jsonify({'success': success})

# Trong blueprints/budget_bp.py

@budget_bp.route('/verify/request/<string:request_id>', methods=['GET'])
# KHÔNG CÓ @login_required Ở ĐÂY
def public_verify_request(request_id):
    """Trang xác thực công khai (Dành cho quét QR)."""
    db_manager = current_app.db_manager
    
    # Chỉ lấy các thông tin cơ bản để đối chiếu (Không lấy thông tin nhạy cảm quá sâu)
    query = """
        SELECT 
            R.RequestID, R.RequestDate, R.Amount, R.Reason, R.Status,
            U.SHORTNAME as RequesterName,
            M.BudgetName
        FROM dbo.EXPENSE_REQUEST R
        LEFT JOIN [GD - NGUOI DUNG] U ON R.UserCode = U.USERCODE
        LEFT JOIN dbo.BUDGET_MASTER M ON R.BudgetCode = M.BudgetCode
        WHERE R.RequestID = ?
    """
    data = db_manager.get_data(query, (request_id,))
    
    if not data:
        return render_template('verify_result.html', error="Không tìm thấy phiếu này trên hệ thống!")
        
    req = data[0]
    
    # Logic kiểm tra an toàn: Chỉ hiện nếu phiếu ĐÃ DUYỆT
    if req['Status'] != 'APPROVED' and req['Status'] != 'PAID':
         return render_template('verify_result.html', error="CẢNH BÁO: Phiếu này CHƯA ĐƯỢC DUYỆT!")
         
    return render_template('verify_result.html', req=req)

@budget_bp.route('/budget/report/ytd', methods=['GET'])
@login_required
@permission_required('VIEW_BUDGET_REPORT') # Áp dụng quyền mới
def budget_ytd_report():

    # --- 1. BẢO MẬT: CHẶN USER KHÔNG PHẢI ADMIN ---
    user_role = session.get('user_role', '').strip().upper()
    
    
    
    budget_service  = current_app.budget_service 
    
    current_year = datetime.now().year
    current_month = datetime.now().month # Lấy tháng hiện tại để hiển thị trên header
    
    dept_code = session.get('bo_phan', 'KD') # Mặc định bộ phận người dùng
    
    # Lấy tham số filter nếu có
    year_filter = request.args.get('year', current_year, type=int)
    
    # SỬA ĐỔI: Logic lấy dept_filter
    is_admin = session.get('user_role') == config.ROLE_ADMIN
    
    if is_admin:
        # Nếu là Admin và không có param dept trên URL, mặc định xem TOÀN CÔNG TY
        dept_filter = request.args.get('dept', 'ALL')
    else:
        # User thường chỉ xem được bộ phận của mình
        dept_filter = dept_code

    # [BỔ SUNG LOG]
    try:
        current_app.db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='VIEW_BUDGET_REPORT',
            severity='WARNING',
            details=f"Xem báo cáo ngân sách YTD",
            ip_address=get_user_ip()
        )
    except: pass
    # Lấy dữ liệu (đã được gom nhóm theo ReportGroup và chỉ trả về 1 dòng cho mỗi Group từ service)
    report_data = budget_service.get_ytd_budget_report(dept_filter, year_filter)
    
    # Vì service đã trả về danh sách phẳng các Group (final_report), ta không cần loop group nữa.
    # Tuy nhiên, để tương thích với template đang dùng cấu trúc grouped_report (dict), ta sẽ chuyển đổi một chút.
    # Hoặc tốt hơn: Truyền thẳng report_data vào template và sửa template để loop list thay vì dict.
    
    # Ở đây tôi sẽ giữ cấu trúc dict `grouped_report` để ít thay đổi template nhất có thể,
    # nhưng thực tế mỗi 'group' chỉ có 1 item chính nó.
    
    grouped_report = {}
    for row in report_data:
        group_name = row['GroupName']
        grouped_report[group_name] = row # Gán thẳng row vào dict
        
    # Tính tổng toàn công ty (Grand Total)
    grand_total = {
        'Month_Plan': sum(r['Month_Plan'] for r in report_data),
        'Month_Actual': sum(r['Month_Actual'] for r in report_data),
        'Month_Diff': sum(r['Month_Diff'] for r in report_data),
        'YTD_Plan': sum(r['YTD_Plan'] for r in report_data),
        'YTD_Actual': sum(r['YTD_Actual'] for r in report_data),
        'YTD_Diff': sum(r['YTD_Diff'] for r in report_data),
        'Year_Plan': sum(r['Year_Plan'] for r in report_data)
    }

    return render_template(
        'budget_ytd_report.html',
        grouped_report=grouped_report,
        grand_total=grand_total,
        current_year=current_year,
        current_month=current_month,
        year_filter=year_filter,
        dept_filter=dept_filter,
        is_admin=is_admin
    )

@budget_bp.route('/api/budget/group_details', methods=['GET'])
@login_required
def api_get_group_details():
    """API: Lấy chi tiết phiếu chi theo ReportGroup."""
    budget_service  = current_app.budget_service 
    
    group_name = request.args.get('group_name')
    year = request.args.get('year', datetime.now().year, type=int)
    
    if not group_name:
        return jsonify({'error': 'Thiếu tên nhóm'}), 400
        
    try:
        details = budget_service.get_expense_details_by_group(group_name, year)
        return jsonify(details)
    except Exception as e:
        current_app.logger.error(f"Lỗi lấy chi tiết nhóm {group_name}: {e}")
        return jsonify({'error': str(e)}), 500