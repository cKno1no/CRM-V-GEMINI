# blueprints/budget_bp.py

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash

from utils import login_required
from datetime import datetime

budget_bp = Blueprint('budget_bp', __name__)

@budget_bp.route('/budget/dashboard', methods=['GET'])
@login_required
def budget_dashboard():
    """Giao diện chính: Xem ngân sách & Tạo đề nghị."""
    from app import budget_service, db_manager # Import cục bộ tránh vòng lặp
    
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
    
    return render_template('budget_dashboard.html', 
                           budget_codes=budget_codes, 
                           my_requests=my_requests,
                           dept_code=dept_code)

@budget_bp.route('/budget/approval', methods=['GET'])
@login_required
def budget_approval():
    """Giao diện Duyệt cho Quản lý."""
    from app import budget_service
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
    from app import db_manager
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
    """API: Kiểm tra số dư khi user chọn mã chi phí."""
    from app import budget_service
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
def api_submit_request():
    """API: Gửi đề nghị thanh toán."""
    from app import budget_service
    data = request.json
    
    result = budget_service.create_expense_request(
        user_code=session.get('user_code'),
        dept_code=session.get('bo_phan', 'KD'),
        budget_code=data.get('budget_code'),
        amount=float(data.get('amount')),
        reason=data.get('reason'),
        object_id=data.get('object_id') # [UPDATE] Lấy thêm ObjectID
    )
    return jsonify(result)

@budget_bp.route('/api/budget/approve', methods=['POST'])
@login_required
def api_approve_request():
    """API: Duyệt/Từ chối."""
    from app import budget_service
    data = request.json
    success = budget_service.approve_request(
        data.get('request_id'),
        session.get('user_code'),
        data.get('action'), 
        data.get('note')
    )
    return jsonify({'success': success})

@budget_bp.route('/budget/print/<string:request_id>', methods=['GET'])
@login_required
def print_request_voucher(request_id):
    """Trang in phiếu."""
    from app import budget_service
    req = budget_service.get_request_detail_for_print(request_id)
    if not req: return "Không tìm thấy", 404
    # Chỉ cho in nếu đã duyệt (Bảo mật quy trình)
    if req['Status'] != 'APPROVED': return "Phiếu chưa được duyệt, không thể in.", 403
    
    return render_template('print_expense_voucher.html', req=req)

@budget_bp.route('/budget/payment', methods=['GET'])
@login_required
def budget_payment_queue():
    """Giao diện Hàng đợi Thanh toán (Chỉ dành cho Admin & Kế toán trưởng)."""
    from app import budget_service
    
    # 1. Lấy thông tin quyền hạn từ Session
    user_role = session.get('user_role', '').strip().upper()
    
    # Lấy chức vụ (vừa thêm ở Bước 1)
    user_chuc_vu = session.get('chuc_vu', '').strip().upper()
    
    # 2. KIỂM TRA QUYỀN (LOGIC MỚI)
    # Chỉ cho phép nếu là ADMIN hoặc Chức vụ là 'KT TRUONG'
    if user_role != 'ADMIN' and user_chuc_vu != 'KT TRUONG':
        flash("Bạn không có quyền truy cập vào trang thực hiện thanh toán.", "danger")
        return redirect(url_for('budget_bp.budget_dashboard'))
        
    # 3. Nếu đúng quyền, lấy dữ liệu và hiển thị
    approved_list = budget_service.get_approved_requests_for_payment()
    
    return render_template('budget_payment_queue.html', 
                           approved_list=approved_list, 
                           pending_count=len(approved_list),
                           now=datetime.now())

@budget_bp.route('/api/budget/pay', methods=['POST'])
@login_required
def api_confirm_payment():
    """API: Xác nhận đã chi tiền."""
    from app import budget_service
    data = request.json
    success = budget_service.process_payment(
        data.get('request_id'),
        session.get('user_code'),
        data.get('payment_ref'),
        data.get('payment_date')
    )
    return jsonify({'success': success})

# Trong blueprints/budget_bp.py

@budget_bp.route('/verify/request/<string:request_id>', methods=['GET'])
# KHÔNG CÓ @login_required Ở ĐÂY
def public_verify_request(request_id):
    """Trang xác thực công khai (Dành cho quét QR)."""
    from app import db_manager
    
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
def budget_ytd_report():

    # --- 1. BẢO MẬT: CHẶN USER KHÔNG PHẢI ADMIN ---
    user_role = session.get('user_role', '').strip().upper()
    
    if user_role != 'ADMIN':
        flash("Bạn không có quyền truy cập Báo cáo này. Vui lòng liên hệ Admin.", "danger")
        return redirect(url_for('budget_bp.budget_dashboard'))
    """Báo cáo so sánh Ngân sách vs Thực tế (YTD)."""
    
    from app import budget_service
    
    current_year = datetime.now().year
    current_month = datetime.now().month # Lấy tháng hiện tại để hiển thị trên header
    
    dept_code = session.get('bo_phan', 'KD') # Mặc định bộ phận người dùng
    
    # Lấy tham số filter nếu có
    year_filter = request.args.get('year', current_year, type=int)
    
    # SỬA ĐỔI: Logic lấy dept_filter
    is_admin = session.get('user_role') == 'ADMIN'
    
    if is_admin:
        # Nếu là Admin và không có param dept trên URL, mặc định xem TOÀN CÔNG TY
        dept_filter = request.args.get('dept', 'ALL')
    else:
        # User thường chỉ xem được bộ phận của mình
        dept_filter = dept_code

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
    from app import budget_service
    
    group_name = request.args.get('group_name')
    year = request.args.get('year', datetime.now().year, type=int)
    
    if not group_name:
        return jsonify({'error': 'Thiếu tên nhóm'}), 400
        
    try:
        details = budget_service.get_expense_details_by_group(group_name, year)
        return jsonify(details)
    except Exception as e:
        print(f"Lỗi lấy chi tiết nhóm {group_name}: {e}")
        return jsonify({'error': str(e)}), 500