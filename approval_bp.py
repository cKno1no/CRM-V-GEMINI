from flask import current_app
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, current_app
# FIX: Chỉ import các helper từ utils.py (và get_user_ip nếu nó được chuyển từ app.py)
from utils import login_required, permission_required, get_user_ip, record_activity # Import thêm
from datetime import datetime, timedelta
from db_manager import safe_float # Cần cho format/validation
# LƯU Ý: Không import các service như approval_service, order_approval_service TỪ app ở đây.
import config


approval_bp = Blueprint('approval_bp', __name__)

# [HÀM HELPER CHUYỂN TỪ APP.PY]
# Hàm này cần được định nghĩa ở đây hoặc chuyển sang utils.py


@approval_bp.route('/quote_approval', methods=['GET', 'POST'])
@login_required
@permission_required('APPROVE_QUOTE')
def quote_approval_dashboard():
    """ROUTE: Dashboard Duyệt Chào Giá."""
    
    # FIX: Import Services Cần thiết Cục bộ
   
    approval_service  = current_app.approval_service
    task_service  = current_app.task_service

    user_code = session.get('user_code')
    today = datetime.now().strftime('%Y-%m-%d')
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    date_from_str = request.form.get('date_from') or request.args.get('date_from')
    date_to_str = request.form.get('date_to') or request.args.get('date_to')
    
    date_from_str = date_from_str or seven_days_ago
    date_to_str = date_to_str or today
        
    quotes_for_review = approval_service.get_quotes_for_approval(user_code, date_from_str, date_to_str)
    
    # [FIX] Lấy user_division từ session
    user_division = session.get('user_division') 

    salesman_list = []
    try:
        # Nếu user là Admin/GM thì division có thể là None để lấy tất cả (tùy logic của get_eligible_helpers)
        salesman_list = task_service.get_eligible_helpers(division=user_division)
    except Exception as e:
        current_app.logger.error(f"Lỗi tải danh sách NVKD cho Quote Approval: {e}")

    return render_template(
        'quote_approval.html',
        quotes=quotes_for_review,
        current_user_code=user_code,
        date_from=date_from_str, 
        date_to=date_to_str,
        salesman_list=salesman_list
    )

@approval_bp.route('/sales_order_approval', methods=['GET', 'POST'])
@login_required
@permission_required('APPROVE_ORDER')
def sales_order_approval_dashboard():
    """ROUTE: Dashboard Duyệt Đơn hàng Bán."""
    
    # FIX: Import Services Cần thiết Cục bộ
    
    order_approval_service  = current_app.order_approval_service

    user_code = session.get('user_code')
    today = datetime.now().strftime('%Y-%m-%d')
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    date_from_str = request.form.get('date_from') or request.args.get('date_from')
    date_to_str = request.form.get('date_to') or request.args.get('date_to')
    
    date_from_str = date_from_str or seven_days_ago
    date_to_str = date_to_str or today
    # Lấy role từ session
    user_role = session.get('user_role', '')

    # Truyền đúng 4 tham số theo thứ tự
    orders_for_review = order_approval_service.get_orders_for_approval(
        user_code, 
        user_role,      # <--- Cần thêm tham số này vào vị trí số 2
        date_from_str, 
        date_to_str
    )
    
    return render_template(
        'sales_order_approval.html', 
        orders=orders_for_review,
        current_user_code=user_code,
        date_from=date_from_str,
        date_to=date_to_str
    )

@approval_bp.route('/quick_approval', methods=['GET'])
@login_required
@permission_required('VIEW_QUICK_APPROVAL')
def quick_approval_form():
    """ROUTE: Form Phê duyệt Nhanh (Ghi đè) cho Giám đốc."""
    
    # FIX: Import Services Cần thiết Cục bộ
     # ADD db_manager
    db_manager  = current_app.db_manager
    approval_service  = current_app.approval_service 
    order_approval_service = current_app.order_approval_service 


    user_role = session.get('user_role', '').strip().upper()
    user_code = session.get('user_code')
    
    # [CONFIG]: Check quyền vào trang duyệt nhanh
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM]:
        flash("Bạn không có quyền truy cập chức năng này.", 'danger')
        return redirect(url_for('index'))

    today = datetime.now().strftime('%Y-%m-%d')
    ninety_days_ago = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

    pending_quotes = approval_service.get_quotes_for_approval(
        user_code, ninety_days_ago, today
    )
    
    # [FIX] Thêm tham số user_role vào đây
    orders = order_approval_service.get_orders_for_approval(
        user_code, 
        user_role, # <--- BỔ SUNG THAM SỐ NÀY
        ninety_days_ago, 
        today
    )
    
    # LOG VIEW_QUICK_APPROVAL (BỔ SUNG)
    try:
        db_manager.write_audit_log(
            user_code=user_code,
            action_type='VIEW_QUICK_APPROVAL',
            severity='WARNING', 
            details=f"Truy cập Form Duyệt Nhanh. Tải {len(pending_quotes)} BG, {len(orders)} ĐH.",
            ip_address=get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi ghi log VIEW_QUICK_APPROVAL: {e}")

    return render_template(
        'quick_approval_form.html', 
        quotes=pending_quotes, 
        orders=orders
    )

# [APIs]

@approval_bp.route('/api/approve_quote', methods=['POST'])
@login_required
@permission_required('APPROVE_QUOTE')
@record_activity('APPROVE_QUOTE') # <--- Gắn thẻ vào đây
def api_approve_quote():
    """API: Thực hiện duyệt Chào Giá."""
    
    # FIX: Import Services Cần thiết Cục bộ
    db_manager  = current_app.db_manager
    approval_service  = current_app.approval_service 
     # ADD db_manager
    
    data = request.json
    quotation_no = data.get('quotation_no')
    quotation_id = data.get('quotation_id')
    object_id = data.get('object_id')
    employee_id = data.get('employee_id')
    approval_ratio = data.get('approval_ratio')
    
    current_user_code = session.get('user_code')
    user_ip = get_user_ip() # Giả định get_user_ip đã được định nghĩa ở đây hoặc trong utils

    try:
        result = approval_service.approve_quotation(
            quotation_no=quotation_no,
            quotation_id=quotation_id,
            object_id=object_id,
            employee_id=employee_id,
            approval_ratio=approval_ratio,
            current_user=current_user_code
        )
        
        if result['success']:
            # GHI LOG APPROVE QUOTE (BỔ SUNG)
            db_manager.write_audit_log(
                user_code=current_user_code,
                action_type='APPROVE_QUOTE',
                severity='CRITICAL',
                details=f"Duyệt Báo giá: {quotation_no} (ID: {quotation_id})",
                ip_address=user_ip
            )
            return jsonify({'success': True, 'message': result['message']})
        else:
            return jsonify({'success': False, 'message': result['message']}), 400
            
    except Exception as e:
        error_msg = f"Lỗi SQL/Nghiệp vụ: {str(e)}"
        return jsonify({'success': False, 'message': error_msg}), 400

@approval_bp.route('/api/approve_order', methods=['POST'])
@login_required
@permission_required('APPROVE_ORDER')
@record_activity('APPROVE_ORDER')
def api_approve_order():
    """API: Thực hiện duyệt Đơn hàng Bán."""
    
    order_approval_service = current_app.order_approval_service
    db_manager = current_app.db_manager
    
    data = request.json
    order_id = data.get('order_id')       # DDH/2026/01/054 (Mã hiển thị)
    sorder_id = data.get('sorder_id')     # SO20260000000120 (Primary Key QUAN TRỌNG)
    client_id = data.get('client_id')     
    salesman_id = data.get('salesman_id')   
    approval_ratio = data.get('approval_ratio')
    
    current_user_code = session.get('user_code')
    user_ip = get_user_ip()
    
    # Kiểm tra kỹ SOrderID
    if not current_user_code or not sorder_id:
        return jsonify({'success': False, 'message': 'Lỗi dữ liệu: Thiếu SOrderID hệ thống.'}), 400

    try:
        result = order_approval_service.approve_sales_order(
            sorder_no=order_id,        # Vẫn truyền để log hiển thị
            sorder_id=sorder_id,       # <--- Service sẽ dùng cái này để Query/Update
            object_id=client_id,       
            employee_id=salesman_id,   
            approval_ratio=approval_ratio,
            current_user=current_user_code
        )
        
        if result['success']:
            db_manager.write_audit_log(
                user_code=current_user_code,
                action_type='APPROVE_ORDER',
                severity='INFO',
                details=f"Duyệt Đơn hàng ID: {sorder_id} ({order_id})",
                ip_address=user_ip
            )
            return jsonify({'success': True, 'message': result['message']})
        else:
            return jsonify({'success': False, 'message': result['message']}), 400

    except Exception as e:
        current_app.logger.error(f"LỖI HỆ THỐNG API DUYỆT DHB: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

# API Lấy chi tiết báo giá (SỬA LỖI: Dùng <path:quote_id>)
@approval_bp.route('/api/get_quote_details/<path:quote_id>', methods=['GET'])
@login_required
def api_get_quote_details(quote_id):
    """API: Trả về chi tiết báo giá (mặt hàng)."""
    
    approval_service  = current_app.approval_service
    
    try:
        details = approval_service.get_quote_details(quote_id)
        return jsonify(details)
    except Exception as e:
        current_app.logger.error(f"Lỗi API lấy chi tiết báo giá {quote_id}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết.'}), 500

# API LẤY CHI TIẾT COST OVERRIDE (API BỊ THIẾU GÂY LỖI 404)
@approval_bp.route('/api/get_quote_cost_details/<path:quote_id>', methods=['GET'])
@login_required
def api_get_quote_cost_details(quote_id):
    """API: Trả về chi tiết các mặt hàng cần bổ sung Cost Override."""
    
    approval_service  = current_app.approval_service
    
    try:
        # Giả định hàm service này tồn tại
        details = approval_service.get_quote_cost_override_details(quote_id)
        return jsonify(details)
    except Exception as e:
        current_app.logger.error(f"Lỗi API lấy chi tiết Cost Override {quote_id}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết Cost Override.'}), 500

@approval_bp.route('/api/get_order_details/<string:sorder_id>', methods=['GET'])
@login_required
def api_get_order_details(sorder_id):
    """API: Trả về chi tiết Đơn hàng Bán (mặt hàng)."""
    
    # FIX: Import Services Cần thiết Cục bộ
    order_approval_service  = current_app.order_approval_service
    
    try:
        details = order_approval_service.get_order_details(sorder_id)
        return jsonify(details)
    except Exception as e:
        current_app.logger.error(f"Lỗi API lấy chi tiết DHB {sorder_id}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết.'}), 500

@approval_bp.route('/api/quote/update_salesman', methods=['POST'])
@login_required
@permission_required('CHANGE_QUOTE_SALESMAN')
def api_update_quote_salesman():
    """API: Cập nhật NVKD (SalesManID) cho một Chào giá."""
    
    approval_service = current_app.approval_service 
    
    data = request.json
    quotation_id = data.get('quotation_id')
    new_salesman_id = data.get('new_salesman_id')
    
    # [QUAN TRỌNG] Lấy user code hiện tại để check quyền duyệt lại
    current_user_code = session.get('user_code')
    
    if not quotation_id or not new_salesman_id:
        return jsonify({'success': False, 'message': 'Thiếu QuotationID hoặc NVKD mới.'}), 400
        
    try:
        # 1. Update DB
        update_result = approval_service.update_quote_salesman(quotation_id, new_salesman_id)
        
        if update_result['success']:
            # 2. [MỚI] Lấy dữ liệu refresh giao diện
            refresh_data = approval_service.get_quote_refresh_data(quotation_id, current_user_code)
            
            return jsonify({
                'success': True, 
                'message': update_result['message'],
                'data': refresh_data # Trả về NVKD mới và Status mới
            })
        else:
            return jsonify({'success': False, 'message': update_result['message']}), 500
            
    except Exception as e:
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

# === API BỊ THIẾU GÂY LỖI 404 NÀY ===
@approval_bp.route('/api/save_quote_cost_override', methods=['POST'])
@login_required
@permission_required('OVERRIDE_QUOTE_COST')
def api_save_quote_cost_override():
    """API: Thực hiện lưu giá Cost override vào CSDL và tính toán lại ratio."""
    
    approval_service  = current_app.approval_service
    
    data = request.json
    quote_id = data.get('quote_id')
    updates = data.get('updates') # Danh sách các transaction_id và cost/note
    
    if not quote_id or not updates:
        return jsonify({'success': False, 'message': 'Thiếu QuotationID hoặc danh sách cập nhật.'}), 400
        
    try:
        result = approval_service.upsert_cost_override(quote_id, updates, session.get('user_code'))
        
        if result['success']:
            # Sau khi lưu thành công, tính toán lại tỷ số duyệt trong service (giả định)
            # approval_service.recalculate_ratio(quote_id) 
            return jsonify({'success': True, 'message': 'Cập nhật Cost Override thành công. Tỷ số duyệt sẽ được tính lại.'})
        else:
            return jsonify({'success': False, 'message': result['message']}), 500
            
    except Exception as e:
        current_app.logger.error(f"LỖI API LƯU COST OVERRIDE: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500
# ==================================
@approval_bp.route('/quote_input_table', methods=['GET', 'POST'])
@login_required
def quote_input_table():
    """ROUTE: Bảng nhập liệu Bám sát Báo giá (Action Plan)."""
    
    # Import Service xử lý logic khách hàng
     
    customer_service   = current_app.customer_service

    user_code = session.get('user_code')
    
    # 1. Xử lý bộ lọc ngày (Mặc định 30 ngày)
    today = datetime.now()
    default_from = (today - timedelta(days=30)).strftime('%Y-%m-%d')
    default_to = today.strftime('%Y-%m-%d')
    
    date_from = request.form.get('date_from') or request.args.get('date_from') or default_from
    date_to = request.form.get('date_to') or request.args.get('date_to') or default_to
    
    # 2. Gọi hàm lấy dữ liệu từ CustomerService
    quotes = customer_service.get_quotes_for_input(user_code, date_from, date_to)
    
    # 3. Định nghĩa danh sách cho Dropdown (Status & Action)
    quote_statuses = [
        ('CHỜ', 'CHỜ - Đang theo dõi'),
        ('WIN', 'WIN - Thắng'),
        ('LOST', 'LOST - Thua'),
        ('CANCEL', 'CANCEL - Hủy'),
        ('DELAY', 'DELAY - Trì hoãn')
    ]
    
    actions = [
        ('', '-- Chọn hành động --'),
        ('GOI_DIEN', 'Gọi điện'),
        ('GUI_MAIL', 'Gửi Email'),
        ('GAP_MAT', 'Gặp mặt'),
        ('ZALO', 'Nhắn Zalo'),
        ('DEMO', 'Gửi mẫu/Demo'),
        ('THUONG_LUONG', 'Thương lượng giá')
    ]
    
    return render_template(
        'quote_table_input.html',
        quotes=quotes,
        date_from=date_from,
        date_to=date_to,
        quote_statuses=quote_statuses,
        actions=actions
    )


@approval_bp.route('/api/update_quote_status', methods=['POST'])
@login_required
def api_update_quote_status():
    """API: Cập nhật trạng thái/hành động cho Báo giá (Upsert)."""
    db_manager = current_app.db_manager
    import config
    
    data = request.json
    quote_id = data.get('quote_id')
    
    if not quote_id:
        return jsonify({'success': False, 'message': 'Thiếu Quote ID'}), 400
        
    # Lấy các trường dữ liệu
    status_code = data.get('status_code')
    loss_reason = data.get('loss_reason')
    action_1 = data.get('action_1')
    action_2 = data.get('action_2')
    
    # Xử lý ngày giờ (nếu rỗng gửi None)
    time_start = data.get('time_start') or None
    time_complete = data.get('time_complete') or None
    
    user_code = session.get('user_code')

    # Query UPSERT (Nếu tồn tại thì Update, chưa thì Insert)
    # Lưu ý: Cần đảm bảo bảng HD_CAP NHAT BAO GIA có khóa chính là (MA_BAO_GIA, NGAY_CAP_NHAT) hoặc xử lý logic phù hợp.
    # Ở đây dùng logic đơn giản: INSERT dòng mới nhất với ngày hiện tại (Lịch sử)
    
    query = f"""
        MERGE {config.TEN_BANG_CAP_NHAT_BG} AS TARGET
        USING (SELECT ? AS MA_BAO_GIA) AS SOURCE
        ON (TARGET.MA_BAO_GIA = SOURCE.MA_BAO_GIA AND CAST(TARGET.NGAY_CAP_NHAT AS DATE) = CAST(GETDATE() AS DATE))
        WHEN MATCHED THEN
            UPDATE SET 
                TINH_TRANG_BG = ?, 
                LY_DO_THUA = ?, 
                MA_HANH_DONG_1 = ?, 
                MA_HANH_DONG_2 = ?,
                THOI_GIAN_PHAT_SINH = ?,
                THOI_GIAN_HOAN_TAT = ?,
                NGUOI_CAP_NHAT = ?,
                NGAY_CAP_NHAT = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (MA_BAO_GIA, TINH_TRANG_BG, LY_DO_THUA, MA_HANH_DONG_1, MA_HANH_DONG_2, THOI_GIAN_PHAT_SINH, THOI_GIAN_HOAN_TAT, NGUOI_CAP_NHAT, NGAY_CAP_NHAT)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE());
    """
    
    # Tham số cho MERGE (Lặp lại 2 lần cho Update và Insert)
    params = (
        quote_id, 
        status_code, loss_reason, action_1, action_2, time_start, time_complete, user_code, # Update
        quote_id, status_code, loss_reason, action_1, action_2, time_start, time_complete, user_code  # Insert
    )
    
    try:
        if db_manager.execute_non_query(query, params):
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'message': 'Lỗi SQL'}), 500
    except Exception as e:
        current_app.logger.error(f"Lỗi update quote status: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500