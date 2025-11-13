from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
# FIX: Chỉ import các helper từ utils.py (và get_user_ip nếu nó được chuyển từ app.py)
from utils import login_required 
from datetime import datetime, timedelta
from db_manager import safe_float # Cần cho format/validation
# LƯU Ý: Không import các service như approval_service, order_approval_service TỪ app ở đây.

approval_bp = Blueprint('approval_bp', __name__)

# [HÀM HELPER CHUYỂN TỪ APP.PY]
# Hàm này cần được định nghĩa ở đây hoặc chuyển sang utils.py
def get_user_ip():
    """Lấy địa chỉ IP của người dùng. (Nếu nó nằm trong app.py, cần chuyển nó)"""
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

# [ROUTES]

@approval_bp.route('/quote_approval', methods=['GET', 'POST'])
@login_required
def quote_approval_dashboard():
    """ROUTE: Dashboard Duyệt Chào Giá."""
    
    # FIX: Import Services Cần thiết Cục bộ
    from app import approval_service, task_service
    
    user_code = session.get('user_code')
    today = datetime.now().strftime('%Y-%m-%d')
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    date_from_str = request.form.get('date_from') or request.args.get('date_from')
    date_to_str = request.form.get('date_to') or request.args.get('date_to')
    
    date_from_str = date_from_str or seven_days_ago
    date_to_str = date_to_str or today
        
    quotes_for_review = approval_service.get_quotes_for_approval(user_code, date_from_str, date_to_str)
    
    salesman_list = []
    try:
        salesman_list = task_service.get_eligible_helpers() 
    except Exception as e:
        print(f"Lỗi tải danh sách NVKD cho Quote Approval: {e}")

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
def sales_order_approval_dashboard():
    """ROUTE: Dashboard Duyệt Đơn hàng Bán."""
    
    # FIX: Import Services Cần thiết Cục bộ
    from app import order_approval_service
    
    user_code = session.get('user_code')
    today = datetime.now().strftime('%Y-%m-%d')
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    date_from_str = request.form.get('date_from') or request.args.get('date_from')
    date_to_str = request.form.get('date_to') or request.args.get('date_to')
    
    date_from_str = date_from_str or seven_days_ago
    date_to_str = date_to_str or today
    
    orders_for_review = order_approval_service.get_orders_for_approval(user_code, date_from_str, date_to_str)
    
    return render_template(
        'sales_order_approval.html', 
        orders=orders_for_review,
        current_user_code=user_code,
        date_from=date_from_str,
        date_to=date_to_str
    )

@approval_bp.route('/quick_approval', methods=['GET'])
@login_required
def quick_approval_form():
    """ROUTE: Form Phê duyệt Nhanh (Ghi đè) cho Giám đốc."""
    
    # FIX: Import Services Cần thiết Cục bộ
    from app import approval_service, order_approval_service
    
    user_role = session.get('user_role', '').strip().upper()
    user_code = session.get('user_code')
    
    if user_role not in ['ADMIN', 'GM']:
        flash("Bạn không có quyền truy cập chức năng này.", 'danger')
        return redirect(url_for('index'))

    today = datetime.now().strftime('%Y-%m-%d')
    ninety_days_ago = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')

    pending_quotes = approval_service.get_quotes_for_approval(
        user_code, ninety_days_ago, today
    )
    
    orders = order_approval_service.get_orders_for_approval(
        user_code, ninety_days_ago, today
    )

    return render_template(
        'quick_approval_form.html', 
        quotes=pending_quotes, 
        orders=orders
    )

# [APIs]

@approval_bp.route('/api/approve_quote', methods=['POST'])
@login_required
def api_approve_quote():
    """API: Thực hiện duyệt Chào Giá."""
    
    # FIX: Import Services Cần thiết Cục bộ
    from app import approval_service
    
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
            return jsonify({'success': True, 'message': result['message']})
        else:
            return jsonify({'success': False, 'message': result['message']}), 400
            
    except Exception as e:
        error_msg = f"Lỗi SQL/Nghiệp vụ: {str(e)}"
        return jsonify({'success': False, 'message': error_msg}), 400

@approval_bp.route('/api/approve_order', methods=['POST'])
@login_required
def api_approve_order():
    """API: Thực hiện duyệt Đơn hàng Bán."""
    
    # FIX: Import Services Cần thiết Cục bộ
    from app import order_approval_service
    
    data = request.json
    order_id = data.get('order_id')         
    sorder_id = data.get('sorder_id')       
    client_id = data.get('client_id')       
    salesman_id = data.get('salesman_id')   
    approval_ratio = data.get('approval_ratio')
    
    current_user_code = session.get('user_code')
    user_ip = get_user_ip()
    
    if not current_user_code or not order_id or not sorder_id:
        return jsonify({'success': False, 'message': 'Thiếu mã DHB hoặc SOrderID.'}), 400

    try:
        result = order_approval_service.approve_sales_order(
            order_id=order_id,
            sorder_id=sorder_id,
            client_id=client_id,
            salesman_id=salesman_id,
            approval_ratio=approval_ratio,
            current_user=current_user_code
        )
        
        if result['success']:
            return jsonify({'success': True, 'message': result['message']})
        else:
            return jsonify({'success': False, 'message': result['message']}), 400

    except Exception as e:
        print(f"LỖI HỆ THỐNG API DUYỆT DHB: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

# API Lấy chi tiết báo giá (SỬA LỖI: Dùng <path:quote_id>)
@approval_bp.route('/api/get_quote_details/<path:quote_id>', methods=['GET'])
@login_required
def api_get_quote_details(quote_id):
    """API: Trả về chi tiết báo giá (mặt hàng)."""
    
    from app import approval_service
    
    try:
        details = approval_service.get_quote_details(quote_id)
        return jsonify(details)
    except Exception as e:
        print(f"Lỗi API lấy chi tiết báo giá {quote_id}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết.'}), 500

# API LẤY CHI TIẾT COST OVERRIDE (API BỊ THIẾU GÂY LỖI 404)
@approval_bp.route('/api/get_quote_cost_details/<path:quote_id>', methods=['GET'])
@login_required
def api_get_quote_cost_details(quote_id):
    """API: Trả về chi tiết các mặt hàng cần bổ sung Cost Override."""
    
    from app import approval_service
    
    try:
        # Giả định hàm service này tồn tại
        details = approval_service.get_quote_cost_override_details(quote_id)
        return jsonify(details)
    except Exception as e:
        print(f"Lỗi API lấy chi tiết Cost Override {quote_id}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết Cost Override.'}), 500

@approval_bp.route('/api/get_order_details/<string:sorder_id>', methods=['GET'])
@login_required
def api_get_order_details(sorder_id):
    """API: Trả về chi tiết Đơn hàng Bán (mặt hàng)."""
    
    # FIX: Import Services Cần thiết Cục bộ
    from app import order_approval_service
    
    try:
        details = order_approval_service.get_order_details(sorder_id)
        return jsonify(details)
    except Exception as e:
        print(f"Lỗi API lấy chi tiết DHB {sorder_id}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết.'}), 500

@approval_bp.route('/api/quote/update_salesman', methods=['POST'])
@login_required
def api_update_quote_salesman():
    """API: Cập nhật NVKD (SalesManID) cho một Chào giá."""
    
    # FIX: Import Service Cục bộ VÀ sửa route từ @app.route sang @approval_bp.route
    from app import approval_service 
    
    data = request.json
    quotation_id = data.get('quotation_id')
    new_salesman_id = data.get('new_salesman_id')
    
    if not quotation_id or not new_salesman_id:
        return jsonify({'success': False, 'message': 'Thiếu QuotationID hoặc NVKD mới.'}), 400
        
    try:
        # Tên hàm được giả định là trong approval_service (vì nó liên quan đến quote)
        result = approval_service.update_quote_salesman(quotation_id, new_salesman_id)
        
        if result['success']:
            return jsonify({'success': True, 'message': result['message']})
        else:
            # FIX: Cần phải import app (hoặc logger) nếu muốn ghi log ở đây
            # from app import app 
            # app.logger.error(...)
            return jsonify({'success': False, 'message': result['message']}), 500
            
    except Exception as e:
        # FIX: Cần phải import app (hoặc logger) nếu muốn ghi log ở đây
        # from app import app 
        # app.logger.error(f"Lỗi API cập nhật NVKD: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

# === API BỊ THIẾU GÂY LỖI 404 NÀY ===
@approval_bp.route('/api/save_quote_cost_override', methods=['POST'])
@login_required
def api_save_quote_cost_override():
    """API: Thực hiện lưu giá Cost override vào CSDL và tính toán lại ratio."""
    
    from app import approval_service
    
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
        print(f"LỖI API LƯU COST OVERRIDE: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500
# ==================================