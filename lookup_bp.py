from flask import Blueprint, request, session, jsonify, render_template, redirect, url_for, flash, current_app
from utils import login_required, permission_required
from forms import SalesLookupForm  # <--- IMPORT FORM BẢO MẬT
import config 

# [HÀM HELPER CẦN THIẾT]
def get_user_ip():
    """Lấy địa chỉ IP của người dùng."""
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

lookup_bp = Blueprint('lookup_bp', __name__, url_prefix='/sales')

# [ROUTES]

@lookup_bp.route('/sales_lookup', methods=['GET', 'POST'])
@login_required
@permission_required('VIEW_SALES_LOOKUP')
def sales_lookup_dashboard():
    lookup_service = current_app.lookup_service
    db_manager = current_app.db_manager
    
    user_role = session.get('user_role', '').strip().upper()
    is_admin_or_gm = user_role in [config.ROLE_ADMIN, config.ROLE_GM]
    is_manager = user_role == 'MANAGER'
    
    show_block_3 = is_admin_or_gm
    show_block_2 = is_admin_or_gm or is_manager
    
    lookup_results = {}
    
    # 1. Khởi tạo Form
    form = SalesLookupForm(request.form if request.method == 'POST' else request.args)
    
    # Biến để giữ lại giá trị input khi render lại trang (tránh mất chữ user vừa gõ)
    input_item_search = ''
    input_object_id = ''

    if request.method == 'POST' and form.validate():
        # [SỬA] Lấy đúng tên trường item_search
        item_search = form.item_search.data 
        
        # Lấy object_id từ form (hoặc request.form nếu chưa đưa vào form)
        # Ưu tiên lấy từ form nếu có field object_id, nếu không lấy thủ công
        object_id = form.object_id.data if hasattr(form, 'object_id') else request.form.get('object_id', '').strip()
        
        # Cập nhật biến hiển thị lại
        input_item_search = item_search
        input_object_id = object_id

        if not item_search and not object_id:
             flash("Vui lòng nhập thông tin để tra cứu.", 'warning')
        else:
            # Gọi Service
            lookup_results = lookup_service.get_sales_lookup_data(
                item_search, object_id 
            )
            
            # LOG
            try:
                db_manager.write_audit_log(
                    user_code=session.get('user_code'),
                    action_type='API_SALES_LOOKUP',
                    severity='INFO',
                    details=f"Tra cứu: item='{item_search}', kh='{object_id}'",
                    ip_address=get_user_ip()
                )
            except Exception as e:
                current_app.logger.error(f"Lỗi ghi log API_SALES_LOOKUP: {e}")
    
    elif request.method == 'POST' and not form.validate():
        for field, errors in form.errors.items():
            for error in errors:
                flash(f"Lỗi nhập liệu [{field}]: {error}", 'danger')

    return render_template(
        'sales_lookup_dashboard.html', 
        form=form, 
        results=lookup_results,
        show_block_2=show_block_2,
        show_block_3=show_block_3,
        # [QUAN TRỌNG] Truyền lại giá trị user vừa nhập để ô input không bị trắng xóa
        item_search=input_item_search, 
        object_id=input_object_id,
        object_id_display=request.form.get('object_id_display', '')
    )

@lookup_bp.route('/total_replenishment', methods=['GET'])
@login_required
@permission_required('VIEW_TOTAL_REPLENISH')
def total_replenishment_dashboard():
    """ROUTE: Báo cáo Dự phòng Tồn kho Tổng thể."""
    db_manager = current_app.db_manager
    
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM]:
        flash("Bạn không có quyền truy cập chức năng này.", 'danger')
        return redirect(url_for('index'))

    try:
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='VIEW_TOTAL_REPLENISHMENT',
            severity='WARNING', 
            details="Truy cập Báo cáo Dự phòng Tồn kho Tổng thể",
            ip_address=get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi ghi log total_replenishment: {e}")

    try:
        sp_data = db_manager.execute_sp_multi(config.SP_REPLENISH_TOTAL, None)
        alert_list = sp_data[0] if sp_data else []
    except Exception as e:
        flash(f"Lỗi thực thi Stored Procedure: {e}", 'danger')
        alert_list = []
        
    return render_template('total_replenishment.html', alert_list=alert_list)

@lookup_bp.route('/export_total_replenishment', methods=['GET'])
@login_required
def export_total_replenishment():
    """ROUTE: Xuất Excel (Stub)."""
    db_manager = current_app.db_manager
    user_role = session.get('user_role', '').strip().upper()
    
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM]:
        flash("Bạn không có quyền truy cập.", 'danger')
        return redirect(url_for('index'))
    
    try:
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='EXPORT_REPLENISHMENT',
            severity='CRITICAL', 
            details="Xuất Excel Báo cáo Dự phòng Tổng thể",
            ip_address=get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi log export: {e}")
        
    flash("Chức năng Xuất Excel đang phát triển.", 'warning')
    return redirect(url_for('lookup_bp.total_replenishment_dashboard')) 

@lookup_bp.route('/customer_replenishment', methods=['GET'])
@login_required
@permission_required('VIEW_CUST_REPLENISH')
def customer_replenishment_dashboard():
    """ROUTE: Dự báo Dự phòng Khách hàng."""
    db_manager = current_app.db_manager
    
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM, config.ROLE_MANAGER]:
        flash("Bạn không có quyền truy cập.", 'danger')
        return redirect(url_for('index'))

    try:
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='VIEW_CUSTOMER_REPLENISHMENT',
            severity='WARNING', 
            details="Truy cập Dự báo Dự phòng Khách hàng",
            ip_address=get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi log customer_replenishment: {e}")

    return render_template('customer_replenishment.html')


# [APIs]

@lookup_bp.route('/api/khachhang/<string:ten_tat>', methods=['GET'])
@login_required 
def api_khachhang(ten_tat):
    """API tra cứu Khách hàng (Autocomplete)."""
    # Nên thêm validate đầu vào ở đây nếu cần thiết
    customer_service = current_app.customer_service
    data = customer_service.get_customer_by_name(ten_tat)
    return jsonify(data)

@lookup_bp.route('/api/multi_lookup', methods=['POST'])
@login_required 
def api_multi_lookup():
    """API: Tra cứu nhanh (Multi)."""
    lookup_service = current_app.lookup_service
    db_manager = current_app.db_manager
    
    # Lấy dữ liệu thô
    raw_search = request.form.get('item_search', '')
    
    # [SECURE] Validate sơ bộ (Chống input quá dài hoặc ký tự lạ)
    if len(raw_search) > 100 or ';' in raw_search or '--' in raw_search:
         return jsonify({'error': 'Từ khóa không hợp lệ (Phát hiện ký tự cấm).'}), 400

    item_search = raw_search.strip()
    if not item_search:
        return jsonify({'error': 'Vui lòng nhập Tên hoặc Mã Mặt hàng.'}), 400
        
    try:
        data = lookup_service.get_multi_lookup_data(item_search)
        
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='API_QUICK_LOOKUP',
            severity='INFO',
            details=f"Tra nhanh (Multi): item='{item_search}'",
            ip_address=get_user_ip()
        )
        return jsonify(data)
    except Exception as e:
        current_app.logger.error(f"Lỗi API Multi Lookup: {e}")
        return jsonify({'error': 'Lỗi máy chủ nội bộ.'}), 500

@lookup_bp.route('/api/get_order_detail_drilldown/<path:voucher_no>', methods=['GET'])
@login_required
def api_get_order_detail_drilldown(voucher_no):
    """API: Tra cứu chi tiết ĐH Bán."""
    sales_service = current_app.sales_service
    db_manager = current_app.db_manager
    
    # [SECURE] Validate VoucherNo (Chỉ cho phép chữ số, gạch ngang, gạch chéo)
    # Ví dụ: SO/2025/01/001
    import re
    if not re.match(r'^[\w\/\-\.]+$', voucher_no):
        return jsonify({'error': 'Mã chứng từ không hợp lệ.'}), 400
    
    # Dùng tham số hóa (?) trong câu query bên dưới (Đã có sẵn trong code cũ nhưng cần nhắc lại)
    sorder_id_query = f"SELECT TOP 1 SOrderID FROM {config.ERP_OT2001} WHERE VoucherNo = ?"
    sorder_id_data = db_manager.get_data(sorder_id_query, (voucher_no,))
    
    if not sorder_id_data:
         return jsonify({'error': f'Không tìm thấy SOrderID cho mã DHB {voucher_no}'}), 404
         
    sorder_id = sorder_id_data[0]['SOrderID']
    details = sales_service.get_order_detail_drilldown(sorder_id)
    return jsonify(details)

@lookup_bp.route('/api/backorder_details/<string:inventory_id>', methods=['GET'])
@login_required 
def api_get_backorder_details(inventory_id):
    """API: BackOrder Detail."""
    lookup_service = current_app.lookup_service
    db_manager = current_app.db_manager
    
    if not inventory_id:
        return jsonify({'error': 'Thiếu mã hàng.'}), 400
        
    try:
        data = lookup_service.get_backorder_details(inventory_id)
        
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='API_BACKORDER_DETAIL',
            severity='INFO',
            details=f"Xem chi tiết BackOrder: {inventory_id}",
            ip_address=get_user_ip()
        )
        return jsonify(data)
    except Exception as e:
        current_app.logger.error(f"Lỗi API Backorder: {e}")
        return jsonify({'error': 'Lỗi xử lý.'}), 500
        
@lookup_bp.route('/api/replenishment_details/<path:group_code>', methods=['GET'])
@login_required
def api_get_replenishment_details(group_code):
    """API: Chi tiết nhóm hàng."""
    db_manager = current_app.db_manager
    user_role = session.get('user_role', '').strip().upper()
    
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM]:
        return jsonify({'error': 'Không có quyền.'}), 403

    if not group_code:
        return jsonify({'error': 'Thiếu mã nhóm.'}), 400

    try:
        # Gọi SP với tham số hóa (Đã an toàn)
        data = db_manager.execute_sp_multi(config.SP_REPLENISH_GROUP, (group_code,))
        
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='VIEW_REPLENISH_DETAIL',
            severity='INFO',
            details=f"Xem chi tiết dự phòng nhóm: {group_code}",
            ip_address=get_user_ip()
        )
        return jsonify(data[0] if data else [])
    except Exception as e:
        current_app.logger.error(f"Lỗi API Replenish Detail: {e}")
        return jsonify({'error': 'Lỗi máy chủ.'}), 500
        
@lookup_bp.route('/api/customer_replenishment/<string:customer_id>', methods=['GET'])
@login_required
def api_get_customer_replenishment_data(customer_id):
    """API: Dự phòng theo KH."""
    db_manager = current_app.db_manager
    user_role = session.get('user_role', '').strip().upper()
    
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM, config.ROLE_MANAGER]:
        return jsonify({'error': 'Không có quyền.'}), 403

    if not customer_id:
        return jsonify({'error': 'Thiếu mã KH.'}), 400

    try:
        sp_data = db_manager.execute_sp_multi(config.SP_CROSS_SELL_GAP, (customer_id,))
        
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='API_CUSTOMER_REPLENISH',
            severity='INFO',
            details=f"Tra cứu dự phòng KH: {customer_id}",
            ip_address=get_user_ip()
        )
        return jsonify(sp_data[0] if sp_data else [])
    except Exception as e:
        current_app.logger.error(f"Lỗi API Cust Replenish: {e}")
        return jsonify({'error': 'Lỗi máy chủ.'}), 500