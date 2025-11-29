from flask import Blueprint, request, session, jsonify, Response, render_template, redirect, url_for, flash
from utils import login_required
from datetime import datetime
import json
import config 

# [HÀM HELPER CẦN THIẾT] (ĐÃ BỔ SUNG)
def get_user_ip():
    """Lấy địa chỉ IP của người dùng."""
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

# FIX: ĐỊNH NGHĨA BLUEPRINT VỚI URL PREFIX ĐỂ KHỚP VỚI CẤU TRÚC /sales/
lookup_bp = Blueprint('lookup_bp', __name__, url_prefix='/sales')


# [ROUTES]

@lookup_bp.route('/sales_lookup', methods=['GET', 'POST'])
@login_required
def sales_lookup_dashboard():
    """ROUTE: Dashboard tra cứu thông tin bán hàng. (Phục vụ /sales/sales_lookup)"""
    
    # FIX: Import Services Cục bộ
    from app import lookup_service, db_manager # ADD db_manager
    
    user_role = session.get('user_role', '').strip().upper()
    is_admin_or_gm = user_role in [config.ROLE_ADMIN, config.ROLE_GM]
    is_manager = user_role == 'MANAGER'
    
    show_block_3 = is_admin_or_gm
    show_block_2 = is_admin_or_gm or is_manager
    
    item_search = ""
    object_id = ""
    object_id_display = ""
    lookup_results = {} 
    
    if request.method == 'POST':
        item_search = request.form.get('item_search', '').strip()
        object_id = request.form.get('object_id', '').strip() 
        object_id_display = request.form.get('object_id_display', '').strip()
        
        if not item_search:
            flash("Vui lòng nhập Tên hoặc Mã Mặt hàng để tra cứu.", 'warning')
        else:
            lookup_results = lookup_service.get_sales_lookup_data(
                item_search, object_id 
            )
        
        # LOG API_SALES_LOOKUP (Tra cứu Form) (BỔ SUNG)
        try:
            db_manager.write_audit_log(
                user_code=session.get('user_code'),
                action_type='API_SALES_LOOKUP',
                severity='INFO',
                details=f"Tra cứu (Form): item='{item_search}', kh='{object_id}'",
                ip_address=get_user_ip()
            )
        except Exception as e:
            print(f"Lỗi ghi log API_SALES_LOOKUP: {e}")

    return render_template(
        'sales_lookup_dashboard.html', 
        item_search=item_search,
        object_id=object_id,
        object_id_display=object_id_display,
        results=lookup_results,
        show_block_2=show_block_2,
        show_block_3=show_block_3
    )

@lookup_bp.route('/total_replenishment', methods=['GET'])
@login_required
def total_replenishment_dashboard():
    """
    ROUTE: Hiển thị trang Báo cáo Dự phòng Tồn kho Tổng thể. (Phục vụ /sales/total_replenishment)
    """
    # FIX: Import Services Cục bộ VÀ helper
    from app import db_manager, get_user_ip 
    
    # 1. Kiểm tra Quyền
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM]:
        flash("Bạn không có quyền truy cập chức năng này.", 'danger')
        return redirect(url_for('index'))

    # 2. Ghi Log
    try:
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='VIEW_TOTAL_REPLENISHMENT',
            severity='WARNING', 
            details="Truy cập Báo cáo Dự phòng Tồn kho Tổng thể",
            ip_address=get_user_ip()
        )
    except Exception as e:
        print(f"Lỗi ghi log total_replenishment: {e}")

    # 3. Gọi SP
    try:
        sp_data = db_manager.execute_sp_multi(config.SP_REPLENISH_TOTAL, None)
        alert_list = sp_data[0] if sp_data else []
    except Exception as e:
        flash(f"Lỗi thực thi Stored Procedure: {e}", 'danger')
        alert_list = []
        
    return render_template(
        'total_replenishment.html', 
        alert_list=alert_list
    )

@lookup_bp.route('/export_total_replenishment', methods=['GET'])
@login_required
def export_total_replenishment():
    """ROUTE: Xử lý xuất dữ liệu dự phòng tồn kho ra Excel."""
    from app import db_manager, get_user_ip # ADD get_user_ip
    
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM]:
        flash("Bạn không có quyền truy cập chức năng này.", 'danger')
        return redirect(url_for('index'))
    
    # LOG EXPORT_REPLENISHMENT (BỔ SUNG)
    try:
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='EXPORT_REPLENISHMENT',
            severity='CRITICAL', 
            details="Xuất Excel Báo cáo Dự phòng Tổng thể",
            ip_address=get_user_ip()
        )
    except Exception as e:
        print(f"Lỗi ghi log EXPORT_REPLENISHMENT: {e}")
        
    flash("Chức năng Xuất Excel chưa được hoàn thành.", 'warning')
    return redirect(url_for('lookup_bp.total_replenishment_dashboard')) 


# =========================================================================
# ROUTE MỚI: DỰ PHÒNG KHÁCH HÀNG
# =========================================================================

@lookup_bp.route('/customer_replenishment', methods=['GET'])
@login_required
def customer_replenishment_dashboard():
    """
    ROUTE: Hiển thị trang Dự báo Dự phòng Khách hàng. (Phục vụ /sales/customer_replenishment)
    """
    # FIX: Import Services Cục bộ VÀ helper
    from app import db_manager, get_user_ip 
    
    # 1. Kiểm tra Quyền (Thêm quyền SALES vì đây là báo cáo KH)
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM, config.ROLE_MANAGER]:
        flash("Bạn không có quyền truy cập chức năng này.", 'danger')
        return redirect(url_for('index'))

    # 2. Ghi Log
    try:
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='VIEW_CUSTOMER_REPLENISHMENT',
            severity='WARNING', 
            details="Truy cập Dự báo Dự phòng Khách hàng",
            ip_address=get_user_ip()
        )
    except Exception as e:
        print(f"Lỗi ghi log customer_replenishment: {e}")

    # 3. Render Template
    return render_template('customer_replenishment.html')


# [APIs]

@lookup_bp.route('/api/khachhang/<string:ten_tat>', methods=['GET'])
@login_required 
def api_khachhang(ten_tat):
    """API tra cứu Khách hàng (Autocomplete)."""
    
    # FIX: Import Services Cục bộ
    from app import customer_service
    
    data = customer_service.get_customer_by_name(ten_tat)
    return jsonify(data)

@lookup_bp.route('/api/multi_lookup', methods=['POST'])
@login_required 
def api_multi_lookup():
    """API: Tra cứu Tồn kho/Giá QĐ/BO cho nhiều mã (Tra nhanh)."""
    
    # FIX: Import Services Cục bộ
    from app import lookup_service, db_manager # ADD db_manager
    
    item_search = request.form.get('item_search', '').strip()
    
    if not item_search:
        return jsonify({'error': 'Vui lòng nhập Tên hoặc Mã Mặt hàng.'}), 400
        
    try:
        data = lookup_service.get_multi_lookup_data(item_search)
        
        # LOG API_QUICK_LOOKUP (BỔ SUNG)
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='API_QUICK_LOOKUP',
            severity='INFO',
            details=f"Tra nhanh (Multi): item='{item_search}'",
            ip_address=get_user_ip()
        )
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': f'Lỗi server: {e}'}), 500

@lookup_bp.route('/api/get_order_detail_drilldown/<path:voucher_no>', methods=['GET'])
@login_required
def api_get_order_detail_drilldown(voucher_no):
    """API: Tra cứu chi tiết ĐH Bán bằng VoucherNo."""
    
    # FIX: Import Services Cục bộ
    from app import db_manager, sales_service
    
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
    """API: Lấy chi tiết BackOrder (PO) cho một mã hàng."""
    
    # FIX: Import Services Cục bộ
    from app import lookup_service, db_manager # ADD db_manager
    
    if not inventory_id:
        return jsonify({'error': 'Vui lòng cung cấp Mã Mặt hàng.'}), 400
        
    try:
        data = lookup_service.get_backorder_details(inventory_id)
        
        # LOG API_BACKORDER_DETAIL (BỔ SUNG)
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='API_BACKORDER_DETAIL',
            severity='INFO',
            details=f"Xem chi tiết BackOrder cho: {inventory_id}",
            ip_address=get_user_ip()
        )
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': f'Lỗi server: {e}'}), 500
        
@lookup_bp.route('/api/replenishment_details/<path:group_code>', methods=['GET'])
@login_required
def api_get_replenishment_details(group_code):
    """API: Lấy chi tiết InventoryID cho một Nhóm Varchar05 (Req 1)."""
    
    # FIX: Import Services Cục bộ
    from app import db_manager
    
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM]: # Chỉ Admin và GM mới được xem chi tiết tổng thể
        return jsonify({'error': 'Không có quyền.'}), 403

    if not group_code:
        return jsonify({'error': 'Thiếu mã nhóm (Varchar05).'}), 400

    try:
        data = db_manager.execute_sp_multi(config.SP_REPLENISH_GROUP, (group_code,))
        
        # LOG VIEW_REPLENISH_DETAIL (BỔ SUNG)
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='VIEW_REPLENISH_DETAIL',
            severity='INFO',
            details=f"Xem chi tiết dự phòng nhóm: {group_code}",
            ip_address=get_user_ip()
        )
        
        return jsonify(data[0] if data else [])
    except Exception as e:
        return jsonify({'error': f'Lỗi server: {e}'}), 500
        
@lookup_bp.route('/api/customer_replenishment/<string:customer_id>', methods=['GET'])
@login_required
def api_get_customer_replenishment_data(customer_id):
    """
    API: Lấy dữ liệu Dự phòng Khách hàng (Nhóm hàng) cho một mã KH. (Phục vụ AJAX)
    """
    # FIX: Import Services Cục bộ
    from app import db_manager 
    
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM, config.ROLE_MANAGER]:
        return jsonify({'error': 'Không có quyền.'}), 403

    if not customer_id:
        return jsonify({'error': 'Thiếu mã khách hàng.'}), 400

    try:
        # Giả định SP là 'dbo.sp_GetCustomerReplenishmentNeeds'
        sp_data = db_manager.execute_sp_multi(config.SP_CROSS_SELL_GAP, (customer_id,))
        
        # LOG API_CUSTOMER_REPLENISH (BỔ SUNG)
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='API_CUSTOMER_REPLENISH',
            severity='INFO',
            details=f"Tra cứu dự phòng cho KH: {customer_id}",
            ip_address=get_user_ip()
        )
        
        # SP trả về kết quả trong tập hợp đầu tiên
        return jsonify(sp_data[0] if sp_data else [])
    except Exception as e:
        print(f"Lỗi API get_customer_replenishment_data: {e}")
        return jsonify({'error': f'Lỗi server: {e}'}), 500