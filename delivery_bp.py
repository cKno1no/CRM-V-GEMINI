from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
# FIX: Import login_required từ utils.py (và loại bỏ các import dịch vụ/helper từ app ở đây)
from utils import login_required 
from datetime import datetime, timedelta

delivery_bp = Blueprint('delivery_bp', __name__)

# [HÀM HELPER CẦN THIẾT]
def get_user_ip():
    """Lấy địa chỉ IP của người dùng."""
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

# [ROUTES]

@delivery_bp.route('/delivery_dashboard', methods=['GET'])
@login_required
def delivery_dashboard():
    """ROUTE: Hiển thị Bảng Điều phối Giao vận (2 Tab)."""
    
    # FIX: Import Delivery Service Cục bộ
    from app import delivery_service, db_manager # ADD db_manager
    
    # --- LOGIC PHÂN QUYỀN (TỪ APP.PY GỐC) ---
    user_role = session.get('user_role', '').strip().upper()
    user_bo_phan = session.get('bo_phan', '').strip() 
    
    is_admin_or_gm = user_role in ['ADMIN', 'GM']
    is_thu_ky = user_bo_phan == '3. THU KY'
    is_kho = user_bo_phan == '5. KHO'
    
    can_edit_planner = is_admin_or_gm
    can_view_dispatch = is_admin_or_gm or is_kho or is_thu_ky
    can_edit_dispatch = is_admin_or_gm or is_kho
    # ----------------------------------------

    grouped_tasks_json, ungrouped_tasks_json = delivery_service.get_planning_board_data()
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    today_weekday = datetime.now().strftime('%A').upper() 
    
    # --- LOGIC CHO TAB 2 (KHO - Dùng danh sách LXH LẺ TỪ APP.PY GỐC) ---
    dispatch_pool = [t for t in ungrouped_tasks_json if t['DeliveryStatus'] != 'Da Giao']
    
    # 1a. Hôm nay phải giao
    kho_hom_nay = [t for t in dispatch_pool if t['Planned_Day'] == today_weekday or t['Planned_Day'] == 'URGENT']
    
    # 1b. Trong tuần sẽ giao
    kho_trong_tuan = [t for t in dispatch_pool if t['Planned_Day'] not in ['POOL', 'URGENT', 'WITHIN_WEEK', 'PICKUP', today_weekday]]
    
    # 1c. Sắp xếp trong tuần
    kho_sap_xep = [t for t in dispatch_pool if t['Planned_Day'] == 'WITHIN_WEEK']
    
    # Đã Giao
    kho_da_giao = [t for t in ungrouped_tasks_json if t['DeliveryStatus'] == 'Da Giao']
    # --------------------------------------------------------------------
    
    # LOG VIEW_DELIVERY_DASHBOARD (BỔ SUNG)
    try:
        db_manager.write_audit_log(
            session.get('user_code'), 'VIEW_DELIVERY_DASHBOARD', 'INFO', 
            "Truy cập Bảng Điều phối Giao vận", 
            get_user_ip()
        )
    except Exception as e:
        print(f"Lỗi ghi log VIEW_DELIVERY_DASHBOARD: {e}")

    return render_template(
        'delivery_dashboard.html',
        grouped_tasks_json=grouped_tasks_json,   
        ungrouped_tasks_json=ungrouped_tasks_json, 
        kho_hom_nay=kho_hom_nay,
        kho_trong_tuan=kho_trong_tuan,
        kho_sap_xep=kho_sap_xep,
        kho_da_giao=kho_da_giao, 
        current_date_str=today_str,
        current_weekday_str=today_weekday,
        can_edit_planner=can_edit_planner,
        can_view_dispatch=can_view_dispatch,
        can_edit_dispatch=can_edit_dispatch
    )

# [APIs]

@delivery_bp.route('/api/delivery/set_day', methods=['POST'])
@login_required
def api_delivery_set_day():
    """API: (Thư ký) Kéo thả 1 LXH hoặc 1 Nhóm KH vào 1 ngày kế hoạch."""
    
    # FIX: Import Delivery Service Cục bộ
    from app import delivery_service, db_manager # ADD db_manager
    
    user_role = session.get('user_role', '').strip().upper()
    user_code = session.get('user_code')
    if user_role not in ['ADMIN', 'GM']:
        return jsonify({'success': False, 'message': 'Bạn không có quyền thực hiện thao tác này.'}), 403
        
    data = request.json
    
    voucher_id = data.get('voucher_id') 
    object_id = data.get('object_id')   
    new_day = data.get('new_day')       
    old_day = data.get('old_day')

    if not new_day or not old_day or (not voucher_id and not object_id):
        return jsonify({'success': False, 'message': 'Thiếu dữ liệu cần thiết.'}), 400

    try:
        success = delivery_service.set_planned_day(voucher_id, object_id, new_day, user_code, old_day)
        
        if success:
            # LOG SET_DELIVERY_PLAN (BỔ SUNG)
            db_manager.write_audit_log(
                user_code, 'SET_DELIVERY_PLAN', 'INFO', 
                f"Kéo thả plan: {voucher_id or object_id} từ {old_day} sang {new_day}", 
                get_user_ip()
            )
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'message': 'Lỗi CSDL khi cập nhật Kế hoạch.'}), 500

    except Exception as e:
        print(f"LỖI API set_day: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

@delivery_bp.route('/api/delivery/set_status', methods=['POST'])
@login_required
def api_delivery_set_status():
    """API: (Kho) Cập nhật trạng thái Đã Soạn/Đã Giao."""
    
    # FIX: Import Delivery Service Cục bộ
    from app import delivery_service, db_manager # ADD db_manager
    
    user_role = session.get('user_role', '').strip().upper()
    user_bo_phan = session.get('bo_phan', '').strip()
    user_code = session.get('user_code')

    if user_role not in ['ADMIN', 'GM'] and user_bo_phan != '5. KHO':
        return jsonify({'success': False, 'message': 'Bạn không có quyền thực hiện thao tác này.'}), 403

    data = request.json
    voucher_id = data.get('voucher_id')
    new_status = data.get('new_status') 

    if not all([voucher_id, new_status]):
        return jsonify({'success': False, 'message': 'Thiếu VoucherID hoặc Trạng thái.'}), 400
        
    try:
        success = delivery_service.set_delivery_status(voucher_id, new_status, user_code)
        if success:
            # LOG SET_DELIVERY_STATUS (BỔ SUNG)
            severity = 'CRITICAL' if new_status == 'Da Giao' else 'WARNING'
            db_manager.write_audit_log(
                user_code, 'SET_DELIVERY_STATUS', severity, 
                f"Cập nhật trạng thái LXH {voucher_id} thành {new_status}", 
                get_user_ip()
            )
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'message': 'Lỗi CSDL khi cập nhật Trạng thái.'}), 500

    except Exception as e:
        print(f"LỖI API set_status: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

@delivery_bp.route('/api/delivery/get_items/<string:voucher_id>', methods=['GET'])
@login_required
def api_delivery_get_items(voucher_id):
    """API: Lấy chi tiết mặt hàng LXH cho Modal xác nhận."""
    
    # FIX: Import Delivery Service Cục bộ
    from app import delivery_service
    
    user_role = session.get('user_role', '').strip().upper()
    user_bo_phan = session.get('bo_phan', '').strip()
    if user_role not in ['ADMIN', 'GM'] and user_bo_phan not in ['5. KHO', '3. THU KY']:
        return jsonify({'error': 'Bạn không có quyền xem dữ liệu này.'}), 403
        
    items = delivery_service.get_delivery_items(voucher_id)
    return jsonify(items)