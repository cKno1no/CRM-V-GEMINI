from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
# FIX: Import login_required từ utils.py (và loại bỏ các import dịch vụ/helper từ app ở đây)
from utils import login_required 
from datetime import datetime, timedelta
import config
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
    from app import delivery_service, db_manager
    
    # [CONFIG]: Chuẩn hóa Phân quyền
    user_role = session.get('user_role', '').strip().upper()
    user_bo_phan = session.get('bo_phan', '').strip() 
    
    is_admin_or_gm = user_role in [config.ROLE_ADMIN, config.ROLE_GM]
    is_thu_ky = str(user_bo_phan) == str(config.DEPT_THUKY)
    is_kho = str(user_bo_phan) == str(config.DEPT_KHO)
    
    can_edit_planner = is_admin_or_gm
    can_view_dispatch = is_admin_or_gm or is_kho or is_thu_ky
    can_edit_dispatch = is_admin_or_gm or is_kho

    grouped_tasks_json, ungrouped_tasks_json = delivery_service.get_planning_board_data()
    
    today_str = datetime.now().strftime('%Y-%m-%d')
    today_weekday = datetime.now().strftime('%A').upper() 
    
    # Filter lists for dispatch view
    dispatch_pool = [t for t in ungrouped_tasks_json if t['DeliveryStatus'] != config.DELIVERY_STATUS_DONE]
    
    kho_hom_nay = [t for t in dispatch_pool if t['Planned_Day'] == today_weekday or t['Planned_Day'] == 'URGENT']
    kho_trong_tuan = [t for t in dispatch_pool if t['Planned_Day'] not in ['POOL', 'URGENT', 'WITHIN_WEEK', 'PICKUP', today_weekday]]
    kho_sap_xep = [t for t in dispatch_pool if t['Planned_Day'] == 'WITHIN_WEEK']
    kho_da_giao = [t for t in ungrouped_tasks_json if t['DeliveryStatus'] == config.DELIVERY_STATUS_DONE]
    
    # Log access
    try:
        from app import get_user_ip
        db_manager.write_audit_log(
            session.get('user_code'), 'VIEW_DELIVERY_DASHBOARD', 'INFO', 
            "Truy cập Bảng Điều phối Giao vận", get_user_ip()
        )
    except: pass

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
    from app import delivery_service, db_manager
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM]:
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

    # [CONFIG]: Check quyền Kho & Admin
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM] and str(user_bo_phan) != str(config.DEPT_KHO):
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
    # [CONFIG]: Check quyền xem chi tiết
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM] and \
       str(user_bo_phan) not in [str(config.DEPT_KHO), str(config.DEPT_THUKY)]:
        return jsonify({'error': 'Bạn không có quyền xem dữ liệu này.'}), 403
        
    items = delivery_service.get_delivery_items(voucher_id)
    return jsonify(items)