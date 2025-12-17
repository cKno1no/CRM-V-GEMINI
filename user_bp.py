from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, current_app
from utils import login_required, permission_required
import config

user_bp = Blueprint('user_bp', __name__)

def check_admin_access():
    return session.get('user_role', '').strip().upper() == config.ROLE_ADMIN

@user_bp.route('/user_management', methods=['GET'])
@login_required
def user_management_page():
    if not check_admin_access():
        flash("Bạn không có quyền truy cập trang quản trị.", "danger")
        return redirect(url_for('index'))
    
    # Truyền Groups tính năng vào template để vẽ giao diện chia cột
    return render_template('user_management.html', feature_groups=config.SYSTEM_FEATURES_GROUPS)

# --- API ENDPOINTS ---

@user_bp.route('/api/users/list', methods=['GET'])
@login_required
def api_get_users():
    if not check_admin_access(): return jsonify([]), 403
    
    # --- ĐOẠN CẦN SỬA ---
    # Code cũ: users = current_app.user_service.get_all_users()
    
    # Code mới: Lấy division từ session và truyền vào
    user_division = session.get('division')
    users = current_app.user_service.get_all_users(division=user_division)
    # --------------------
    
    return jsonify(users)

@user_bp.route('/api/users/detail/<string:user_code>', methods=['GET'])
@login_required
def api_get_user_detail(user_code):
    if not check_admin_access(): return jsonify({}), 403
    user = current_app.user_service.get_user_detail(user_code)
    return jsonify(user)

@user_bp.route('/api/users/update', methods=['POST'])
@login_required
def api_update_user():
    if not check_admin_access(): return jsonify({'success': False}), 403
    data = request.json
    success = current_app.user_service.update_user(data)
    return jsonify({'success': success})

@user_bp.route('/api/permissions/matrix', methods=['GET'])
@login_required
def api_get_permissions():
    if not check_admin_access(): return jsonify({}), 403
    roles = current_app.user_service.get_all_roles()
    matrix = current_app.user_service.get_permissions_matrix()
    return jsonify({'roles': roles, 'matrix': matrix})

@user_bp.route('/api/permissions/save', methods=['POST'])
@login_required
def api_save_permissions():
    if not check_admin_access(): return jsonify({'success': False}), 403
    data = request.json
    role_id = data.get('role_id')
    features = data.get('features', [])
    success = current_app.user_service.update_permissions(role_id, features)
    return jsonify({'success': success})

@user_bp.route('/api/user/set_theme', methods=['POST'])
@login_required
def api_set_user_theme():
    """API: Lưu theme người dùng chọn vào CSDL."""
    data = request.json
    theme = data.get('theme', 'light')
    user_code = session.get('user_code')
    
    # 1. Update DB
    current_app.user_service.update_user_theme_preference(user_code, theme)
    
    # 2. Update Session hiện tại (để F5 không bị mất)
    session['theme'] = theme
    
    return jsonify({'success': True})

@user_bp.route('/api/pet/status', methods=['GET'])
@login_required
def get_pet_status():
    # Lấy thông tin Skin và Điểm từ DB
    # Logic: Nếu Doanh số tháng > Target -> Trạng thái = "HAPPY"
    return jsonify({
        'skin': 'iron_man_robot',
        'mood': 'happy',
        'points': 1500
    })

@user_bp.route('/profile')
@login_required
@permission_required('VIEW_PROFILE') # <--- THÊM DÒNG NÀY ĐỂ KHÓA TRANG
def profile():
    """Hiển thị trang hồ sơ & cửa hàng."""
    user_service = current_app.user_service
    user_code = session.get('user_code')
    
    # 1. Lấy thông tin Stats (Level, XP)
    user_stats = user_service.get_user_stats(user_code)
    
    # 2. Lấy kho đồ cá nhân
    inventory = user_service.get_user_inventory(user_code)
    
    # 3. Lấy danh sách Shop
    shop_items = user_service.get_shop_items(user_code)
    
    return render_template(
        'user_profile.html', 
        user_stats=user_stats, 
        inventory=inventory,
        shop_items=shop_items
        # [QUAN TRỌNG] ĐÃ XÓA DÒNG: user_context=session
        # Không được truyền user_context ở đây nữa, 
        # hãy để factory.py tự động inject biến user_context có hàm .can()
    )

# --- API ENDPOINTS ---

@user_bp.route('/api/user/buy_item', methods=['POST'])
@login_required
def buy_item():
    item_code = request.json.get('item_code')
    if not item_code:
        return jsonify({'success': False, 'message': 'Thiếu mã vật phẩm'}), 400
        
    result = current_app.user_service.buy_item(session.get('user_code'), item_code)
    return jsonify(result)

@user_bp.route('/api/user/equip_item', methods=['POST'])
@login_required
def equip_item():
    item_code = request.json.get('item_code')
    if not item_code:
        return jsonify({'success': False, 'message': 'Thiếu mã vật phẩm'}), 400
        
    result = current_app.user_service.equip_item(session.get('user_code'), item_code)
    
    # Nếu thành công, cập nhật luôn vào Session để giao diện đổi ngay lập tức
    if result['success']:
        # Cần logic check item type để update đúng session key (theme/pet)
        # Tạm thời client sẽ reload trang để cập nhật
        pass
        
    return jsonify(result)

@user_bp.route('/api/user/upload_avatar', methods=['POST'])
@login_required
def upload_avatar():
    if 'avatar' not in request.files:
        return jsonify({'success': False, 'message': 'Không có file'})
    
    file = request.files['avatar']
    if file.filename == '':
        return jsonify({'success': False, 'message': 'Chưa chọn file'})
        
    if file:
        result = current_app.user_service.update_avatar(session.get('user_code'), file)
        
        # Cập nhật session avatar nếu thành công
        if result['success']:
            session['avatar_url'] = result['url']
            
        return jsonify(result)

@user_bp.route('/api/user/change_password', methods=['POST'])
@login_required
def change_password():
    data = request.json
    result = current_app.user_service.change_password(
        session.get('user_code'), 
        data.get('current_password'), 
        data.get('new_password')
    )
    return jsonify(result)