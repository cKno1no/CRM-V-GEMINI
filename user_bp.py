from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, current_app
from utils import login_required
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