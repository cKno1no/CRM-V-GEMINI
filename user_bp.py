from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, flash, current_app
from utils import login_required, permission_required
import config
import pandas as pd # <--- [THÊM] Import thư viện này để xử lý lỗi ngày tháng

user_bp = Blueprint('user_bp', __name__)

def check_admin_access():
    return session.get('user_role', '').strip().upper() == config.ROLE_ADMIN

@user_bp.route('/user_management', methods=['GET'])
@login_required
def user_management_page():
    if not check_admin_access():
        flash("Bạn không có quyền truy cập trang quản trị.", "danger")
        return redirect(url_for('index'))
    return render_template('user_management.html', feature_groups=config.SYSTEM_FEATURES_GROUPS)

# --- API ENDPOINTS ---


@user_bp.route('/api/users/list', methods=['GET'])
@login_required
def api_get_users():
    if not check_admin_access(): return jsonify([]), 403
    user_division = session.get('division')
    users = current_app.user_service.get_all_users(division=user_division)
    
    # --- [ĐOẠN CODE CẦN THÊM ĐỂ SỬA LỖI] ---
    # Chuyển đổi NaT (của Pandas) thành None để jsonify không bị lỗi
    for u in users:
        # Kiểm tra nếu CreatedDate là NaT (Not a Time)
        if 'CreatedDate' in u and pd.isna(u['CreatedDate']):
            u['CreatedDate'] = None
    # ---------------------------------------

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
    data = request.json
    theme = data.get('theme', 'light')
    user_code = session.get('user_code')
    current_app.user_service.update_user_theme_preference(user_code, theme)
    session['theme'] = theme
    return jsonify({'success': True})

@user_bp.route('/api/pet/status', methods=['GET'])
@login_required
def get_pet_status():
    return jsonify({
        'skin': 'iron_man_robot',
        'mood': 'happy',
        'points': 1500
    })

@user_bp.route('/profile')
@login_required
def profile():
    user_code = session.get('user_code')
    user_data = current_app.user_service.get_user_profile(user_code)
    
    if not user_data:
        flash("Không tìm thấy thông tin người dùng.", "danger")
        return redirect(url_for('index'))
    
    # [FIX] Ép kiểu Level để tránh lỗi so sánh template
    try:
        user_data['Level'] = int(user_data.get('Level', 1))
    except:
        user_data['Level'] = 1

    inventory_sql = """
        SELECT 
            T1.*, 
            T2.ItemType, 
            T2.ItemName,
            T2.MinLevel 
        FROM TitanOS_UserInventory T1
        LEFT JOIN TitanOS_SystemItems T2 ON T1.ItemCode = T2.ItemCode
        WHERE T1.UserCode = ? AND T1.IsActive = 1
    """
    inventory = current_app.db_manager.get_data(inventory_sql, (user_code,))
    
    items = current_app.db_manager.get_data(
        "SELECT * FROM TitanOS_SystemItems WHERE IsActive = 1 ORDER BY Price ASC"
    )

    return render_template(
        'user_profile.html', 
        user=user_data,
        inventory=inventory,
        items=items
    )

@user_bp.route('/api/user/buy_item', methods=['POST'])
@login_required
def buy_item():
    user_code = session.get('user_code')
    item_code = request.json.get('item_code')
    result = current_app.user_service.buy_item(user_code, item_code)
    return jsonify(result)

@user_bp.route('/api/user/equip_item', methods=['POST'])
@login_required
def equip_item():
    user_code = session.get('user_code')
    item_code = request.json.get('item_code')
    result = current_app.user_service.equip_item(user_code, item_code)
    if result.get('success') and item_code in ['light', 'dark', 'fantasy', 'adorable']:
        session['theme'] = item_code
        current_app.user_service.update_user_theme_preference(user_code, item_code)
    return jsonify(result)

@user_bp.route('/api/user/upload_avatar', methods=['POST'])
@login_required
def upload_avatar():
    if 'avatar' not in request.files: return jsonify({'success': False, 'message': 'Không có file'})
    file = request.files['avatar']
    if file.filename == '': return jsonify({'success': False, 'message': 'Chưa chọn file'})
    if file:
        result = current_app.user_service.update_avatar(session.get('user_code'), file)
        if result['success']: session['avatar_url'] = result['url']
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

# =========================================================================
# 3. API MAILBOX (HÒM THƯ) - [ĐÃ FIX LỖI NaT]
# =========================================================================

@user_bp.route('/api/mailbox', methods=['GET'])
@login_required
def get_mailbox():
    """Lấy danh sách thư (Xử lý lỗi NaTType của Pandas)."""
    user_code = session.get('user_code')
    db_manager = current_app.db_manager
    
    sql = "SELECT * FROM TitanOS_Game_Mailbox WHERE UserCode = ? ORDER BY IsClaimed ASC, CreatedTime DESC"
    
    try:
        rows = db_manager.get_data(sql, (user_code,))
        if not rows: return jsonify([])
            
        clean_mails = []
        for row in rows:
            mail = dict(row)
            
            # [FIX] Dùng pd.isna() để bắt NaT và None chuẩn xác
            if 'ClaimedTime' in mail and pd.isna(mail['ClaimedTime']):
                mail['ClaimedTime'] = None
            if 'CreatedTime' in mail and pd.isna(mail['CreatedTime']):
                mail['CreatedTime'] = None
            
            clean_mails.append(mail)

        return jsonify(clean_mails)
    except Exception as e:
        current_app.logger.error(f"Lỗi lấy hòm thư: {e}")
        return jsonify([])

@user_bp.route('/api/mailbox/claim', methods=['POST'])
@login_required
def claim_mail():
    user_code = session.get('user_code')
    mail_id = request.json.get('mail_id')
    
    conn = None
    try:
        conn = current_app.db_manager.get_transaction_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT Total_XP, Total_Coins FROM TitanOS_Game_Mailbox WHERE MailID=? AND UserCode=? AND IsClaimed=0", (mail_id, user_code))
        mail = cursor.fetchone()
        if not mail:
            conn.rollback()
            return jsonify({'success': False, 'msg': 'Thư không tồn tại hoặc đã nhận.'})
            
        xp, coins = mail[0] or 0, mail[1] or 0
        
        cursor.execute("SELECT Level, CurrentXP, TotalCoins FROM TitanOS_UserStats WHERE UserCode=?", (user_code,))
        stats = cursor.fetchone()
        
        if not stats:
            cursor.execute("INSERT INTO TitanOS_UserStats (UserCode, Level, CurrentXP, TotalCoins) VALUES (?, 1, 0, 0)", (user_code,))
            lvl, curr_xp, curr_coins = 1, 0, 0
        else:
            lvl, curr_xp, curr_coins = stats[0], stats[1], stats[2]
            
        new_xp = curr_xp + xp
        new_coins = curr_coins + coins
        new_lvl = lvl
        
        level_up = False
        loop_guard = 0
        while loop_guard < 50:
            cursor.execute("SELECT XP_Required, Coin_Reward FROM TitanOS_Game_Levels WHERE Level=?", (new_lvl,))
            req = cursor.fetchone()
            req_xp = req[0] if req else 999999
            
            if new_xp >= req_xp:
                new_xp -= req_xp
                new_lvl += 1
                new_coins += (req[1] or 0)
                level_up = True
                loop_guard += 1
            else:
                break
                
        cursor.execute("UPDATE TitanOS_UserStats SET Level=?, CurrentXP=?, TotalCoins=? WHERE UserCode=?", (new_lvl, new_xp, new_coins, user_code))
        cursor.execute("UPDATE TitanOS_Game_Mailbox SET IsClaimed=1, ClaimedTime=GETDATE() WHERE MailID=?", (mail_id,))
        
        conn.commit()
        return jsonify({'success': True, 'level_up': level_up, 'new_level': new_lvl, 'coins_earned': coins})
        
    except Exception as e:
        if conn: conn.rollback()
        current_app.logger.error(f"Lỗi Claim Mail: {e}")
        return jsonify({'success': False, 'msg': str(e)})
    finally:
        if conn: conn.close()

@user_bp.route('/api/user/use_rename_card', methods=['POST'])
@login_required
def api_use_rename_card():
    data = request.json
    new_nickname = data.get('nickname', '').strip()
    
    if not new_nickname:
        return jsonify({'success': False, 'message': 'Tên không được để trống!'})
    if len(new_nickname) > 20:
        return jsonify({'success': False, 'message': 'Tên quá dài (tối đa 20 ký tự).'})

    result = current_app.user_service.use_rename_card(session.get('user_code'), new_nickname)
    
    if result['success']:
        session['user_shortname'] = new_nickname
        
    return jsonify(result)