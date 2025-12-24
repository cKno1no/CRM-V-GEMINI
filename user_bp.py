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
def profile():
    user_code = session.get('user_code')
    
    # [FIX LỖI] Sử dụng get_user_profile thay vì get_user_stats
    # Hàm này trả về đầy đủ: Info, Stats (Game), Profile (Flex)
    user_data = current_app.user_service.get_user_profile(user_code)
    
    if not user_data:
        flash("Không tìm thấy thông tin người dùng.", "danger")
        return redirect(url_for('index'))
    
    # 2. [FIX] Lấy Túi đồ (Inventory) + KẾT HỢP ItemType từ bảng SystemItems
    # Phải JOIN bảng để lấy được ItemType cho bộ lọc bên HTML
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
    
    # 3. Lấy Danh sách vật phẩm Shop
    items = current_app.db_manager.get_data(
        "SELECT * FROM TitanOS_SystemItems WHERE IsActive = 1 ORDER BY Price ASC"
    )

    return render_template(
        'user_profile.html', 
        user=user_data,
        inventory=inventory,
        items=items
    )

# =========================================================================
# 2. API GAMIFICATION (SHOP, INVENTORY, SETTINGS)
# =========================================================================

@user_bp.route('/api/user/buy_item', methods=['POST'])
@login_required
def buy_item():
    """API Mua vật phẩm."""
    user_code = session.get('user_code')
    item_code = request.json.get('item_code')
    result = current_app.user_service.buy_item(user_code, item_code)
    return jsonify(result)

@user_bp.route('/api/user/equip_item', methods=['POST'])
@login_required
def equip_item():
    """API Trang bị/Sử dụng vật phẩm."""
    user_code = session.get('user_code')
    item_code = request.json.get('item_code')
    result = current_app.user_service.equip_item(user_code, item_code)
    
    # Nếu là Theme, cập nhật vào session để hiện ngay lập tức
    if result.get('success') and item_code in ['light', 'dark', 'fantasy', 'adorable']:
        session['theme'] = item_code
        # Cập nhật theme mặc định vào DB
        current_app.user_service.update_user_theme_preference(user_code, item_code)
        
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


# =========================================================================
# 3. API MAILBOX (HÒM THƯ)
# =========================================================================
# =========================================================================
# 3. API MAILBOX (HÒM THƯ) - [ĐÃ FIX LỖI LOADING & TREO DB]
# =========================================================================

@user_bp.route('/api/mailbox', methods=['GET'])
@login_required
def get_mailbox():
    """Lấy danh sách thư (Không dùng Pandas để tránh lỗi NaT)."""
    user_code = session.get('user_code')
    db_manager = current_app.db_manager
    
    sql = "SELECT * FROM TitanOS_Game_Mailbox WHERE UserCode = ? ORDER BY IsClaimed ASC, CreatedTime DESC"
    
    try:
        rows = db_manager.get_data(sql, (user_code,))
        if not rows: return jsonify([])
            
        clean_mails = []
        for row in rows:
            mail = dict(row)
            # Xử lý ngày tháng null
            if not mail.get('ClaimedTime'): mail['ClaimedTime'] = None
            if not mail.get('CreatedTime'): mail['CreatedTime'] = None
            
            clean_mails.append(mail)

        return jsonify(clean_mails)
    except Exception as e:
        current_app.logger.error(f"Lỗi lấy hòm thư: {e}")
        return jsonify([])

@user_bp.route('/api/mailbox/claim', methods=['POST'])
@login_required
def claim_mail():
    """Nhận thưởng (Có Transaction an toàn & Finally close connection)."""
    user_code = session.get('user_code')
    mail_id = request.json.get('mail_id')
    
    conn = None
    try:
        conn = current_app.db_manager.get_transaction_connection()
        cursor = conn.cursor()
        
        # 1. Check thư
        cursor.execute("SELECT Total_XP, Total_Coins FROM TitanOS_Game_Mailbox WHERE MailID=? AND UserCode=? AND IsClaimed=0", (mail_id, user_code))
        mail = cursor.fetchone()
        if not mail:
            conn.rollback()
            return jsonify({'success': False, 'msg': 'Thư không tồn tại hoặc đã nhận.'})
            
        xp, coins = mail[0] or 0, mail[1] or 0
        
        # 2. Lấy Stats hiện tại
        cursor.execute("SELECT Level, CurrentXP, TotalCoins FROM TitanOS_UserStats WHERE UserCode=?", (user_code,))
        stats = cursor.fetchone()
        
        if not stats:
            cursor.execute("INSERT INTO TitanOS_UserStats (UserCode, Level, CurrentXP, TotalCoins) VALUES (?, 1, 0, 0)", (user_code,))
            lvl, curr_xp, curr_coins = 1, 0, 0
        else:
            lvl, curr_xp, curr_coins = stats[0], stats[1], stats[2]
            
        # 3. Tính toán Level Up
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
                
        # 4. Update DB
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
    