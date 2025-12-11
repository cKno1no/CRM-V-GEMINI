# app.py
# --- PHIÊN BẢN APPLICATION FACTORY (ĐÃ SỬA LỖI TRÙNG ROUTE) ---

from flask import render_template, request, redirect, url_for, flash, session
import config

# 1. IMPORT TỪ FACTORY VÀ UTILS
from factory import create_app
from utils import login_required

# 2. KHỞI TẠO APP TỪ NHÀ MÁY
# (Hàm này đã bao gồm việc đăng ký route /attachments/ và các service)
app = create_app()

# =========================================================================
# HELPER FUNCTIONS
# =========================================================================

def get_user_ip():
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

# =========================================================================
# ROUTES XÁC THỰC (LOGIN / LOGOUT / PASSWORD)
# =========================================================================

@app.route('/login', methods=['GET', 'POST'])
def login():
    # Nếu đã login, điều hướng luôn
    if session.get('logged_in'):
        user_role = session.get('user_role', '').strip().upper()
        if user_role in [config.ROLE_ADMIN]: 
            return redirect(url_for('executive_bp.ceo_cockpit_dashboard'))
        return redirect(url_for('portal_bp.portal_dashboard'))

    message = None
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user_ip = get_user_ip() 

        # --- SỬ DỤNG DB MANAGER ĐÃ ĐƯỢC INJECT VÀO APP ---
        # Truy vấn user
        query = f"""
            SELECT TOP 1 [USERCODE], [USERNAME], [SHORTNAME], [ROLE], [CAP TREN], [BO PHAN], [CHUC VU], [PASSWORD]
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE ([USERCODE] = ? OR [USERNAME] = ?) AND [PASSWORD] = ?
        """
        # Lưu ý: app.db_manager đã có sẵn từ factory
        user_data = app.db_manager.get_data(query, (username, username, password))

        if user_data:
            user = user_data[0]
            
            # Thiết lập Session
            session['logged_in'] = True
            session.permanent = True
            session['user_code'] = user.get('USERCODE')
            session['username'] = user.get('USERNAME')
            session['user_shortname'] = user.get('SHORTNAME')
            
            user_role = str(user.get('ROLE') or '').strip().upper()
            session['user_role'] = user_role
            
            session['cap_tren'] = user.get('CAP TREN', '')
            session['bo_phan'] = "".join((user.get('BO PHAN') or '').split()).upper()
            session['chuc_vu'] = str(user.get('CHUC VU') or '').strip().upper()
            
            # Security Stamp
            session['security_hash'] = user.get('PASSWORD')

            # Ghi Log
            app.db_manager.write_audit_log(
                user_code=user.get('USERCODE'),
                action_type='LOGIN_SUCCESS',
                severity='INFO',
                details=f"Login thành công: {user_role}",
                ip_address=user_ip
            )

            flash(f"Đăng nhập thành công! Chào mừng {user.get('SHORTNAME')}.", 'success')
            
            # Điều hướng
            if user_role in [config.ROLE_ADMIN]: 
                return redirect(url_for('executive_bp.ceo_cockpit_dashboard'))
            else:
                return redirect(url_for('portal_bp.portal_dashboard'))
        else:
            # Ghi log thất bại
            app.db_manager.write_audit_log(
                user_code=username, 
                action_type='LOGIN_FAILED', 
                severity='WARNING', 
                details=f"Sai mật khẩu hoặc User không tồn tại", 
                ip_address=user_ip
            )
            message = "Tên đăng nhập hoặc mật khẩu không đúng."
            flash(message, 'danger')
            
    return render_template('login.html', message=message)

@app.route('/logout')
def logout():
    user_code = session.get('user_code', 'GUEST')
    user_ip = get_user_ip()
    
    # Ghi log trước khi xóa session
    app.db_manager.write_audit_log(
        user_code=user_code, 
        action_type='LOGOUT', 
        severity='INFO', 
        details="User đăng xuất", 
        ip_address=user_ip
    )
    
    session.clear() 
    flash("Bạn đã đăng xuất thành công.", 'success')
    return redirect(url_for('login'))

@app.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    if request.method == 'POST':
        old_password = request.form.get('old_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')
        user_code = session.get('user_code')
        user_ip = get_user_ip()
        
        if not old_password or not new_password:
            flash("Vui lòng nhập đầy đủ thông tin.", 'warning')
            return render_template('change_password.html')
            
        if new_password != confirm_password:
            flash("Mật khẩu mới và xác nhận không khớp.", 'danger')
            return render_template('change_password.html')
            
        # Kiểm tra pass cũ
        query_check = f"SELECT [PASSWORD] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
        user_data = app.db_manager.get_data(query_check, (user_code,))
        
        if user_data:
            current_db_pass = user_data[0]['PASSWORD']
            
            if current_db_pass == old_password:
                # Cập nhật pass mới
                update_query = f"UPDATE {config.TEN_BANG_NGUOI_DUNG} SET [PASSWORD] = ? WHERE USERCODE = ?"
                
                if app.db_manager.execute_non_query(update_query, (new_password, user_code)):
                    # Ghi log
                    app.db_manager.write_audit_log(
                        user_code=user_code, 
                        action_type='CHANGE_PASSWORD', 
                        severity='INFO', 
                        details="Đổi mật khẩu thành công", 
                        ip_address=user_ip
                    )
                    flash("Đổi mật khẩu thành công!", 'success')
                    return redirect(url_for('index'))
                else:
                    flash("Lỗi hệ thống khi cập nhật cơ sở dữ liệu.", 'danger')
            else:
                flash("Mật khẩu hiện tại không chính xác.", 'danger')
        else:
            flash("Không tìm thấy thông tin người dùng.", 'danger')
            
    return render_template('change_password.html')

@app.route('/', methods=['GET'])
@login_required
def index():
    """Trang chủ (Directory)"""
    user_code = session.get('user_code')
    return render_template('index_redesign.html', user_code=user_code)

# =========================================================================
# MAIN
# =========================================================================
if __name__ == '__main__':
    # Chạy ứng dụng
    app.run(debug=True, host='0.0.0.0', port=5000)

    #from waitress import serve
    #serve(
    #        app, 
    #        host='0.0.0.0', 
     #       port=5000, 
      #      threads=6,              # Quan trọng: Xử lý đa luồng
       #     connection_limit=200,   # Giới hạn kết nối
        #    channel_timeout=20      # Timeout (giây)
        #)
    # app.py
# ...



