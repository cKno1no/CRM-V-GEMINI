# app.py
# --- PHIÊN BẢN APPLICATION FACTORY (ĐÃ TÍCH HỢP BỘ LỌC CHUẨN HÓA) ---

from flask import render_template, request, redirect, url_for, flash, session
import config
from datetime import datetime 

# 1. IMPORT TỪ FACTORY VÀ UTILS
from factory import create_app
from utils import login_required, get_user_ip, record_activity # [FIX] Import get_user_ip từ utils

# 2. KHỞI TẠO APP TỪ NHÀ MÁY
app = create_app()

# =========================================================================
# [MỚI] GLOBAL SECURITY MIDDLEWARE: CHẶN TRUY CẬP SAI CỔNG
# =========================================================================
@app.before_request
def check_port_access():
    """
    Kiểm tra quyền truy cập Port trước mỗi request.
    Ngăn chặn user STDP đã login chéo sang Port 5000 và ngược lại.
    """
    # Bỏ qua các file tĩnh (static) để không làm chậm trang
    if request.endpoint and 'static' in request.endpoint:
        return

    # Chỉ kiểm tra nếu user đã đăng nhập
    if session.get('logged_in'):
        # 1. Lấy thông tin Division từ Session (đã lưu lúc login)
        user_division = str(session.get('division') or '').strip().upper()
        
        # 2. Lấy Port hiện tại
        current_port = '80'
        if ':' in request.host:
            current_port = request.host.split(':')[-1]

        # 3. Logic Chặn (Giống hệt lúc Login)
        violation = False
        error_msg = ""

        # Case A: User STDP đi lạc vào Port 5000
        if user_division == 'STDP' and current_port == '5000':
            violation = True
            error_msg = "⛔ TRUY CẬP BỊ TỪ CHỐI: Tài khoản Hà Nội (STDP) không được phép truy cập cổng Sài Gòn (5000)."

        # Case B: User SG (Non-STDP) đi lạc vào Port 5050
        elif user_division != 'STDP' and current_port == '5050':
            violation = True
            error_msg = "⛔ TRUY CẬP BỊ TỪ CHỐI: Tài khoản Sài Gòn không được phép truy cập cổng Hà Nội (5050)."

        # XỬ LÝ VI PHẠM
        if violation:
            # Ghi log cảnh báo
            app.db_manager.write_audit_log(
                user_code=session.get('user_code', 'UNKNOWN'), 
                action_type='SECURITY_VIOLATION', 
                severity='WARNING', 
                details=f"Cố tình truy cập sai cổng {current_port} (Div: {user_division})", 
                ip_address=get_user_ip() # [FIX] Dùng hàm từ utils
            )
            
            # Xóa session ngay lập tức (Force Logout)
            session.clear()
            
            # Trả về trang lỗi hoặc redirect về Login với thông báo
            flash(error_msg, 'danger')
            return redirect(url_for('login'))

# =========================================================================
# 3. GLOBAL TEMPLATE FILTERS (BỘ LỌC CHUẨN HÓA DỮ LIỆU)
# =========================================================================

@app.template_filter('format_tr')
def format_tr(value):
    """[CHUẨN HÓA TIỀN TỆ] - Format Triệu (tr)"""
    if value is None or value == '': return "0 tr"
    try:
        val = float(value)
        if val == 0: return "0 tr"
        in_million = val / 1000000.0
        if abs(in_million) >= 1000: return "{:,.0f} tr".format(in_million)
        formatted = "{:,.1f}".format(in_million)
        if formatted.endswith('.0'): return "{:,.0f} tr".format(in_million)
        return f"{formatted} tr"
    except: return "0 tr"

@app.template_filter('format_date')
def format_date(value):
    """[CHUẨN HÓA NGÀY THÁNG] - dd/mm/yyyy"""
    if not value: return "-"
    if isinstance(value, datetime) or hasattr(value, 'strftime'):
        return value.strftime('%d/%m/%Y')
    if isinstance(value, str):
        try:
            if '-' in value:
                date_obj = datetime.strptime(value[:10], '%Y-%m-%d')
                return date_obj.strftime('%d/%m/%Y')
            elif '/' in value: return value 
        except: pass
    return str(value)

@app.template_filter('format_number')
def format_number(value):
    """[CHUẨN HÓA SỐ LƯỢNG]"""
    if value is None or value == '': return "-"
    try:
        val = float(value)
        if val == 0: return "0"
        return "{:,.0f}".format(val)
    except: return str(value)

# =========================================================================
# ROUTES XÁC THỰC (LOGIN / LOGOUT / PASSWORD)
# =========================================================================

@app.route('/login', methods=['GET', 'POST'])
@record_activity('LOGIN') # <--- CHỈ CẦN THÊM DÒNG NÀY LÀ XONG
def login():
    # Nếu đã login, điều hướng luôn
    if session.get('logged_in'):
        user_role = session.get('user_role', '').strip().upper()
        if user_role in [config.ROLE_ADMIN]: 
            return redirect(url_for('executive_bp.ceo_cockpit_page'))
        return redirect(url_for('portal_bp.portal_dashboard'))

    message = None
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user_ip = get_user_ip() # [FIX] Dùng hàm từ utils

        # Truy vấn lấy thông tin User
        query = f"""
            SELECT TOP 1 [USERCODE], [USERNAME], [SHORTNAME], [ROLE], [CAP TREN], [BO PHAN], [CHUC VU], [PASSWORD], [Division], [THEME]
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE ([USERCODE] = ? OR [USERNAME] = ?) AND [PASSWORD] = ?
        """
        user_data = app.db_manager.get_data(query, (username, username, password))

        if user_data:
            user = user_data[0]
            
            # --- [LOGIC MỚI] KIỂM TRA PORT & PHÂN LUỒNG ---
            current_port = '80'
            if ':' in request.host:
                current_port = request.host.split(':')[-1]
            
            user_division = str(user.get('Division') or '').strip().upper()
            
            # Rule chặn
            if user_division == 'STDP' and current_port == '5000':
                message = "⚠️ SAI CỔNG TRUY CẬP: Tài khoản Hà Nội (STDP) vui lòng đăng nhập ở cổng 5050."
                flash(message, 'danger')
                return render_template('login.html', message=message)

            if user_division != 'STDP' and current_port == '5050':
                message = "⚠️ SAI CỔNG TRUY CẬP: Tài khoản Sài Gòn vui lòng đăng nhập ở cổng 5000."
                flash(message, 'danger')
                return render_template('login.html', message=message)
            
            # --- THIẾT LẬP SESSION ---
            session.clear() 
            
            session['logged_in'] = True
            session.permanent = True
            session['user_code'] = user.get('USERCODE')
            session['username'] = user.get('USERNAME')
            session['user_shortname'] = user.get('SHORTNAME')
            session['division'] = user_division
            user_role = str(user.get('ROLE') or '').strip().upper()
            session['user_role'] = user_role
            session['theme'] = user.get('THEME') or 'light'
            session['cap_tren'] = user.get('CAP TREN', '')
            session['bo_phan'] = "".join((user.get('BO PHAN') or '').split()).upper()
            session['chuc_vu'] = str(user.get('CHUC VU') or '').strip().upper()
            
            session['security_hash'] = user.get('PASSWORD')

            # Tải quyền hạn
            if user_role == config.ROLE_ADMIN:
                session['permissions'] = ['__ALL__'] 
            else:
                perm_query = f"SELECT FeatureCode FROM {config.TABLE_SYS_PERMISSIONS} WHERE RoleID = ?"
                perms_data = app.db_manager.get_data(perm_query, (user_role,))
                session['permissions'] = [row['FeatureCode'] for row in perms_data]

            # Ghi Log
            app.db_manager.write_audit_log(
                user_code=user.get('USERCODE'),
                action_type='LOGIN_SUCCESS',
                severity='INFO',
                details=f"Login thành công tại Port {current_port}: {user_role}",
                ip_address=user_ip
            )

            flash(f"Đăng nhập thành công! Chào mừng {user.get('SHORTNAME')}.", 'success')
            
            if user_role in [config.ROLE_ADMIN]: 
                return redirect(url_for('executive_bp.ceo_cockpit_page'))
            else:
                return redirect(url_for('portal_bp.portal_dashboard'))
        else:
            # Login thất bại
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
    user_ip = get_user_ip() # [FIX] Dùng hàm từ utils
    
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
        user_ip = get_user_ip() # [FIX] Dùng hàm từ utils
        
        if not old_password or not new_password:
            flash("Vui lòng nhập đầy đủ thông tin.", 'warning')
            return render_template('change_password.html')
            
        if new_password != confirm_password:
            flash("Mật khẩu mới và xác nhận không khớp.", 'danger')
            return render_template('change_password.html')
            
        query_check = f"SELECT [PASSWORD] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
        user_data = app.db_manager.get_data(query_check, (user_code,))
        
        if user_data:
            current_db_pass = user_data[0]['PASSWORD']
            
            if current_db_pass == old_password:
                update_query = f"UPDATE {config.TEN_BANG_NGUOI_DUNG} SET [PASSWORD] = ? WHERE USERCODE = ?"
                
                if app.db_manager.execute_non_query(update_query, (new_password, user_code)):
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

# 1. Inject Config vào Context để base.html đọc được biến 'config.DIVISOR_VIEW'
@app.context_processor
def inject_global_vars():
    return dict(config=config)


# =========================================================================
# MAIN
# =========================================================================
if __name__ == '__main__':
    print("!!! CẢNH BÁO: ĐANG CHẠY CHẾ ĐỘ DEV. KHÔNG DÙNG CHO PRODUCTION !!!")
    app.run(debug=True, host='0.0.0.0', port=5000)