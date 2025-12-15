# app.py
# --- PHIÊN BẢN APPLICATION FACTORY (ĐÃ TÍCH HỢP BỘ LỌC CHUẨN HÓA) ---

from flask import render_template, request, redirect, url_for, flash, session
import config
from datetime import datetime # Import thêm để xử lý ngày tháng

# 1. IMPORT TỪ FACTORY VÀ UTILS
from factory import create_app
from utils import login_required

# 2. KHỞI TẠO APP TỪ NHÀ MÁY
app = create_app()

# =========================================================================
# 3. GLOBAL TEMPLATE FILTERS (BỘ LỌC CHUẨN HÓA DỮ LIỆU)
# =========================================================================

@app.template_filter('format_tr')
def format_tr(value):
    """
    [CHUẨN HÓA TIỀN TỆ]
    Chuyển đổi số thành đơn vị Triệu (tr) với định dạng #,###.
    Ví dụ: 
      - 1,200,000 -> "1.2 tr"
      - 1,500,000,000 -> "1,500 tr"
      - 0 hoặc None -> "0 tr"
    """
    if value is None or value == '':
        return "0 tr"
    try:
        val = float(value)
        if val == 0:
            return "0 tr"
            
        # Chia cho 1 triệu
        in_million = val / 1000000.0
        
        # Logic hiển thị:
        # - Dùng dấu phẩy (,) ngăn cách hàng nghìn (Chuẩn IT/Quốc tế)
        # - Dùng dấu chấm (.) ngăn cách thập phân
        
        # Nếu số >= 1 tỷ (1000 triệu) -> Không cần số lẻ thập phân
        if abs(in_million) >= 1000:
            return "{:,.0f} tr".format(in_million)
        
        # Nếu < 1 tỷ -> Lấy 1 số thập phân
        formatted = "{:,.1f}".format(in_million)
        
        # Nếu kết quả là số chẵn (vd: 120.0) -> Bỏ đuôi .0 thành 120
        if formatted.endswith('.0'):
            return "{:,.0f} tr".format(in_million)
            
        return f"{formatted} tr"
    except:
        return "0 tr"

@app.template_filter('format_date')
def format_date(value):
    """
    [CHUẨN HÓA NGÀY THÁNG]
    Chuyển đổi mọi định dạng ngày về: dd/mm/yyyy
    Ví dụ: 2025-12-15 -> 15/12/2025
    """
    if not value:
        return "-"
    
    # Nếu là object datetime của Python
    if isinstance(value, datetime) or hasattr(value, 'strftime'):
        return value.strftime('%d/%m/%Y')
    
    # Nếu là chuỗi (string) từ SQL
    if isinstance(value, str):
        try:
            # Thử parse các dạng phổ biến
            if '-' in value:
                # Dạng YYYY-MM-DD
                date_obj = datetime.strptime(value[:10], '%Y-%m-%d')
                return date_obj.strftime('%d/%m/%Y')
            elif '/' in value:
                # Nếu đã có dạng / thì giữ nguyên hoặc format lại nếu cần
                return value 
        except:
            pass
    
    return str(value)

@app.template_filter('format_number')
def format_number(value):
    """
    [CHUẨN HÓA SỐ LƯỢNG]
    Dành cho các cột Số lượng, Số lần... (Không có đơn vị tiền tệ)
    Ví dụ: 1234 -> "1,234" (Null -> "-")
    """
    if value is None or value == '':
        return "-"
    try:
        val = float(value)
        if val == 0: return "0"
        # Chỉ dùng dấu phẩy ngăn cách hàng nghìn, làm tròn số nguyên
        return "{:,.0f}".format(val)
    except:
        return str(value)

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

        query = f"""
            SELECT TOP 1 [USERCODE], [USERNAME], [SHORTNAME], [ROLE], [CAP TREN], [BO PHAN], [CHUC VU], [PASSWORD], [Division], [THEME]
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE ([USERCODE] = ? OR [USERNAME] = ?) AND [PASSWORD] = ?
        """
        user_data = app.db_manager.get_data(query, (username, username, password))

        if user_data:
            user = user_data[0]
            
            # Thiết lập Session
            session['logged_in'] = True
            session.permanent = True
            session['user_code'] = user.get('USERCODE')
            session['username'] = user.get('USERNAME')
            session['user_shortname'] = user.get('SHORTNAME')
            session['division'] = user.get('Division') 
            user_role = str(user.get('ROLE') or '').strip().upper()
            session['user_role'] = user_role
            session['theme'] = user.get('THEME') or 'light'
            session['cap_tren'] = user.get('CAP TREN', '')
            session['bo_phan'] = "".join((user.get('BO PHAN') or '').split()).upper()
            session['chuc_vu'] = str(user.get('CHUC VU') or '').strip().upper()
            
            # Security Stamp
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

# =========================================================================
# MAIN
# =========================================================================
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)