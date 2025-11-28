# utils.py

from flask import session, redirect, url_for, flash, request
from functools import wraps

# Chuyển login_required từ app.py sang đây
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # 1. Kiểm tra session cơ bản (User chưa đăng nhập)
        if not session.get('logged_in'):
            flash("Vui lòng đăng nhập để truy cập.", 'info')
            return redirect(url_for('login', next=request.url))
            
        # 2. [MỚI] Kiểm tra nâng cao: Đối chiếu Session với Database
        user_code = session.get('user_code')
        current_session_hash = session.get('security_hash') 
        
        # Chỉ kiểm tra nếu đã có user_code và security_hash
        if user_code and current_session_hash:
            try:
                # Truy cập db_manager thông qua current_app (tránh import vòng lặp)
                db = current_app.db_manager
                
                # Truy vấn mật khẩu hiện tại trong DB
                query = f"SELECT [PASSWORD] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
                data = db.get_data(query, (user_code,))
                
                # Nếu không tìm thấy user HOẶC mật khẩu trong DB đã khác mật khẩu trong Session
                if not data or data[0]['PASSWORD'] != current_session_hash:
                    session.clear() # Xóa sạch session -> Đăng xuất
                    flash("Phiên đăng nhập hết hạn hoặc mật khẩu đã được thay đổi. Vui lòng đăng nhập lại.", "warning")
                    return redirect(url_for('login'))
                    
            except Exception as e:
                print(f"Lỗi kiểm tra bảo mật session: {e}")
                # Tùy chọn: Có thể cho qua hoặc logout nếu lỗi DB để an toàn

        return f(*args, **kwargs)
    return decorated_function

# Chuyển truncate_content (từ db_manager.py hoặc app.py) sang đây
def truncate_content(text, max_lines=5):
    """
    Cắt nội dung văn bản dài thành tối đa N dòng, giữ định dạng xuống dòng và thêm '...'.
    """
    if not text:
        return ""
        
    lines = text.split('\n')
    
    if len(lines) <= max_lines:
        return text 

    truncated_lines = lines[:max_lines]
    return '\n'.join(truncated_lines) + '...'