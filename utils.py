# utils.py
from flask import session, redirect, url_for, flash, request, current_app, jsonify
from functools import wraps
import config
import os
from datetime import datetime
from werkzeug.utils import secure_filename


def get_user_ip():
    """Lấy IP người dùng, hỗ trợ cả trường hợp qua Proxy/Load Balancer"""
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr
# --- Decorator Login ---
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('logged_in'):
            return redirect(url_for('login', next=request.url))
            
        user_code = session.get('user_code')
        security_hash = session.get('security_hash')
        
        if user_code and security_hash:
            try:
                db = current_app.db_manager
                query = f"SELECT [PASSWORD] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
                data = db.get_data(query, (user_code,))
                
                if not data or data[0]['PASSWORD'] != security_hash:
                    session.clear()
                    flash("Phiên đăng nhập hết hạn.", "warning")
                    return redirect(url_for('login'))
            except Exception:
                pass 
                
        return f(*args, **kwargs)
    return decorated_function

# --- Hàm Helper Xử lý Chuỗi ---
def truncate_content(text, max_lines=5):
    if not text: return ""
    lines = text.split('\n')
    if len(lines) <= max_lines: return text 
    return '\n'.join(lines[:max_lines]) + '...'

# --- Hàm Helper Xử lý File (MỚI THÊM) ---
def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in config.ALLOWED_EXTENSIONS

def save_uploaded_files(files):
    """Xử lý lưu các file và trả về chuỗi tên file ngăn cách bởi dấu phẩy."""
    saved_filenames = []
    
    if not hasattr(config, 'UPLOAD_FOLDER') or not config.UPLOAD_FOLDER:
        return ""
        
    if not os.path.exists(config.UPLOAD_FOLDER):
        os.makedirs(config.UPLOAD_FOLDER)
        
    now_str = datetime.now().strftime("%Y%m%d%H%M%S")

    for file in files:
        if file and allowed_file(file.filename):
            filename_clean = secure_filename(file.filename)
            unique_filename = f"{now_str}_{filename_clean}"
            try:
                file.save(os.path.join(config.UPLOAD_FOLDER, unique_filename))
                saved_filenames.append(unique_filename)
            except Exception as e:
                print(f"Lỗi lưu file {filename_clean}: {e}")
                
    return ', '.join(saved_filenames)

def permission_required(feature_code):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if not session.get('logged_in'):
                return redirect(url_for('login'))
            
            # 1. ADMIN luôn qua
            if session.get('user_role') == config.ROLE_ADMIN:
                return f(*args, **kwargs)

            # 2. Check quyền
            if feature_code not in session.get('permissions', []):
                msg = f"Bạn không có quyền truy cập chức năng này ({feature_code})."
                
                # [LOGIC MỚI QUAN TRỌNG]
                # A. Nếu là API Call (AJAX/Fetch) -> Trả về JSON lỗi 403 Forbidden
                # Giúp Frontend nhận biết lỗi mà không bị redirect sang trang HTML
                if request.is_json or request.path.startswith('/api/'):
                    return jsonify({'success': False, 'message': msg}), 403
                
                # B. Nếu là truy cập Trang (GET) -> Flash message và Reload lại trang trước đó
                flash(msg, "danger")
                
                # request.referrer chứa URL của trang trước đó mà người dùng đang đứng
                # Nếu có referrer -> Redirect về đó ("Load lại tại chỗ")
                # [FIXED] Nếu không có referrer (gõ trực tiếp URL) -> Fallback về trang chủ (index)
                return redirect(request.referrer or url_for('index'))
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator   