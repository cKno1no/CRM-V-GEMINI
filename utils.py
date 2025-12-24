# utils.py
from flask import session, redirect, url_for, flash, request, current_app, jsonify
from functools import wraps
import config
import os
from datetime import datetime
from werkzeug.utils import secure_filename

# [FIX] Đã thêm hàm này để khắc phục lỗi 'get_user_ip not found'
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
                # Sử dụng tham số binding để tránh SQL Injection
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

# --- Hàm Helper Xử lý File ---
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
                current_app.logger.error(f"Lỗi lưu file {filename_clean}: {e}")
                
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
                
                # A. Nếu là API Call (AJAX/Fetch) -> Trả về JSON lỗi 403 Forbidden
                if request.is_json or request.path.startswith('/api/'):
                    return jsonify({'success': False, 'message': msg}), 403
                
                # B. Nếu là truy cập Trang (GET) -> Flash message và Reload lại trang trước đó
                flash(msg, "danger")
                
                # Redirect về referrer hoặc trang chủ
                return redirect(request.referrer or url_for('index'))
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def record_activity(activity_code):
    """
    Decorator để tự động ghi điểm XP khi thực hiện hành động thành công.
    Chỉ ghi nhận khi Request là POST (thao tác dữ liệu) và không có lỗi xảy ra.
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # 1. Chạy hàm gốc (VD: Tạo báo cáo, Lưu đơn hàng...)
            response = f(*args, **kwargs)
            
            # 2. Sau khi chạy xong, kiểm tra nếu là POST và thành công (không lỗi)
            # (Thường các hàm POST thành công sẽ trả về Redirect 302 hoặc JSON 200)
            if request.method == 'POST':
                try:
                    # Lấy user hiện tại
                    user_code = session.get('user_code')
                    if user_code:
                        # Gọi Service ghi log (Lazy import để tránh vòng lặp)
                        from flask import current_app
                        if hasattr(current_app, 'gamification_service'):
                            current_app.gamification_service.log_activity(user_code, activity_code)
                except Exception as e:
                    # Nếu lỗi ghi điểm thì bỏ qua, không làm crash app chính
                    print(f"⚠️ Gamification Error: {e}")
            
            return response
        return decorated_function
    return decorator