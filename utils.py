# utils.py

from flask import session, redirect, url_for, flash, request
from functools import wraps

# Chuyển login_required từ app.py sang đây
def login_required(f):
    """Decorator kiểm tra session login thủ công."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if session.get('logged_in') != True:
            flash("Vui lòng đăng nhập để truy cập trang này.", 'info')
            return redirect(url_for('login', next=request.url))
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