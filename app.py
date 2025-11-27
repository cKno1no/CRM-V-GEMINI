# app.py - FIX VÒNG LẶP IMPORT BẰNG CÁCH ĐỊNH NGHĨA DECORATOR SỚM

from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, session, Response, Blueprint
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from functools import wraps # <--- CẦN CÓ WRAPS
from operator import itemgetter
from db_manager import safe_float, DBManager 
from config import TEN_BANG_NGUOI_DUNG # Cần cho logic login

import os
import io 
import redis 
import json 
from werkzeug.utils import secure_filename 

# =========================================================================
# I. ĐỊNH NGHĨA DECORATOR VÀ HÀM CỐT LÕI (FIXED POSITION)
# =========================================================================

# IMPORT DECORATOR TỪ UTILS
from utils import login_required # <--- ĐÃ SỬA

# =========================================================================
# II. IMPORT CONFIG, BLUEPRINTS VÀ SERVICES (Sau khi login_required được định nghĩa)
# =========================================================================
import config
from sales_service import SalesService, InventoryService
from customer_service import CustomerService 
from quotation_approval_service import QuotationApprovalService 
from sales_order_approval_service import SalesOrderApprovalService 
from services.sales_lookup_service import SalesLookupService 
from services.task_service import TaskService
from services.chatbot_service import ChatbotService 
from services.ar_aging_service import ARAgingService 
from services.delivery_service import DeliveryService 
from services.budget_service import BudgetService

# Import các Blueprints mới
from blueprints.crm_bp import crm_bp
from blueprints.kpi_bp import kpi_bp
from blueprints.portal_bp import portal_bp # Import mới
from blueprints.approval_bp import approval_bp
from blueprints.delivery_bp import delivery_bp
from blueprints.task_bp import task_bp
from blueprints.chat_bp import chat_bp
from blueprints.lookup_bp import lookup_bp
from blueprints.budget_bp import budget_bp
from blueprints.commission_bp import commission_bp
from blueprints.executive_bp import executive_bp

# =========================================================================
# III. KHỞI TẠO ỨNG DỤNG VÀ DỊCH VỤ
# =========================================================================
app = Flask(__name__, static_url_path='/attachments', static_folder='attachments') 
app.secret_key = config.APP_SECRET_KEY
app.config['UPLOAD_FOLDER'] = config.UPLOAD_FOLDER_PATH
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=6)

# Khởi tạo REDIS
try:
    redis_client = redis.Redis(host=config.REDIS_HOST, port=config.REDIS_PORT, db=0, decode_responses=True)
    redis_client.ping()
except Exception as e:
    redis_client = None 

# Khởi tạo DB Manager
db_manager = DBManager()
# === THÊM DÒNG NÀY ===
# Gắn db_manager vào app để các blueprint có thể truy cập qua current_app
app.db_manager = db_manager

print("="*50)
print(f"!!! CHAN DOAN KET NOI (PYTHON) !!!")
print(f"!!! SERVER: {config.DB_SERVER}")
print(f"!!! DATABASE: {config.DB_NAME}")
print(f"!!! USER: {config.DB_UID}")
print("="*50)

# Khởi tạo các Tầng Dịch vụ (Service Layer)
sales_service = SalesService(db_manager)
inventory_service = InventoryService(db_manager)
customer_service = CustomerService(db_manager)
approval_service = QuotationApprovalService(db_manager)
order_approval_service = SalesOrderApprovalService(db_manager)
lookup_service = SalesLookupService(db_manager)
task_service = TaskService(db_manager)

ar_aging_service = ARAgingService(db_manager)
delivery_service = DeliveryService(db_manager)
# Khởi tạo ChatbotService (TRUYỀN THÊM delivery_service)
chatbot_service = ChatbotService(lookup_service, customer_service, delivery_service, redis_client) # <-- THÊM delivery_service
budget_service = BudgetService(db_manager)

# =========================================================================
# IV. HÀM AUTH & HELPER CỐT LÕI (Giữ lại ở Core)
# =========================================================================

def get_user_ip():
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

def save_uploaded_files(files):
    """Xử lý lưu các file và trả về chuỗi tên file ngăn cách bởi dấu phẩy."""
    saved_filenames = []
    
    # Đảm bảo thư mục upload tồn tại
    if not hasattr(config, 'UPLOAD_FOLDER') or not config.UPLOAD_FOLDER:
        print("LỖI CẤU HÌNH: Thiếu config.UPLOAD_FOLDER")
        return ""
        
    if not os.path.exists(config.UPLOAD_FOLDER):
        os.makedirs(config.UPLOAD_FOLDER)
        
    now_str = datetime.now().strftime("%Y%m%d%H%M%S")

    for file in files:
        if file and allowed_file(file.filename):
            # Tạo tên file duy nhất: TIMESTAMP_FILENAME
            filename_clean = secure_filename(file.filename)
            unique_filename = f"{now_str}_{filename_clean}"
            
            try:
                file.save(os.path.join(config.UPLOAD_FOLDER, unique_filename))
                saved_filenames.append(unique_filename)
            except Exception as e:
                print(f"LỖI LƯU FILE {filename_clean}: {e}")
                # Tiếp tục với các file khác
                
    return ', '.join(saved_filenames)

# =========================================================================
# V. ROUTES CHÍNH (LOGIN, LOGOUT, INDEX)
# =========================================================================

@app.context_processor
def inject_user():
    """Tạo đối tượng current_user giả để truy cập thông tin user trong template."""
    return dict(current_user={'is_authenticated': session.get('logged_in', False),
                             'usercode': session.get('user_code'),
                             'username': session.get('username'),
                             'shortname': session.get('user_shortname'),
                             'role': session.get('user_role'),
                             'cap_tren': session.get('cap_tren'),
                             'bo_phan': session.get('bo_phan')})

@app.route('/login', methods=['GET', 'POST'])
def login():
    if session.get('logged_in'):
        # Đã đăng nhập, chuyển đến trang chủ (index_redesign.html)
        return redirect(url_for('portal_bp.portal_dashboard')) 

    message = None
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        # === SỬA LỖI: Lấy IP ngay lập tức ===
        # Phải định nghĩa user_ip ở đây để cả 2 trường hợp (thành công/thất bại) đều dùng được
        user_ip = get_user_ip() 
        # === KẾT THÚC SỬA LỖI ===

        # GỌI DBManager ĐỂ XỬ LÝ LOGIN.
        # 1. SỬA CÂU SQL: Thêm [CHUC VU] vào danh sách cột cần lấy
        query = f"""
            SELECT TOP 1 [USERCODE], [USERNAME], [SHORTNAME], [ROLE], [CAP TREN], [BO PHAN], [CHUC VU]
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE ([USERCODE] = ? OR [USERNAME] = ?) AND [PASSWORD] = ?
        """
        user_data = db_manager.get_data(query, (username, username, password))

        if user_data:
            user = user_data[0]
            # --- CẬP NHẬT SESSION VÀ LẤY BỘ PHẬN ---
            session['logged_in'] = True
            session.permanent = True # <--- BẮT BUỘC ĐỂ CẤU HÌNH PERMANENT_SESSION_LIFETIME CÓ HIỆU LỰC
            session['user_code'] = user.get('USERCODE')
            session['username'] = user.get('USERNAME')
            session['user_shortname'] = user.get('SHORTNAME')
            session['user_role'] = user.get('ROLE')
            session['cap_tren'] = user.get('CAP TREN', '')

            bo_phan_raw = user.get('BO PHAN', '')
            # 2. .split() sẽ cắt chuỗi dựa trên BẤT KỲ ký tự trắng nào (space, tab, nbsp...)
            #    VD: ['6.', 'KTTC']
            # 3. .join() ghép chúng lại không có khoảng trắng
            #    VD: "6.KTTC"
            normalized_bo_phan = "".join(bo_phan_raw.split()).upper()
            session['bo_phan'] = normalized_bo_phan

            # 2. THÊM DÒNG NÀY: Lưu Chức vụ vào Session để dùng sau này
            # (Dùng .strip().upper() để chuẩn hóa tránh lỗi khoảng trắng/hoa thường)
            session['chuc_vu'] = str(user.get('CHUC VU') or '').strip().upper()
            # ----------------------------------------

            # --- GHI LOG (Requirement 1: Login thành công) ---
            db_manager.write_audit_log(
                user_code=user.get('USERCODE'),
                action_type='LOGIN_SUCCESS',
                severity='INFO',
                details=f"Login thành công với vai trò: {user.get('ROLE')}, {normalized_bo_phan}",
                ip_address=user_ip
            )
            # --- KẾT THÚC GHI LOG ---

            flash(f"Đăng nhập thành công! Chào mừng {user.get('SHORTNAME')}.", 'success')
            
            return redirect(url_for('portal_bp.portal_dashboard'))
            # ---------------------------------------------
        else:
            # --- GHI LOG (Requirement 3: Cảnh báo Login thất bại) ---
            db_manager.write_audit_log(
                user_code=username, # Ghi lại username đã cố gắng login
                action_type='LOGIN_FAILED',
                severity='WARNING', # Mức độ Cảnh báo
                details=f"Đăng nhập thất bại. Password: {password}",
                ip_address=user_ip
            )
            # --- KẾT THÚC GHI LOG ---
            
            message = "Tên đăng nhập hoặc mật khẩu không chính xác."
            flash(message, 'danger')
            
    return render_template('login.html', message=message)

@app.route('/change_password', methods=['GET', 'POST'])
@login_required
def change_password():
    """Chức năng đổi mật khẩu (Lưu Text Thuần - Không Hash)."""
    
    if request.method == 'POST':
        old_password = request.form.get('old_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')
        
        user_code = session.get('user_code')
        
        # 1. Kiểm tra input
        if not old_password or not new_password:
            flash("Vui lòng nhập đầy đủ thông tin.", 'warning')
            return render_template('change_password.html')
            
        if new_password != confirm_password:
            flash("Mật khẩu mới và xác nhận không khớp.", 'danger')
            return render_template('change_password.html')
            
        # 2. Kiểm tra mật khẩu cũ (Truy vấn trực tiếp từ DB)
        # Lưu ý: Sử dụng config.TEN_BANG_NGUOI_DUNG đã có sẵn trong app.py
        query_check = f"SELECT [PASSWORD] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
        user_data = db_manager.get_data(query_check, (user_code,))
        
        if user_data:
            current_db_pass = user_data[0]['PASSWORD']
            
            # So sánh text thuần (Theo yêu cầu)
            if current_db_pass == old_password:
                # 3. Cập nhật mật khẩu mới
                update_query = f"UPDATE {config.TEN_BANG_NGUOI_DUNG} SET [PASSWORD] = ? WHERE USERCODE = ?"
                
                if db_manager.execute_non_query(update_query, (new_password, user_code)):
                    # Ghi Log Audit
                    db_manager.write_audit_log(
                        user_code=user_code,
                        action_type='CHANGE_PASSWORD',
                        severity='INFO',
                        details="Đổi mật khẩu thành công",
                        ip_address=get_user_ip()
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


@app.route('/logout')
def logout():
    """Logic Đăng xuất."""

    user_code = session.get('user_code', 'GUEST')
    user_ip = get_user_ip() 
    
    # 1. GHI LOG LOGOUT (BỔ SUNG)
    db_manager.write_audit_log(
        user_code=user_code,
        action_type='LOGOUT',
        severity='INFO',
        details="User đăng xuất",
        ip_address=user_ip
    )
    # 1. Xóa tất cả các biến session
    session.clear() 
    
    # 2. Hiển thị thông báo (tùy chọn)
    flash("Bạn đã đăng xuất thành công.", 'success')
    
    # 3. LUÔN TRẢ VỀ CHUYỂN HƯỚNG
    return redirect(url_for('login'))

@app.route('/', methods=['GET'])
@login_required
def index():
    """Trang chủ (Directory) hiển thị danh sách các Dashboard/Module."""
    user_code = session.get('user_code')
    return render_template('index_redesign.html', user_code=user_code)


# =========================================================================
# VI. ĐĂNG KÝ BLUEPRINTS
# =========================================================================

app.register_blueprint(crm_bp)
app.register_blueprint(kpi_bp)
app.register_blueprint(approval_bp)
app.register_blueprint(delivery_bp)
app.register_blueprint(task_bp)
app.register_blueprint(lookup_bp)
app.register_blueprint(chat_bp)
app.register_blueprint(portal_bp) # Đăng ký
app.register_blueprint(budget_bp)
app.register_blueprint(commission_bp)
app.register_blueprint(executive_bp)


if __name__ == '__main__':
    
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



