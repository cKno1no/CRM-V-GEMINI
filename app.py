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

# Import các Blueprints mới
from blueprints.crm_bp import crm_bp
from blueprints.kpi_bp import kpi_bp

from blueprints.approval_bp import approval_bp
from blueprints.delivery_bp import delivery_bp
from blueprints.task_bp import task_bp
from blueprints.chat_bp import chat_bp
from blueprints.lookup_bp import lookup_bp


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

print("="*50)
print(f"!!! CHẨN ĐOÁN KẾT NỐI (PYTHON) !!!")
print(f"!!! Đang kết nối tới SERVER: {config.DB_SERVER}")
print(f"!!! Đang kết nối tới DATABASE: {config.DB_NAME}")
print(f"!!! Đang sử dụng USER: {config.DB_UID}")
print("="*50)

# Khởi tạo các Tầng Dịch vụ (Service Layer)
sales_service = SalesService(db_manager)
inventory_service = InventoryService(db_manager)
customer_service = CustomerService(db_manager)
approval_service = QuotationApprovalService(db_manager)
order_approval_service = SalesOrderApprovalService(db_manager)
lookup_service = SalesLookupService(db_manager)
task_service = TaskService(db_manager)
chatbot_service = ChatbotService(lookup_service, customer_service, redis_client)
ar_aging_service = ARAgingService(db_manager)
delivery_service = DeliveryService(db_manager)


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
                             'cap_tren': session.get('cap_tren')})

@app.route('/login', methods=['GET', 'POST'])
def login():
    if session.get('logged_in'):
        # Đã đăng nhập, chuyển đến trang chủ (index_redesign.html)
        return redirect(url_for('index')) 

    message = None
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        # === SỬA LỖI: Lấy IP ngay lập tức ===
        # Phải định nghĩa user_ip ở đây để cả 2 trường hợp (thành công/thất bại) đều dùng được
        user_ip = get_user_ip() 
        # === KẾT THÚC SỬA LỖI ===

        # GỌI DBManager ĐỂ XỬ LÝ LOGIN.
        query = f"""
            SELECT TOP 1 [USERCODE], [USERNAME], [SHORTNAME], [ROLE], [CAP TREN], [BO PHAN]
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
            session['bo_phan'] = user.get('BO PHAN', '').strip().upper() 
            # ----------------------------------------

            # --- GHI LOG (Requirement 1: Login thành công) ---
            db_manager.write_audit_log(
                user_code=user.get('USERCODE'),
                action_type='LOGIN_SUCCESS',
                severity='INFO',
                details=f"Login thành công với vai trò {user.get('ROLE')}",
                ip_address=user_ip
            )
            # --- KẾT THÚC GHI LOG ---

            flash(f"Đăng nhập thành công! Chào mừng {user.get('SHORTNAME')}.", 'success')
            
            # --- LOGIC CHUYỂN HƯỚNG MỚI (YÊU CẦU A, B, C) ---
            user_role = session.get('user_role', '').strip().upper()
            department = session.get('bo_phan', '')

            # a/ Nếu là admin, load index_redesign (index.html)
            if user_role == 'ADMIN':
                return redirect(url_for('index'))
            
            # b/ Nếu thuộc [GD - NGUOI DUNG].[BO PHAN] = 02. KINH DOANH, thì load realtime_dashboard
            elif department == '2. KINH DOANH':
                return redirect(url_for('realtime_dashboard'))
                
            # c/ Còn lại, load dashboard
            else:
                return redirect(url_for('dashboard_reports'))
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

@app.route('/logout')
def logout():
    """Logic Đăng xuất."""
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

if __name__ == '__main__':
    from waitress import serve
    serve(app, host='0.0.0.0', port=5000)