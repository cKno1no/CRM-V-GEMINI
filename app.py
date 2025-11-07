# app.py - Backend Python (VERSION HOÀN CHỈNH: ĐIỀU HƯỚNG LOGIN -> INDEX)

from flask import Flask, render_template, request, redirect, url_for, jsonify, flash, session, Response
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
from functools import wraps
import os
from werkzeug.utils import secure_filename 
from operator import itemgetter
from db_manager import safe_float
from config import TEN_BANG_NGUOI_DUNG, TEN_BANG_LOAI_BAO_CAO, TEN_BANG_BAO_CAO
# =========================================================================
# IMPORT TỪ CÁC MODULE MỚI (Sử dụng import trực tiếp)
# =========================================================================
import config
from db_manager import DBManager, safe_float, parse_filter_string, evaluate_condition
from sales_service import SalesService, InventoryService
from customer_service import CustomerService 
from quotation_approval_service import QuotationApprovalService 
from sales_order_approval_service import SalesOrderApprovalService 

# --- BỔ SUNG IMPORTS MỚI ---
# TÌM VÀ SỬA ĐOẠN IMPORT NÀY:
from services.sales_lookup_service import SalesLookupService # <--- SỬA THÀNH CÚ PHÁP ĐÚNG
# ...
from routes import sales_bp # <--- IMPORT BLUEPRINT TRA CỨU
from services.task_service import TaskService
# =========================================================================
# KHỞI TẠO ỨNG DỤNG VÀ DỊCH VỤ (SERVICE INJECTION)
# =========================================================================
app = Flask(__name__, static_url_path='/attachments', static_folder='attachments') 
app.secret_key = config.APP_SECRET_KEY
app.config['UPLOAD_FOLDER'] = config.UPLOAD_FOLDER_PATH
# BỔ SUNG HOẶC SỬA ĐỔI CẤU HÌNH NÀY:
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=6) # <-- Đặt thời gian sống tối đa là 6 tiếng
# 1. KHỞI TẠO TẦNG TRUY CẬP DỮ LIỆU
db_manager = DBManager()

# 2. KHỞI TẠO CÁC TẦNG DỊCH VỤ
sales_service = SalesService(db_manager)
inventory_service = InventoryService(db_manager)
customer_service = CustomerService(db_manager) # <--- KHỞI TẠO SERVICE MỚI
approval_service = QuotationApprovalService(db_manager) # <--- KHỞI TẠO SERVICE DUYỆT MỚI
order_approval_service = SalesOrderApprovalService(db_manager) # <--- THÊM
# --- KHỞI TẠO SERVICE TRA CỨU MỚI ---
lookup_service = SalesLookupService(db_manager)
task_service = TaskService(db_manager) # <--- KHỞI TẠO TASK SERVICE MỚI
sales_order_approval_service = SalesOrderApprovalService(db_manager)
quotation_approval_service = QuotationApprovalService(db_manager)
# =========================================================================
# HÀM HELPER VÀ XỬ LÝ LOGIN/AUTH
# =========================================================================

def login_required(f):
    """Decorator kiểm tra session login thủ công."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if session.get('logged_in') != True:
            flash("Vui lòng đăng nhập để truy cập trang này.", 'info')
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename):
    """Kiểm tra định dạng file có hợp lệ hay không."""
    if not hasattr(config, 'ALLOWED_EXTENSIONS') or not config.ALLOWED_EXTENSIONS:
        # Nếu chưa định nghĩa, mặc định không cho phép file nào
        return False
        
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in config.ALLOWED_EXTENSIONS

# CRM STDD/app.py (Trong hàm login)

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

@app.route('/login', methods=['GET', 'POST'])
def login():
    if session.get('logged_in'):
        # Đã đăng nhập, chuyển đến trang chủ (index_redesign.html)
        return redirect(url_for('index')) 

    message = None
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        # GỌI DBManager ĐỂ XỬ LÝ LOGIN. ĐÃ SỬA [PHONG BAN] thành [BO PHAN].
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
            # FIX: Lấy giá trị từ cột 'BO PHAN'
            session['bo_phan'] = user.get('BO PHAN', '').strip().upper() 
            # ----------------------------------------

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
            message = "Tên đăng nhập hoặc mật khẩu không chính xác."
            flash(message, 'danger')
            
    return render_template('login.html', message=message)

@app.route('/logout')
def logout():
    session.pop('logged_in', None)
    session.pop('user_code', None)
    session.pop('username', None)
    session.pop('user_shortname', None)
    session.pop('user_role', None)
    session.pop('cap_tren', None)
    flash('Bạn đã đăng xuất.', 'success')
    return redirect(url_for('login'))

@app.context_processor
def inject_user():
    """Tạo đối tượng current_user giả để truy cập thông tin user trong template."""
    return dict(current_user={'is_authenticated': session.get('logged_in', False),
                             'usercode': session.get('user_code'),
                             'username': session.get('username'),
                             'shortname': session.get('user_shortname'),
                             'role': session.get('user_role'),
                             'cap_tren': session.get('cap_tren')})

# =========================================================================
# ROUTE ĐIỀU HƯỚNG CHÍNH (TRANG CHỦ)
# =========================================================================

@app.route('/', methods=['GET'])
@login_required
def index():
    """Trang chủ (Directory) hiển thị danh sách các Dashboard/Module."""
    user_code = session.get('user_code')
    return render_template('index_redesign.html', user_code=user_code)


# =========================================================================
# MODULE 1: CRM (Báo cáo & Nhập liệu)
# =========================================================================

@app.route('/dashboard', methods=['GET', 'POST']) # FIX: THÊM 'POST' ĐỂ CHẠY TÌM KIẾM
@login_required
def dashboard_reports(): # Đổi tên hàm (từ dashboard thành dashboard_reports)
    """Hiển thị trang Dashboard - Danh sách báo cáo."""
    
    query_users = f"""
        SELECT [USERCODE], [USERNAME], [SHORTNAME] 
        FROM {config.TEN_BANG_NGUOI_DUNG} 
        WHERE [PHONG BAN] IS NOT NULL AND [PHONG BAN] NOT LIKE '9. DU HOC%'
        ORDER BY [SHORTNAME] 
    """ 
    users_data = db_manager.get_data(query_users)
    
    today = datetime.now()
    thirty_days_ago = today - timedelta(days=30)
    default_date_from = thirty_days_ago.strftime('%Y-%m-%d')
    default_date_to = today.strftime('%Y-%m-%d')

    # 1. Khởi tạo biến
    where_conditions = []
    where_params = [] 
    
    # --- 1. Filter Persistence Logic (Robust Fix) ---
    
    # 1. Collect filter values, prioritizing URL arguments (for GET/Pagination)
    selected_user = request.args.get('nv_bao_cao')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    kh_search_term = request.args.get('kh_search')
    text_search_term = request.args.get('text_search')

    # 2. If it's a POST request (new filter submitted), overwrite with form data.
    if request.method == 'POST':
        selected_user = request.form.get('nv_bao_cao')
        date_from = request.form.get('date_from')
        date_to = request.form.get('date_to')
        kh_search_term = request.form.get('kh_search')
        text_search_term = request.form.get('text_search')
        
    # 3. Handle default values (if None/empty string results from 1 or 2)
    # Apply defaults for dates
    date_from = date_from or default_date_from
    date_to = date_to or default_date_to
    # Filters should default to empty string ('')
    selected_user = selected_user or ''
    kh_search_term = kh_search_term or ''
    text_search_term = text_search_term or ''


    # --- 2. Pagination Setup ---
    PER_PAGE = 20
    # Read page number from URL args, default to 1
    try:
        # Note: page is only expected in GET args (for pagination), not POST form.
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1
        
    offset = (page - 1) * PER_PAGE
    
    try:
        dt_from = datetime.strptime(date_from, '%Y-%m-%d')
        dt_to = datetime.strptime(date_to, '%Y-%m-%d')
        if (dt_to - dt_from).days > 365:
            dt_from = dt_to - timedelta(days=365)
            date_from = dt_from.strftime('%Y-%m-%d')
            flash("Khoảng thời gian lọc tối đa là 1 năm. Kết quả đã được giới hạn.", "warning")
            
    except ValueError:
        date_from = default_date_from
        date_to = default_date_to
        
    where_conditions.append(f"T1.NGAY BETWEEN ? AND ?")
    where_params.extend([date_from, date_to])

    # (Logic lọc Admin, User, KH, Text Search giữ nguyên...)
    current_user_role_cleaned = session.get('user_role').strip().upper() if session.get('user_role') else ''
    if current_user_role_cleaned != 'ADMIN':
        query_admin = f"SELECT [USERCODE] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE UPPER(RTRIM([ROLE])) = 'ADMIN'"
        admin_data = db_manager.get_data(query_admin)
        if admin_data:
            admin_codes = [user['USERCODE'] for user in admin_data if user['USERCODE']]
            if admin_codes:
                admin_codes_str = ", ".join(f"'{code}'" for code in admin_codes)
                where_conditions.append(f"T1.NGUOI NOT IN ({admin_codes_str})")

    if selected_user and selected_user != '':
        where_conditions.append(f"T1.NGUOI = ?")
        where_params.append(selected_user)
    if kh_search_term and kh_search_term.strip() != '':
        where_conditions.append(f"T3.[TEN DOI TUONG] LIKE ?")
        where_params.append(f'%{kh_search_term}%')
    if text_search_term and text_search_term.strip() != '':
        terms = [t.strip() for t in text_search_term.split(';') if t.strip()]
        if terms:
            or_conditions = []
            for term in terms:
                or_conditions.append(f"(T1.[NOI DUNG 2] LIKE ? OR T1.[DANH GIA 2] LIKE ?)")
                like_param = f'%{term}%'
                where_params.extend([like_param, like_param])
            where_conditions.append("(" + " OR ".join(or_conditions) + ")")

    where_clause = " AND ".join(where_conditions)
    
    # --- 3. Execute Queries & Calculate Totals ---
    
    # 3a. Query for Total Count
    count_query = f"""
SELECT COUNT(T1.STT) AS Total
FROM {config.TEN_BANG_BAO_CAO} AS T1
LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T2 ON T1.NGUOI = T2.USERCODE
LEFT JOIN {config.TEN_BANG_KHACH_HANG} AS T3 ON T1.[KHACH HANG] = T3.[MA DOI TUONG]
WHERE {where_clause}
"""
    
    # 3b. Query for Paginated Reports (Using OFFSET/FETCH for SQL Server 2012+)
    report_query = f"""
SELECT 
    T1.STT AS ID_KEY, T1.NGAY, T2.SHORTNAME AS NV, T3.[TEN DOI TUONG] AS KH,
    T1.[NOI DUNG 2] AS [NOI DUNG 1], T1.[DANH GIA 2] AS [DANH GIA 1],
    T1.ATTACHMENTS
FROM {config.TEN_BANG_BAO_CAO} AS T1
LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T2 ON T1.NGUOI = T2.USERCODE
LEFT JOIN {config.TEN_BANG_KHACH_HANG} AS T3 ON T1.[KHACH HANG] = T3.[MA DOI TUONG]
WHERE {where_clause}
ORDER BY T1.STT DESC
OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
"""
    
    # Execute Count Query
    total_count_data = db_manager.get_data(count_query, tuple(where_params))
    total_reports = total_count_data[0]['Total'] if total_count_data and total_count_data[0].get('Total') is not None else 0
    total_pages = (total_reports + PER_PAGE - 1) // PER_PAGE if total_reports > 0 else 1

    # Execute Report Query (Parameters include offset and PER_PAGE)
    report_params = tuple(where_params) + (offset, PER_PAGE)
    report_data = db_manager.get_data(report_query, report_params)
    
    # 4. ÁP DỤNG TRUNCATE (Cắt nội dung) VÀ TÍNH TỆP
    def truncate_content(text, max_lines=5):
        if not text: return ""
        lines = text.split('\n')
        if len(lines) <= max_lines: return text 
        return '\n'.join(lines[:max_lines]) + '...'
    
    if report_data:
        for row in report_data:
            # Áp dụng hàm Tóm tắt/Cắt nội dung cho các cột hiển thị
            row['NOI DUNG 1'] = truncate_content(row.get('NOI DUNG 1', ''))
            row['DANH GIA 1'] = truncate_content(row.get('DANH GIA 1', ''))
            attachments = row.get('ATTACHMENTS')
            file_count = len([f for f in attachments.split(';') if f.strip()]) if attachments else 0
            row['FILE_COUNT'] = file_count
            row['ID_KEY'] = str(row['ID_KEY'])

    return render_template(
        'dashboard.html', 
        users=users_data,
        reports=report_data or [],
        selected_user=selected_user,
        date_from=date_from,
        date_to=date_to,
        kh_search_term=kh_search_term,
        text_search_term=text_search_term,
        # --- Pagination Variables ---
        page=page,
        per_page=PER_PAGE,
        total_reports=total_reports,
        total_pages=total_pages
    )


@app.route('/report_detail_page/<string:report_stt>', methods=['GET'])
@login_required 
def report_detail_page(report_stt):
    """ROUTE: Render trang chi tiết sau khi click."""
    
    current_user_id = session.get('user_code')
    current_user_role = session.get('user_role').strip().upper() if session.get('user_role') else ''

    if current_user_role != 'ADMIN':
        query_auth = f"""
            SELECT T1.NGUOI AS NguoiBaoCao, T2.[CAP TREN] AS CapTrenBaoCao
            FROM {config.TEN_BANG_BAO_CAO} AS T1
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T2 ON T1.NGUOI = T2.USERCODE
            WHERE T1.STT = ?
        """
        auth_data = db_manager.get_data(query_auth, (report_stt,))
        
        if not auth_data or not current_user_id:
            flash("Lỗi truy vấn hoặc phiên đăng nhập không hợp lệ.", 'danger')
            return redirect(url_for('dashboard_reports'))

        report_owner = auth_data[0]['NguoiBaoCao'].strip().upper()
        report_supervisor = auth_data[0]['CapTrenBaoCao'].strip().upper()
        current_user_id_cleaned = current_user_id.strip().upper()

        is_owner = current_user_id_cleaned == report_owner
        is_supervisor = current_user_id_cleaned == report_supervisor
        
        if not (is_owner or is_supervisor):
             flash("Bạn không có quyền xem chi tiết báo cáo này.", 'danger')
             return redirect(url_for('dashboard_reports'))

    # Lấy dữ liệu chi tiết
    query = f"""
        SELECT TOP 1
            T1.STT, T1.NGAY, T1.LOAI, T1.[KHACH HANG],
            T1.[NOI DUNG 1], T1.[NOI DUNG 2], T1.[NOI DUNG 3], T1.[NOI DUNG 4], T1.[NOI DUNG 5],
            T1.[DANH GIA 1], T1.[DANH GIA 2], T1.[DANH GIA 3], T1.[DANH GIA 4], T1.[DANH GIA 5],
            T1.ATTACHMENTS, 
            T4.[DIEN GIAI] AS Loai_DienGiai,
            T2.USERNAME AS NV_Fullname,
            T3.[TEN DOI TUONG] AS KH_FullName
        FROM {config.TEN_BANG_BAO_CAO} AS T1
        LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T2 ON T1.NGUOI = T2.USERCODE
        LEFT JOIN {config.TEN_BANG_KHACH_HANG} AS T3 ON T1.[KHACH HANG] = T3.[MA DOI TUONG]
        LEFT JOIN {config.TEN_BANG_LOAI_BAO_CAO} AS T4 ON T1.LOAI = T4.LOAI
        WHERE T1.STT = ?
    """
    report_data = db_manager.get_data(query, (report_stt,))
    
    if report_data:
        report = report_data[0]
        attachments_str = report.get('ATTACHMENTS')
        file_names = [f for f in attachments_str.split(';') if f.strip()] if attachments_str else []
        report['ATTACHMENT_LIST'] = file_names
        return render_template('report_detail_page.html', report=report)
    else:
        return render_template('report_detail_page.html', error_message=f"Báo cáo với STT {report_stt} không tìm thấy."), 404


@app.route('/nhaplieu', methods=['GET', 'POST'])
@login_required
def nhap_lieu():
    """Hàm xử lý Form nhập liệu và logic INSERT/GET."""
    
    # 1. Lấy dữ liệu danh mục (Giữ nguyên)
    query_users = f"""
        SELECT [USERCODE], [USERNAME], [SHORTNAME] 
        FROM {TEN_BANG_NGUOI_DUNG} 
        WHERE [PHONG BAN] IS NOT NULL AND [PHONG BAN] NOT LIKE '9. DU HOC%'
        ORDER BY [SHORTNAME] 
    """ 
    users_data = db_manager.get_data(query_users)
    query_loai = f"SELECT LOAI, [DIEN GIAI] FROM {TEN_BANG_LOAI_BAO_CAO} WHERE NHOM = 1 ORDER BY LOAI"
    loai_data = db_manager.get_data(query_loai)
    
    message = None
    default_usercode = session.get('user_code')
    
    if request.method == 'POST':
        # --- 1. Xử lý File Upload và Lấy dữ liệu Form ---
        
        # Lấy file từ Form (Giả định tên trường là 'attachments')
        files = request.files.getlist('attachments')
        attachments_str = save_uploaded_files(files) # Dùng hàm helper mới
        
        data = request.form
        
        # --- Lấy dữ liệu và Chuẩn hóa (Sửa lỗi 23000 tiềm năng) ---
        
        ngay_bao_cao = data.get('ngay_bao_cao') or datetime.now().strftime('%Y-%m-%d')
        loai = data.get('loai') or None
        nguoi_bao_cao_code = data.get('nv_bao_cao') or session.get('user_code')
        nguoi_lam_he_thong_value = session.get('user_code') or None
        ma_khach_hang_value = data.get('ma_doi_tuong_kh') or None
        
        # Chuyển chuỗi rỗng thành None (chuẩn hóa cho DB)
        hien_dien_truoc_1 = data.get('nhansu_hengap_1') or None
        hien_dien_truoc_2 = data.get('nhansu_hengap_2') or None
        
        # NỘI DUNG VÀ ĐÁNH GIÁ (Giữ nguyên là string, nhưng loại bỏ .replace("'", "''") 
        # vì chúng ta dùng Parameterized Query)
        noi_dung_1 = data.get('noi_dung_1') or None
        noi_dung_2 = data.get('noi_dung_2') or None
        noi_dung_3 = data.get('noi_dung_3') or None
        noi_dung_4 = data.get('noi_dung_4') or None
        noi_dung_5 = data.get('noi_dung_5') or None
        
        # ĐÁNH GIÁ (Cần đảm bảo là số hoặc None. Nếu DB là INT, cần chuyển đổi)
        # Giả định: DB có thể chấp nhận string hoặc là kiểu NVARCHAR/VARCHAR
        danh_gia_1 = data.get('danh_gia_1') or None
        danh_gia_2 = data.get('danh_gia_2') or None
        danh_gia_3 = data.get('danh_gia_3') or None
        danh_gia_4 = data.get('danh_gia_4') or None
        danh_gia_5 = data.get('danh_gia_5') or None
        
        attachments_str = attachments_str or None


        # --- 2. Xây dựng lệnh INSERT SQL (Parameterized Query) ---
        try:
            insert_query = f"""
                INSERT INTO {TEN_BANG_BAO_CAO} (
                    NGAY, LOAI, NGUOI, [NGUOI LAM], 
                    [NOI DUNG 1], [NOI DUNG 2], [NOI DUNG 3], [NOI DUNG 4], [NOI DUNG 5],
                    [DANH GIA 1], [DANH GIA 2], [DANH GIA 3], [DANH GIA 4], [DANH GIA 5],
                    [KHACH HANG], [HIEN DIEN TRUOC 1], [HIEN DIEN TRUOC 2], ATTACHMENTS
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            # Chuẩn bị Params Tuple (Thứ tự phải khớp với câu lệnh INSERT)
            params = (
                ngay_bao_cao, loai, nguoi_bao_cao_code, nguoi_lam_he_thong_value,
                noi_dung_1, noi_dung_2, noi_dung_3, noi_dung_4, noi_dung_5,
                danh_gia_1, danh_gia_2, danh_gia_3, danh_gia_4, danh_gia_5,
                ma_khach_hang_value, hien_dien_truoc_1, hien_dien_truoc_2, attachments_str
            )
            
            # 3. Thực thi INSERT
            if db_manager.execute_non_query(insert_query, params): # Cần truyền params cho execute_sql
                return redirect(url_for('dashboard_reports', success_message='Lưu thành công!'))
            else:
                message = "error: Thất bại khi thực thi INSERT SQL."
        except Exception as e:
            print(f"LỖI TẠO BẢN GHI: {e}")
            message = f"error: Lỗi hệ thống SQL: {e}"

    # 3. Render giao diện (GET)
    return render_template(
        'nhap_lieu.html', 
        users=users_data,
        loai_bao_cao=loai_data,
        message=message,
        now_date=datetime.now(),
        default_usercode=default_usercode
    )

# --- Hàm helper get_next_nhansu_ma (Nếu chưa chuyển vào DBManager) ---
def get_next_nhansu_ma(ma_cong_ty):
    query = f"SELECT TOP 1 MA FROM dbo.{config.TEN_BANG_NHAN_SU_LH} WHERE [CONG TY] = ? ORDER BY MA DESC"
    latest_ma = db_manager.get_data(query, (ma_cong_ty,))
    # ... (Logic tính toán next_stt) ...
    pass

@app.route('/nhansu_nhaplieu', methods=['GET', 'POST'])
@login_required
def nhansu_nhaplieu():
    """Hàm xử lý form nhập liệu Nhân sự Liên hệ."""
    message = None
    default_ma_khachhang = None
    default_ten_khachhang = None
    
    if request.method == 'GET':
        kh_code = request.args.get('kh_code')
        if kh_code:
            default_ma_khachhang = kh_code.strip()
            # Gọi hàm get_khachhang_by_ma từ DBManager
            default_ten_khachhang = db_manager.get_khachhang_by_ma(default_ma_khachhang) 
    
    if request.method == 'POST':
        data = request.form
        ma_cong_ty = data.get('ma_cong_ty_kh')
        
        if not ma_cong_ty:
            message = "error: Vui lòng chọn Công ty trước khi lưu."
        else:
            new_ma = get_next_nhansu_ma(ma_cong_ty) # Giả định hàm này tồn tại
            
            if not new_ma:
                 message = "error: Không thể tạo Mã Nhân sự (MA) mới."
            else:
                # ... (Logic lấy dữ liệu form: ten_ho, chuc_vu, v.v.) ...
                
                insert_query = f"""
                    INSERT INTO dbo.{config.TEN_BANG_NHAN_SU_LH} (
                        MA, [TEN HO], [TEN THUONG GOI], [CONG TY], [CHUC VU], [SO DTDD 1], 
                        [DIA CHI EMAIL], [GHI CHU], [GHI CHU DAC BIET], [NGUOI TAO], [NGAY TAO],
                        [QUE QUAN], [GIA DINH]
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)
                """
                # ... (Logic chuẩn bị 'params' tuple) ...
                
                if db_manager.execute_non_query(insert_query, params):
                    message = "success: Lưu Nhân sự liên hệ thành công. Mã mới: " + new_ma
                    default_ma_khachhang = ma_cong_ty 
                    default_ten_khachhang = db_manager.get_khachhang_by_ma(default_ma_khachhang) 
                else:
                    message = "error: Thất bại khi thực thi INSERT SQL cho Nhân sự."

    # FIX: LỆNH RETURN ĐƯỢC DI CHUYỂN RA NGOÀI CÙNG
    return render_template('nhansu_nhaplieu.html', 
        message=message,
        default_ma_khachhang=default_ma_khachhang,
        default_ten_khachhang=default_ten_khachhang
    )


# =========================================================================
# MODULE 2: KPI (Sales & Real-time)
# =========================================================================

@app.route('/sales_dashboard', methods=['GET', 'POST'])
@login_required
def sales_dashboard():
    """ROUTE: Bảng Tổng hợp Hiệu suất Sales."""
    
    current_year = datetime.now().year
    DIVISOR = 1000000.0

    # === THÊM LOGIC LẤY THÔNG TIN TỪ SESSION (ĐÃ SỬA LỖI) ===
    user_code = session.get('user_code') # Lấy Mã NV từ session
    # Kiểm tra quyền Admin
    is_admin = session.get('user_role', '').strip().upper() == 'ADMIN' 
    
    # Kiểm tra nếu user_code không tồn tại (chỉ là biện pháp an toàn)
    if not user_code:
        flash("Lỗi phiên đăng nhập: Không tìm thấy mã nhân viên.", 'danger')
        return redirect(url_for('login'))


    # 1. GỌI TẦNG SERVICE
    summary_data = sales_service.get_sales_performance_data(current_year, user_code, is_admin)
    
    # 2. SẮP XẾP VÀ CHIA ĐƠN VỊ
    summary_data = sorted(summary_data, key=itemgetter('RegisteredSales'), reverse=True)
    
    total_registered_sales_raw = 0
    total_monthly_sales_raw = 0
    total_ytd_sales_raw = 0
    total_orders_raw = 0
    total_pending_orders_amount_raw = 0

    for row in summary_data:
        # Tính tổng trước khi chia (để tránh lỗi làm tròn)
        total_registered_sales_raw += row.get('RegisteredSales', 0)
        total_monthly_sales_raw += row.get('CurrentMonthSales', 0)
        total_ytd_sales_raw += row.get('TotalSalesAmount', 0)
        total_orders_raw += row.get('TotalOrders', 0)
        total_pending_orders_amount_raw += row.get('PendingOrdersAmount', 0)
        
        # Chia cho 1 Triệu để hiển thị
        row['RegisteredSales'] /= DIVISOR
        row['CurrentMonthSales'] /= DIVISOR
        row['TotalSalesAmount'] /= DIVISOR
        row['PendingOrdersAmount'] /= DIVISOR
    
    # 3. TÍNH TỔNG CỘNG (Đã chia)
    total_registered_sales = total_registered_sales_raw / DIVISOR
    total_monthly_sales = total_monthly_sales_raw / DIVISOR
    total_ytd_sales = total_ytd_sales_raw / DIVISOR
    total_orders = total_orders_raw
    total_pending_orders_amount = total_pending_orders_amount_raw / DIVISOR
    
    return render_template(
        'sales_dashboard.html', 
        summary=summary_data,
        current_year=current_year,
        total_registered_sales=total_registered_sales,
        total_monthly_sales=total_monthly_sales,
        total_ytd_sales=total_ytd_sales,
        total_orders=total_orders,
        total_pending_orders_amount=total_pending_orders_amount
    )


@app.route('/sales_detail/<string:employee_id>', methods=['GET'])
@login_required
def sales_detail(employee_id):
    """ROUTE: Chi tiết Hiệu suất theo Khách hàng."""
    current_year = datetime.now().year
    
    # 1. GỌI TẦNG SERVICE
    registered_clients, new_business_clients, total_poa_amount_raw, total_registered_sales_raw = \
        sales_service.get_client_details_for_salesman(employee_id, current_year)
    
    # Lấy tên nhân viên
    salesman_query = f"SELECT SHORTNAME FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
    salesman_name_data = db_manager.get_data(salesman_query, (employee_id,))
    salesman_name = salesman_name_data[0]['SHORTNAME'] if salesman_name_data else employee_id

    # 2. HỢP NHẤT VÀ TÍNH TỔNG
    final_client_summary = registered_clients + new_business_clients
    
    total_ytd_sales = sum(row.get('TotalSalesAmount', 0) for row in final_client_summary)
    total_monthly_sales = sum(row.get('CurrentMonthSales', 0) for row in final_client_summary)
    
    # Sử dụng giá trị tổng DSĐK thô đã tính toán trong service
    total_registered_sales_display = total_registered_sales_raw / 1000000 
    
    return render_template(
        'sales_details.html', 
        employee_id=employee_id,
        salesman_name=salesman_name,
        client_summary=final_client_summary,
        total_registered_sales=total_registered_sales_display,
        total_ytd_sales=total_ytd_sales,
        total_monthly_sales=total_monthly_sales,
        total_poa_amount=total_poa_amount_raw / 1000000, 
        current_year=current_year
    )

@app.route('/realtime_dashboard', methods=['GET', 'POST'])
# CRM STDD/app.py (Hàm realtime_dashboard - ĐÃ LÀM SẠCH VÀ HỢP NHẤT)

@app.route('/realtime_dashboard', methods=['GET', 'POST'])
@login_required
def realtime_dashboard():
    current_year = datetime.now().year
    
    # --- LẤY THÔNG TIN USER VÀ QUYỀN ---
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    is_admin = user_role == 'ADMIN'
    
    selected_salesman = None
    
    # LOGIC: Xác định giá trị lọc cuối cùng
    if is_admin:
        # Admin: Xử lý bộ lọc POST
        if request.method == 'POST':
            filter_value = request.form.get('salesman_filter')
            if filter_value and filter_value.strip() != '':
                selected_salesman = filter_value.strip()
            else:
                selected_salesman = None # Admin chọn 'Tất cả' hoặc GET mặc định
        else:
            selected_salesman = None # Mặc định Admin xem tất cả
            
    else:
        # User thường: BẮT BUỘC dùng user_code hiện tại.
        selected_salesman = user_code
        
    # --- KẾT THÚC LOGIC LỌC VÀ PHÂN QUYỀN ---

    # 1. Lấy danh sách nhân viên để hiển thị trong bộ lọc (chỉ Admin cần)
    users_data = []
    if is_admin:
        query_users = f"""
        SELECT [USERCODE], [USERNAME], [SHORTNAME] 
        FROM {config.TEN_BANG_NGUOI_DUNG} 
        WHERE [PHONG BAN] IS NOT NULL AND RTRIM([PHONG BAN]) != '9. DU HOC'
        ORDER BY [SHORTNAME] 
        """
        users_data = db_manager.get_data(query_users)
        
    # 2. Lấy tên nhân viên để hiển thị trong tiêu đề
    salesman_name = "TẤT CẢ NHÂN VIÊN"
    if selected_salesman:
        # Đảm bảo selected_salesman không phải là chuỗi rỗng trước khi tra cứu DB
        if selected_salesman.strip():
            name_data = db_manager.get_data(f"SELECT SHORTNAME FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?", (selected_salesman,))
            salesman_name = name_data[0]['SHORTNAME'] if name_data else selected_salesman
    
    # 3. GỌI DỮ LIỆU (Đảm bảo truyền None cho SQL NULL)
    salesman_param_for_sp = selected_salesman.strip() if selected_salesman else None
    sp_params = (salesman_param_for_sp, current_year)
    
    # GỌI SERVICE: Chạy SP Multi-Result
    all_results = db_manager.execute_sp_multi('dbo.sp_GetRealtimeSalesKPI', sp_params)

    if not all_results or len(all_results) < 5:
        flash("Lỗi tải dữ liệu: Không đủ 5 bộ kết quả từ Stored Procedure. Vui lòng kiểm tra SP.", 'danger')
        all_results = [[]] * 5
        
    kpi_summary = all_results[0][0] if all_results[0] else {} 
    pending_orders = all_results[1]
    top_orders = all_results[2]
    top_quotes = all_results[3]
    upcoming_deliveries = all_results[4]

    return render_template(
        'realtime_dashboard.html', 
        kpi_summary=kpi_summary, 
        pending_orders=pending_orders, 
        top_orders=top_orders,
        top_quotes=top_quotes,
        upcoming_deliveries=upcoming_deliveries,
        users=users_data,
        selected_salesman=selected_salesman,
        salesman_name=salesman_name,
        current_year=current_year,
        is_admin=is_admin
    )

# =========================================================================
# MODULE 3: CAPITAL GOVERNANCE (Tồn kho)
# =========================================================================

@app.route('/inventory_aging', methods=['GET', 'POST'])
@login_required
def inventory_aging_dashboard():
    """ROUTE: Phân tích Tuổi hàng Tồn kho."""
    DIVISOR = 1000000.0
    
    # Lấy tham số lọc từ form (Sử dụng .get() an toàn)
    item_filter_term = request.form.get('item_filter', '').strip()
    category_filter = request.form.get('category_filter', '').strip() 
    qty_filter = request.form.get('qty_filter', '').strip()      
    value_filter = request.form.get('value_filter', '').strip()
    # THAM SỐ MỚI
    i05id_filter = request.form.get('i05id_filter', '').strip() 

    # 2. GỌI TẦNG SERVICE (Logic lọc và tính toán)
    filtered_data, totals = inventory_service.get_inventory_aging_data(
        item_filter_term, 
        category_filter, 
        qty_filter, 
        value_filter, 
        i05id_filter # FIX: Đã thêm tham số còn thiếu
    )

    # 3. Trả về template
    return render_template(
        'inventory_aging.html', 
        aging_data=filtered_data, 
        item_filter_term=item_filter_term,
        category_filter=category_filter,
        qty_filter=qty_filter,
        value_filter=value_filter,
        i05id_filter=i05id_filter, # Truyền lại giá trị lọc mới
        
        # Dữ liệu KPI Tiles (Đã chia 1M)
        kpi_total_inventory=totals['total_inventory'] / DIVISOR,
        kpi_total_quantity=totals['total_quantity'],
        kpi_new_6_months=totals['total_new_6_months'] / DIVISOR,
        kpi_over_2_years=totals['total_over_2_years'] / DIVISOR,
        kpi_clc_value=totals['total_clc_value'] / DIVISOR # KPI MỚI
    )
@app.route('/quote_input_table', methods=['GET', 'POST']) # THÊM POST ĐỂ XỬ LÝ LỌC NGÀY
@login_required
def quote_input_table():
    """ROUTE: Hiển thị Form nhập liệu Báo giá dạng bảng."""
    user_code = session.get('user_code')
    today = datetime.now()
    seven_days_ago = today - timedelta(days=7)

    # 1. Thu thập tham số lọc (Ưu tiên Form POST nếu có, sau đó là URL GET)
    date_from_str = request.form.get('date_from') or request.args.get('date_from')
    date_to_str = request.form.get('date_to') or request.args.get('date_to')
    
    # 2. Xử lý Mặc định 7 ngày
    if not date_from_str:
        date_from_str = seven_days_ago.strftime('%Y-%m-%d')
    if not date_to_str:
        date_to_str = today.strftime('%Y-%m-%d')
    
    # 3. Gọi Service để lấy dữ liệu Báo giá
    quotes_data = customer_service.get_quotes_for_input(user_code, date_from_str, date_to_str)
    
    # 4. Định nghĩa các hằng số Dropdown cho template (Giữ nguyên)
    QUOTE_STATUSES = [
        ('CHỜ', '1. CHỜ'), ('DELAY', '2. DELAY'), ('WIN', '3. WIN'), 
        ('LOST', '4. LOST'), ('HOLD', '5. HOLD'), ('CANCEL', '6. HỦY/TRÙNG'),
    ]
    ACTIONS = [
        ('N/A', '--- Chọn ---'), ('CALL', 'Gọi điện (Call)'),
        ('EMAIL', 'Gửi email (Email)'), ('MEET', 'Gặp mặt (Meeting)'),
        ('SAMPLE', 'Gửi mẫu (Sample)'), ('FOLLOWUP', 'Follow Up'),
    ]

    return render_template(
        'quote_table_input.html', 
        quotes=quotes_data,
        quote_statuses=QUOTE_STATUSES,
        actions=ACTIONS,
        date_from=date_from_str,
        date_to=date_to_str
    )


# =========================================================================
# API ROUTES (Giữ nguyên tại app.py để dễ quản lý)
# =========================================================================


@app.route('/api/khachhang/<string:ten_tat>', methods=['GET'])
@login_required 
def api_khachhang(ten_tat):
    """API tra cứu Khách hàng (Autocomplete). (SỬ DỤNG IT1202)"""
    
    # Giả định config.TEN_BANG_KHACH_HANG là IT1202 (hoặc bạn dùng config.ERP_IT1202)
    # Dựa trên code trước, IT1202 là config.ERP_IT1202
    
    query = f"""
        SELECT TOP 5 T1.ObjectID AS ID, T1.ShortObjectName AS FullName, T1.Address AS DiaChi
        FROM {config.ERP_IT1202} AS T1 
        WHERE T1.ShortObjectName LIKE ? OR T1.ObjectID LIKE ? 
        ORDER BY T1.ShortObjectName
    """
    like_param = f'%{ten_tat}%'
    
    # API sẽ tra cứu ShortObjectName và ObjectID, trả về ID (ObjectID) và FullName (ShortObjectName)
    data = db_manager.get_data(query, (like_param, like_param))
    
    # LƯU Ý QUAN TRỌNG: API này phải trả về {ID, FullName}
    return jsonify(data) if data else (jsonify({'error': 'Không tìm thấy'}), 404)

@app.route('/api/inventory/<string:search_term>', methods=['GET'])
@login_required
def api_inventory_lookup(search_term):
    """API tra cứu Mặt hàng (Autocomplete) theo mã hoặc tên."""
    
    # Logic: Lấy 5 mặt hàng khớp với mã hoặc tên
    query = f"""
        SELECT TOP 5 T1.InventoryID AS ID, T1.InventoryName AS FullName, T1.UnitID 
        FROM {config.ERP_ITEM_PRICING} AS T1
        WHERE T1.InventoryID LIKE ? OR T1.InventoryName LIKE ? 
        ORDER BY T1.InventoryID
    """
    like_param = f'%{search_term}%'
    data = db_manager.get_data(query, (like_param, like_param))
    
    if data:
        # Định dạng dữ liệu trả về cho dropdown
        dropdown_data = []
        for row in data:
            # Tạo chuỗi hiển thị: Mã - Tên (Đơn vị)
            text = f"{row['ID']} - {row['FullName']} ({row['UnitID']})"
            dropdown_data.append({'id': row['ID'], 'text': text})
        return jsonify(dropdown_data)
    else:
        return jsonify([])    

@app.route('/api/khachhang/ref/<string:ma_doi_tuong>', methods=['GET'])
@login_required
def api_get_reference_data(ma_doi_tuong):
    """API MỚI: Lấy thông tin tham chiếu (COUNT Nhân sự)."""
    query_count = f"SELECT COUNT(T1.ID) AS CountNLH FROM dbo.{config.TEN_BANG_NHAN_SU_LH} AS T1 WHERE T1.[CONG TY] = ?"
    count_data = db_manager.get_data(query_count, (ma_doi_tuong,))
    count_nlh = count_data[0]['CountNLH'] if count_data and count_data[0]['CountNLH'] is not None else 0
    return jsonify({'CountNLH': count_nlh})

@app.route('/api/nhansu_by_khachhang/<string:ma_doi_tuong>', methods=['GET'])
@login_required
def api_nhansu_by_khachhang(ma_doi_tuong):
    """API lấy danh sách tên và chức vụ nhân sự liên hệ thuộc một Khách hàng."""
    query = f"SELECT TOP 10 [TEN HO], [TEN THUONG GOI], [CHUC VU], [SO DTDD 1] FROM dbo.{config.TEN_BANG_NHAN_SU_LH} WHERE [CONG TY] = ? ORDER BY MA DESC"
    data = db_manager.get_data(query, (ma_doi_tuong,))
    return jsonify(data) if data else jsonify([])

@app.route('/api/nhansu_ddl_by_khachhang/<string:ma_doi_tuong>', methods=['GET'])
@login_required
def api_nhansu_ddl_by_khachhang(ma_doi_tuong):
    """API MỚI: Lấy danh sách Nhân sự cho Dropdown."""
    query = f"SELECT MA, [TEN THUONG GOI], [CHUC VU], [TEN HO] FROM dbo.{config.TEN_BANG_NHAN_SU_LH} WHERE [CONG TY] = ? ORDER BY [TEN HO]"
    data = db_manager.get_data(query, (ma_doi_tuong,))
    if data:
        dropdown_data = []
        for row in data:
            ten_goi = row['TEN THUONG GOI'].strip() or row['TEN HO'].strip()
            chuc_vu = row['CHUC VU'].strip() or 'N/A'
            dropdown_data.append({'id': row['MA'], 'text': f"{ten_goi} ({chuc_vu})"})
        return jsonify(dropdown_data)
    else:
        return jsonify([])

@app.route('/api/defaults/<string:loai_code>', methods=['GET'])
@login_required
def api_defaults(loai_code):
    """API tra cứu DLOOKUP cho tiêu đề và nội dung mặc định."""
    query = f"SELECT [LOAI], [MAC DINH], [TEN] FROM dbo.{config.TEN_BANG_NOI_DUNG_HD} WHERE [LOAI] LIKE ?"
    like_param = f'{loai_code}%'
    data = db_manager.get_data(query, (like_param,))
    if data:
        defaults = {}
        for row in data:
            if row['LOAI'].endswith('H'):
                defaults[row['LOAI']] = row['TEN']
            elif row['LOAI'].endswith('M'):
                defaults[row['LOAI']] = row['MAC DINH']
        return jsonify(defaults)
    else:
        return jsonify({}), 404

@app.route('/api/tinh_thanh', methods=['GET'])
@login_required
def api_tinh_thanh():
    """API lấy danh sách tỉnh thành từ bảng GIAI TRINH."""
    query = f"SELECT GIAI_TRINH FROM dbo.{config.TEN_BANG_GIAI_TRINH} WHERE [LOAI GIAI TRINH] = N'tinh thanh vn' ORDER BY GIAI_TRINH"
    data = db_manager.get_data(query)
    return jsonify([row['GIAI_TRINH'] for row in data]) if data else jsonify([])

# CRM STDD/app.py (Thêm vào phần API ROUTES)

@app.route('/api/update_quote_status', methods=['POST'])
@login_required
def api_update_quote_status():
    """API: Xử lý lưu trạng thái Báo giá mới bằng AJAX.
    FIX: Xử lý chuỗi rỗng cho datetime để tránh lỗi ValueError.
    """
    data = request.json
    user_code = session.get('user_code')
    
    quote_id = data.get('quote_id')
    status_code = data.get('status_code')
    loss_reason = data.get('loss_reason', '')
    action_1 = data.get('action_1', '')
    action_2 = data.get('action_2', '')
    
    # Hai trường ngày giờ mới
    time_start_str = data.get('time_start')
    time_complete_str = data.get('time_complete')
    
    if not quote_id or not status_code:
        return jsonify({'success': False, 'message': 'Thiếu Mã báo giá hoặc Trạng thái.'}), 400

    # Chuyển đổi chuỗi thời gian (ISO format: YYYY-MM-DDTHH:MM) sang đối tượng datetime
    
    # FIX: Kiểm tra chuỗi rỗng trước khi gọi strptime
    
    time_start = None
    if time_start_str:
        try:
            # Format datetime-local là '%Y-%m-%dT%H:%M'
            time_start = datetime.strptime(time_start_str, '%Y-%m-%dT%H:%M')
        except ValueError:
            # Nếu chuỗi không hợp lệ, giữ là None (hoặc xử lý lỗi cụ thể hơn)
            pass
            
    time_complete = None
    if time_complete_str:
        try:
            time_complete = datetime.strptime(time_complete_str, '%Y-%m-%dT%H:%M')
        except ValueError:
            pass
    
    insert_query = f"""
        INSERT INTO {config.TEN_BANG_CAP_NHAT_BG} (
            MA_BAO_GIA, NV_CAP_NHAT, NGAY_CAP_NHAT, 
            TINH_TRANG_BG, LY_DO_THUA, MA_HANH_DONG_1, MA_HANH_DONG_2, 
            THOI_GIAN_PHAT_SINH, THOI_GIAN_HOAN_TAT
        ) VALUES (?, ?, GETDATE(), ?, ?, ?, ?, ?, ?)
    """
    
    # Chú ý: Cần đảm bảo `db_manager` của bạn xử lý tốt việc truyền giá trị `None` cho các cột DATETIME trong SQL Server.
    # Thư viện như pyodbc thường chuyển đổi None thành NULL trong SQL.
    params = (
        quote_id, user_code, status_code, 
        loss_reason, action_1, action_2, 
        time_start, time_complete
    )

    try:
        db_manager.execute_non_query(insert_query, params)
        return jsonify({'success': True, 'message': 'Cập nhật trạng thái báo giá thành công!'})
    except Exception as e:
        app.logger.error(f"Lỗi khi cập nhật trạng thái báo giá {quote_id}: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

# CRM STDD/app.py (Route mới)

@app.route('/quote_approval', methods=['GET', 'POST'])
@login_required
def quote_approval_dashboard():
    """ROUTE: Dashboard Duyệt Chào Giá. Đã khôi phục logic lọc GET và POST."""
    user_code = session.get('user_code')
    today = datetime.now().strftime('%Y-%m-%d')
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    # Ưu tiên: 1. POST (Lọc ngày) -> 2. GET (URL) -> 3. Mặc định (7 ngày)
    
    # 1. Thu thập tham số lọc
    date_from_str = request.form.get('date_from') or request.args.get('date_from')
    date_to_str = request.form.get('date_to') or request.args.get('date_to')
    
    # 2. Áp dụng mặc định 7 ngày CHỈ KHI KHÔNG CÓ BẤT KỲ THAM SỐ NÀO (cả POST và GET)
    if not date_from_str:
        date_from_str = seven_days_ago
        
    if not date_to_str:
        date_to_str = today
        
    # 3. Gọi Service để lấy dữ liệu Báo giá trong khoảng ngày
    quotes_for_review = approval_service.get_quotes_for_approval(user_code, date_from_str, date_to_str)
    
    return render_template(
        'quote_approval.html',
        quotes=quotes_for_review,
        current_user_code=user_code,
        date_from=date_from_str, 
        date_to=date_to_str     
    )
# CRM STDD/app.py - API Handler cho duyệt báo giá

@app.route('/api/approve_quote', methods=['POST'])
@login_required
def api_approve_quote():
    """API: Thực hiện duyệt Chào Giá, gọi Service và xử lý lỗi Rollback."""
    
    data = request.json
    quotation_no = data.get('quotation_no')
    quotation_id = data.get('quotation_id')
    object_id = data.get('object_id')
    employee_id = data.get('employee_id')
    approval_ratio = data.get('approval_ratio')
    
    current_user_code = session.get('user_code')

    try:
        # Gọi Service Layer (nơi có 'raise e')
        result = quotation_approval_service.approve_quotation(
            quotation_no=quotation_no,
            quotation_id=quotation_id,
            object_id=object_id,
            employee_id=employee_id,
            approval_ratio=approval_ratio,
            current_user=current_user_code
        )
        
        # Nếu Service trả về thành công (success: True)
        if result['success']:
            return jsonify({'success': True, 'message': result['message']})
        else:
            # Trường hợp service bắt lỗi nhưng không re-raise (rất hiếm)
            return jsonify({'success': False, 'message': result['message']}), 400
            
    except Exception as e:
        # BẮT BUỘC BẮT LỖI VÀ TRẢ VỀ MÃ LỖI HTTP 400/500
        error_msg = f"Lỗi SQL/Nghiệp vụ: {str(e)}"
        print(f"LỖI FATAL DUYỆT BG (API CATCH): {error_msg}")
        return jsonify({'success': False, 'message': error_msg}), 400 # Trả về 400 để kích hoạt khối .catch() trên Frontend

@app.route('/api/get_quote_details/<string:quote_id>', methods=['GET'])
@login_required
def api_get_quote_details(quote_id):
    """API: Trả về chi tiết báo giá (mặt hàng) cho AJAX."""
    
    try:
        # Gọi Service Layer để lấy dữ liệu
        details = approval_service.get_quote_details(quote_id)
        
        # Service đã định dạng số, chỉ cần trả về JSON
        return jsonify(details)
    
    except Exception as e:
        app.logger.error(f"Lỗi API lấy chi tiết báo giá {quote_id}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết.'}), 500

@app.route('/sales_order_approval', methods=['GET', 'POST'])
@login_required
def sales_order_approval_dashboard():
    """ROUTE: Dashboard Duyệt Đơn hàng Bán."""
    user_code = session.get('user_code')
    today = datetime.now().strftime('%Y-%m-%d')
    seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    
    date_from_str = request.form.get('date_from') or request.args.get('date_from')
    date_to_str = request.form.get('date_to') or request.args.get('date_to')
    
    date_from_str = date_from_str or seven_days_ago
    date_to_str = date_to_str or today
    
    orders_for_review = order_approval_service.get_orders_for_approval(user_code, date_from_str, date_to_str)
    
    return render_template(
        'sales_order_approval.html', 
        orders=orders_for_review,
        current_user_code=user_code,
        date_from=date_from_str,
        date_to=date_to_str
    )

# CRM STDD/app.py (Trong khu vực API Routes)

@app.route('/api/get_order_details/<string:sorder_id>', methods=['GET'])
@login_required
def api_get_order_details(sorder_id):
    """API: Trả về chi tiết Đơn hàng Bán (mặt hàng) cho AJAX."""
    
    try:
        # Gọi Service Layer để lấy dữ liệu. Truyền SOrderID trực tiếp.
        details = order_approval_service.get_order_details(sorder_id)
        
        return jsonify(details)
    
    except Exception as e:
        app.logger.error(f"Lỗi API lấy chi tiết DHB {sorder_id}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết.'}), 500



@app.route('/api/get_order_detail_drilldown/<path:voucher_no>', methods=['GET'])
@login_required
def api_get_order_detail_drilldown(voucher_no):
    """API: Tra cứu SOrderID bằng VoucherNo (dùng path) rồi gọi service drill-down."""
    try:
        # FIX 1: Dùng <path:voucher_no> để chấp nhận dấu '/' trong URL
        # FIX 2: Tra cứu SOrderID trước
        sorder_id_query = f"SELECT TOP 1 SOrderID FROM {config.ERP_OT2001} WHERE VoucherNo = ?"
        sorder_id_data = db_manager.get_data(sorder_id_query, (voucher_no,))
        
        if not sorder_id_data:
             return jsonify({'error': f'Không tìm thấy SOrderID cho mã DHB {voucher_no}'}), 404
             
        sorder_id = sorder_id_data[0]['SOrderID']

        # GỌI SERVICE DRILL-DOWN BẰNG SORDERID
        details = sales_service.get_order_detail_drilldown(sorder_id)
        
        return jsonify(details)
    except Exception as e:
        # Bắt lỗi Python/SQL, không phải lỗi 404
        app.logger.error(f"Lỗi API Drill-down DHB {voucher_no}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết đơn hàng.'}), 500
# =========================================================================
# ĐĂNG KÝ BLUEPRINT VÀ KHỞI ĐỘNG ỨNG DỤNG
# =========================================================================
# --- ĐĂNG KÝ BLUEPRINT MỚI ---
app.register_blueprint(sales_bp, url_prefix='/sales')
 # Các route Tra cứu Bán hàng sẽ được truy cập tại /sales/sales_lookup

# =========================================================================
# MODULE 4: TASK MANAGEMENT (ĐẦU VIỆC)
# =========================================================================

@app.route('/task_dashboard', methods=['GET', 'POST'])
@login_required
def task_dashboard():
    """ROUTE: Dashboard Quản lý Đầu việc hàng ngày."""
    
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    is_admin = user_role == 'ADMIN'
    current_month = datetime.now().month
    
    # Lấy Mã cấp trên từ Session (Đã được xử lý ở hàm login)
    supervisor_code = session.get('cap_tren')


    # BỔ SUNG: Xử lý tham số view_mode (Req 2)
    view_mode = request.args.get('view', 'USER').upper()
    filter_type = request.args.get('filter') or 'ALL'
    text_search_term = request.args.get('search') or request.form.get('search') or ''

    # Check if user has ADMIN or MANAGER role for button visibility (Yêu cầu 2)
    can_manage_view = is_admin or user_role == 'MANAGER'

    # 1. XỬ LÝ TẠO TASK MỚI
    if request.method == 'POST' and 'create_task' in request.form:
        title = request.form.get('task_title')
        priority = request.form.get('task_priority') or 'NORMAL'

        # Lấy CAP TREN từ session (Giả định đã có user_data['CAP TREN'])
        supervisor_code = session.get('cap_tren') 
        
        # Bổ sung logic lấy ObjectID và Attachment (Nếu có)
        object_id = request.form.get('object_id') 
        task_type = request.form.get('task_type')
        
        # Logic File Attachment
        attachments_filename = None
        # Cần thêm logic xử lý file upload TƯƠNG TỰ như route nhaplieu nếu Task cho phép upload file vật lý
        
        if title:
            if task_service.create_new_task(
                user_code, 
                title, 
                supervisor_code, 
                attachments=attachments_filename, 
                task_type=task_type, 
                object_id=object_id
            ):
                flash("Task mới đã được tạo thành công!", 'success')
            else:
                flash("Lỗi khi tạo Task. Vui lòng thử lại.", 'danger')
            return redirect(url_for('task_dashboard'))
    
    # 2. GỌI DỮ LIỆU CHÍNH
    
    # KPI Summary (30 ngày qua)
    kpi_summary = task_service.get_kpi_summary(user_code, is_admin=is_admin)
    
    # KHỐI 1: Kanban (3 ngày)
    kanban_tasks = task_service.get_kanban_tasks(user_code, is_admin=is_admin, view_mode=view_mode)
    # KHỐI 2: Mặc định tải Task Rủi ro (Pending/Help) trong 30 ngày qua
    # Xử lý lọc khi click KPI (GET request)
    # KHỐI 2: History (30 ngày)
    filter_type = request.args.get('filter') or 'ALL'
    risk_history_tasks = task_service.get_filtered_tasks(
        user_code, 
        filter_type=filter_type, 
        is_admin=is_admin, 
        view_mode=view_mode, 
        text_search_term=text_search_term 
    )
    
    # Định nghĩa các tùy chọn cho Form/Modal
    PRIORITY_OPTIONS = [('LOW', 'Thấp'), ('NORMAL', 'Bình thường'), ('HIGH', 'Cao')]
    STATUS_OPTIONS = [
        ('PENDING', 'Đang xử lý'), 
        ('COMPLETED', 'Đã hoàn thành'), 
        ('HELP_NEEDED', 'Cần hỗ trợ')
    ]
    
    return render_template(
        'task_dashboard.html',
        kpi=kpi_summary,
        kanban_tasks=kanban_tasks, 
        history_tasks=risk_history_tasks, 
        is_admin=is_admin,
        current_date=datetime.now().strftime('%Y-%m-%d'),
        active_filter=filter_type,
        view_mode=view_mode,
        can_manage_view=can_manage_view, 
        text_search_term=text_search_term 
    )

@app.route('/api/task/note', methods=['POST'])
@login_required
def api_add_supervisor_note():
    """API: Cấp trên thêm ghi chú/phản hồi."""
    data = request.json
    supervisor_code = session.get('user_code')
    
    success = task_service.add_supervisor_note(
        task_id=data.get('task_id'),
        supervisor_code=supervisor_code,
        note=data.get('note_content')
    )
    if success:
        # TODO: Cần logic gửi email thông báo cho NV
        return jsonify({'success': True, 'message': 'Ghi chú đã được lưu.'})
    return jsonify({'success': False, 'message': 'Lỗi lưu ghi chú.'}), 500

# app.py (Trong khu vực API ROUTES)

@app.route('/api/task/toggle_priority/<int:task_id>', methods=['POST'])
@login_required
def api_toggle_task_priority(task_id):
    """API: Thay đổi Priority thành HIGH (hoặc ngược lại) khi nhấn biểu tượng sao."""
    
    current_task_data = task_service.get_task_by_id(task_id) 
    if not current_task_data:
        return jsonify({'success': False, 'message': 'Task không tồn tại.'}), 404
        
    # Logic: Nếu đang là HIGH, chuyển sang NORMAL. Ngược lại, chuyển sang HIGH.
    current_priority = current_task_data.get('Priority', 'NORMAL')
    new_priority = 'NORMAL' if current_priority == 'HIGH' else 'HIGH'
    
    # SỬ DỤNG HÀM MỚI
    success = task_service.update_task_priority(task_id, new_priority) 
    
    if success:
        return jsonify({'success': True, 'new_priority': new_priority}), 200
    return jsonify({'success': False, 'message': 'Lỗi CSDL khi cập nhật ưu tiên.'}), 500

@app.route('/api/get_eligible_helpers', methods=['GET'])
@login_required
def api_get_eligible_helpers():
    """API: Trả về danh sách Helper đủ điều kiện (Usercode - Shortname)."""
    try:
        helpers = task_service.get_eligible_helpers()
        formatted_helpers = [{'code': h['USERCODE'], 'name': f"{h['USERCODE']} - {h['SHORTNAME']}"} for h in helpers]
        return jsonify(formatted_helpers)
    except Exception as e:
        app.logger.error(f"Lỗi API lấy danh sách helper: {e}")
        return jsonify([]), 500

@app.route('/api/task/update', methods=['POST'])
@login_required
def api_update_task():
    """API: Cập nhật tiến độ Task (Hành động của NV) và Xử lý Task Hỗ trợ (Req 3)."""
    data = request.json
    user_code = session.get('user_code')
    
    task_id = data.get('task_id')
    status = data.get('status')
    helper_code = data.get('helper_code') # SỬA LỖI CÚ PHÁP Ở ĐÂY
    
    # 1. XỬ LÝ TẠO TASK HỖ TRỢ/GIAO VIỆC
    if status and status.upper() == 'HELP_NEEDED':
        # ... (Kiểm tra helper_code và logic tạo task mới giữ nguyên)
        if helper_code:
            original_task = task_service.get_task_by_id(task_id)
            if original_task:
                # LẤY TASKTYPE VÀ OBJECTID GỐC
                original_task_type = original_task.get('TaskType', 'KHAC')
                # Tự động tạo Task mới cho Helper
                task_service.create_help_request_task(
                    helper_code=helper_code,
                    original_task_id=task_id,
                    current_user_code=user_code,
                    original_title=original_task.get('Title', 'N/A'),
                    original_object_id=data.get('object_id', None),
                    original_detail_content=data.get('detail_content', ''),
                    new_task_type=original_task_type # TRUYỀN THAM SỐ CẦN THIẾT
                )
            
        else:
            return jsonify({'success': False, 'message': 'Vui lòng chọn người cần hỗ trợ.'}), 400

    # --- 2. CẬP NHẬT TASK GỐC ---
    success = task_service.update_task_progress(
        task_id=task_id,
        object_id=data.get('object_id', None),
        content=data.get('detail_content', ''),
        status=status,
        helper_code=helper_code, # TRUYỀN THAM SỐ MỚI
        completed_date=data.get('status') == 'COMPLETED'
    )
    if success:
        return jsonify({'success': True, 'message': 'Tiến độ Task đã được cập nhật.'})
    return jsonify({'success': False, 'message': 'Lỗi cập nhật CSDL.'}), 500

@app.route('/api/get_quote_cost_details/<string:quotation_id>', methods=['GET'])
@login_required
def api_get_quote_cost_details(quotation_id):
    """API: Trả về chi tiết các mặt hàng cần bổ sung Cost cho Modal."""
    try:
        # Gọi service để lấy dữ liệu chi tiết
        details = approval_service.get_quote_cost_details(quotation_id)
        
        # Định dạng tiền tệ và số lượng
        for detail in details:
            detail['QuoQuantity'] = f"{safe_float(detail.get('QuoQuantity')):.0f}"
            detail['UnitPrice'] = f"{safe_float(detail.get('UnitPrice')):,.0f}"
            detail['Recievedprice'] = f"{safe_float(detail.get('Recievedprice')):,.0f}"
        
        return jsonify(details)
    
    except Exception as e:
        app.logger.error(f"Lỗi API lấy chi tiết Cost Override cho {quotation_id}: {e}")
        return jsonify({'error': 'Lỗi nội bộ khi truy vấn chi tiết bổ sung Cost.'}), 500


@app.route('/api/save_quote_cost_override', methods=['POST'])
@login_required
def api_save_quote_cost_override():
    """API: Lưu dữ liệu Cost và Note vào bảng BOSUNG_CHAOGIA."""
    data = request.json
    user_code = session.get('user_code')
    
    updates = data.get('updates')
    
    if not updates or not user_code:
        return jsonify({'success': False, 'message': 'Thiếu dữ liệu cập nhật hoặc người dùng.'}), 400
        
    try:
        if approval_service.upsert_cost_override(updates, user_code):
            return jsonify({'success': True, 'message': 'Lưu Cost Override thành công.'})
        else:
            return jsonify({'success': False, 'message': 'Lỗi CSDL khi thực hiện lưu trữ Cost.'}), 500

    except Exception as e:
        app.logger.error(f"Lỗi API lưu Cost Override: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

@app.route('/api/approve_order', methods=['POST'])
@login_required # Giả định decorater này tồn tại
def api_approve_order():
    """API: Thực hiện duyệt Đơn hàng Bán, cập nhật status và ghi log DUYETCT."""
    
    data = request.json
    order_id = data.get('order_id')         # MACT (VoucherNo)
    sorder_id = data.get('sorder_id')       # MasoCT (SOrderID)
    client_id = data.get('client_id')       # MaKH
    salesman_id = data.get('salesman_id')   # NGUOILAM
    approval_ratio = data.get('approval_ratio') # TySoDuyet
    
    current_user_code = session.get('user_code')
    
    if not current_user_code:
        return jsonify({'success': False, 'message': 'Phiên đăng nhập hết hạn.'}), 401
    
    if not order_id or not sorder_id:
        return jsonify({'success': False, 'message': 'Thiếu mã DHB hoặc SOrderID.'}), 400

    try:
        result = sales_order_approval_service.approve_sales_order(
            order_id=order_id,
            sorder_id=sorder_id,
            client_id=client_id,
            salesman_id=salesman_id,
            approval_ratio=approval_ratio,
            current_user=current_user_code
        )
        
        if result['success']:
            return jsonify({'success': True, 'message': result['message']})
        else:
            return jsonify({'success': False, 'message': result['message']}), 400

    except Exception as e:
        print(f"LỖI HỆ THỐNG API DUYỆT DHB: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

if __name__ == '__main__':
    # Sử dụng Waitress WSGI server để xử lý kết nối SSE ổn định hơn
    # host='0.0.0.0' để cho phép truy cập từ mạng LAN
    from waitress import serve
    serve(app, host='0.0.0.0', port=5000)
    
    # HOẶC giữ lại đoạn code phát triển cũ nhưng tắt reloader và debug
    # app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False) 
    # Tuy nhiên, sử dụng Waitress là cách ổn định hơn cho SSE.