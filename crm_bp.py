from flask import current_app
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, current_app
# FIX: Import login_required từ app.py
from utils import login_required, truncate_content, save_uploaded_files, permission_required # Import thêm
from datetime import datetime, timedelta
import config 
from config import TEN_BANG_NGUOI_DUNG, TEN_BANG_LOAI_BAO_CAO, TEN_BANG_BAO_CAO, TEN_BANG_KHACH_HANG, TEN_BANG_NHAN_SU_LH


crm_bp = Blueprint('crm_bp', __name__)

# [HÀM HELPER CẦN THIẾT]
def get_user_ip():
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

# [ROUTES]

@crm_bp.route('/dashboard', methods=['GET', 'POST'])
@login_required
@permission_required('VIEW_REPORT_LIST') # Áp dụng quyền mới
def dashboard_reports():
    """Hiển thị trang Dashboard - Danh sách báo cáo. (Đã chuyển logic)"""
    
    # FIX: Import Services Cần thiết NGAY TẠY ĐÂY
    db_manager = current_app.db_manager

    query_users = f"""
        SELECT [USERCODE], [USERNAME], [SHORTNAME] 
        FROM {TEN_BANG_NGUOI_DUNG} 
        WHERE [PHONG BAN] IS NOT NULL AND [PHONG BAN] NOT LIKE '9. DU HOC%'
        ORDER BY [SHORTNAME] 
    """ 
    users_data = db_manager.get_data(query_users)
    
    today = datetime.now()
    thirty_days_ago = today - timedelta(days=30)
    default_date_from = thirty_days_ago.strftime('%Y-%m-%d')
    default_date_to = today.strftime('%Y-%m-%d')

    # 1. Khởi tạo biến và Logic Filter Persistence
    where_conditions = []
    where_params = [] 
    
    selected_user = request.args.get('nv_bao_cao')
    date_from = request.args.get('date_from')
    date_to = request.args.get('date_to')
    kh_search_term = request.args.get('kh_search')
    text_search_term = request.args.get('text_search')

    if request.method == 'POST':
        selected_user = request.form.get('nv_bao_cao')
        date_from = request.form.get('date_from')
        date_to = request.form.get('date_to')
        kh_search_term = request.form.get('kh_search')
        text_search_term = request.form.get('text_search')
        
    date_from = date_from or default_date_from
    date_to = date_to or default_date_to
    selected_user = selected_user or ''
    kh_search_term = kh_search_term or ''
    text_search_term = text_search_term or ''

    # 2. Pagination Setup
    PER_PAGE = 20
    try:
        page = int(request.args.get('page', 1))
    except ValueError:
        page = 1
        
    offset = (page - 1) * PER_PAGE
    
    # 3. Lọc Ngày
    where_conditions.append(f"T1.NGAY BETWEEN ? AND ?")
    where_params.extend([date_from, date_to])

    # 4. Lọc theo User, KH, Text Search
    current_user_role_cleaned = session.get('user_role').strip().upper() if session.get('user_role') else ''
    if current_user_role_cleaned != config.ROLE_ADMIN:
        query_admin = f"SELECT [USERCODE] FROM {TEN_BANG_NGUOI_DUNG} WHERE UPPER(RTRIM([ROLE])) = config.ROLE_ADMIN"
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
        # CẬP NHẬT: Tìm theo tên ngắn hoặc tên đầy đủ trong IT1202
        where_conditions.append(f"(T3.ShortObjectName LIKE ? OR T3.ObjectName LIKE ?)")
        where_params.extend([f'%{kh_search_term}%', f'%{kh_search_term}%'])
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
    
    # 5. Execute Queries
    # 5. Execute Queries
    # CẬP NHẬT: Join với config.ERP_IT1202 thay vì TEN_BANG_KHACH_HANG
    count_query = f"""
        SELECT COUNT(T1.STT) AS Total
        FROM {TEN_BANG_BAO_CAO} AS T1
        LEFT JOIN {TEN_BANG_NGUOI_DUNG} AS T2 ON T1.NGUOI = T2.USERCODE
        LEFT JOIN {config.ERP_IT1202} AS T3 ON T1.[KHACH HANG] = T3.ObjectID 
        WHERE {where_clause}
    """

    # CẬP NHẬT: Lấy T3.ShortObjectName AS KH
    report_query = f"""
        SELECT 
            T1.STT AS ID_KEY, T1.NGAY, T2.SHORTNAME AS NV, 
            ISNULL(T3.ShortObjectName, T3.ObjectName) AS KH, -- Ưu tiên tên ngắn, nếu null lấy tên đầy đủ
            T1.[NOI DUNG 2] AS [NOI DUNG 1], T1.[DANH GIA 2] AS [DANH GIA 1],
            T1.ATTACHMENTS
        FROM {TEN_BANG_BAO_CAO} AS T1
        LEFT JOIN {TEN_BANG_NGUOI_DUNG} AS T2 ON T1.NGUOI = T2.USERCODE
        LEFT JOIN {config.ERP_IT1202} AS T3 ON T1.[KHACH HANG] = T3.ObjectID
        WHERE {where_clause}
        ORDER BY T1.STT DESC
        OFFSET ? ROWS FETCH NEXT ? ROWS ONLY
    """
    
    total_count_data = db_manager.get_data(count_query, tuple(where_params))
    total_reports = total_count_data[0]['Total'] if total_count_data and total_count_data[0].get('Total') is not None else 0
    total_pages = (total_reports + PER_PAGE - 1) // PER_PAGE if total_reports > 0 else 1

    report_params = tuple(where_params) + (offset, PER_PAGE)
    report_data = db_manager.get_data(report_query, report_params)
    
    if report_data:
        for row in report_data:
            row['NOI DUNG 1'] = truncate_content(row.get('NOI DUNG 1', ''))
            row['DANH GIA 1'] = truncate_content(row.get('DANH GIA 1', ''))
            attachments = row.get('ATTACHMENTS')
            file_count = len([f for f in attachments.split(';') if f.strip()]) if attachments else 0
            row['FILE_COUNT'] = file_count
            row['ID_KEY'] = str(row['ID_KEY'])
    
    # LOG VIEW_REPORT_DASHBOARD (BỔ SUNG)
    try:
        
        db_manager  = current_app.db_manager
        get_user_ip  = current_app.get_user_ip
        
        details = f"Xem Dashboard: User={selected_user}, KH={kh_search_term}, Text={text_search_term}, Page={page}"
        db_manager.write_audit_log(
            session.get('user_code'), 'VIEW_REPORT_DASHBOARD', 'INFO', details, get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi ghi log VIEW_REPORT_DASHBOARD: {e}")

    return render_template(
        'dashboard.html', 
        users=users_data,
        reports=report_data or [],
        selected_user=selected_user,
        date_from=date_from,
        date_to=date_to,
        kh_search_term=kh_search_term,
        text_search_term=text_search_term,
        page=page,
        total_reports=total_reports,
        total_pages=total_pages
    )

@crm_bp.route('/report_detail_page/<string:report_stt>', methods=['GET'])
@login_required 
def report_detail_page(report_stt):
    """ROUTE: Render trang chi tiết sau khi click. (Đã chuyển logic)"""
    
    # FIX: Import Services Cần thiết NGAY TẠY ĐÂY
    db_manager = current_app.db_manager
    
    current_user_id = session.get('user_code')
    current_user_role = session.get('user_role').strip().upper() if session.get('user_role') else ''

    if current_user_role != config.ROLE_ADMIN:
        query_auth = f"""
            SELECT T1.NGUOI AS NguoiBaoCao, T2.[CAP TREN] AS CapTrenBaoCao
            FROM {TEN_BANG_BAO_CAO} AS T1
            LEFT JOIN {TEN_BANG_NGUOI_DUNG} AS T2 ON T1.NGUOI = T2.USERCODE
            WHERE T1.STT = ?
        """
        auth_data = db_manager.get_data(query_auth, (report_stt,))
        
        if not auth_data or not current_user_id:
            flash("Lỗi truy vấn hoặc phiên đăng nhập không hợp lệ.", 'danger')
            return redirect(url_for('crm_bp.dashboard_reports'))

        report_owner = auth_data[0]['NguoiBaoCao'].strip().upper()
        report_supervisor = auth_data[0]['CapTrenBaoCao'].strip().upper()
        current_user_id_cleaned = current_user_id.strip().upper()

        is_owner = current_user_id_cleaned == report_owner
        is_supervisor = current_user_id_cleaned == report_supervisor
        
        if not (is_owner or is_supervisor):
             flash("Bạn không có quyền xem chi tiết báo cáo này.", 'danger')
             return redirect(url_for('crm_bp.dashboard_reports'))

    # Lấy dữ liệu chi tiết
    query = f"""
        SELECT TOP 1
            T1.STT, T1.NGAY, T1.LOAI, T1.[KHACH HANG] AS KH_Ma,
            T1.[NOI DUNG 1], T1.[NOI DUNG 2], T1.[NOI DUNG 3], T1.[NOI DUNG 4], T1.[NOI DUNG 5],
            T1.[DANH GIA 1], T1.[DANH GIA 2], T1.[DANH GIA 3], T1.[DANH GIA 4], T1.[DANH GIA 5],
            T1.ATTACHMENTS, 
            T4.[DIEN GIAI] AS Loai_DienGiai,
            T2.USERNAME AS NV_Fullname,
            T3.[TEN DOI TUONG] AS KH_FullName
        FROM {TEN_BANG_BAO_CAO} AS T1
        LEFT JOIN {TEN_BANG_NGUOI_DUNG} AS T2 ON T1.NGUOI = T2.USERCODE
        LEFT JOIN {TEN_BANG_KHACH_HANG} AS T3 ON T1.[KHACH HANG] = T3.[MA DOI TUONG]
        LEFT JOIN {TEN_BANG_LOAI_BAO_CAO} AS T4 ON T1.LOAI = T4.LOAI
        WHERE T1.STT = ?
    """
    report_data = db_manager.get_data(query, (report_stt,))
    
    if report_data:
        # LOG VIEW_REPORT_DETAIL (BỔ SUNG)
        try:
            db_manager  = current_app.db_manager
            get_user_ip  = current_app.get_user_ip

            db_manager.write_audit_log(
                current_user_id, 'VIEW_REPORT_DETAIL', 'WARNING', 
                f"Xem Chi tiết BC #{report_stt} của KH: {report_data[0].get('KH_FullName')}", 
                get_user_ip()
            )
        except Exception as e:
            current_app.logger.error(f"Lỗi ghi log VIEW_REPORT_DETAIL: {e}")

        report = report_data[0]
        # FIX: Gọi trực tiếp hàm đã import, KHÔNG dùng current_app.save_uploaded_files
        attachments_str = save_uploaded_files(request.files.getlist('attachment_file'))
        file_names = [f for f in attachments_str.split(';') if f.strip()] if attachments_str else []
        report['ATTACHMENT_LIST'] = file_names
        return render_template('report_detail_page.html', report=report)
    else:
        return render_template('report_detail_page.html', error_message=f"Báo cáo với STT {report_stt} không tìm thấy."), 404

@crm_bp.route('/nhaplieu', methods=['GET', 'POST'])
@login_required
def nhap_lieu():
    """Hàm xử lý Form nhập liệu và logic INSERT/GET. (Đã chuyển logic)"""
    
    # FIX: Import Services Cần thiết NGAY TẠI ĐÂY
    
    db_manager  = current_app.db_manager
    

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
        attachments_str = save_uploaded_files(request.files.getlist('attachment_file')) # FIX: Dùng đúng tên input file
        
        data = request.form
        
        ngay_bao_cao = data.get('ngay_bao_cao') or datetime.now().strftime('%Y-%m-%d')
        loai = data.get('loai') or None
        nguoi_bao_cao_code = data.get('nv_bao_cao') or session.get('user_code')
        nguoi_lam_he_thong_value = session.get('user_code') or None
        ma_khach_hang_value = data.get('ma_doi_tuong_kh') or None
        
        hien_dien_truoc_1 = data.get('nhansu_hengap_1') or None
        hien_dien_truoc_2 = data.get('nhansu_hengap_2') or None
        
        noi_dung_1 = data.get('noi_dung_1') or None
        noi_dung_2 = data.get('noi_dung_2') or None
        noi_dung_3 = data.get('noi_dung_3') or None
        noi_dung_4 = data.get('noi_dung_4') or None
        noi_dung_5 = data.get('noi_dung_5') or None
        
        danh_gia_1 = data.get('danh_gia_1') or None
        danh_gia_2 = data.get('danh_gia_2') or None
        danh_gia_3 = data.get('danh_gia_3') or None
        danh_gia_4 = data.get('danh_gia_4') or None
        danh_gia_5 = data.get('danh_gia_5') or None
        
        attachments_str = attachments_str or None

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
            
            params = (
                ngay_bao_cao, loai, nguoi_bao_cao_code, nguoi_lam_he_thong_value,
                noi_dung_1, noi_dung_2, noi_dung_3, noi_dung_4, noi_dung_5,
                danh_gia_1, danh_gia_2, danh_gia_3, danh_gia_4, danh_gia_5,
                ma_khach_hang_value, hien_dien_truoc_1, hien_dien_truoc_2, attachments_str
            )
            
            if db_manager.execute_non_query(insert_query, params):
                # LOG REPORT CREATE (BỔ SUNG)
                db_manager  = current_app.db_manager
                get_user_ip  = current_app.get_user_ip

                db_manager.write_audit_log(
                    nguoi_bao_cao_code, 'REPORT_CREATE', 'INFO', 
                    f"Tạo báo cáo mới (Loại: {loai}, KH: {ma_khach_hang_value})", 
                    get_user_ip()
                )
                return redirect(url_for('crm_bp.dashboard_reports', success_message='Lưu thành công!'))
            else:
                message = "error: Thất bại khi thực thi INSERT SQL."
        except Exception as e:
            current_app.logger.error(f"LỖI TẠO BẢN GHI: {e}")
            message = f"error: Lỗi hệ thống SQL: {e}"

    return render_template(
        'nhap_lieu.html', 
        users=users_data,
        loai_bao_cao=loai_data,
        message=message,
        now_date=datetime.now(),
        default_usercode=default_usercode
    )

@crm_bp.route('/nhansu_nhaplieu', methods=['GET', 'POST'])
@login_required
def nhansu_nhaplieu():
    """Hàm xử lý form nhập liệu Nhân sự Liên hệ. (Đã chuyển logic)"""
    
    # FIX: Import Services Cần thiết NGAY TẠY ĐÂY
    db_manager = current_app.db_manager
    
    message = None
    default_ma_khachhang = None
    default_ten_khachhang = None
    
    if request.method == 'GET':
        kh_code = request.args.get('kh_code')
        if kh_code:
            default_ma_khachhang = kh_code.strip()
            default_ten_khachhang = db_manager.get_khachhang_by_ma(default_ma_khachhang) 
    
    if request.method == 'POST':
        data = request.form
        ma_cong_ty = data.get('ma_cong_ty_kh')
        
        # NOTE: Logic INSERT POST sẽ cần được chuyển đầy đủ nếu cần chạy
        pass

    return render_template('nhansu_nhaplieu.html', 
        message=message,
        default_ma_khachhang=default_ma_khachhang,
        default_ten_khachhang=default_ten_khachhang
    )


# [APIs hỗ trợ CRM]

@crm_bp.route('/api/khachhang/ref/<string:ma_doi_tuong>', methods=['GET'])
@login_required
def api_get_reference_data(ma_doi_tuong):
    """API MỚI: Lấy thông tin tham chiếu (COUNT Nhân sự)."""
    # FIX: Import Services Cần thiết NGAY TẠY ĐÂY
    db_manager = current_app.db_manager
    
    query_count = f"SELECT COUNT(T1.ID) AS CountNLH FROM dbo.{config.TEN_BANG_NHAN_SU_LH} AS T1 WHERE T1.[CONG TY] = ?"
    count_data = db_manager.get_data(query_count, (ma_doi_tuong,))
    count_nlh = count_data[0]['CountNLH'] if count_data and count_data[0]['CountNLH'] is not None else 0
    return jsonify({'CountNLH': count_nlh})
    
@crm_bp.route('/api/nhansu_ddl_by_khachhang/<string:ma_doi_tuong>', methods=['GET'])
@login_required
def api_nhansu_ddl_by_khachhang(ma_doi_tuong):
    """API MỚI: Lấy danh sách Nhân sự cho Dropdown."""
    # FIX: Import Services Cần thiết NGAY TẠY ĐÂY
    db_manager = current_app.db_manager
    
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

@crm_bp.route('/api/defaults/<string:loai_code>', methods=['GET'])
@login_required
def api_defaults(loai_code):
    """API tra cứu DLOOKUP cho tiêu đề và nội dung mặc định."""
    # FIX: Import Services Cần thiết NGAY TẠY ĐÂY
    db_manager = current_app.db_manager
    
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

@crm_bp.route('/api/nhansu/list/<string:ma_doi_tuong>', methods=['GET'])
@login_required
def api_nhansu_list(ma_doi_tuong):
    """API: Lấy danh sách chi tiết nhân sự theo khách hàng (cho bảng tham chiếu)."""
    db_manager = current_app.db_manager
    
    # Truy vấn đầy đủ các trường cần thiết cho bảng hiển thị
    query = f"""
        SELECT 
            MA AS ShortName, 
            ([TEN HO] + ' ' + [TEN THUONG GOI]) AS FullName,
            [CHUC VU] AS Title,
            [DIEN THOAI 1] AS Phone,
            [EMAIL] AS Email,
            [GHI CHU] AS Note
        FROM dbo.{config.TEN_BANG_NHAN_SU_LH} 
        WHERE [CONG TY] = ? 
        ORDER BY [TEN HO]
    """
    
    data = db_manager.get_data(query, (ma_doi_tuong,))
    
    if data:
        return jsonify(data)
    else:
        return jsonify([])

# Trong crm_bp.py

# Trong file crm_bp.py

@crm_bp.route('/api/nhansu_by_khachhang/<string:ma_doi_tuong>', methods=['GET'])
@login_required
def api_get_nhansu_list(ma_doi_tuong):
    """API: Lấy danh sách nhân sự chi tiết cho bảng tham chiếu (Đã sửa cột SO DTDD 1)."""
    db_manager = current_app.db_manager
    import config # Đảm bảo đã import config
    
    # SỬA TÊN CỘT [DIEN THOAI...] THÀNH [SO DTDD 1]
    query = f"""
        SELECT 
            MA AS ShortName,
            ISNULL([TEN HO], [TEN THUONG GOI]) AS FullName,
            [CHUC VU] AS Title,
            [SO DTDD 1] AS Phone,  -- <--- CẬP NHẬT TÊN CỘT CHÍNH XÁC TẠI ĐÂY
            [DIA CHI EMAIL] AS Email,
            [GHI CHU] AS Note
        FROM dbo.{config.TEN_BANG_NHAN_SU_LH} 
        WHERE [CONG TY] = ? 
        ORDER BY [TEN HO]
    """
    
    try:
        data = db_manager.get_data(query, (ma_doi_tuong,))
        return jsonify(data if data else [])
    except Exception as e:
        current_app.logger.error(f"Lỗi API Nhân sự: {e}")
        return jsonify([])
    

@crm_bp.route('/sales/backlog', methods=['GET', 'POST'])
@login_required
def sales_backlog_page():
    sales_service = current_app.sales_service
    task_service = current_app.task_service
    
    user_role = session.get('user_role', '').strip().upper()
    user_code = session.get('user_code')
    
    today = datetime.now()
    default_from = (today - timedelta(days=90)).strftime('%Y-%m-%d')
    default_to = today.strftime('%Y-%m-%d')
    
    date_from = request.form.get('date_from') or request.args.get('date_from') or default_from
    date_to = request.form.get('date_to') or request.args.get('date_to') or default_to
    
    # Logic Phân Quyền Lọc (Giữ nguyên)
    salesman_list = []
    selected_salesman = ''
    
    if user_role == config.ROLE_ADMIN:
        salesman_list = task_service.get_eligible_helpers(division=None)
        selected_salesman = request.form.get('salesman_id') or request.args.get('salesman_id') or ''
    else:
        selected_salesman = user_code

    # Gọi Service (Lấy toàn bộ)
    result = sales_service.get_sales_backlog(date_from, date_to, selected_salesman)
    all_details = result['details']
    summary = result['summary']

    # [CHANGE] KHÔNG CẮT LIST NỮA. GỬI TOÀN BỘ XUỐNG CHO DATATABLES XỬ LÝ
    
    return render_template(
        'sales_backlog.html',
        data=all_details,  # Gửi full list
        summary=summary,
        date_from=date_from,
        date_to=date_to,
        salesman_list=salesman_list,
        selected_salesman=selected_salesman,
        is_admin=(user_role == config.ROLE_ADMIN)
    )