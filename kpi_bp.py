
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, current_app
# FIX: Chỉ import các helper từ utils.py (Đã được định nghĩa sớm và không gây vòng lặp)
from utils import login_required, truncate_content, permission_required # Import thêm 
from datetime import datetime, timedelta
# Import các thư viện tiêu chuẩn không phụ thuộc vào app.py
from db_manager import safe_float 
from operator import itemgetter 
import config 

# Khởi tạo Blueprint (Không cần url_prefix vì các route này là routes cấp cao)
kpi_bp = Blueprint('kpi_bp', __name__)

# [HÀM HELPER CẦN THIẾT]
def get_user_ip():
    """Lấy địa chỉ IP của người dùng."""
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr


# [ROUTES]

# --- HÀM TẠO KEY CACHE (Đặt ngay trên route sales_dashboard) ---
def make_dashboard_cache_key():
    """Tạo key duy nhất cho mỗi User + Bộ lọc ngày"""
    user_code = session.get('user_code', 'anon')
    # Lấy tham số từ URL (GET) hoặc Form (POST) để cache đúng dữ liệu đang xem
    # Nếu là POST (Lọc form), ta lấy từ form, nếu GET lấy từ args hoặc mặc định
    if request.method == 'POST':
        date_from = request.form.get('date_from', 'def_from')
        date_to = request.form.get('date_to', 'def_to')
        salesman = request.form.get('salesman_id', 'all')
    else:
        date_from = request.args.get('date_from', 'def_from')
        date_to = request.args.get('date_to', 'def_to')
        salesman = request.args.get('salesman_id', 'all')
        
    # Key ví dụ: sales_dash_KD010_2025-01-01_2025-01-31_all
    return f"sales_dash_{user_code}_{date_from}_{date_to}_{salesman}"

@kpi_bp.route('/sales_dashboard', methods=['GET', 'POST'])
@login_required
@permission_required('VIEW_SALES_DASHBOARD') # Áp dụng quyền mới
def sales_dashboard():
    """ROUTE: Bảng Tổng hợp Hiệu suất Sales (ĐÃ CÓ CACHE)"""
    
    # 1. TẠO KEY VÀ KIỂM TRA CACHE
    # Chỉ cache khi request là GET (xem mặc định) hoặc POST (Lọc).
    # Tuy nhiên, cẩn thận với Flash message, cache HTML sẽ lưu luôn flash message cũ.
    # Giải pháp an toàn nhất cho Dashboard nặng: Cache DỮ LIỆU TÍNH TOÁN, không cache HTML.
    
    cache_key = make_dashboard_cache_key()
    
    # Thử lấy dữ liệu đã tính toán từ Redis
    # current_app.cache được lấy từ factory đã setup
    cached_data = current_app.cache.get(cache_key)
    
    if cached_data:
        # HIT CACHE: Có dữ liệu rồi -> Render ngay lập tức
        current_app.logger.info(f"CACHE HIT: {cache_key}")
        # Giải nén dữ liệu từ cache và return template
        return render_template('sales_dashboard.html', **cached_data)

    # =========================================================
    # MISS CACHE: Nếu không có cache, chạy logic tính toán cũ
    # =========================================================
    
    sales_service = current_app.sales_service
    db_manager = current_app.db_manager
    
    current_year = datetime.now().year
    
    user_role = session.get('user_role', '').strip().upper()
    is_admin = (user_role == config.ROLE_ADMIN) 
    user_code = session.get('user_code')
    user_division = session.get('division')

    if not user_code:
        flash("Lỗi phiên đăng nhập: Không tìm thấy mã nhân viên.", 'danger')
        return redirect(url_for('login'))

    # 1. Lấy dữ liệu
    summary_data = sales_service.get_sales_performance_data(
        current_year, 
        user_code, 
        is_admin, 
        division=user_division 
    )
    
    # Sắp xếp
    summary_data = sorted(summary_data, key=itemgetter('RegisteredSales'), reverse=True)
    
    # Khởi tạo biến tổng RAW (VNĐ)
    total_registered_sales_raw = 0
    total_monthly_sales_raw = 0
    total_ytd_sales_raw = 0
    total_orders_raw = 0
    total_pending_orders_amount_raw = 0

    # 2. Tính tổng
    for row in summary_data:
        total_registered_sales_raw += row.get('RegisteredSales', 0)
        total_monthly_sales_raw += row.get('CurrentMonthSales', 0)
        total_ytd_sales_raw += row.get('TotalSalesAmount', 0)
        total_orders_raw += row.get('TotalOrders', 0)
        total_pending_orders_amount_raw += row.get('PendingOrdersAmount', 0)
    
    # 3. [QUAN TRỌNG] TẠO BIẾN KPI_SUMMARY MÀ HTML CẦN
    # HTML dùng filter | format_tr nên ta truyền số RAW (VNĐ) vào
    kpi_summary = {
        'Sales_YTD': total_ytd_sales_raw,
        'Sales_CurrentMonth': total_monthly_sales_raw,
        'Sales_LastYear': 0, # Hiện tại chưa tính được năm ngoái trong view này, để 0 hoặc bổ sung logic
        'PendingOrdersAmount': total_pending_orders_amount_raw
    }

    # =========================================================
    # LƯU VÀO CACHE TRƯỚC KHI TRẢ VỀ
    # =========================================================
    
    # Đóng gói toàn bộ biến cần thiết cho template vào 1 dictionary
    render_context = {
        'summary': summary_data,
        'current_year': current_year,
        'kpi_summary': kpi_summary,
        'total_registered_sales': total_registered_sales_raw,
        'total_monthly_sales': total_monthly_sales_raw,
        'total_ytd_sales': total_ytd_sales_raw,
        'total_orders': total_orders_raw,
        'total_pending_orders_amount': total_pending_orders_amount_raw
    }
    
    # Lưu vào Redis trong 6 tiếng
    current_app.cache.set(cache_key, render_context, timeout=21600)

    # 4. Ghi Log
    try:
        db_manager.write_audit_log(
            user_code, 'VIEW_SALES_DASHBOARD', 'INFO', 
            "Truy cập Dashboard Tổng hợp Hiệu suất Sales", 
            get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi ghi log: {e}")
        
    # 5. Render Template
    return render_template('sales_dashboard.html', **render_context)

@kpi_bp.route('/sales_detail/<string:employee_id>', methods=['GET'])
@login_required
def sales_detail(employee_id):
    """ROUTE: Chi tiết Hiệu suất theo Khách hàng."""
    
    # FIX: Import DBManager và Sales Service Cục bộ
    
    db_manager  = current_app.db_manager
    sales_service  = current_app.sales_service 

    current_year = datetime.now().year
    DIVISOR = 1000000.0
    
    # Gọi Sales Service để lấy chi tiết khách hàng
    registered_clients, new_business_clients, total_poa_amount_raw, total_registered_sales_raw = \
        sales_service.get_client_details_for_salesman(employee_id, current_year)
    
    # Lấy tên nhân viên
    salesman_name_data = db_manager.get_data(f"SELECT SHORTNAME FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?", (employee_id,))
    salesman_name = salesman_name_data[0]['SHORTNAME'] if salesman_name_data else employee_id

    final_client_summary = registered_clients + new_business_clients
    
    # Tính tổng KPI cho trang chi tiết
    total_ytd_sales = sum(row.get('TotalSalesAmount', 0) for row in final_client_summary)
    total_monthly_sales = sum(row.get('CurrentMonthSales', 0) for row in final_client_summary)
    
    total_registered_sales_display = total_registered_sales_raw / DIVISOR
    
    # LOG VIEW_SALES_DETAIL (BỔ SUNG)
    try:
        db_manager.write_audit_log(
            session.get('user_code'), 'VIEW_SALES_DETAIL', 'INFO', 
            f"Xem chi tiết hiệu suất cho NV: {employee_id}", 
            get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi ghi log VIEW_SALES_DETAIL: {e}")

    return render_template(
        'sales_details.html', 
        employee_id=employee_id,
        salesman_name=salesman_name,
        client_summary=final_client_summary,
        total_registered_sales=total_registered_sales_display,
        total_ytd_sales=total_ytd_sales,
        total_monthly_sales=total_monthly_sales,
        total_poa_amount=total_poa_amount_raw / DIVISOR, 
        current_year=current_year
    )

def make_realtime_cache_key():
    """Key phụ thuộc vào User đang xem và Filter Salesman họ chọn"""
    user_code = session.get('user_code', 'anon')
    
    # Lấy tham số filter (từ POST hoặc GET)
    if request.method == 'POST':
        salesman_filter = request.form.get('salesman_filter', 'all')
    else:
        # Nếu GET (mặc định vào trang), filter có thể rỗng
        salesman_filter = request.args.get('salesman_filter', 'all')
        
    # Key ví dụ: realtime_KD010_all_2025
    return f"realtime_{user_code}_{salesman_filter}_{datetime.now().year}"

@kpi_bp.route('/realtime_dashboard', methods=['GET', 'POST'])
@login_required
@permission_required('VIEW_REALTIME_KPI') # Áp dụng quyền mới
def realtime_dashboard():
    """ROUTE: Dashboard KPI Bán hàng Thời gian thực (CACHE 60s)."""
    
    # =========================================================
    # 1. KIỂM TRA CACHE
    # =========================================================
    cache_key = make_realtime_cache_key()
    
    # Thử lấy dữ liệu từ Redis
    cached_data = current_app.cache.get(cache_key)
    
    if cached_data:
        # HIT CACHE: Có dữ liệu -> Render ngay (Siêu nhanh)
        # Vì là realtime nên log mức debug hoặc info tùy bạn
        # current_app.logger.info(f"REALTIME CACHE HIT: {cache_key}")
        return render_template('realtime_dashboard.html', **cached_data)

    # =========================================================
    # 2. MISS CACHE: TÍNH TOÁN DỮ LIỆU (LOGIC CŨ)
    # =========================================================
    
    db_manager = current_app.db_manager 

    current_year = datetime.now().year
    user_division = session.get('division')
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    is_admin = user_role == config.ROLE_ADMIN
    
    selected_salesman = None
    
    # Logic Lọc (POST/GET)
    if is_admin:
        if request.method == 'POST':
            filter_value = request.form.get('salesman_filter')
            selected_salesman = filter_value.strip() if filter_value and filter_value.strip() != '' else None
        else:
            selected_salesman = None
    else:
        selected_salesman = user_code
        
    users_data = []
    if is_admin:
        query_users = f"""
        SELECT [USERCODE], [USERNAME], [SHORTNAME] 
        FROM {config.TEN_BANG_NGUOI_DUNG} 
        WHERE [PHONG BAN] IS NOT NULL 
        AND RTRIM([PHONG BAN]) != '9. DU HOC'
        AND [Division] = ? 
        ORDER BY [SHORTNAME] 
        """
        users_data = db_manager.get_data(query_users, (user_division,))
        
    salesman_name = "TẤT CẢ NHÂN VIÊN"
    if selected_salesman and selected_salesman.strip():
        name_data = db_manager.get_data(f"SELECT SHORTNAME FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?", (selected_salesman,))
        salesman_name = name_data[0]['SHORTNAME'] if name_data else selected_salesman
    
    # Chuẩn bị tham số cho SP
    salesman_param_for_sp = selected_salesman.strip() if selected_salesman else None
    sp_params = (salesman_param_for_sp, current_year)
    
    # Gọi SP (Nặng)
    all_results = db_manager.execute_sp_multi(config.SP_GET_REALTIME_KPI, sp_params)

    if not all_results or len(all_results) < 5:
        flash("Lỗi dữ liệu: SP không trả về đủ 5 bảng kết quả.", 'warning')
        kpi_summary = {}
        pending_orders = []
        top_orders = []
        top_quotes = []
        upcoming_deliveries = []
    else:
        kpi_summary = all_results[0][0] if all_results[0] else {}
        pending_orders = all_results[1] if all_results[1] else [] 
        top_orders = all_results[2] if all_results[2] else []
        top_quotes = all_results[3] if all_results[3] else []
        upcoming_deliveries = all_results[4] if all_results[4] else []
    
    # Đồng bộ số liệu Backlog (Logic Fix lệch số)
    sales_service = current_app.sales_service
    start_date = f"{current_year}-01-01"
    end_date = datetime.now().strftime('%Y-%m-%d')

    try:
        backlog_data = sales_service.get_sales_backlog(start_date, end_date, salesman_param_for_sp)
        
        real_pending_value = backlog_data['summary']['total_backlog']
        real_pending_count = backlog_data['summary']['count']
        
        if kpi_summary:
            kpi_summary['PendingValue'] = real_pending_value
            kpi_summary['PendingOrdersAmount'] = real_pending_value 
            kpi_summary['PendingCount'] = real_pending_count
    except Exception as e:
        current_app.logger.error(f"Lỗi đồng bộ Backlog Realtime: {e}")

    # =========================================================
    # 3. LƯU CACHE VÀ RETURN
    # =========================================================
    
    # Đóng gói dữ liệu vào Dictionary context
    render_context = {
        'kpi_summary': kpi_summary,
        'pending_orders': pending_orders,
        'top_orders': top_orders,
        'top_quotes': top_quotes,
        'upcoming_deliveries': upcoming_deliveries,
        'users': users_data,
        'selected_salesman': selected_salesman,
        'salesman_name': salesman_name,
        'current_year': current_year,
        'is_admin': is_admin
    }
    
    # Lưu Cache trong 6 tiếng (Vì là Realtime nên time ngắn)
    current_app.cache.set(cache_key, render_context, timeout=18600)
    
    return render_template('realtime_dashboard.html', **render_context)

@kpi_bp.route('/inventory_aging', methods=['GET', 'POST'])
@login_required
@permission_required('VIEW_INVENTORY_AGING')
def inventory_aging_dashboard():
    """ROUTE: Phân tích Tuổi hàng Tồn kho (HTMX Ready)."""
    
    db_manager = current_app.db_manager
    inventory_service = current_app.inventory_service
    DIVISOR = config.DIVISOR_VIEW # Dùng config chuẩn

    # Lấy filters (Hỗ trợ cả GET và POST từ HTMX)
    if request.method == 'POST':
        item_filter_term = request.form.get('item_filter', '').strip()
        category_filter = request.form.get('category_filter', '').strip()
        i05id_filter = request.form.get('i05id_filter', '').strip()
        value_filter = request.form.get('value_filter', '').strip()
        qty_filter = request.form.get('qty_filter', '').strip()
    else:
        item_filter_term = request.args.get('item_filter', '').strip()
        category_filter = request.args.get('category_filter', '').strip()
        i05id_filter = request.args.get('i05id_filter', '').strip()
        value_filter = request.args.get('value_filter', '').strip()
        qty_filter = request.args.get('qty_filter', '').strip()

    # Gọi Service
    filtered_data, totals = inventory_service.get_inventory_aging_data(
        item_filter_term, category_filter, qty_filter, value_filter, i05id_filter
    )
    
    # Context dữ liệu
    context = {
        'aging_data': filtered_data,
        'totals': totals,
        # Các biến filter để điền lại vào form nếu cần
        'item_filter_term': item_filter_term,
        'category_filter': category_filter,
        # ... (các biến KPI tổng để hiển thị ở header)
        'kpi_total_inventory': totals['total_inventory'],
        'kpi_new_6_months': totals['total_new_6_months'],
        'kpi_clc_value': totals['total_clc_value'],
        'kpi_over_2_years': totals['total_over_2_years']
    }

    # [HTMX LOGIC] 
    # Nếu request có header 'HX-Request', chỉ trả về phần dòng bảng (Partial)
    if request.headers.get('HX-Request'):
        return render_template('partials/_inventory_table_rows.html', **context)

    # Nếu truy cập bình thường, trả về toàn trang (Full Layout)
    return render_template('inventory_aging.html', **context)


@kpi_bp.route('/api/inventory/group_detail/<string:group_id>', methods=['GET'])
@login_required
def api_inventory_group_detail(group_id):
    """
    API trả về các dòng chi tiết (<tr>) của một nhóm hàng.
    Dùng cho Lazy Load HTMX.
    """
    inventory_service = current_app.inventory_service
    
    # 1. Lấy toàn bộ dữ liệu (Vì đang dùng Cache nên tốc độ rất nhanh, không lo query lại DB)
    # Nếu muốn tối ưu hơn nữa, bạn cần viết hàm service get_items_by_group_id(group_id) riêng.
    # Nhưng với Cache hiện tại, lọc từ RAM Python vẫn cực nhanh.
    all_groups, _ = inventory_service.get_inventory_aging_data(
        item_filter_term='', category_filter='', qty_filter='', value_filter='', i05id_filter=''
    )
    
    # 2. Tìm nhóm đang cần
    # Giả định GroupID là mã nhóm
    group_data = next((g for g in all_groups if g['GroupID'] == group_id), None)
    
    if not group_data:
        return '<tr class="bg-light"><td colspan="10" class="text-center text-muted p-3">Không tìm thấy dữ liệu.</td></tr>'

    # 3. Trả về Partial HTML (chỉ các dòng tr)
    return render_template('partials/_inventory_lazy_items.html', items=group_data['Items'])

@kpi_bp.route('/ar_aging', methods=['GET', 'POST'])
@login_required
@permission_required('VIEW_AR_AGING') # Áp dụng quyền mới
def ar_aging_dashboard():
    """ROUTE: Hiển thị Dashboard Công nợ Quá hạn (AR Aging)."""
    
    # FIX: Import AR Aging Service Cục bộ
    
    
    ar_aging_service = current_app.ar_aging_service
    db_manager = current_app.db_manager

    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    user_bo_phan = session.get('bo_phan', '').strip().upper() # <-- THÊM DÒNG NÀY
    customer_name_filter = request.form.get('customer_name', '')
    
    # Gọi AR Aging Service
    aging_data = ar_aging_service.get_ar_aging_summary(
        user_code, 
        user_role, 
        user_bo_phan, # <-- TRUYỀN THAM SỐ MỚI
        customer_name_filter
    )
    
    # Tính tổng KPI Công nợ
    kpi_total_debt = sum(row.get('TotalDebt', 0) for row in aging_data)
    kpi_total_overdue = sum(row.get('TotalOverdueDebt', 0) for row in aging_data)
    kpi_over_180 = sum(row.get('Debt_Over_180', 0) for row in aging_data)
    
    # LOG VIEW_AR_AGING (BỔ SUNG)
    try:
        db_manager.write_audit_log(
            user_code, 'VIEW_AR_AGING', 'WARNING', 
            f"Truy cập Dashboard Công nợ (Filter: {customer_name_filter or 'ALL'})", 
            get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi ghi log VIEW_AR_AGING: {e}")

    return render_template(
        'ar_aging.html', 
        aging_data=aging_data,
        customer_name_filter=customer_name_filter,
        kpi_total_debt=kpi_total_debt,
        kpi_total_overdue=kpi_total_overdue,
        kpi_over_180=kpi_over_180
    )


@kpi_bp.route('/ar_aging_detail', methods=['GET', 'POST'])
@login_required
@permission_required('VIEW_AR_AGING') # Áp dụng quyền mới
def ar_aging_detail_dashboard():
    """ROUTE: Hiển thị báo cáo chi tiết công nợ quá hạn theo VoucherNo."""
    
    
    
    ar_aging_service = current_app.ar_aging_service
    db_manager = current_app.db_manager
    task_service = current_app.task_service

    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    is_admin_or_manager = user_role in [config.ROLE_ADMIN, config.ROLE_GM, config.ROLE_MANAGER]
    
    # Xử lý Filters
    customer_name_filter = request.form.get('customer_name', '')
    filter_salesman_id = request.form.get('salesman_filter')
    customer_id_filter = request.args.get('customer_id') 

    # Lấy danh sách NVKD (Cần cho form lọc)
    salesman_list = task_service.get_eligible_helpers() 
    
    # Lọc data (Dùng hàm mới)
    aging_details = ar_aging_service.get_ar_aging_details_by_voucher(
        user_code, 
        user_role, 
        customer_id=customer_id_filter,
        customer_name=customer_name_filter,
        filter_salesman_id=filter_salesman_id # Truyền tham số NVKD được chọn
    )
    
    # LOG VIEW_AR_AGING_DETAIL (BỔ SUNG)
    try:
        db_manager.write_audit_log(
            user_code, 'VIEW_AR_AGING_DETAIL', 'WARNING', 
            f"Truy cập Chi tiết Công nợ (KH: {customer_id_filter or customer_name_filter or 'ALL'})", 
            get_user_ip()
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi ghi log VIEW_AR_AGING_DETAIL: {e}")

    # Tính Subtotal cho Nợ Quá hạn (Yêu cầu 1)
    total_overdue = sum(row.get('Debt_Total_Overdue', 0) for row in aging_details)

    return render_template(
        'ar_aging_detail.html', 
        aging_details=aging_details,
        customer_name_filter=customer_name_filter,
        filter_salesman_id=filter_salesman_id,
        salesman_list=salesman_list,
        total_overdue=total_overdue,
        is_admin_or_manager=is_admin_or_manager
    )

    # Thêm vào file kpi_bp.py (sau các route hiện có)

@kpi_bp.route('/ar_aging_detail_single', methods=['GET'])
@login_required
def ar_aging_detail_single_customer():
    """ROUTE: Trang chi tiết công nợ cho MỘT KHÁCH HÀNG (Drill-down)."""
    
    ar_aging_service = current_app.ar_aging_service
    # get_user_ip = current_app.get_user_ip  <-- XÓA DÒNG NÀY (Gây lỗi)
    
    db_manager = current_app.db_manager

    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    
    customer_id = request.args.get('object_id') 
    if not customer_id:
        flash("Thiếu Mã Khách hàng để xem chi tiết.", 'danger')
        return redirect(url_for('kpi_bp.ar_aging_dashboard'))
    
    # 1. Fetch KPI Summary cho KH
    kpi_summary = ar_aging_service.get_single_customer_aging_summary(
        customer_id, user_code, user_role
    )

    if not kpi_summary:
        flash(f"Không tìm thấy dữ liệu công nợ cho Mã KH {customer_id}.", 'warning')
        return redirect(url_for('kpi_bp.ar_aging_dashboard'))

    # 2. Fetch Detail Vouchers
    aging_details = ar_aging_service.get_ar_aging_details_by_voucher(
        user_code, 
        user_role, 
        customer_id=customer_id,
        filter_salesman_id=None 
    )
    
    total_overdue = kpi_summary['TotalOverdueDebt']

    # 3. LOG
    try:
        # Gọi hàm get_user_ip() trực tiếp (đã được định nghĩa ở đầu file kpi_bp.py)
        db_manager.write_audit_log(
            user_code, 'VIEW_AR_AGING_DETAIL_SINGLE', 'INFO', 
            f"Xem Chi tiết Công nợ KH: {customer_id}", 
            get_user_ip() 
        )
    except Exception as e:
        current_app.logger.error(f"Lỗi ghi log VIEW_AR_AGING_DETAIL_SINGLE: {e}")

    return render_template(
        'ar_aging_detail_single.html', 
        kpi=kpi_summary,
        aging_details=aging_details,
        total_overdue=total_overdue
    )

@kpi_bp.route('/sales/profit_analysis', methods=['GET', 'POST'])
@login_required
@permission_required('VIEW_PROFIT_ANALYSIS') # Áp dụng quyền mới
def profit_analysis_dashboard():
    """ROUTE: Dashboard Phân tích Lợi nhuận Gộp."""
    # --- CÁCH GỌI MỚI (DÙNG) ---
    sales_service = current_app.sales_service
    db_manager = current_app.db_manager
    
    user_code = session.get('user_code')
    is_admin = session.get('user_role', '').strip().upper() == config.ROLE_ADMIN
    
    # [UPDATED] Mặc định từ đầu năm (1/1) đến hiện tại
    today = datetime.now()
    default_from = datetime(today.year, 1, 1).strftime('%Y-%m-%d') # Sửa tại đây
    default_to = today.strftime('%Y-%m-%d')
    
    date_from = request.form.get('date_from') or request.args.get('date_from') or default_from
    date_to = request.form.get('date_to') or request.args.get('date_to') or default_to
    
    # Gọi Service
    # Gọi Service
    details, summary = sales_service.get_profit_analysis(date_from, date_to, user_code, is_admin)
    
    # Lấy danh sách nhân viên để Admin lọc (nếu cần mở rộng sau này)
    salesman_list = []
    if is_admin:
        salesman_list = db_manager.get_data("SELECT USERCODE, SHORTNAME FROM [GD - NGUOI DUNG] WHERE [BO PHAN] LIKE '%KINH DOANH%'")

    return render_template(
        'profit_dashboard.html',
        details=details,
        summary=summary,
        date_from=date_from,
        date_to=date_to,
        salesman_list=salesman_list,
        is_admin=is_admin
    )