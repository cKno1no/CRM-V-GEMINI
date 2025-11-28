from flask import Blueprint, render_template, request, redirect, url_for, flash, session
# FIX: Chỉ import các helper từ utils.py (Đã được định nghĩa sớm và không gây vòng lặp)
from utils import login_required, truncate_content 
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

@kpi_bp.route('/sales_dashboard', methods=['GET', 'POST'])
@login_required
def sales_dashboard():
    """ROUTE: Bảng Tổng hợp Hiệu suất Sales."""
    
    # FIX: Import Sales Service Cục bộ
    from app import sales_service, db_manager # ADD db_manager
    
    current_year = datetime.now().year
    DIVISOR = 1000000.0 # Để chuyển đổi từ tiền tệ sang triệu đồng

    user_code = session.get('user_code')
    is_admin = session.get('user_role', '').strip().upper() == 'ADMIN' 
    
    if not user_code:
        flash("Lỗi phiên đăng nhập: Không tìm thấy mã nhân viên.", 'danger')
        return redirect(url_for('login'))

    # Gọi Sales Service để lấy dữ liệu hiệu suất
    summary_data = sales_service.get_sales_performance_data(current_year, user_code, is_admin)
    
    # Sắp xếp và tính tổng
    summary_data = sorted(summary_data, key=itemgetter('RegisteredSales'), reverse=True)
    
    total_registered_sales_raw = 0
    total_monthly_sales_raw = 0
    total_ytd_sales_raw = 0
    total_orders_raw = 0
    total_pending_orders_amount_raw = 0

    for row in summary_data:
        # Tính tổng RAW
        total_registered_sales_raw += row.get('RegisteredSales', 0)
        total_monthly_sales_raw += row.get('CurrentMonthSales', 0)
        total_ytd_sales_raw += row.get('TotalSalesAmount', 0)
        total_orders_raw += row.get('TotalOrders', 0)
        total_pending_orders_amount_raw += row.get('PendingOrdersAmount', 0)
        
        # Chuẩn bị dữ liệu hiển thị (Chia cho DIVISOR)
        row['RegisteredSales'] /= DIVISOR
        row['CurrentMonthSales'] /= DIVISOR
        row['TotalSalesAmount'] /= DIVISOR
        row['PendingOrdersAmount'] /= DIVISOR
    
    # Chuẩn bị tổng KPI hiển thị
    total_registered_sales = total_registered_sales_raw / DIVISOR
    total_monthly_sales = total_monthly_sales_raw / DIVISOR
    total_ytd_sales = total_ytd_sales_raw / DIVISOR
    total_orders = total_orders_raw
    total_pending_orders_amount = total_pending_orders_amount_raw / DIVISOR
    
    # LOG VIEW_SALES_DASHBOARD (BỔ SUNG)
    try:
        db_manager.write_audit_log(
            user_code, 'VIEW_SALES_DASHBOARD', 'INFO', 
            "Truy cập Dashboard Tổng hợp Hiệu suất Sales", 
            get_user_ip()
        )
    except Exception as e:
        print(f"Lỗi ghi log VIEW_SALES_DASHBOARD: {e}")
        
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

@kpi_bp.route('/sales_detail/<string:employee_id>', methods=['GET'])
@login_required
def sales_detail(employee_id):
    """ROUTE: Chi tiết Hiệu suất theo Khách hàng."""
    
    # FIX: Import DBManager và Sales Service Cục bộ
    from app import db_manager, sales_service
    
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
        print(f"Lỗi ghi log VIEW_SALES_DETAIL: {e}")

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

@kpi_bp.route('/realtime_dashboard', methods=['GET', 'POST'])
@login_required
def realtime_dashboard():
    """ROUTE: Dashboard KPI Bán hàng Thời gian thực."""
    
    # FIX: Import DBManager Cục bộ
    from app import db_manager
    
    current_year = datetime.now().year
    
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    is_admin = user_role == 'ADMIN'
    
    selected_salesman = None
    
    # Logic Lọc (POST/GET)
    if is_admin:
        if request.method == 'POST':
            filter_value = request.form.get('salesman_filter')
            selected_salesman = filter_value.strip() if filter_value and filter_value.strip() != '' else None
        else:
            selected_salesman = None
    else:
        # Nếu không phải Admin, chỉ thấy dữ liệu của mình
        selected_salesman = user_code
        
    users_data = []
    if is_admin:
        # Lấy danh sách nhân viên để Admin lọc
        query_users = f"""
        SELECT [USERCODE], [USERNAME], [SHORTNAME] 
        FROM {config.TEN_BANG_NGUOI_DUNG} 
        WHERE [PHONG BAN] IS NOT NULL AND RTRIM([PHONG BAN]) != '9. DU HOC'
        ORDER BY [SHORTNAME] 
        """
        users_data = db_manager.get_data(query_users)
        
    salesman_name = "TẤT CẢ NHÂN VIÊN"
    if selected_salesman and selected_salesman.strip():
        # Lấy tên salesman đã chọn
        name_data = db_manager.get_data(f"SELECT SHORTNAME FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?", (selected_salesman,))
        salesman_name = name_data[0]['SHORTNAME'] if name_data else selected_salesman
    
    # Chuẩn bị tham số cho Stored Procedure
    salesman_param_for_sp = selected_salesman.strip() if selected_salesman else None
    sp_params = (salesman_param_for_sp, current_year)
    
    # Gọi Stored Procedure trả về 5 bộ kết quả
    all_results = db_manager.execute_sp_multi('dbo.sp_GetRealtimeSalesKPI', sp_params)

    if not all_results or len(all_results) < 5:
        flash("Lỗi tải dữ liệu: Không đủ 5 bộ kết quả từ Stored Procedure. Vui lòng kiểm tra SP.", 'danger')
        all_results = [[]] * 5
        
    kpi_summary = all_results[0][0] if all_results[0] else {} # Kết quả đầu tiên là KPI Summary
    pending_orders = all_results[1]
    top_orders = all_results[2]
    top_quotes = all_results[3]
    upcoming_deliveries = all_results[4]

    # LOG VIEW_REALTIME_DASHBOARD (BỔ SUNG)
    try:
        db_manager.write_audit_log(
            user_code, 'VIEW_REALTIME_DASHBOARD', 'INFO', 
            f"Truy cập Dashboard Realtime (Filter: {selected_salesman or 'ALL'})", 
            get_user_ip()
        )
    except Exception as e:
        print(f"Lỗi ghi log VIEW_REALTIME_DASHBOARD: {e}")
        
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

@kpi_bp.route('/inventory_aging', methods=['GET', 'POST'])
@login_required
def inventory_aging_dashboard():
    """ROUTE: Phân tích Tuổi hàng Tồn kho."""
    
    # FIX: Import Inventory Service Cục bộ
    from app import inventory_service, db_manager # ADD db_manager
    
    DIVISOR = 1000000.0
    
    # Lấy điều kiện lọc từ form
    item_filter_term = request.form.get('item_filter', '').strip()
    category_filter = request.form.get('category_filter', '').strip() 
    qty_filter = request.form.get('qty_filter', '').strip()      
    value_filter = request.form.get('value_filter', '').strip()
    i05id_filter = request.form.get('i05id_filter', '').strip() 

    # Gọi Inventory Service
    filtered_data, totals = inventory_service.get_inventory_aging_data(
        item_filter_term, 
        category_filter, 
        qty_filter, 
        value_filter, 
        i05id_filter
    )
    
    # LOG VIEW_INVENTORY_AGING (BỔ SUNG)
    try:
        db_manager.write_audit_log(
            session.get('user_code'), 'VIEW_INVENTORY_AGING', 'WARNING', 
            f"Truy cập Phân tích Tuổi hàng Tồn kho (Filter: {item_filter_term})", 
            get_user_ip()
        )
    except Exception as e:
        print(f"Lỗi ghi log VIEW_INVENTORY_AGING: {e}")

    return render_template(
        'inventory_aging.html', 
        aging_data=filtered_data, 
        item_filter_term=item_filter_term,
        category_filter=category_filter,
        qty_filter=qty_filter,
        value_filter=value_filter,
        i05id_filter=i05id_filter,
        
        # KPI Tổng: Chia cho DIVISOR để hiển thị triệu đồng
        kpi_total_inventory=totals['total_inventory'] / DIVISOR,
        kpi_total_quantity=totals['total_quantity'],
        kpi_new_6_months=totals['total_new_6_months'] / DIVISOR,
        kpi_over_2_years=totals['total_over_2_years'] / DIVISOR,
        kpi_clc_value=totals['total_clc_value'] / DIVISOR
    )
    
@kpi_bp.route('/ar_aging', methods=['GET', 'POST'])
@login_required
def ar_aging_dashboard():
    """ROUTE: Hiển thị Dashboard Công nợ Quá hạn (AR Aging)."""
    
    # FIX: Import AR Aging Service Cục bộ
    from app import ar_aging_service, db_manager # ADD db_manager
    
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
        print(f"Lỗi ghi log VIEW_AR_AGING: {e}")

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
def ar_aging_detail_dashboard():
    """ROUTE: Hiển thị báo cáo chi tiết công nợ quá hạn theo VoucherNo."""
    
    from app import ar_aging_service, db_manager, task_service # Thêm task_service
    
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    is_admin_or_manager = user_role in ['ADMIN', 'GM', 'MANAGER']
    
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
        print(f"Lỗi ghi log VIEW_AR_AGING_DETAIL: {e}")

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
    
    from app import ar_aging_service, db_manager, get_user_ip 
    
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    
    customer_id = request.args.get('object_id') # Lấy ObjectID từ URL
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

    # 2. Fetch Detail Vouchers cho KH đó (sử dụng hàm cũ, chỉ lọc theo ID)
    aging_details = ar_aging_service.get_ar_aging_details_by_voucher(
        user_code, 
        user_role, 
        customer_id=customer_id,
        filter_salesman_id=None # Không cần lọc NVKD nữa vì đã lọc theo KH
    )
    
    total_overdue = kpi_summary['TotalOverdueDebt']

    # 3. LOG VIEW_AR_AGING_DETAIL_SINGLE
    try:
        db_manager.write_audit_log(
            user_code, 'VIEW_AR_AGING_DETAIL_SINGLE', 'INFO', 
            f"Xem Chi tiết Công nợ KH: {customer_id}", 
            get_user_ip()
        )
    except Exception as e:
        print(f"Lỗi ghi log VIEW_AR_AGING_DETAIL_SINGLE: {e}")

    return render_template(
        'ar_aging_detail_single.html', 
        kpi=kpi_summary,
        aging_details=aging_details,
        total_overdue=total_overdue
    )

@kpi_bp.route('/sales/profit_analysis', methods=['GET', 'POST'])
@login_required
def profit_analysis_dashboard():
    """ROUTE: Dashboard Phân tích Lợi nhuận Gộp."""
    from app import sales_service, db_manager
    
    user_code = session.get('user_code')
    is_admin = session.get('user_role', '').strip().upper() == 'ADMIN'
    
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