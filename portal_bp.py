# portal_bp.py
from flask import Blueprint, render_template, session, redirect, url_for, current_app, flash, request
from datetime import datetime

portal_bp = Blueprint('portal_bp', __name__)

# ---------------------------------------------------------
# [NEW] HÀM TẠO KEY CACHE CHO PORTAL
# ---------------------------------------------------------
def make_portal_cache_key():
    """Key cache phụ thuộc vào User đang đăng nhập"""
    user_code = session.get('user_code', 'anon')
    # Key ví dụ: portal_data_KD010
    return f"portal_data_{user_code}"

@portal_bp.route('/portal')
def portal_dashboard():
    # Kiểm tra đăng nhập (Giữ nguyên logic của bạn)
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    # 1. KIỂM TRA CACHE
    cache_key = make_portal_cache_key()
    
    # Thử lấy dữ liệu Dashboard từ Redis
    # Lưu ý: Chỉ cache cục dữ liệu nặng (dashboard_data), không cache session hay datetime
    dashboard_data = current_app.cache.get(cache_key)
    
    # 2. MISS CACHE: TÍNH TOÁN DỮ LIỆU TỪ SQL
    if not dashboard_data:
        # current_app.logger.info(f"PORTAL CACHE MISS: {cache_key}")
        
        portal_service = current_app.portal_service
        # db_manager = current_app.db_manager (Không dùng thì bỏ qua)

        user_code = session.get('user_code')
        bo_phan = session.get('bo_phan', '').strip().upper()
        role = session.get('user_role', '').strip().upper()
        
        try:
            # Gọi Service (Query nặng)
            dashboard_data = portal_service.get_all_dashboard_data(user_code, bo_phan, role)
            
            # Lưu vào Redis trong 3 tiếng (300 giây)
            if dashboard_data:
                current_app.cache.set(cache_key, dashboard_data, timeout=10800)
                
        except Exception as e:
            current_app.logger.error(f"Lỗi tải dữ liệu Portal: {e}")
            dashboard_data = {} # Trả về rỗng để không crash trang

    # 3. RENDER TEMPLATE
    # Kết hợp dữ liệu từ Cache (dashboard_data) và dữ liệu thực (session, datetime)
    return render_template(
        'portal_dashboard.html',
        user=session,                                   # Luôn lấy session hiện tại
        now_date=datetime.now().strftime('%d/%m/%Y'),   # Luôn lấy giờ hiện tại
        
        # Sử dụng .get() để an toàn hơn nếu cache cũ thiếu key
        sales_kpi=dashboard_data.get('sales_kpi'),
        tasks=dashboard_data.get('tasks'),
        # approvals=dashboard_data.get('approvals'), <-- Đã xóa theo code cũ
        orders_stat=dashboard_data.get('orders_stat'),
        overdue_debt=dashboard_data.get('overdue_debt'),
        active_quotes=dashboard_data.get('active_quotes'),
        pending_deliveries=dashboard_data.get('pending_deliveries'),
        orders_flow=dashboard_data.get('orders_flow'),
        recent_reports=dashboard_data.get('recent_reports'),
        urgent_replenish=dashboard_data.get('urgent_replenish'),
        errors=dashboard_data.get('errors') 
    )

# ---------------------------------------------------------
# [NEW] ROUTE LÀM MỚI DỮ LIỆU (XÓA CACHE)
# Gọi link này khi user bấm nút "Làm mới" trên giao diện
# ---------------------------------------------------------
@portal_bp.route('/portal/refresh')
def refresh_portal():
    if not session.get('logged_in'):
        return redirect(url_for('login'))
        
    cache_key = make_portal_cache_key()
    current_app.cache.delete(cache_key)
    
    flash("Đã cập nhật dữ liệu mới nhất.", "success")
    return redirect(url_for('portal_bp.portal_dashboard'))