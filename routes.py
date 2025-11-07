import config # <--- BỔ SUNG DÒNG NÀY
from flask import Blueprint, render_template, request, session, redirect, url_for, flash
from services.sales_lookup_service import SalesLookupService
from db_manager import DBManager 
from datetime import datetime, timedelta

# Định nghĩa Blueprint
sales_bp = Blueprint('sales_bp', __name__)

def is_admin_check_simple(session):
    """Kiểm tra quyền Admin dựa trên session."""
    # Kiểm tra nếu user_role đã được lưu là 'ADMIN' (chữ hoa) sau khi login
    return session.get('user_role', '').strip().upper() == 'ADMIN'


@sales_bp.route('/sales_lookup', methods=['GET', 'POST'])
def sales_lookup_dashboard():
    """ROUTE: Dashboard tra cứu thông tin bán hàng."""
    
    # 1. Setup Environment & Services (Lấy từ global context hoặc khởi tạo)
    # Vì file app.py của bạn khởi tạo db_manager ở cấp global, ta cần truy cập nó.
    # Trong môi trường Blueprint, ta thường dùng current_app/g, nhưng ở đây ta khởi tạo lại cho đơn giản:
    db_manager = DBManager() # Khởi tạo lại DBManager
    lookup_service = SalesLookupService(db_manager) 

    # 2. Lấy thông tin người dùng
    user_code = session.get('user_code', 'GUEST') 
    # SỬ DỤNG HÀM KIỂM TRA QUYỀN ĐƠN GIẢN (Đã xử lý ở hàm login)
    is_admin = session.get('user_role', '').strip().upper() == 'ADMIN'
    
    # 3. Thu thập dữ liệu Form/URL
    inventory_ids = request.form.get('inventory_ids') or request.args.get('inventory_ids', '')
    object_id = request.form.get('object_id') or request.args.get('object_id', '')
    lookup_results = []
    
    # 4. Xử lý Request
    if request.method == 'POST' or (request.method == 'GET' and inventory_ids):
        if inventory_ids:
            # Gọi Service
            lookup_results = lookup_service.get_sales_lookup_data(
                inventory_ids, 
                object_id, 
                is_admin
            )
        else:
            flash("Vui lòng nhập Mã Mặt hàng để tra cứu.", 'warning')

    # 5. Render Template
    return render_template(
        'sales_lookup_dashboard.html',
        is_admin=is_admin,
        inventory_ids=inventory_ids,
        object_id=object_id,
        results=lookup_results
    )