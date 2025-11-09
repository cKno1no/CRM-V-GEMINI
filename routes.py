# D:\CRM STDD\routes.py

import config 
# --- SỬA LỖI: Thêm 'jsonify' vào dòng import ---
from flask import Blueprint, render_template, request, session, redirect, url_for, flash, jsonify
# --- KẾT THÚC SỬA LỖI ---
from db_manager import DBManager 
from datetime import datetime, timedelta

# Định nghĩa Blueprint
sales_bp = Blueprint('sales_bp', __name__)

def is_admin_check_simple(session):
    """Kiểm tra quyền Admin dựa trên session."""
    return session.get('user_role', '').strip().upper() == 'ADMIN'


@sales_bp.route('/sales_lookup', methods=['GET', 'POST'])
def sales_lookup_dashboard():
    """
    ROUTE: Dashboard tra cứu thông tin bán hàng.
    """
    
    from app import lookup_service 
    
    # 1. Lấy thông tin người dùng
    user_code = session.get('user_code', 'GUEST') 
    
    user_role = session.get('user_role', '').strip().upper()
    is_admin_or_gm = user_role in ['ADMIN', 'GM']
    is_manager = user_role == 'MANAGER'
    
    show_block_3 = is_admin_or_gm
    show_block_2 = is_admin_or_gm or is_manager
    
    # 2. Thu thập dữ liệu Form/URL
    item_search = ""
    object_id = ""
    object_id_display = ""
    lookup_results = {} 
    
    # 3. Xử lý Request
    if request.method == 'POST':
        item_search = request.form.get('item_search', '').strip()
        object_id = request.form.get('object_id', '').strip() 
        object_id_display = request.form.get('object_id_display', '').strip()
        
        if not item_search:
            flash("Vui lòng nhập Tên hoặc Mã Mặt hàng để tra cứu.", 'warning')
        else:
            lookup_results = lookup_service.get_sales_lookup_data(
                item_search, 
                object_id 
            )
            
            if not lookup_results.get('block1') and not lookup_results.get('block2') and not lookup_results.get('block3'):
                flash(f"Không tìm thấy mặt hàng nào phù hợp với điều kiện tra cứu (Khách hàng: {object_id_display or 'Tất cả'}, Mặt hàng: '{item_search}').", 'info')

    # 5. Render Template
    return render_template(
        'sales_lookup_dashboard.html',
        item_search=item_search,
        object_id=object_id,
        object_id_display=object_id_display,
        results=lookup_results,
        
        show_block_2=show_block_2,
        show_block_3=show_block_3
    )

# --- API MỚI (YÊU CẦU 5) ---
@sales_bp.route('/api/quick_lookup', methods=['POST'])
def api_quick_lookup():
    """
    API: Tra cứu nhanh Tồn kho/Giá QĐ (Không lọc KH)
    """
    from app import lookup_service 
    
    item_search = request.form.get('item_search', '').strip()
    
    if not item_search:
        return jsonify({'error': 'Vui lòng nhập Tên hoặc Mã Mặt hàng.'}), 400
        
    try:
        data = lookup_service.get_quick_lookup_data(item_search)
        return jsonify(data)
    except Exception as e:
        print(f"LỖI API quick_lookup: {e}")
        return jsonify({'error': f'Lỗi server: {e}'}), 500