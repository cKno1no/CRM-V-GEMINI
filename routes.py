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
    return session.get('user_role', '').strip().upper() == config.ROLE_ADMIN


@sales_bp.route('/sales_lookup', methods=['GET', 'POST'])
def sales_lookup_dashboard():
    """
    ROUTE: Dashboard tra cứu thông tin bán hàng.
    """
    
    from app import lookup_service 
    from app import db_manager # <-- THÊM VÀO ĐÂY
    # 1. Lấy thông tin người dùng
    user_code = session.get('user_code', 'GUEST') 
    
    user_role = session.get('user_role', '').strip().upper()
    is_admin_or_gm = user_role in [config.ROLE_ADMIN, config.ROLE_GM]
    is_manager = user_role == config.ROLE_MANAGER
    
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
        
        # --- ĐÂY LÀ NƠI GHI LOG CHO NÚT "TRA CỨU" ---
        try:
            log_details = f"Tra cứu (Form): item='{item_search}', kh='{object_id}'"
            db_manager.write_audit_log(
                user_code=session.get('user_code'),
                action_type='API_SALES_LOOKUP',
                severity='INFO',
                details=log_details,
                ip_address=request.remote_addr # Tạm dùng remote_addr
            )
        except Exception as e:
            print(f"Lỗi ghi log sales_lookup: {e}")
        # --- KẾT THÚC GHI LOG ---


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

@sales_bp.route('/api/multi_lookup', methods=['POST'])
def api_multi_lookup():
    """
    API: Tra cứu nhiều mã (ngăn cách bằng dấu phẩy)
    """
    from app import lookup_service 
    from app import db_manager # <-- THÊM VÀO ĐÂY
    item_search = request.form.get('item_search', '').strip()
    
    if not item_search:
        return jsonify({'error': 'Vui lòng nhập Tên hoặc Mã Mặt hàng.'}), 400
        
    try:
        # --- ĐÂY LÀ NƠI GHI LOG CHO NÚT "TRA NHANH TỒN" ---
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='API_QUICK_LOOKUP',
            severity='INFO',
            details=f"Tra nhanh (Multi): item='{item_search}'",
            ip_address=request.remote_addr
        )
        # --- KẾT THÚC GHI LOG ---
        # Gọi hàm MỚI
        data = lookup_service.get_multi_lookup_data(item_search)
        return jsonify(data)
    except Exception as e:
        print(f"LỖI API multi_lookup: {e}")
        return jsonify({'error': f'Lỗi server: {e}'}), 500

@sales_bp.route('/api/backorder_details/<string:inventory_id>', methods=['GET'])
def api_get_backorder_details(inventory_id):
    """
    API: Lấy chi tiết BackOrder (PO) cho một mã hàng
    """
    from app import lookup_service, db_manager # Import tại chỗ
    
    if not inventory_id:
        return jsonify({'error': 'Vui lòng cung cấp Mã Mặt hàng.'}), 400
        
    try:
        # --- GHI LOG (Requirement 2 - Ghi log hành động xem) ---
        db_manager.write_audit_log(
            user_code=session.get('user_code'),
            action_type='API_BACKORDER_DETAIL',
            severity='INFO',
            details=f"Xem chi tiết BackOrder cho: {inventory_id}",
            ip_address=request.remote_addr # Tạm dùng remote_addr
        )
        # --- KẾT THÚC GHI LOG ---
        
        data = lookup_service.get_backorder_details(inventory_id)
        return jsonify(data)
        
    except Exception as e:
        print(f"LỖI API backorder_details: {e}")
        return jsonify({'error': f'Lỗi server: {e}'}), 500