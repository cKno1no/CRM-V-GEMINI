from flask import Blueprint, render_template, request, jsonify, current_app, session, flash, redirect, url_for
from utils import login_required, permission_required

customer_analysis_bp = Blueprint('customer_analysis_bp', __name__)

@customer_analysis_bp.route('/customer_360/<string:object_id>', methods=['GET'])
@login_required
@permission_required('VIEW_CUSTOMER_360')
def customer_360_view(object_id):
    service = current_app.customer_analysis_service
    user_code = session.get('user_code')
    user_role = session.get('user_role')

    # --- 1. KIỂM TRA QUYỀN DỮ LIỆU (DATA ACCESS) ---
    allow_access, msg_access = service.check_data_access_permission(user_code, user_role, object_id)
    if not allow_access:
        flash(f"Từ chối truy cập: {msg_access}", "danger")
        return redirect(url_for('index')) # Hoặc trang trước đó

    # --- 2. KIỂM TRA GIỚI HẠN (RATE LIMIT) ---
    allow_limit, msg_limit = service.check_daily_view_limit(user_code, user_role)
    if not allow_limit:
        flash(f"Cảnh báo: {msg_limit}", "warning")
        return redirect(url_for('index'))

    # --- 3. LẤY DỮ LIỆU NẾU ĐƯỢC PHÉP ---
    info = service.get_customer_info(object_id)
    if not info: return "Khách hàng không tồn tại", 404
    
    header_metrics = service.get_header_metrics(object_id)

    return render_template(
        'customer_profile.html', 
        customer=info,
        metrics=header_metrics,
        object_id=object_id
    )

@customer_analysis_bp.route('/api/customer_360/charts/<string:object_id>', methods=['GET'])
@login_required
def api_customer_charts(object_id):
    service = current_app.customer_analysis_service
    try:
        return jsonify({
            'success': True,
            # [NEW] Gọi hàm cấu trúc đơn hàng thay vì trend 5 năm
            'sales_structure': service.get_sales_structure_stock_vs_order(object_id),
            
            'category': service.get_category_analysis(object_id),
            'top_products': service.get_top_products(object_id),
            'missed_opps': service.get_missed_opportunities_quotes(object_id),
            'price_compliance': service.get_price_analysis_candlestick(object_id)
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500

# API MỚI CHO DRILL-DOWN
@customer_analysis_bp.route('/api/customer_360/drilldown', methods=['POST'])
@login_required
def api_customer_drilldown():
    service = current_app.customer_analysis_service
    data = request.json
    object_id = data.get('object_id')
    drill_type = data.get('drill_type') # YEAR_SALES, CATEGORY
    filter_value = data.get('filter_value') # 2024, 'I04'
    
    try:
        details = service.get_drilldown_details(object_id, drill_type, filter_value)
        return jsonify({'success': True, 'data': details})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500