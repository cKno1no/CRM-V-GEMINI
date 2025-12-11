# blueprints/cross_sell_bp.py

from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for, current_app
from utils import login_required
from datetime import datetime

cross_sell_bp = Blueprint('cross_sell_bp', __name__)

@cross_sell_bp.route('/cross_sell_dashboard', methods=['GET'])
@login_required
def dashboard():
    """Giao diện chính Cross-Sell DNA."""
    
    # Import tại chỗ để tránh vòng lặp
    db_manager = current_app.db_manager 
    from services.cross_sell_service import CrossSellService
    
    service = CrossSellService(db_manager)
    
    current_year = datetime.now().year
    
    # Lấy dữ liệu tổng hợp
    data = service.get_cross_sell_dna(year=current_year)
    
    return render_template(
        'cross_sell_dashboard.html',
        buckets=data['buckets'],
        summary=data['summary'],
        master_dna=data['master_dna'],
        current_year=current_year
    )

@cross_sell_bp.route('/api/cross_sell/detail/<string:client_id>', methods=['GET'])
@login_required
def api_detail(client_id):
    """API trả về chi tiết (cho phần mở rộng của bảng)."""
    db_manager = current_app.db_manager
    from services.cross_sell_service import CrossSellService
    
    service = CrossSellService(db_manager)
    current_year = datetime.now().year
    
    result = service.get_customer_gap_analysis(client_id, year=current_year)
    
    return jsonify(result)