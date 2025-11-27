# blueprints/executive_bp.py

from flask import Blueprint, render_template, session, redirect, url_for, flash
from utils import login_required
from datetime import datetime

executive_bp = Blueprint('executive_bp', __name__)

@executive_bp.route('/ceo_cockpit', methods=['GET'])
@login_required
def ceo_cockpit_dashboard():
    """
    ROUTE: Bảng điều hành trung tâm dành cho CEO/GM.
    """
    # 1. Kiểm tra quyền hạn (Chỉ ADMIN hoặc GM được vào)
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in ['ADMIN', 'GM']:
        flash("Bạn không có quyền truy cập CEO Cockpit.", "danger")
        return redirect(url_for('portal_bp.portal_dashboard'))

    # 2. Khởi tạo Service
    from app import db_manager
    from services.executive_service import ExecutiveService
    
    exec_service = ExecutiveService(db_manager)
    
    # 3. Lấy dữ liệu
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # A. Scorecards
    kpi_summary = exec_service.get_kpi_scorecards(current_year, current_month)
    
    # B. Charts Data (Biểu đồ)
    # Lấy chart doanh thu/lợi nhuận
    profit_chart_data = exec_service.get_profit_trend_chart()
    
    # C. Action Center & Leaderboard
    pending_actions = exec_service.get_pending_actions_count()
    top_sales = exec_service.get_top_sales_leaderboard(current_year)
    
    # Dữ liệu giả lập cho Inventory Donut (Nếu chưa muốn gọi query nặng ngay)
    # Bạn có thể thay bằng exec_service.get_inventory_structure() sau này
    
    return render_template(
        'ceo_cockpit.html',
        kpi_summary=kpi_summary,
        # Truyền thẳng các biến con để template dễ dùng (hoặc truyền nguyên object)
        profit_summary={ 
            'GrossProfit': kpi_summary['GrossProfit'], 
            'AvgMargin': kpi_summary['AvgMargin'] 
        },
        risk_summary={
            'Debt_Over_180': kpi_summary['Debt_Over_180'],
            'TotalOverdueDebt': kpi_summary['TotalOverdueDebt'],
            'Inventory_Over_2Y': kpi_summary['Inventory_Over_2Y']
        },
        chart_data=profit_chart_data, # Dữ liệu vẽ biểu đồ JS
        pending_actions=pending_actions,
        top_sales=top_sales
    )