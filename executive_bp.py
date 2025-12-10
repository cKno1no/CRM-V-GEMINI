# blueprints/executive_bp.py

from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify
from utils import login_required
from datetime import datetime
import config

executive_bp = Blueprint('executive_bp', __name__)

@executive_bp.route('/ceo_cockpit', methods=['GET'])
@login_required
def ceo_cockpit_dashboard():
    """
    ROUTE: Bảng điều hành trung tâm dành cho CEO/GM.
    """
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in [config.ROLE_ADMIN]:
        flash("Bạn không có quyền truy cập CEO Cockpit.", "danger")
        return redirect(url_for('portal_bp.portal_dashboard'))

    from app import db_manager
    from services.executive_service import ExecutiveService
    
    exec_service = ExecutiveService(db_manager)
    
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # A. Scorecards
    kpi_summary = exec_service.get_kpi_scorecards(current_year, current_month)
    
    # B. Charts Data (Updated)
    profit_chart_data = exec_service.get_profit_trend_chart()
    inventory_chart_data = exec_service.get_inventory_aging_chart_data()
    category_perf_data = exec_service.get_top_categories_performance(current_year)
    sales_funnel_data = exec_service.get_sales_funnel_data()
    
    # C. Action & Leaderboard
    pending_actions = exec_service.get_pending_actions_count()
    top_sales = exec_service.get_top_sales_leaderboard(current_year)
    
    return render_template(
        'ceo_cockpit.html',
        kpi_summary=kpi_summary,
        # Object Profit (YTD)
        profit_summary={ 
            'GrossProfit': kpi_summary['GrossProfit_YTD'], 
            'AvgMargin': kpi_summary['AvgMargin_YTD'] 
        },
        # Object Finance
        finance_summary={
            'TotalExpenses': kpi_summary['TotalExpenses_YTD'],
            'CrossSellProfit': kpi_summary['CrossSellProfit_YTD']
        },
        # Object Risk
        risk_summary={
            'Debt_Over_180': kpi_summary['Debt_Over_180'],
            'TotalOverdueDebt': kpi_summary['TotalOverdueDebt'],
            'Inventory_Over_2Y': kpi_summary['Inventory_Over_2Y']
        },
        
        # --- CHARTS DATA ---
        chart_data=profit_chart_data,              # Biểu đồ xu hướng Tài chính
        inventory_chart_data=inventory_chart_data, # Biểu đồ Tồn kho
        category_perf_data=category_perf_data,     # Biểu đồ Nhóm hàng
        sales_funnel_data=sales_funnel_data,       # Biểu đồ Phễu
        
        # --- LISTS ---
        pending_actions=pending_actions,
        top_sales=top_sales
    )

@executive_bp.route('/analysis/comparison', methods=['GET'])
@login_required
def comparison_dashboard():
    """Trang phân tích so sánh số liệu quản trị giữa 2 năm."""
    from app import db_manager
    from services.executive_service import ExecutiveService
    
    exec_service = ExecutiveService(db_manager)
    
    # Mặc định so sánh Năm nay vs Năm ngoái
    current_year = datetime.now().year
    
    try:
        year1 = int(request.args.get('year1', current_year - 1))
        year2 = int(request.args.get('year2', current_year))
    except ValueError:
        year1 = current_year - 1
        year2 = current_year
    
    # Lấy dữ liệu so sánh
    comp_data = exec_service.get_comparison_data(year1, year2)
    
    # Tính Delta (Chênh lệch)
    metrics = comp_data['metrics']
    delta = {}
    
    # Duyệt qua các chỉ số để tính % tăng trưởng
    for key in metrics['y1']:
        val1 = metrics['y1'][key]
        val2 = metrics['y2'][key]
        diff = val2 - val1
        
        # Tính %: Nếu năm cũ = 0 thì không chia được
        if val1 > 0:
            percent = (diff / val1) * 100
        elif val1 == 0 and val2 > 0:
            percent = 100.0
        else:
            percent = 0.0
            
        # [FIX QUAN TRỌNG] Dùng key 'percent' để khớp với HTML
        delta[key] = {'diff': diff, 'percent': percent}

    return render_template(
        'comparison_dashboard.html',
        year1=year1,
        year2=year2,
        m1=metrics['y1'],
        m2=metrics['y2'],
        delta=delta,
        chart_data=comp_data['chart']
    )

@executive_bp.route('/api/executive/drilldown', methods=['GET'])
@login_required
def api_executive_drilldown():
    """API trả về dữ liệu chi tiết cho Modal."""
    metric = request.args.get('metric')
    try:
        year = int(request.args.get('year', datetime.now().year))
    except ValueError:
        year = datetime.now().year
    
    from app import db_manager
    from services.executive_service import ExecutiveService
    exec_service = ExecutiveService(db_manager)
    
    data = exec_service.get_drilldown_data(metric, year)
    return jsonify(data)