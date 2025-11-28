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
    user_role = session.get('user_role', '').strip().upper()
    if user_role not in ['ADMIN', 'GM']:
        flash("Bạn không có quyền truy cập CEO Cockpit.", "danger")
        return redirect(url_for('portal_bp.portal_dashboard'))

    from app import db_manager
    from services.executive_service import ExecutiveService
    
    exec_service = ExecutiveService(db_manager)
    
    current_year = datetime.now().year
    current_month = datetime.now().month
    
    # A. Scorecards (đã bao gồm Expense & Cashflow)
    kpi_summary = exec_service.get_kpi_scorecards(current_year, current_month)
    
    # B. Charts Data
    profit_chart_data = exec_service.get_profit_trend_chart()
    
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
        # Object Finance (Mới)
        finance_summary={
            'TotalExpenses': kpi_summary['TotalExpenses_YTD'],
            # NetCashFlow giờ đây đã được thay thế bởi CrossSellProfit
            'CrossSellProfit': kpi_summary['CrossSellProfit_YTD']
        },
        risk_summary={
            'Debt_Over_180': kpi_summary['Debt_Over_180'],
            'TotalOverdueDebt': kpi_summary['TotalOverdueDebt'],
            'Inventory_Over_2Y': kpi_summary['Inventory_Over_2Y']
        },
        chart_data=profit_chart_data,
        pending_actions=pending_actions,
        top_sales=top_sales
    )