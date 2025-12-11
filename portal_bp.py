# portal_bp.py
from flask import Blueprint, render_template, session, redirect, url_for, current_app
from services.portal_service import PortalService

from datetime import datetime

portal_bp = Blueprint('portal_bp', __name__)


@portal_bp.route('/portal')
def portal_dashboard():
    if not session.get('logged_in'):
        return redirect(url_for('login'))
    
    portal_service = current_app.portal_service
    db_manager = current_app.db_manager

    user_code = session.get('user_code')
    bo_phan = session.get('bo_phan', '').strip().upper()
    role = session.get('user_role', '').strip().upper()
    
    dashboard_data = portal_service.get_all_dashboard_data(user_code, bo_phan, role)

    return render_template(
        'portal_dashboard.html',
        user=session,
        now_date=datetime.now().strftime('%d/%m/%Y'),
        sales_kpi=dashboard_data['sales_kpi'],
        tasks=dashboard_data['tasks'],
        # approvals=dashboard_data['approvals'],  <-- Đã xóa
        orders_stat=dashboard_data['orders_stat'], # <-- Mới thêm
        overdue_debt=dashboard_data['overdue_debt'],
        active_quotes=dashboard_data['active_quotes'],
        pending_deliveries=dashboard_data['pending_deliveries'],
        orders_flow=dashboard_data['orders_flow'],
        recent_reports=dashboard_data['recent_reports'],
        urgent_replenish=dashboard_data['urgent_replenish'],
        errors=dashboard_data.get('errors') 
    )