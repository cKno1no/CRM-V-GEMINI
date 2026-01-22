# blueprints/executive_bp.py

from flask import Blueprint, render_template, session, redirect, url_for, flash, request, jsonify, current_app, g
from utils import login_required, permission_required, get_user_ip 
from datetime import datetime
import config

# Import User model để fallback
try:
    from models.user import User
except ImportError:
    User = None

executive_bp = Blueprint('executive_bp', __name__)

@executive_bp.route('/ceo_cockpit', methods=['GET'])
@login_required
@permission_required('VIEW_CEO_COCKPIT') 
def ceo_cockpit_page():
    """
    Render trang khung (Skeleton). 
    """
    today = datetime.now()
    
    # [FIX & FAIL-SAFE] Lấy User Context
    user_context = getattr(g, 'user', None)
    
    # Nếu utils.py chưa set g.user, ta tự load thủ công để tránh lỗi template
    if user_context is None and 'user_code' in session:
        try:
            db = current_app.db_manager
            # Load lại user
            u_data = db.get_data(f"SELECT * FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?", (session['user_code'],))
            if u_data and User:
                user_context = User(u_data[0])
                g.user = user_context # Gán ngược lại cho các hàm sau dùng
        except Exception as e:
            current_app.logger.error(f"Fail-safe load user error: {e}")

    return render_template(
        'ceo_cockpit.html',
        current_year=today.year,
        current_month=today.month,
        user_context=user_context 
    )

@executive_bp.route('/api/executive/dashboard_data', methods=['GET'])
@login_required
@permission_required('VIEW_CEO_COCKPIT')
def api_get_dashboard_data():
    """
    API trả về dữ liệu Dashboard JSON (Đã tối ưu Caching).
    """
    try:
        # Không cần user_code ở đây vì Service sẽ lấy dữ liệu tổng hợp
        today = datetime.now()
        current_year = today.year
        current_month = today.month

        # Gọi Service
        db_manager = current_app.db_manager
        from services.executive_service import ExecutiveService
        srv = ExecutiveService(db_manager)
        
        # [UPDATED] Gọi hàm có Caching thay vì tính toán lại từ đầu
        # Hàm này trả về cấu trúc dict đầy đủ: { kpi, charts, lists, summaries... }
        response_data = srv.get_dashboard_data_cached(current_year, current_month)
        
        return jsonify({'success': True, 'data': response_data})

    except Exception as e:
        current_app.logger.error(f"API Dashboard Error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500

@executive_bp.route('/analysis/comparison', methods=['GET'])
@login_required
@permission_required('VIEW_COMPARISON') 
def comparison_dashboard():
    db_manager = current_app.db_manager
    from services.executive_service import ExecutiveService
    exec_service = ExecutiveService(db_manager)
    
    current_year = datetime.now().year
    try:
        year1 = int(request.args.get('year1', current_year - 1))
        year2 = int(request.args.get('year2', current_year))
    except ValueError:
        year1 = current_year - 1
        year2 = current_year
    
    comp_data = exec_service.get_comparison_data(year1, year2)
    metrics = comp_data['metrics']
    delta = {}
    
    for key in metrics['y1']:
        val1 = metrics['y1'][key]
        val2 = metrics['y2'][key]
        diff = val2 - val1
        delta[key] = {'diff': diff, 'percent': (diff / val1 * 100) if val1 > 0 else (100.0 if val2 > 0 else 0.0)}

    # Fail-safe user context cho trang này luôn
    user_context = getattr(g, 'user', None)
    
    return render_template(
        'comparison_dashboard.html',
        year1=year1, year2=year2,
        m1=metrics['y1'], m2=metrics['y2'],
        delta=delta,
        chart_data=comp_data['chart'],
        user_context=user_context
    )

@executive_bp.route('/api/executive/drilldown', methods=['GET'])
@login_required
@permission_required('VIEW_CEO_COCKPIT')
def api_executive_drilldown():
    metric = request.args.get('metric')
    try:
        year = int(request.args.get('year', datetime.now().year))
    except ValueError:
        year = datetime.now().year
    
    db_manager = current_app.db_manager
    from services.executive_service import ExecutiveService
    exec_service = ExecutiveService(db_manager)
    
    data = exec_service.get_drilldown_data(metric, year)
    return jsonify(data)