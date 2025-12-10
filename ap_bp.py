from flask import Blueprint, render_template, request, jsonify, session, redirect, url_for
from utils import login_required
import config

ap_bp = Blueprint('ap_bp', __name__)

@ap_bp.route('/ap_aging', methods=['GET', 'POST'])
@login_required
def ap_aging_dashboard():
    """Giao diện Quản lý Nợ Phải Trả."""
    from app import db_manager
    from services.ap_aging_service import APAgingService
    
    # Check quyền: Chỉ Admin, Kế toán, hoặc Mua hàng (Giả định)
    user_role = session.get('user_role', '').strip().upper()
    bo_phan = session.get('bo_phan', '').strip()
    
    # Logic quyền đơn giản (Mở rộng tùy nhu cầu)
    if user_role not in [config.ROLE_ADMIN, config.ROLE_GM] and 'KT' not in bo_phan:
         # flash("Bạn không có quyền truy cập.", "warning")
         # return redirect(url_for('index'))
         pass

    service = APAgingService(db_manager)
    
    vendor_filter = request.form.get('vendor_name', '').strip()
    debt_type_filter = request.form.get('debt_type', 'ALL') # Lấy tham số mới
    # Lấy dữ liệu
    
    aging_data = service.get_ap_aging_summary(vendor_filter, debt_type_filter)
    
    # YÊU CẦU 5: KPI Tổng & Quá hạn LOẠI TRỪ GTG, VLP
    kpi_total_payable = sum(row['TotalDebt'] for row in aging_data if row['DebtType'] not in ['GTG', 'VLP'])
    kpi_total_overdue = sum(row['TotalOverdueDebt'] for row in aging_data if row['DebtType'] not in ['GTG', 'VLP'])
    
    # YÊU CẦU 6: KPI mới cho GTG và VLP
    kpi_gtg_total = sum(row['TotalDebt'] for row in aging_data if row['DebtType'] == 'GTG')
    kpi_vlp_total = sum(row['TotalDebt'] for row in aging_data if row['DebtType'] == 'VLP')

    return render_template(
        'ap_aging.html',
        aging_data=aging_data,
        vendor_filter=vendor_filter,
        debt_type_filter=debt_type_filter,
        
        # Truyền KPI mới
        kpi_total_payable=kpi_total_payable,
        kpi_total_overdue=kpi_total_overdue,
        kpi_gtg_total=kpi_gtg_total,
        kpi_vlp_total=kpi_vlp_total
    )

# --- SỬA: API lấy chi tiết phải nhận thêm tham số ---
@ap_bp.route('/api/ap_detail/<string:vendor_id>', methods=['GET'])
@login_required
def api_ap_detail(vendor_id):
    from app import db_manager
    from services.ap_aging_service import APAgingService
    
    # Lấy debt_type từ query string (?type=BANK)
    debt_type = request.args.get('type', 'SUPPLIER') 
    
    service = APAgingService(db_manager)
    # Truyền vào service
    data = service.get_ap_details(vendor_id, debt_type)
    return jsonify(data)