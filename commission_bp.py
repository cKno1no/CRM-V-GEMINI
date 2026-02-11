# blueprints/commission_bp.py

from flask import Blueprint, render_template, request, jsonify, session, current_app
from utils import login_required, permission_required 
from datetime import datetime
import config

commission_bp = Blueprint('commission_bp', __name__)

@commission_bp.route('/commission/request', methods=['GET'])
@login_required
@permission_required('CREATE_COMMISSION')
def commission_request_page():
    """Giao diện tạo đề xuất hoa hồng."""
    today = datetime.now()
    default_from = datetime(today.year, 1, 1).strftime('%Y-%m-%d')
    default_to = today.strftime('%Y-%m-%d')
    
    return render_template('commission_request.html', 
                           date_from=default_from, 
                           date_to=default_to)

# --- APIs ---

@commission_bp.route('/api/commission/create', methods=['POST'])
@login_required
def api_create_proposal():
    db_manager = current_app.db_manager
    from services.commission_service import CommissionService
    service = CommissionService(db_manager)
    data = request.json
    
    ma_so = service.create_proposal(
        user_code=session.get('user_code'),
        customer_id=data.get('customer_id'),
        date_from=data.get('date_from'),
        date_to=data.get('date_to'),
        commission_rate_percent=float(data.get('rate')),
        note=data.get('note', '')
    )
    
    if ma_so:
        # Lấy Details
        details = db_manager.get_data(f"SELECT * FROM {config.TABLE_COMMISSION_DETAIL} WHERE MA_SO = ? ORDER BY VoucherDate DESC", (ma_so,)) or []
        master = db_manager.get_data(f"SELECT * FROM {config.TABLE_COMMISSION_MASTER} WHERE MA_SO = ?", (ma_so,))[0]
        
        # [MỚI] Lấy Recipients (Lúc tạo mới thường rỗng, nhưng cứ trả về cho chuẩn)
        recipients = service.get_proposal_recipients(ma_so) or []

        return jsonify({
            'success': True, 
            'ma_so': ma_so,
            'master': master,
            'details': details,
            'recipients': recipients # [MỚI]
        })
    else:
        return jsonify({'success': False, 'message': 'Lỗi tạo phiếu.'}), 500

@commission_bp.route('/api/commission/toggle_item', methods=['POST'])
@login_required
def api_toggle_item():
    db_manager = current_app.db_manager
    from services.commission_service import CommissionService
    service = CommissionService(db_manager)
    data = request.json
    
    if service.toggle_invoice(data.get('detail_id'), data.get('is_checked')):
        master = db_manager.get_data(f"SELECT DOANH_SO_CHON, GIA_TRI_CHI FROM {config.TABLE_COMMISSION_MASTER} WHERE MA_SO = ?", (data.get('ma_so'),))
        return jsonify({'success': True, 'master': master[0]})
    return jsonify({'success': False}), 500

# [MỚI] API Thêm người nhận tiền thủ công
@commission_bp.route('/api/commission/add_contact', methods=['POST'])
@login_required
def api_add_contact_manual():
    db_manager = current_app.db_manager
    from services.commission_service import CommissionService
    service = CommissionService(db_manager)
    data = request.json
    ma_so = data.get('ma_so')
    
    success = service.add_manual_detail(
        ma_so=ma_so,
        contact_name=data.get('contact_name'),
        bank_name=data.get('bank_name'),
        bank_account=data.get('bank_account'),
        amount=float(data.get('amount', 0))
    )
    
    if success:
        # [MỚI] Trả về danh sách Recipients mới nhất để vẽ bảng
        recipients = service.get_proposal_recipients(ma_so)
        master = db_manager.get_data(f"SELECT DOANH_SO_CHON, GIA_TRI_CHI FROM {config.TABLE_COMMISSION_MASTER} WHERE MA_SO = ?", (ma_so,))
        
        return jsonify({
            'success': True, 
            'master': master[0],
            'recipients': recipients # [MỚI]
        })
        
    return jsonify({'success': False, 'message': 'Lỗi thêm người nhận'}), 500

@commission_bp.route('/api/commission/submit', methods=['POST'])
@login_required
def api_submit_proposal():
    db_manager = current_app.db_manager
    from services.commission_service import CommissionService
    service = CommissionService(db_manager)
    data = request.json
    result = service.submit_to_payment_request(data.get('ma_so'), session.get('user_code'))
    return jsonify(result)