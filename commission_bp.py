# blueprints/commission_bp.py

from flask import Blueprint, render_template, request, jsonify, session, current_app
from utils import login_required
from datetime import datetime
import config
commission_bp = Blueprint('commission_bp', __name__)

@commission_bp.route('/commission/request', methods=['GET'])
@login_required
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
    """API: Tạo phiếu mới và lấy danh sách hóa đơn."""
    db_manager = current_app.db_manager
    from services.commission_service import CommissionService
    
    service = CommissionService(db_manager)
    data = request.json
    user_code = session.get('user_code')
    
    # 1. Gọi Service tạo phiếu (Stored Procedure)
    ma_so = service.create_proposal(
        user_code=user_code,
        customer_id=data.get('customer_id'),
        date_from=data.get('date_from'),
        date_to=data.get('date_to'),
        commission_rate_percent=float(data.get('rate'))
    )
    
    if ma_so:
        # 1. Lấy Chi tiết (Sửa ORDER BY VoucherDate)
        details = db_manager.get_data(
            f"SELECT * FROM {config.TABLE_COMMISSION_DETAIL} WHERE MA_SO = ? ORDER BY VoucherDate DESC", 
            (ma_so,)
        ) or [] 
        
        master_data = db_manager.get_data(
            f"SELECT * FROM {config.TABLE_COMMISSION_MASTER} WHERE MA_SO = ?", 
            (ma_so,)
        )
        
        if master_data:
            master = master_data[0]
            return jsonify({
                'success': True, 
                'ma_so': ma_so,
                'master': master,
                'details': details
            })
        else:
            # Trường hợp hiếm: SP chạy xong nhưng không Select lại được Master
            return jsonify({'success': False, 'message': 'Lỗi: Không tìm thấy thông tin phiếu vừa tạo.'}), 500
    else:
        return jsonify({'success': False, 'message': 'Lỗi khi thực thi tạo phiếu (SP trả về null).'}), 500

@commission_bp.route('/api/commission/toggle_item', methods=['POST'])
@login_required
def api_toggle_item():
    """API: Tick chọn/bỏ chọn hóa đơn."""
    db_manager = current_app.db_manager
    from services.commission_service import CommissionService
    
    service = CommissionService(db_manager)
    data = request.json
    
    success = service.toggle_invoice(
        detail_id=data.get('detail_id'),
        is_checked=data.get('is_checked')
    )
    
    if success:
        ma_so = data.get('ma_so')
        
        # [FIX]: Dùng f-string với config
        master_data = db_manager.get_data(
            f"SELECT DOANH_SO_CHON, GIA_TRI_CHI FROM {config.TABLE_COMMISSION_MASTER} WHERE MA_SO = ?", 
            (ma_so,)
        )
        
        if master_data:
            return jsonify({'success': True, 'master': master_data[0]})
            
    return jsonify({'success': False}), 500

@commission_bp.route('/api/commission/submit', methods=['POST'])
@login_required
def api_submit_proposal():
    """API: Gửi duyệt."""
    db_manager = current_app.db_manager
    from services.commission_service import CommissionService
    
    service = CommissionService(db_manager)
    data = request.json
    
    result = service.submit_to_payment_request(
        ma_so=data.get('ma_so'),
        user_code=session.get('user_code')
    )
    return jsonify(result)