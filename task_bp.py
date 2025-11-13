from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
# FIX: Chỉ import login_required từ utils.py
from utils import login_required 
from datetime import datetime
from db_manager import safe_float # Cần cho format/validation

task_bp = Blueprint('task_bp', __name__)

# [ROUTES]

@task_bp.route('/task_dashboard', methods=['GET', 'POST'])
@login_required
def task_dashboard():
    """ROUTE: Dashboard Quản lý Đầu việc hàng ngày."""
    
    # FIX: Import Services Cục bộ
    from app import task_service 
    
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    is_admin = user_role == 'ADMIN'
    
    view_mode = request.args.get('view', 'USER').upper()
    filter_type = request.args.get('filter') or 'ALL'
    text_search_term = request.args.get('search') or request.form.get('search') or ''

    can_manage_view = is_admin or user_role == 'MANAGER'

    # 1. XỬ LÝ TẠO TASK MỚI (Logic INSERT)
    if request.method == 'POST' and 'create_task' in request.form:
        title = request.form.get('task_title')
        supervisor_code = session.get('cap_tren')
        object_id = request.form.get('object_id') 
        task_type = request.form.get('task_type')
        
        attachments_filename = None 
        
        if title:
            if task_service.create_new_task(
                user_code, 
                title, 
                supervisor_code, 
                attachments=attachments_filename, 
                task_type=task_type, 
                object_id=object_id
            ):
                flash("Task mới đã được tạo thành công!", 'success')
            else:
                flash("Lỗi khi tạo Task. Vui lòng thử lại.", 'danger')
            return redirect(url_for('task_bp.task_dashboard'))
    
    # 2. GỌI DỮ LIỆU CHÍNH
    kpi_summary = task_service.get_kpi_summary(user_code, is_admin=is_admin)
    kanban_tasks = task_service.get_kanban_tasks(user_code, is_admin=is_admin, view_mode=view_mode)
    risk_history_tasks = task_service.get_filtered_tasks(
        user_code, 
        filter_type=filter_type, 
        is_admin=is_admin, 
        view_mode=view_mode, 
        text_search_term=text_search_term 
    )
    
    return render_template(
        'task_dashboard.html',
        kpi=kpi_summary,
        kanban_tasks=kanban_tasks, 
        history_tasks=risk_history_tasks, 
        is_admin=is_admin,
        current_date=datetime.now().strftime('%Y-%m-%d'),
        active_filter=filter_type,
        view_mode=view_mode,
        can_manage_view=can_manage_view, 
        text_search_term=text_search_term 
    )

# [APIs]

@task_bp.route('/api/task/log_progress', methods=['POST'])
@login_required
def api_log_task_progress():
    """API: Ghi Log Tiến độ mới (Progress, Blocked, Request_Close)."""
    
    # FIX: Import Services Cục bộ
    from app import task_service 
    
    data = request.get_json(silent=True) or {} 
    user_code = session.get('user_code')
    
    task_id = data.get('task_id')
    content = data.get('content', '')
    progress_percent = data.get('progress_percent') 
    log_type = data.get('log_type')
    helper_code = data.get('helper_code')

    if task_id is None or task_id.strip() == '' or \
       content.strip() == '' or \
       progress_percent is None or \
       log_type is None:
        
        return jsonify({'success': False, 'message': 'Thiếu dữ liệu bắt buộc (ID, Nội dung, Phần trăm, Loại Log).'}), 400

    try:
        log_id = task_service.log_task_progress(
            task_id=task_id,
            user_code=user_code,
            progress_percent=int(progress_percent),
            content=content,
            log_type=log_type,
            helper_code=helper_code 
        )
        
        if log_id:
            return jsonify({'success': True, 'message': f'Đã ghi nhận Log #{log_id} thành công!'})
        else:
            return jsonify({'success': False, 'message': 'Lỗi CSDL khi ghi Log.'}), 500

    except Exception as e:
        print(f"LỖI API LOG PROGRESS: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

@task_bp.route('/api/task/history/<int:task_id>', methods=['GET'])
@login_required
def api_get_task_history(task_id):
    """API: Lấy lịch sử Log tiến độ chi tiết cho Modal."""
    
    # FIX: Import Services Cục bộ
    from app import task_service
    
    try:
        logs = task_service.get_task_history_logs(task_id)
        return jsonify(logs)
    except Exception as e:
        print(f"LỖI API GET LOG HISTORY: {e}")
        return jsonify({'error': 'Lỗi khi tải lịch sử.'}), 500

@task_bp.route('/api/task/add_feedback', methods=['POST'])
@login_required
def api_add_supervisor_feedback():
    """API: Cấp trên thêm phản hồi trên LogID cụ thể (Request 3)."""
    
    # FIX: Import Services Cục bộ
    from app import task_service
    
    data = request.json
    supervisor_code = session.get('user_code')
    
    log_id = data.get('log_id')
    feedback = data.get('feedback')
    
    if not log_id or not feedback:
        return jsonify({'success': False, 'message': 'Thiếu LogID hoặc nội dung phản hồi.'}), 400

    try:
        success = task_service.add_supervisor_feedback(log_id, supervisor_code, feedback)
        
        if success:
            return jsonify({'success': True, 'message': f'Phản hồi đã được lưu vào Log #{log_id}.'})
        else:
            return jsonify({'success': False, 'message': 'Lỗi CSDL khi lưu phản hồi.'}), 500

    except Exception as e:
        print(f"LỖI API ADD FEEDBACK: {e}")
        return jsonify({'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}), 500

@task_bp.route('/api/task/toggle_priority/<int:task_id>', methods=['POST'])
@login_required
def api_toggle_task_priority(task_id):
    """API: Thay đổi Priority thành HIGH (hoặc ngược lại) khi nhấn biểu tượng sao."""
    
    # FIX: Import Services Cục bộ
    from app import task_service
    
    current_task_data = task_service.get_task_by_id(task_id) 
    if not current_task_data:
        return jsonify({'success': False, 'message': 'Task không tồn tại.'}), 404
        
    current_priority = current_task_data.get('Priority', 'NORMAL')
    new_priority = 'NORMAL' if current_priority == 'HIGH' else 'HIGH'
    
    success = task_service.update_task_priority(task_id, new_priority) 
    
    if success:
        return jsonify({'success': True, 'new_priority': new_priority}), 200
    return jsonify({'success': False, 'message': 'Lỗi CSDL khi cập nhật ưu tiên.'}), 500


@task_bp.route('/api/get_eligible_helpers', methods=['GET'])
@login_required
def api_get_eligible_helpers():
    """API: Trả về danh sách Helper đủ điều kiện (Usercode - Shortname)."""
    
    # FIX: Import Services Cục bộ
    from app import task_service
    
    try:
        helpers = task_service.get_eligible_helpers()
        formatted_helpers = [{'code': h['USERCODE'], 'name': f"{h['USERCODE']} - {h['SHORTNAME']}"} for h in helpers]
        return jsonify(formatted_helpers)
    except Exception as e:
        print(f"Lỗi API lấy danh sách helper: {e}")
        return jsonify([]), 500

@task_bp.route('/api/task/update', methods=['POST'])
@login_required
def api_update_task():
    """API: WRAPPER CŨ (Để tránh lỗi API nếu vẫn còn gọi từ code cũ)"""
    
    # FIX: Import Services Cục bộ
    from app import task_service
    
    data = request.json
    
    task_id = data.get('task_id')
    object_id = data.get('object_id', None)
    content = data.get('detail_content', '')
    status = data.get('status')
    helper_code = data.get('helper_code') 
    completed_date = data.get('status') == 'COMPLETED'
    
    # Chuyển hướng sang hàm wrapper trong service
    success = task_service.update_task_progress(
        task_id=task_id,
        object_id=object_id,
        content=content,
        status=status,
        helper_code=helper_code, 
        completed_date=completed_date
    )

    if success:
        return jsonify({'success': True, 'message': 'Tiến độ Task đã được cập nhật (qua wrapper).'})
    return jsonify({'success': False, 'message': 'Lỗi cập nhật CSDL.'}), 500