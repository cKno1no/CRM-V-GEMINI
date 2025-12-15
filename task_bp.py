from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify, current_app
# FIX: Chỉ import login_required từ utils.py
from utils import login_required, permission_required # Import thêm
from datetime import datetime
from db_manager import safe_float # Cần cho format/validation
import config 
task_bp = Blueprint('task_bp', __name__)

# [HÀM HELPER CẦN THIẾT]
def get_user_ip():
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

# [ROUTES]

@task_bp.route('/task_dashboard', methods=['GET', 'POST'])
@login_required
@permission_required('VIEW_TASK')
def task_dashboard():
    """ROUTE: Dashboard Quản lý Đầu việc hàng ngày."""
    
    task_service = current_app.task_service 
    db_manager = current_app.db_manager # Cần để ghi log
    
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    
    # Biến này xác định user CÓ PHẢI là Admin thực sự hay không (để hiện nút chuyển view)
    real_is_admin = (user_role == config.ROLE_ADMIN)
    
    # Lấy View Mode từ URL
    view_mode = request.args.get('view', 'USER').upper()
    filter_type = request.args.get('filter') or 'ALL'
    text_search_term = request.args.get('search') or request.form.get('search') or ''

    # LOGIC MỚI: Xác định cờ "is_admin" để truy vấn dữ liệu
    # 1. Nếu đang xem View Cá nhân (USER) -> Coi như không phải Admin để lọc theo UserCode
    # 2. Nếu đang xem View Quản lý (SUPERVISOR) -> Giữ nguyên quyền Admin (để xem tất cả)
    if view_mode == 'USER':
        is_admin_for_query = False 
    else:
        is_admin_for_query = real_is_admin

    can_manage_view = real_is_admin or (user_role == config.ROLE_MANAGER)

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
                # LOG TASK CREATION (BỔ SUNG)
                try:
                    db_manager = current_app.db_manager
                    db_manager.write_audit_log(
                        user_code, 'TASK_CREATE', 'INFO', 
                        f"Tạo Task mới: {title} (Type: {task_type}, Obj: {object_id})", 
                        get_user_ip()
                    )
                except Exception as e:
                    print(f"Lỗi ghi log TASK_CREATE: {e}")
                    
                flash("Task mới đã được tạo thành công!", 'success')
            else:
                flash("Lỗi khi tạo Task. Vui lòng thử lại.", 'danger')
            return redirect(url_for('task_bp.task_dashboard'))
    
    # 2. GỌI DỮ LIỆU CHÍNH (Cập nhật tham số)
    
    # Cập nhật: Truyền view_mode vào get_kpi_summary
    kpi_summary = task_service.get_kpi_summary(
        user_code, 
        is_admin=is_admin_for_query, 
        view_mode=view_mode  # <--- MỚI
    )
    
    kanban_tasks = task_service.get_kanban_tasks(
        user_code, 
        is_admin=is_admin_for_query, 
        view_mode=view_mode
    )
    
    risk_history_tasks = task_service.get_filtered_tasks(
        user_code, 
        filter_type=filter_type, 
        is_admin=is_admin_for_query, 
        view_mode=view_mode, 
        text_search_term=text_search_term 
    )
    
    return render_template(
        'task_dashboard.html',
        kpi=kpi_summary,
        kanban_tasks=kanban_tasks, 
        history_tasks=risk_history_tasks, 
        is_admin=real_is_admin, # Vẫn truyền quyền thật xuống template để hiện các nút chức năng
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
    """API: Ghi Log Tiến độ mới và Tạo Task Hỗ trợ (Fan-out)."""
    
    # FIX: Import Services Cục bộ
    task_service   = current_app.task_service 
    db_manager     = current_app.db_manager 
    
    data = request.get_json(silent=True) or {} 
    user_code = session.get('user_code')
    
    task_id = data.get('task_id')
    content = data.get('content', '')
    progress_percent = data.get('progress_percent') 
    log_type = data.get('log_type')
    
    # Lấy danh sách người hỗ trợ (Mảng)
    helper_codes = data.get('helper_codes', [])
    if not isinstance(helper_codes, list):
         helper_codes = [helper_codes] if helper_codes else []

    if not task_id or not log_type:
        return jsonify({'success': False, 'message': 'Thiếu dữ liệu bắt buộc.'}), 400

    try:
        # --- 1. NẾU LÀ HELP_CALL: TẠO CÁC TASK CON (FAN-OUT) ---
        if log_type == 'HELP_CALL' and helper_codes:
             count = task_service.process_help_request_multicast(
                helper_codes_list=helper_codes,
                original_task_id=task_id,
                current_user_code=user_code,
                detail_content=content
            )
             # Bổ sung thông tin vào nội dung log để dễ theo dõi
             content = f"{content} (Đã gửi yêu cầu đến {count} người)"

        # --- 2. GHI LOG & CẬP NHẬT TRẠNG THÁI TASK GỐC ---
        # Chuyển danh sách helper thành chuỗi để lưu vào cột HelperRequestCode (cho mục đích hiển thị log)
        helper_code_str = ",".join(helper_codes)

        log_id = task_service.log_task_progress(
            task_id=task_id,
            user_code=user_code,
            progress_percent=int(progress_percent),
            content=content,
            log_type=log_type,
            helper_code=helper_code_str 
        )
        
        if log_id:
            # Ghi Audit Log
            db_manager.write_audit_log(
                user_code, 'TASK_LOG_PROGRESS', 'INFO', 
                f"Log tiến độ Task #{task_id} (Type: {log_type})", 
                get_user_ip()
            )
            return jsonify({'success': True, 'message': f'Đã cập nhật thành công!'})
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
    task_service   = current_app.task_service 
    
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
    task_service   = current_app.task_service 
    
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
    task_service   = current_app.task_service 
    
    current_task_data = task_service.get_task_by_id(task_id) 
    if not current_task_data:
        return jsonify({'success': False, 'message': 'Task không tồn tại.'}), 404
        
    current_priority = current_task_data.get('Priority', 'NORMAL')
    new_priority = 'NORMAL' if current_priority == 'HIGH' else 'HIGH'
    
    success = task_service.update_task_priority(task_id, new_priority) 
    
    if success:
        # LOG TASK PRIORITY TOGGLE (BỔ SUNG)
        db_manager = current_app.db_manager
        db_manager.write_audit_log(
            session.get('user_code'), 'TASK_PRIORITY_TOGGLE', 'WARNING', 
            f"Thay đổi ưu tiên Task #{task_id} thành {new_priority}", 
            get_user_ip()
        )
        return jsonify({'success': True, 'new_priority': new_priority}), 200
    return jsonify({'success': False, 'message': 'Lỗi CSDL khi cập nhật ưu tiên.'}), 500


@task_bp.route('/api/get_eligible_helpers', methods=['GET'])
@login_required
def api_get_eligible_helpers():
    """API: Trả về danh sách Helper đủ điều kiện (Usercode - Shortname)."""
    
    # FIX: Import Services Cục bộ
    task_service   = current_app.task_service 
    
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
    task_service   = current_app.task_service 
    
    data = request.json
    
    task_id = data.get('task_id')
    object_id = data.get('object_id', None)
    content = data.get('detail_content', '')
    status = data.get('status')
    

    helper_codes = data.get('helper_codes', []) 
    if not isinstance(helper_codes, list): 
        helper_codes = [helper_codes] if helper_codes else []

    # --- 1. XỬ LÝ TASK HỖ TRỢ ---
    if status and status.upper() == 'HELP_NEEDED':
        if helper_codes:
            # Gọi hàm Multicast mới
            count = task_service.process_help_request_multicast(
                helper_codes_list=helper_codes,
                original_task_id=task_id,
                current_user_code=session.get('user_code'),
                detail_content=data.get('detail_content', '')
            )
            # (Có thể thêm thông báo: "Đã gửi yêu cầu cho X người")
        else:
            return jsonify({'success': False, 'message': 'Vui lòng chọn ít nhất 1 người.'}), 400

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

@task_bp.route('/api/task/recent_updates', methods=['GET'])
@login_required
def api_task_recent_updates():
    """API: Trả về danh sách TaskID có cập nhật trong 15 phút gần nhất."""
    
    task_service   = current_app.task_service 
    
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    is_admin = user_role == config.ROLE_ADMIN
    # Lấy view_mode từ request.args để áp dụng bộ lọc quyền
    view_mode = request.args.get('view', 'USER').upper()
    minutes_ago = int(request.args.get('minutes', 15))
    
    try:
        updated_tasks = task_service.get_recently_updated_tasks(
            user_code, 
            is_admin=is_admin, 
            view_mode=view_mode,
            minutes_ago=minutes_ago
        )
        # Trả về list objects chứa TaskID và LastUpdated
        return jsonify(updated_tasks) 
    except Exception as e:
        print(f"LỖI API GET RECENT UPDATES: {e}")
        return jsonify({'error': 'Lỗi khi tải cập nhật gần nhất.'}), 500