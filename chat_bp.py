from flask import current_app
from flask import Blueprint, request, jsonify, current_app, session, render_template
from utils import login_required
import json
import config

chat_bp = Blueprint('chat_bp', __name__)

# --- HÀM HELPER: Lấy IP người dùng (Định nghĩa nội bộ để tránh lỗi 'Flask object has no attribute') ---
def get_user_ip():
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr

@chat_bp.route('/api/chatbot_query', methods=['POST'])
@login_required
def api_chatbot_query():
    """API: Nhận tin nhắn từ Widget Chatbot và trả về phản hồi."""
    
    # Lấy các services từ current_app
    db_manager = current_app.db_manager  
    chatbot_service = current_app.chatbot_service  

    # Lấy dữ liệu gửi lên
    data = request.json
    message = data.get('message', '').strip()
    theme = data.get('theme', 'light') # <--- Lấy theme từ request
    # Lấy thông tin user từ session
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    user_ip = get_user_ip()

    if not message:
        return jsonify({'response': 'Vui lòng nhập câu hỏi.'})
    
    try:
        # 1. Gọi "bộ não" Chatbot để xử lý
        response_message = chatbot_service.process_message(message, user_code, user_role, theme)
        
        # 2. Ghi log hành động (chỉ ghi nếu cần thiết để tránh spam log DB)
        try:
            db_manager.write_audit_log(
                user_code=user_code,
                action_type='API_CHATBOT_QUERY',
                severity='INFO',
                details=f"User hỏi: {message}",
                ip_address=user_ip
            )
        except Exception as log_error:
            current_app.logger.error(f"Lỗi ghi log chatbot: {log_error}")
        
        # 3. Trả về phản hồi cho Client
        return jsonify({'response': response_message})
        
    except Exception as e:
        current_app.logger.error(f"LỖI API Chatbot: {e}")
        # Ghi log lỗi hệ thống
        try:
            db_manager.write_audit_log(
                user_code=user_code,
                action_type='API_CHATBOT_QUERY_ERROR',
                severity='ERROR',
                details=f"Lỗi câu hỏi: {message}. Exception: {str(e)}",
                ip_address=user_ip
            )
        except:
            pass
            
        return jsonify({'response': f'Lỗi hệ thống: {str(e)}'}), 500

# === THÊM ROUTE NÀY VÀO ===
@chat_bp.route('/assistant', methods=['GET'])
@login_required
def chat_assistant_page():
    """Trang giao diện Chatbot Full màn hình (Có phân quyền)."""
    user_code = session.get('user_code')
    user_role = session.get('user_role', '').strip().upper()
    permissions = session.get('permissions', [])
    
    # 1. Lấy thông tin Level
    
    user_level = 1
    try:
        # [FIX] Đổi get_user_stats thành get_user_profile
        stats = current_app.user_service.get_user_profile(user_code) 
        if stats: user_level = stats.get('Level', 1)
    except Exception as e:
        current_app.logger.error(f"Lỗi check level chatbot: {e}")

    # 2. Kiểm tra điều kiện (Đồng bộ với base.html)
    can_access = (
        (user_role == config.ROLE_ADMIN) or 
        ('USE_CHATBOT' in permissions) or 
        (user_level >= 5)
    )

    # 3. Chặn nếu không đủ quyền
    if not can_access:
        flash(f"⛔ Bạn chưa đủ điều kiện truy cập (Level {user_level}/5). Hãy cày thêm XP!", "warning")
        return redirect(url_for('index')) # Quay về trang chủ

    # 4. Cho phép truy cập
    user_name = session.get('user_name', 'Sếp')
    return render_template('chat_assistant.html', user_name=user_name)