from flask import Blueprint, request, session, jsonify
# FIX: Chỉ import login_required từ utils.py
from utils import login_required
import json

chat_bp = Blueprint('chat_bp', __name__)

# [HÀM HELPER CẦN THIẾT] (Copy từ approval_bp.py hoặc utils nếu cần)
def get_user_ip():
    if request.headers.getlist("X-Forwarded-For"):
       return request.headers.getlist("X-Forwarded-For")[0]
    else:
       return request.remote_addr


@chat_bp.route('/api/chatbot_query', methods=['POST'])
@login_required
def api_chatbot_query():
    """API: Nhận tin nhắn từ Widget Chatbot và trả về phản hồi."""
    
    # FIX: Import Services Cục bộ
    from app import chatbot_service, db_manager 
    
    data = request.json
    message = data.get('message', '').strip()
    
    user_code = session.get('user_code')
    user_ip = get_user_ip() # Get IP here

    if not message:
        return jsonify({'response': 'Vui lòng nhập câu hỏi.'})
        
    user_role = session.get('user_role', '').strip().upper()
    
    try:
        # 1. Gọi "bộ não" Chatbot
        response_message = chatbot_service.process_message(message, user_code, user_role)
        
        # 2. GHI LOG API_CHATBOT_QUERY (BỔ SUNG)
        db_manager.write_audit_log(
            user_code=user_code,
            action_type='API_CHATBOT_QUERY',
            severity='INFO',
            details=f"User hỏi: {message}",
            ip_address=user_ip
        )
        
        # 3. Trả về phản hồi đã định dạng
        return jsonify({'response': response_message})
        
    except Exception as e:
        print(f"LỖI API Chatbot: {e}")
        # GHI LOG LỖI (Recommended)
        db_manager.write_audit_log(
            user_code=user_code,
            action_type='API_CHATBOT_QUERY_ERROR',
            severity='ERROR',
            details=f"Lỗi xử lý câu hỏi: {message}. Lỗi: {str(e)}",
            ip_address=user_ip
        )
        return jsonify({'response': f'Lỗi hệ thống: {str(e)}'}), 500