from flask import Blueprint, request, session, jsonify
# FIX: Chỉ import login_required từ utils.py
from utils import login_required
import json

chat_bp = Blueprint('chat_bp', __name__)

@chat_bp.route('/api/chatbot_query', methods=['POST'])
@login_required
def api_chatbot_query():
    """API: Nhận tin nhắn từ Widget Chatbot và trả về phản hồi."""
    
    # FIX: Import Services Cục bộ
    from app import chatbot_service 
    # Cần import get_user_ip nếu muốn ghi log, giả định nó ở utils.py hoặc được bỏ qua
    
    data = request.json
    message = data.get('message', '').strip()
    
    user_code = session.get('user_code')
    # user_ip = get_user_ip() 

    if not message:
        return jsonify({'response': 'Vui lòng nhập câu hỏi.'})
        
    user_role = session.get('user_role', '').strip().upper()
    
    try:
        # 1. Gọi "bộ não" Chatbot
        response_message = chatbot_service.process_message(message, user_code, user_role)
        
        # 2. Trả về phản hồi đã định dạng
        return jsonify({'response': response_message})
        
    except Exception as e:
        print(f"LỖI API Chatbot: {e}")
        return jsonify({'response': f'Lỗi hệ thống: {str(e)}'}), 500