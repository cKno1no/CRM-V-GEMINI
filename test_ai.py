# test_ai.py
from flask import current_app
import google.generativeai as genai

# Key của bạn (Code cứng để test)
API_KEY = "AIzaSyAWQcf-gTqydDhhER-X4I2O-Et-mBxAiJA"

genai.configure(api_key=API_KEY)

current_app.logger.info("--- ĐANG KIỂM TRA KẾT NỐI ĐẾN GOOGLE AI ---")
try:
    current_app.logger.info(f"Key đang dùng: {API_KEY[:10]}...")
    
    # 1. Liệt kê các model khả dụng
    current_app.logger.info("\n1. Danh sách Model tài khoản này được phép dùng:")
    models = list(genai.list_models())
    found_flash = False
    for m in models:
        if 'generateContent' in m.supported_generation_methods:
            current_app.logger.info(f" - {m.name}")
            if 'flash' in m.name:
                found_flash = True
    
    if not models:
        current_app.logger.error("❌ LỖI: Không tìm thấy model nào. Có thể API Key sai hoặc bị chặn IP.")
    
    # 2. Test thử chat đơn giản
    current_app.logger.info("\n2. Test thử gửi tin nhắn (Model: gemini-1.5-flash):")
    model = genai.GenerativeModel('gemini-1.5-flash')
    response = model.generate_content("Chào bạn, bạn có khỏe không?")
    current_app.logger.info(f"✅ Phản hồi từ AI: {response.text}")

except Exception as e:
    current_app.logger.error(f"\n❌ LỖI NGHIÊM TRỌNG: {e}")

input("\nẤn Enter để thoát...")