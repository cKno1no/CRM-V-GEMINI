import os
import time
import json
import pyodbc
import PyPDF2
import google.generativeai as genai
from dotenv import load_dotenv

# --- CẤU HÌNH ---
load_dotenv()
API_KEY = "AIzaSyCC_qWqKqqupwwUT7mOR_Z75M9eKv8Vil4" # Hoặc lấy từ env

db_server = os.getenv('DB_SERVER')
db_name = os.getenv('DB_NAME')
db_uid = os.getenv('DB_UID')
db_pwd = os.getenv('DB_PWD')

CONN_STR = (
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    f'SERVER={db_server};'
    f'DATABASE={db_name};'
    f'UID={db_uid};PWD={db_pwd}'
)

# Thư mục chứa PDF
LIBRARY_DIR = r'static/uploads/library' 

genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

def get_db_connection():
    return pyodbc.connect(CONN_STR)

def extract_text_from_pdf(pdf_path, max_pages=5):
    """Đọc text từ PDF (chỉ lấy max_pages trang đầu để tiết kiệm token)"""
    text = ""
    try:
        with open(pdf_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            num_pages = len(reader.pages)
            for i in range(min(num_pages, max_pages)):
                page = reader.pages[i]
                text += page.extract_text() + "\n"
        return text, num_pages
    except Exception as e:
        print(f"❌ Lỗi đọc file {pdf_path}: {e}")
        return None, 0

def analyze_document_with_ai(filename, text_content):
    """Gửi text lên AI để lấy Metadata"""
    prompt = f"""
    Bạn là quản thư AI. Hãy phân tích tài liệu có tên file: "{filename}" và nội dung đầu sau:
    ---
    {text_content[:3000]}
    ---
    
    NHIỆM VỤ: Trả về kết quả dưới dạng JSON thuần (không markdown) với các trường sau:
    1. "title": Tiêu đề tài liệu chuẩn hóa (Tiếng Việt, viết hoa chữ cái đầu, bỏ đuôi .pdf).
    2. "category": Phân loại chủ đề (VD: Kỹ thuật, Bán hàng, Nhân sự, Pháp lý, Sản phẩm).
    3. "summary": Tóm tắt nội dung tài liệu trong khoảng 3-4 dòng súc tích.
    4. "keywords": 5 từ khóa chính cách nhau bằng dấu phẩy.
    """
    
    try:
        response = model.generate_content(prompt)
        cleaned_text = response.text.replace('```json', '').replace('```', '').strip()
        return json.loads(cleaned_text)
    except Exception as e:
        print(f"❌ Lỗi AI phân tích: {e}")
        return None

def main():
    print(f"--- BẮT ĐẦU QUÉT KHO TÀI LIỆU TẠI: {LIBRARY_DIR} ---")
    
    if not os.path.exists(LIBRARY_DIR):
        print(f"Không tìm thấy thư mục {LIBRARY_DIR}. Hãy tạo nó và copy file PDF vào.")
        return

    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Duyệt qua tất cả các file trong thư mục (bao gồm thư mục con)
    count = 0
    for root, dirs, files in os.walk(LIBRARY_DIR):
        for file in files:
            if not file.lower().endswith('.pdf'): continue
            
            file_path = os.path.join(root, file)
            rel_path = os.path.relpath(file_path, start=os.getcwd()) # Đường dẫn tương đối để lưu DB
            
            # 1. Kiểm tra xem file đã có trong DB chưa (tránh trùng lặp)
            cursor.execute("SELECT MaterialID FROM TRAINING_MATERIALS WHERE FileName = ?", (file,))
            if cursor.fetchone():
                print(f"⏩ Bỏ qua (Đã tồn tại): {file}")
                continue

            print(f"\n📄 Đang xử lý: {file}...")
            
            # 2. Đọc nội dung PDF
            pdf_text, total_pages = extract_text_from_pdf(file_path)
            if not pdf_text: continue

            # 3. Gọi AI phân tích
            print("   -> Đang gọi Gemini AI phân tích...")
            ai_data = analyze_document_with_ai(file, pdf_text)
            
            if ai_data:
                # 4. Lưu vào Database
                # Mẹo: Tự động tạo Course ảo nếu chưa có, hoặc gán vào Course "General"
                # Ở đây tôi insert thẳng vào TRAINING_MATERIALS, sếp có thể map CourseID sau
                
                # Lưu file JSON nội dung để Chatbot dùng sau này (Split View)
                json_content = [{"page": 1, "content": pdf_text}] # Demo lưu trang 1, thực tế nên lưu full
                with open(file_path + ".json", 'w', encoding='utf-8') as f:
                    json.dump(json_content, f, ensure_ascii=False)

                sql = """
                    INSERT INTO TRAINING_MATERIALS 
                    (FileName, FilePath, TotalPages, Summary, CreatedDate, AI_Processed, CourseID)
                    VALUES (?, ?, ?, ?, GETDATE(), 1, NULL) -- CourseID NULL chờ admin xếp lớp sau
                """
                # Tạm thời lưu Title và Category vào Summary hoặc tạo cột mới nếu sếp muốn
                # Ở đây tôi lưu Title vào FileName hiển thị cho đẹp
                final_summary = f"**Chủ đề:** {ai_data['category']}\n**Từ khóa:** {ai_data['keywords']}\n\n{ai_data['summary']}"
                
                cursor.execute(sql, (ai_data['title'], f"/{rel_path}".replace("\\", "/"), total_pages, final_summary))
                conn.commit()
                
                print(f"✅ Đã thêm: {ai_data['title']} ({ai_data['category']})")
                count += 1
                
                # Nghỉ 2s để tránh rate limit của Google
                time.sleep(2)
            else:
                print("⚠️ Không lấy được dữ liệu từ AI.")

    print(f"\n🎉 HOÀN TẤT! Đã import thành công {count} tài liệu.")
    conn.close()

if __name__ == "__main__":
    main()