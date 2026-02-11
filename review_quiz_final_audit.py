import os
import time
import json
import pyodbc
import re
import google.generativeai as genai
from dotenv import load_dotenv
from PIL import Image

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


genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash')

IMAGE_BASE_DIR = os.path.join("static", "images", "N3H")



def get_db_connection():
    return pyodbc.connect(CONN_STR)

def clean_json_string(text):
    text = re.sub(r"```json|```", "", text).strip()
    s = text.find('{')
    e = text.rfind('}')
    if s != -1 and e != -1:
        return text[s:e+1]
    return "{}"

def audit_expert_content(row):
    """
    AI đóng vai trò 'Người duyệt cuối' (Approver).
    Chỉ sửa CorrectAnswer nếu sai nghiêm trọng.
    Chủ yếu tập trung bổ sung Explanation.
    """
    q_code = row.QuestionCode
    
    # 1. Tìm ảnh
    image_path = os.path.join(IMAGE_BASE_DIR, f"{q_code}.jpg")
    img_obj = None
    has_image = False
    if os.path.exists(image_path):
        try:
            img_obj = Image.open(image_path)
            has_image = True
        except: pass

    # 2. Prompt "Bảo vệ Chuyên gia"
    prompt = f"""
    Bạn là Chuyên gia Kỹ thuật cấp cao.
    
    CÂU HỎI: {row.Content}
    ĐÁP ÁN HIỆN TẠI (Được tổng hợp từ các kỹ sư giỏi nhất): 
    "{row.CorrectAnswer}"
    
    {'[HÃY NHÌN ẢNH ĐÍNH KÈM ĐỂ ĐỐI CHIẾU]' if has_image else ''}
    
    NHIỆM VỤ CỦA BẠN:
    1. Đánh giá ĐÁP ÁN HIỆN TẠI:
       - Nếu nó đúng về mặt kỹ thuật/logic (dù diễn đạt chưa hoàn hảo): HÃY GIỮ NGUYÊN.
       - Chỉ sửa nếu nó SAI KIẾN THỨC CƠ BẢN hoặc TRÁI NGƯỢC VỚI HÌNH ẢNH.
    
    2. Viết GIẢI THÍCH (Explanation):
       - Nếu cột giải thích đang trống, hãy viết 1 đoạn ngắn (dưới 30 từ) giải thích tại sao đáp án đó đúng.
       - Nếu đã có giải thích, hãy chuốt lại cho hay hơn.
       
    OUTPUT JSON:
    {{
        "is_wrong": true/false,           // Có sai nghiêm trọng không?
        "new_answer": "...",              // Chỉ điền nếu is_wrong=true. Nếu đúng, để null hoặc rỗng.
        "explanation": "..."              // Nội dung giải thích bổ sung
    }}
    """
    
    try:
        inputs = [prompt]
        if has_image and img_obj: inputs.append(img_obj)
        
        response = model.generate_content(inputs)
        return json.loads(clean_json_string(response.text))
    except Exception as e:
        print(f"   ❌ Lỗi AI: {e}")
        return None

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    print("🕵️ BẮT ĐẦU AUDIT (FIXED VERSION)...")
    
    sql = """
        SELECT ID, QuestionCode, Content, CorrectAnswer, Explanation 
        FROM TRAINING_QUESTION_BANK 
        WHERE CorrectAnswer IS NOT NULL 
        AND len(CorrectAnswer) > 5
        ORDER BY ID DESC
    """
    questions = cursor.execute(sql).fetchall()
    
    print(f"📋 Tìm thấy {len(questions)} câu hỏi cần duyệt.")
    
    count_fixed = 0
    count_explained = 0
    
    for idx, row in enumerate(questions):
        print(f"\n[{idx+1}/{len(questions)}] Duyệt câu {row.QuestionCode}...")
        
        res = audit_expert_content(row)
        
        if res:
            needs_update = False
            
            # 1. Xử lý CorrectAnswer
            if res.get('is_wrong') == True:
                raw_new_ans = res.get('new_answer')
                
                # [FIX QUAN TRỌNG] Xử lý nếu AI trả về dict/list thay vì string
                if isinstance(raw_new_ans, (dict, list)):
                    # Cố gắng lấy text nếu có, hoặc convert sang string
                    if isinstance(raw_new_ans, dict) and 'text' in raw_new_ans:
                        new_ans = str(raw_new_ans['text'])
                    else:
                        new_ans = json.dumps(raw_new_ans, ensure_ascii=False) # Convert object thành string JSON
                else:
                    new_ans = str(raw_new_ans) if raw_new_ans else ""

                # Chỉ update nếu có nội dung và khác cũ
                if new_ans and new_ans.strip() != "" and new_ans != row.CorrectAnswer:
                    print(f"   ⚠️ PHÁT HIỆN SAI KỸ THUẬT -> Sửa lại.")
                    cursor.execute("UPDATE TRAINING_QUESTION_BANK SET CorrectAnswer = ? WHERE ID = ?", (new_ans, row.ID))
                    count_fixed += 1
                    needs_update = True
            else:
                print("   ✅ Đáp án hợp lý -> Giữ nguyên.")

            # 2. Xử lý Explanation
            raw_expl = res.get('explanation')
            # Tương tự, fix lỗi type cho Explanation
            if isinstance(raw_expl, (dict, list)):
                new_expl = json.dumps(raw_expl, ensure_ascii=False)
            else:
                new_expl = str(raw_expl) if raw_expl else ""

            if new_expl:
                if (not row.Explanation) or (new_expl != row.Explanation):
                    print("   ℹ️ Cập nhật giải thích.")
                    cursor.execute("UPDATE TRAINING_QUESTION_BANK SET Explanation = ? WHERE ID = ?", (new_expl, row.ID))
                    count_explained += 1
                    needs_update = True
            
            if needs_update:
                conn.commit()
        
        time.sleep(1)

    print(f"\n🏁 HOÀN TẤT! Sửa {count_fixed} câu, Giải thích {count_explained} câu.")
    conn.close()

if __name__ == "__main__":
    main()