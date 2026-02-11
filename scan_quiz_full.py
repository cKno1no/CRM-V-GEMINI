import os
import time
import json
import pyodbc
import PyPDF2
import re
import google.generativeai as genai
from dotenv import load_dotenv
from collections import Counter

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

def get_db_connection():
    return pyodbc.connect(CONN_STR)

def clean_json_string(text):
    text = re.sub(r"```json|```", "", text).strip()
    s = text.find('[')
    e = text.rfind(']')
    if s != -1 and e != -1:
        return text[s:e+1]
    return "[]"

# 1. HÀM ĐỌC PDF
def extract_text_smart(pdf_path, max_pages=15):
    try:
        real_path = pdf_path.lstrip('/') if pdf_path.startswith('/') else pdf_path
        real_path = real_path.replace('/', os.sep)
        if not os.path.exists(real_path): return ""
        text = ""
        with open(real_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            for i in range(min(len(reader.pages), max_pages)):
                page_text = reader.pages[i].extract_text()
                if page_text: text += page_text + "\n"
        return text
    except: return ""

# ==============================================================================
# 2. TASK A: TẠO 7 CÂU HỎI MỚI (5 THƯỜNG - 2 KHÓ)
# ==============================================================================
def task_generate_new_questions(material_title, full_text):
    print(f"   [Task A] Đang sáng tạo 7 câu hỏi mới (5 Thường, 2 Khó)...")
    
    # Lấy 15000 ký tự đại diện
    context = full_text[:15000]
    
    prompt = f"""
    Tài liệu: "{material_title}"
    Nội dung trích dẫn:
    {context}
    ...
    
    NHIỆM VỤ: Tạo đúng 07 câu hỏi trắc nghiệm (4 đáp án) để kiểm tra người học.
    
    CẤU TRÚC BẮT BUỘC:
    1. **05 Câu Mức độ Thông hiểu (Normal):** Kiểm tra kiến thức cơ bản trong bài.
    2. **02 Câu Mức độ Vận dụng (Hard):** Câu hỏi tình huống hoặc suy luận, đòi hỏi hiểu sâu mới làm được.
    
    YÊU CẦU:
    - Đáp án phải nằm trong nội dung tài liệu.
    - Giải thích (explain) ngắn gọn tại sao đúng.
    
    OUTPUT JSON:
    [
        {{ 
            "content": "Câu hỏi...", 
            "a": "...", "b": "...", "c": "...", "d": "...", 
            "correct": "A", 
            "explain": "...", 
            "difficulty": "Hard" (hoặc "Normal") 
        }}
    ]
    """
    for attempt in range(3):
        try:
            response = model.generate_content(prompt)
            json_str = clean_json_string(response.text)
            data = json.loads(json_str)
            # Validate số lượng nếu cần, nhưng AI thường làm đúng
            if data: return data
        except Exception as e:
            print(f"      ⚠️ Lỗi sinh câu hỏi (Lần {attempt+1}): {e}")
            time.sleep(2)
    return []

# ==============================================================================
# HÀM PHỤ TRỢ: CHẤM ĐIỂM KHỚP TỪ KHÓA (SCORING)
# ==============================================================================
def calculate_relevance_score(question_row, material_text_lower):
    """
    Tính điểm độ phù hợp của câu hỏi với bài học.
    Score = Số lần các từ quan trọng trong Câu hỏi & Đáp án xuất hiện trong Bài học.
    """
    # 1. Gộp nội dung câu hỏi và đáp án đúng
    content_to_check = f"{question_row.Content} {question_row.CorrectAnswer}"
    
    # 2. Tách từ, lọc bỏ ký tự đặc biệt và từ ngắn
    words = re.findall(r'\w+', content_to_check.lower())
    significant_words = [w for w in words if len(w) > 3] # Chỉ lấy từ dài > 3 ký tự
    
    if not significant_words: return 0
    
    # 3. Đếm số lần xuất hiện trong bài học
    score = 0
    for word in significant_words:
        if word in material_text_lower:
            score += 1
            
    return score

# ==============================================================================
# 3. TASK B: MAP CÂU HỎI CŨ (CÓ SCORING)
# ==============================================================================
def task_map_with_scoring(cursor, material_id, summary, full_text):
    print(f"   [Task B] Quét kho cũ & Chấm điểm phù hợp...")
    
    # 1. Lọc thô bằng SQL (Lấy rộng ra khoảng 50-100 ứng viên)
    try:
        keywords = summary.split()[:30] if summary else full_text[:500].split()
        keywords = [k for k in keywords if len(k) > 3][:10]
    except: keywords = []

    if not keywords: return 0

    conditions = []
    params = []
    for kw in keywords:
        conditions.append("Content LIKE ?")
        params.append(f"%{kw}%")
            
    if not conditions: return 0
    
    # Lấy TOP 100 câu có chứa từ khóa (Lấy dư để chấm điểm lại)
    sql_filter = f"""
        SELECT TOP 100 ID, Content, CorrectAnswer 
        FROM TRAINING_QUESTION_BANK 
        WHERE SourceMaterialID IS NULL 
        AND ({' OR '.join(conditions)})
    """
    candidates = cursor.execute(sql_filter, tuple(params)).fetchall()
    
    if not candidates: return 0

    # 2. CHẤM ĐIỂM (SCORING) - Python Logic
    # Chỉ giữ lại những câu có Score cao (tức là nội dung câu hỏi xuất hiện nhiều trong bài)
    scored_candidates = []
    material_lower = full_text.lower()
    
    for cand in candidates:
        score = calculate_relevance_score(cand, material_lower)
        # Ngưỡng lọc: Ít nhất phải khớp 2 từ khóa quan trọng trở lên
        if score >= 2: 
            scored_candidates.append({'data': cand, 'score': score})
            
    # Sắp xếp theo điểm giảm dần và lấy TOP 20
    scored_candidates.sort(key=lambda x: x['score'], reverse=True)
    top_candidates = [x['data'] for x in scored_candidates[:20]]
    
    if not top_candidates:
        print("      -> Không có câu hỏi nào đạt điểm phù hợp.")
        return 0
        
    print(f"      -> Đã lọc được {len(top_candidates)} câu hỏi có độ khớp cao nhất (Score cao). Gửi AI check...")

    # 3. Gửi AI Check lần cuối (Final Verification)
    short_context = full_text[:8000]
    candidate_list = [{"id": r.ID, "q": r.Content, "a": r.CorrectAnswer} for r in top_candidates]
    
    verify_prompt = f"""
    Tài liệu:
    {short_context}
    ...
    
    Danh sách câu hỏi & đáp án:
    {json.dumps(candidate_list, ensure_ascii=False)}
    
    NHIỆM VỤ QUAN TRỌNG:
    Chỉ chọn những câu hỏi mà **Đáp án (a)** CÓ THỂ ĐƯỢC TÌM THẤY hoặc SUY LUẬN ĐƯỢC từ Tài liệu trên.
    Nếu tài liệu không nhắc đến kiến thức đó, tuyệt đối không chọn.
    
    OUTPUT JSON: [id1, id2...]
    """
    
    for attempt in range(3):
        try:
            res = model.generate_content(verify_prompt)
            json_str = clean_json_string(res.text)
            valid_ids = json.loads(json_str)
            
            valid_ids = [i for i in valid_ids if isinstance(i, int)]
            if valid_ids:
                placeholders = ','.join('?' * len(valid_ids))
                sql_update = f"UPDATE TRAINING_QUESTION_BANK SET SourceMaterialID = ? WHERE ID IN ({placeholders})"
                cursor.execute(sql_update, [material_id] + valid_ids)
                return len(valid_ids)
            return 0
        except Exception as e:
            print(f"      ⚠️ Lỗi AI Map: {e}")
            time.sleep(2)
    return 0

def main():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Lấy tài liệu chưa có câu hỏi (hoặc chạy hết nếu muốn update)
    sql = """
        SELECT MaterialID, FileName, FilePath, Summary 
        FROM TRAINING_MATERIALS 
        WHERE MaterialID NOT IN (
            SELECT DISTINCT SourceMaterialID FROM TRAINING_QUESTION_BANK WHERE SourceMaterialID IS NOT NULL
        )
    """
    materials = cursor.execute(sql).fetchall()
    
    print(f"🚀 Bắt đầu xử lý {len(materials)} tài liệu (Quy trình chuẩn)...")
    
    for idx, m in enumerate(materials):
        print(f"\n[{idx+1}/{len(materials)}] Xử lý: {m.FileName}...")
        
        full_text = extract_text_smart(m.FilePath)
        if not full_text:
            print("   ⚠️ File rỗng. Bỏ qua.")
            continue

        # TASK A: Tạo 7 câu (5 Thường, 2 Khó)
        new_qs = task_generate_new_questions(m.FileName, full_text)
        if new_qs:
            for q in new_qs:
                diff = q.get('difficulty', 'Normal')
                cursor.execute("""
                    INSERT INTO TRAINING_QUESTION_BANK 
                    (Content, OptionA, OptionB, OptionC, OptionD, CorrectAnswer, Explanation, SourceMaterialID, Category, Difficulty, IsAI_Generated, CreatedDate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'Assessment', ?, 1, GETDATE())
                """, (q['content'], q['a'], q['b'], q['c'], q['d'], q['correct'], q.get('explain',''), m.MaterialID, diff))
            print(f"   ✅ Đã tạo {len(new_qs)} câu mới.")
            conn.commit()

        # TASK B: Map câu cũ (Có Scoring)
        mapped = task_map_with_scoring(cursor, m.MaterialID, m.Summary, full_text)
        if mapped:
            print(f"   🔗 Đã map thêm {mapped} câu cũ phù hợp.")
            conn.commit()
            
        time.sleep(1)

    print("\n🎉 HOÀN TẤT!")
    conn.close()

if __name__ == "__main__":
    main()