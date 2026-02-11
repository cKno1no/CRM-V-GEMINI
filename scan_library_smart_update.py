import os
import time
import json
import pyodbc
import PyPDF2
import re
import difflib
import google.generativeai as genai
import random
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
    f'SERVER={db_server};DATABASE={db_name};UID={db_uid};PWD={db_pwd}'
)

LIBRARY_DIR = r'static/uploads/library' 

genai.configure(api_key=API_KEY)
model = genai.GenerativeModel('gemini-2.5-flash') 

def get_db_connection():
    return pyodbc.connect(CONN_STR)

def clean_json_string(text):
    text = re.sub(r"```json|```", "", text).strip()
    s = text.find('{')
    if s == -1: s = text.find('[')
    e = text.rfind('}')
    if e == -1: e = text.rfind(']')
    if s != -1 and e != -1:
        return text[s:e+1]
    return "[]"

# ==============================================================================
# PHASE 1: QUÉT FILE (GIỮ NGUYÊN TỐC ĐỘ CAO)
# ==============================================================================


def extract_text_from_pdf(pdf_path, max_pages=5):
    text = ""
    try:
        with open(pdf_path, 'rb') as f:
            reader = PyPDF2.PdfReader(f)
            num_pages = len(reader.pages)
            
            # 1. Đọc 5 trang đầu (Mục lục/Giới thiệu)
            for i in range(min(num_pages, 5)):
                text += reader.pages[i].extract_text() + "\n"
            
            # 2. Đọc thêm 3 trang ngẫu nhiên ở giữa (Nội dung cốt lõi)
            if num_pages > 10:
                mid_pages = random.sample(range(5, num_pages), min(3, num_pages - 5))
                for i in mid_pages:
                    text += f"\n--- Trích đoạn trang {i} ---\n" + reader.pages[i].extract_text()
                    
        return text, num_pages
    except: return "", 0



def analyze_single_doc(filename, text_content):
    # Prompt chi tiết hơn để tránh nhầm lẫn
    prompt = f"""
    Bạn là Chuyên gia Phân loại Tài liệu Đào tạo. Hãy phân tích nội dung sau:
    Tên file: "{filename}"
    Trích đoạn nội dung: 
    {text_content[:5000]}...
    
    HÃY CHỌN CHÍNH XÁC 1 TRONG 7 CATEGORY SAU:
    
    1. [Kỹ thuật]: Sách hướng dẫn sử dụng (Manual), Thông số kỹ thuật (Spec), Lắp đặt, Bảo trì, Sửa chữa máy móc (Vòng bi, Bơm, Motor...).
       -> Dấu hiệu: Có từ khóa: Bearing, Pump, Hydraulic, Installation, Maintenance, User Manual.
       
    2. [Kinh doanh]: Kỹ năng bán hàng, Đàm phán, CRM, Thị trường, Đối thủ.
    
    3. [Phát triển Tư duy & Kỹ năng]: Leadership, Quản lý thời gian, Tư duy tích cực, Đắc nhân tâm, Sách phát triển bản thân.
    
    4. [Quy định & Chính sách]: Sổ tay nhân viên, Nội quy lao động, Quy chế lương thưởng, Bảo mật thông tin.
    
    5. [Quy trình Vận hành]: ISO, Quy trình Kho, Quy trình Mua hàng, Lưu đồ công việc (Flowchart).
    
    6. [Công cụ & Biểu mẫu]: Hướng dẫn dùng phần mềm (SAP, ERP, Base), Biểu mẫu (Form), Template.
    
    7. [Khác]: Các tài liệu không thuộc nhóm trên.

    OUTPUT JSON:
    {{
        "title": "Tên tiếng Việt chuẩn",
        "category": "Chọn 1 trong 7 nhóm trên", 
        "sub_category": "Chi tiết (VD: Kỹ thuật Vòng bi, Tư duy Lãnh đạo...)",
        "summary": "Tóm tắt ngắn gọn",
        "ver": ""
    }}
    """
    
    for _ in range(3):
        try:
            res = model.generate_content(prompt)
            return json.loads(clean_json_string(res.text))
        except: time.sleep(1)
    
    # [FIX] Không gán bừa vào Quy định nữa
    return {"title": filename, "category": "Khác", "sub_category": "Chưa phân loại", "summary": "Cần kiểm tra lại", "ver": ""}

def run_phase_1(conn):
    print("\n🚀 PHASE 1: QUÉT FILE & NHẬP KHO...")
    cursor = conn.cursor()
    count = 0
    
    if not os.path.exists(LIBRARY_DIR):
        print(f"❌ Thư mục {LIBRARY_DIR} không tồn tại!")
        return

    for root, dirs, files in os.walk(LIBRARY_DIR):
        for file in files:
            if not file.lower().endswith('.pdf'): continue
            
            cursor.execute("SELECT MaterialID FROM TRAINING_MATERIALS WHERE FileName = ?", (file,))
            if cursor.fetchone(): continue

            print(f"📄 Xử lý: {file}...")
            path = os.path.join(root, file)
            text, pages = extract_text_from_pdf(path)
            
            if not text: continue

            meta = analyze_single_doc(file, text)
            meta_json = json.dumps(meta, ensure_ascii=False)
            rel_path = os.path.relpath(path, start=os.getcwd()).replace("\\", "/")
            
            cursor.execute("""
                INSERT INTO TRAINING_MATERIALS (FileName, FilePath, TotalPages, Summary, CreatedDate, AI_Processed)
                VALUES (?, ?, ?, ?, GETDATE(), 1)
            """, (meta['title'], f"/{rel_path}", pages, meta_json))
            conn.commit()
            count += 1
            print(f"   ✅ Đã thêm: {meta['title']} ({meta['category']})")
            time.sleep(1)
            
    print(f"✨ Phase 1 hoàn tất. Đã thêm {count} tài liệu mới.")

# ==============================================================================
# PHASE 2: SMART UPDATE (THAM CHIẾU KHÓA CŨ)
# ==============================================================================
def deduplicate(materials):
    # (Logic lọc trùng lặp giữ nguyên)
    print(f"\n🧹 Lọc trùng lặp cho {len(materials)} tài liệu...")
    grouped = {}
    for m in materials:
        clean = re.sub(r'[\(\[\_\-\s]?(v\d+|ver\d+|final|copy|new)[\)\]]?', '', m['title'], flags=re.IGNORECASE).strip().lower()
        found = False
        for k in grouped:
            if difflib.SequenceMatcher(None, clean, k).ratio() > 0.9:
                grouped[k].append(m)
                found = True
                break
        if not found: grouped[clean] = [m]
    
    final = []
    for k, v in grouped.items():
        final.append(sorted(v, key=lambda x: x['id'])[-1])
    return final

def run_phase_2_smart(conn):
    print("\n🧠 PHASE 2: SMART ASSIGN (THAM CHIẾU KHÓA HỌC CŨ)...")
    cursor = conn.cursor()
    
    # 1. Lấy Tài liệu MỚI (Chưa có CourseID)
    cursor.execute("SELECT MaterialID, FileName, Summary FROM TRAINING_MATERIALS WHERE CourseID IS NULL")
    raw = cursor.fetchall()
    if not raw: 
        print("   -> Không có tài liệu mới cần xếp lớp.")
        return

    # Parse Metadata
    materials = []
    for r in raw:
        try:
            m = json.loads(r.Summary)
            materials.append({"id": r.MaterialID, "title": m.get('title', r.FileName), "cat": m.get('category', 'Khác')})
        except:
            materials.append({"id": r.MaterialID, "title": r.FileName, "cat": "Khác"})

    clean_materials = deduplicate(materials)
    
    # Gom nhóm theo Category để xử lý (Kỹ thuật xử lý riêng, Kinh doanh xử lý riêng)
    materials_by_cat = {}
    for m in clean_materials:
        cat = m['cat']
        if cat not in materials_by_cat: materials_by_cat[cat] = []
        materials_by_cat[cat].append(m)

    # 2. Xử lý từng nhóm Category
    for cat, new_items in materials_by_cat.items():
        if not new_items: continue
        
        print(f"\n📂 Đang xử lý nhóm: {cat} ({len(new_items)} bài mới)...")

        # 2.1. Lấy danh sách KHÓA HỌC CŨ cùng Category
        # (Để AI biết mà gán vào)
        cursor.execute("SELECT CourseID, Title FROM TRAINING_COURSES WHERE Category = ?", (cat,))
        existing_courses = [{"id": row.CourseID, "title": row.Title} for row in cursor.fetchall()]
        
        print(f"   -> Tìm thấy {len(existing_courses)} khóa học cũ liên quan.")

        # 2.2. Prompt Thông minh
        prompt = f"""
        Bạn là Quản lý Đào tạo. 
        
        NHIỆM VỤ: Phân loại các tài liệu MỚI vào các khóa học CŨ hoặc TẠO MỚI.
        
        INPUT 1: DANH SÁCH KHÓA HỌC ĐANG CÓ (Ưu tiên gán vào đây nếu phù hợp):
        {json.dumps(existing_courses, ensure_ascii=False)}
        
        INPUT 2: DANH SÁCH TÀI LIỆU CẦN XẾP LỚP:
        {json.dumps([{'id': m['id'], 'title': m['title']} for m in new_items], ensure_ascii=False)}
        
        YÊU CẦU LOGIC:
        1. Duyệt từng tài liệu mới.
        2. Nếu nội dung tài liệu phù hợp với một khóa học ĐANG CÓ -> Gán vào khóa đó (Action: "ASSIGN").
        3. Nếu tài liệu không thuộc khóa nào -> Gom nhóm các tài liệu lẻ này để tạo KHÓA HỌC MỚI (Action: "CREATE_NEW").
        
        OUTPUT JSON FORMAT:
        {{
            "assignments": [
                {{ "material_id": 123, "course_id": 55, "reason": "Phù hợp khóa Bạc đạn" }},
                ...
            ],
            "new_courses": [
                {{ 
                    "title": "Tên khóa mới", 
                    "desc": "Mô tả", 
                    "material_ids": [124, 125] // Các ID tài liệu thuộc khóa mới này
                }},
                ...
            ]
        }}
        """
        
        try:
            res = model.generate_content(prompt)
            # Fix JSON formatting
            json_text = clean_json_string(res.text)
            plan = json.loads(json_text)
            
            # THỰC THI: 1. Gán vào khóa cũ
            assigned_count = 0
            if "assignments" in plan:
                for item in plan["assignments"]:
                    if item.get("course_id"):
                        cursor.execute("UPDATE TRAINING_MATERIALS SET CourseID = ? WHERE MaterialID = ?", (item['course_id'], item['material_id']))
                        assigned_count += 1
            print(f"   ✅ Đã gán {assigned_count} tài liệu vào khóa cũ.")
            
            # THỰC THI: 2. Tạo khóa mới và gán
            created_count = 0
            if "new_courses" in plan:
                for nc in plan["new_courses"]:
                    # Tạo khóa mới
                    thumb = '/static/img/thumbnails/book.png'
                    if 'Kỹ thuật' in cat: thumb = '/static/img/thumbnails/tech.png'
                    elif 'Kinh doanh' in cat: thumb = '/static/img/thumbnails/sales.png'
                    
                    cursor.execute("""
                        INSERT INTO TRAINING_COURSES (Title, Description, Category, ThumbnailUrl, IsMandatory, CreatedDate, XP_Reward)
                        OUTPUT INSERTED.CourseID
                        VALUES (?, ?, ?, ?, 0, GETDATE(), 300)
                    """, (nc['title'], nc['desc'], cat, thumb))
                    
                    new_course_id = cursor.fetchone()[0]
                    created_count += 1
                    print(f"   + Khởi tạo khóa mới: {nc['title']}")
                    
                    # Gán các bài lẻ vào khóa mới này
                    ids = nc.get('material_ids', [])
                    if ids:
                        valid_ids = [i for i in ids if isinstance(i, int)]
                        if valid_ids:
                            placeholders = ','.join('?' * len(valid_ids))
                            sql_up = f"UPDATE TRAINING_MATERIALS SET CourseID = ? WHERE MaterialID IN ({placeholders})"
                            cursor.execute(sql_up, [new_course_id] + valid_ids)

            conn.commit()
            print(f"   ✨ Hoàn tất nhóm {cat} (Tạo thêm {created_count} khóa).")

        except Exception as e:
            print(f"   ❌ Lỗi xử lý nhóm {cat}: {e}")
            # print(res.text) # Uncomment để debug nếu cần

    print("\n🎉 SMART UPDATE HOÀN TẤT!")

def main():
    conn = get_db_connection()
    run_phase_1(conn)       # Quét file mới
    run_phase_2_smart(conn) # Xếp lớp thông minh (có check khóa cũ)
    conn.close()

if __name__ == "__main__":
    main()