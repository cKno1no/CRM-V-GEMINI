import os
import PyPDF2
from flask import current_app
import google.generativeai as genai
import json

class LibraryService:
    def __init__(self, db_manager):
        self.db = db_manager

    # --- 1. XỬ LÝ FILE PDF KHI UPLOAD ---
    def process_new_document(self, file_path, material_id):
        """
        Đọc PDF, tách text theo từng trang và lưu lại để AI tra cứu sau này.
        (Trong thực tế, đoạn này nên lưu vào Vector DB như ChromaDB/FAISS)
        """
        try:
            reader = PyPDF2.PdfReader(file_path)
            total_pages = len(reader.pages)
            
            full_text_map = [] # Lưu dạng: [{"page": 1, "text": "..."}, ...]
            
            # Chỉ lấy text của 5 trang đầu để AI tóm tắt tổng quan
            intro_text = ""
            
            for i, page in enumerate(reader.pages):
                text = page.extract_text()
                if i < 5: intro_text += text + "\n"
                full_text_map.append({"page": i + 1, "content": text})
            
            # Gọi AI tóm tắt & Phân loại ngay khi upload
            summary = self._ai_categorize_document(intro_text)
            
            # Cập nhật DB
            # Lưu full_text_map vào file JSON riêng hoặc Column riêng để Chatbot đọc nhanh
            # Ở đây tôi giả định lưu path json
            json_path = file_path + ".json"
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(full_text_map, f, ensure_ascii=False)

            sql = "UPDATE TRAINING_MATERIALS SET TotalPages=?, Summary=?, AI_Processed=1 WHERE MaterialID=?"
            self.db.execute_non_query(sql, (total_pages, summary, material_id))
            
            return True
        except Exception as e:
            print(f"Error processing PDF: {e}")
            return False

    def _ai_categorize_document(self, text_content):
        """Dùng Gemini để tóm tắt và phân loại tài liệu"""
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"""
        Bạn là chuyên gia đào tạo. Hãy phân tích đoạn đầu của tài liệu sau:
        "{text_content[:2000]}..."
        
        Nhiệm vụ: Viết 1 đoạn tóm tắt ngắn (khoảng 3 câu) về nội dung chính của tài liệu này để hiển thị trên web.
        """
        try:
            res = model.generate_content(prompt)
            return res.text.strip()
        except:
            return "Tài liệu đào tạo nội bộ."

    # --- 2. CHAT VỚI TÀI LIỆU (RAG MINI) ---
    def chat_with_document(self, material_id, user_question):
        """
        Hàm xử lý logic Chat Split View.
        Đặc biệt: Phải tìm ra SỐ TRANG (Page Number) để UI cuộn tới.
        """
        # 1. Lấy đường dẫn file JSON content
        sql = "SELECT FilePath, FileName FROM TRAINING_MATERIALS WHERE MaterialID = ?"
        data = self.db.get_data(sql, (material_id,))
        if not data: return {"text": "Tài liệu không tồn tại.", "page": None}
        
        file_path = data[0]['FilePath']
        json_path = file_path + ".json"
        
        if not os.path.exists(json_path):
            return {"text": "Tài liệu này chưa được AI xử lý (Index).", "page": None}

        # 2. Load nội dung (Trong thực tế nên dùng Vector Search, ở đây dùng Keyword Search đơn giản)
        with open(json_path, 'r', encoding='utf-8') as f:
            pages = json.load(f)
            
        # Tìm các trang có liên quan nhất tới câu hỏi (Keyword matching đơn giản)
        # Sếp có thể nâng cấp phần này bằng thư viện `difflib` hoặc Vector Embeddings
        relevant_context = ""
        user_keywords = user_question.lower().split()
        
        for p in pages:
            score = 0
            page_text = p['content'].lower()
            for kw in user_keywords:
                if kw in page_text: score += 1
            
            if score > 0:
                # Đánh dấu số trang vào context để AI biết
                relevant_context += f"\n--- [TRANG {p['page']}] ---\n{p['content']}"

        if not relevant_context:
            return {"text": "Tôi không tìm thấy thông tin liên quan trong tài liệu này.", "page": None}

        # 3. Gửi cho Gemini
        model = genai.GenerativeModel('gemini-1.5-flash')
        prompt = f"""
        Bạn là trợ lý học tập AI. User đang xem tài liệu và hỏi.
        
        CÂU HỎI: "{user_question}"
        
        DỮ LIỆU TỪ TÀI LIỆU (Đã đánh dấu trang):
        {relevant_context[:10000]} (Cắt bớt nếu quá dài)
        
        YÊU CẦU:
        1. Trả lời câu hỏi dựa trên dữ liệu.
        2. Quan trọng: Nếu tìm thấy thông tin ở trang nào, hãy trích dẫn bằng cú pháp đặc biệt: [[PAGE:số_trang]]. Ví dụ: [[PAGE:5]].
        3. Nếu user hỏi "nằm ở trang mấy", hãy trả lời và kèm cú pháp trên.
        4. Giải thích ngắn gọn, dễ hiểu.
        """
        
        try:
            response = model.generate_content(prompt)
            reply = response.text
            
            # 4. Trích xuất số trang từ câu trả lời để Frontend cuộn
            target_page = None
            import re
            match = re.search(r'\[\[PAGE:(\d+)\]\]', reply)
            if match:
                target_page = int(match.group(1))
                # Xóa tag system khỏi câu trả lời cho đẹp
                reply = reply.replace(match.group(0), f"(Xem trang {target_page})")
            
            return {
                "text": reply,
                "page": target_page # Trả về số trang để JS cuộn
            }
            
        except Exception as e:
            return {"text": f"Lỗi AI: {str(e)}", "page": None}
    
    # --- 3. LẤY DỮ LIỆU DASHBOARD ---
    def get_training_dashboard(self, user_code):
        """
        Lấy danh sách khóa học thực tế từ DB, tính toán tiến độ dựa trên bảng Progress.
        """
        # Query lấy tất cả Material và gom nhóm theo Course
        # Nếu chưa có Course, ta coi mỗi Material là 1 bài lẻ (Single Lesson)
        sql = """
            SELECT 
                M.MaterialID, M.FileName, M.FilePath, M.Summary, M.TotalPages, M.CreatedDate,
                ISNULL(C.CourseID, 0) as CourseID, 
                ISNULL(C.Title, M.FileName) as CourseTitle,
                ISNULL(C.Description, M.Summary) as CourseDesc,
                ISNULL(C.ThumbnailUrl, '') as Thumbnail,
                ISNULL(C.Category, 'General') as Category,
                ISNULL(C.XP_Reward, 100) as XP,
                ISNULL(P.Status, 'NOT_STARTED') as UserStatus,
                ISNULL(P.LastPageRead, 0) as LastPage
            FROM TRAINING_MATERIALS M
            LEFT JOIN TRAINING_COURSES C ON M.CourseID = C.CourseID
            LEFT JOIN TRAINING_USER_PROGRESS P ON M.MaterialID = P.MaterialID AND P.UserCode = ?
            ORDER BY M.CreatedDate DESC
        """
        raw_data = self.db.get_data(sql, (user_code,))
        
        # Xử lý dữ liệu: Gom nhóm Material vào Course
        courses_map = {}
        
        for row in raw_data:
            c_id = row['CourseID']
            if c_id not in courses_map:
                courses_map[c_id] = {
                    "id": c_id,
                    "title": row['CourseTitle'],
                    "description": row['CourseDesc'],
                    "thumbnail": row['Thumbnail'] or f"https://ui-avatars.com/api/?name={row['CourseTitle']}&background=random&size=512",
                    "category": row['Category'],
                    "xp": row['XP'],
                    "materials": [],
                    "completed_count": 0,
                    "total_count": 0
                }
            
            # Thêm bài học vào course
            courses_map[c_id]['materials'].append({
                "id": row['MaterialID'],
                "title": row['FileName'].replace('.pdf', ''),
                "status": row['UserStatus']
            })
            courses_map[c_id]['total_count'] += 1
            if row['UserStatus'] == 'COMPLETED':
                courses_map[c_id]['completed_count'] += 1

        # Tính % tiến độ cho từng course
        final_list = []
        for c in courses_map.values():
            if c['total_count'] > 0:
                c['progress'] = int((c['completed_count'] / c['total_count']) * 100)
            else:
                c['progress'] = 0
            final_list.append(c)

        return {
            "all": final_list,
            "recent": [c for c in final_list if c['progress'] > 0 and c['progress'] < 100],
            "recommended": [c for c in final_list if c['progress'] == 0][:3] # Gợi ý 3 khóa chưa học
        }

    # --- 2. LẤY CHI TIẾT TÀI LIỆU ĐỂ HỌC ---
    def get_material_content(self, material_id, user_code):
        sql = "SELECT * FROM TRAINING_MATERIALS WHERE MaterialID = ?"
        data = self.db.get_data(sql, (material_id,))
        if not data: return None
        
        material = data[0]
        
        # Lấy tiến độ cũ
        prog_sql = "SELECT LastPageRead FROM TRAINING_USER_PROGRESS WHERE UserCode = ? AND MaterialID = ?"
        prog = self.db.get_data(prog_sql, (user_code, material_id))
        material['last_page'] = prog[0]['LastPageRead'] if prog else 1
        
        # [QUAN TRỌNG] Xử lý đường dẫn file
        # Nếu lưu đường dẫn tuyệt đối (D:\...), cần chuyển thành URL tương đối (/static/...)
        raw_path = material['FilePath']
        if 'static' in raw_path:
            # Cắt lấy phần từ /static trở đi
            material['WebPath'] = '/static' + raw_path.split('static')[1].replace('\\', '/')
        else:
            # Fallback nếu path lạ
            material['WebPath'] = raw_path.replace('\\', '/')

        return material