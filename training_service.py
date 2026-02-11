import random
import re
import difflib
import json
import os
import PyPDF2
from datetime import datetime, timedelta
import google.generativeai as genai
from flask import current_app

class TrainingService:
    def __init__(self, db_manager, gamification_service):
        self.db = db_manager
        self.gamification = gamification_service
        self.ACTIVITY_CODE_WIN = 'DAILY_QUIZ_WIN'

    # =========================================================================
    # PHẦN 1: GAME & DAILY CHALLENGE
    # =========================================================================
    
    # 1. TÌM KIẾM KIẾN THỨC (Cho Chatbot)
    def search_knowledge(self, query):
        if not query: return None
        stop_words = {'là', 'gì', 'của', 'hãy', 'nêu', 'cho', 'biết', 'trong', 'với', 'tại', 'sao', 'như', 'thế', 'nào', 'em', 'anh', 'chị', 'ad', 'bot', 'bạn', 'tôi', 'mình'}
        clean_query = query.lower()
        for char in "?!,.:;\"'()[]{}":
            clean_query = clean_query.replace(char, " ")
        raw_words = clean_query.split()
        keywords = [w for w in raw_words if len(w) > 1 and w not in stop_words]
        if not keywords: return None 

        top_kws = sorted(keywords, key=len, reverse=True)[:4]
        conditions = []
        params = []
        for kw in top_kws:
            conditions.append("Content LIKE ?")
            params.append(f"%{kw}%")
        if not conditions: return None

        sql = f"SELECT TOP 50 ID, Content, CorrectAnswer, Explanation FROM TRAINING_QUESTION_BANK WHERE CorrectAnswer IS NOT NULL AND ({' OR '.join(conditions)})"
        candidates = self.db.get_data(sql, tuple(params))
        if not candidates: return "⚠️ Không tìm thấy kiến thức nào khớp."

        scored_candidates = []
        user_tokens = set(keywords)
        for row in candidates:
            db_content = row['Content'].lower()
            matches = sum(1 for token in user_tokens if token in db_content)
            overlap_score = matches / len(user_tokens)
            scored_candidates.append((overlap_score, row))

        scored_candidates.sort(key=lambda x: x[0], reverse=True)
        if not scored_candidates: return None
        best_score, best_row = scored_candidates[0]
        
        if best_score >= 0.7: return self._format_answer(best_row)
        top_suggestions = [item for item in scored_candidates[:3] if item[0] >= 0.3]
        if not top_suggestions: return "⚠️ Không tìm thấy câu hỏi nào đủ khớp."
        if len(top_suggestions) == 1: return self._format_answer(top_suggestions[0][1])

        msg = f"🤔 **Có phải ý Sếp là:**\n\n"
        for idx, (score, row) in enumerate(top_suggestions):
            msg += f"**{idx+1}.** {row['Content']}\n"
        return msg

    def _format_answer(self, row):
        ans_clean = row['CorrectAnswer'].replace('[', '').replace(']', '')
        explanation = f"\n\n💡 *Giải thích: {row['Explanation']}*" if row['Explanation'] else ""
        return f"📚 **Kiến thức:**\n**Q:** _{row['Content']}_\n\n{ans_clean}{explanation}"

    # 2. PHÂN PHỐI CÂU HỎI (Cho Scheduler chạy định kỳ)
    def distribute_daily_questions(self):
        # Lấy 3 câu hỏi ngẫu nhiên
        sql_q = "SELECT TOP 3 ID, Content, OptionA, OptionB, OptionC, OptionD FROM TRAINING_QUESTION_BANK WHERE CorrectAnswer IS NOT NULL ORDER BY NEWID()"
        questions = self.db.get_data(sql_q)
        if not questions: return []

        # Lấy danh sách user active
        sql_u = "SELECT UserCode FROM [GD - NGUOI DUNG]" 
        users_data = self.db.get_data(sql_u)
        users = [u['UserCode'] for u in users_data]
        if not users: return []

        random.shuffle(users)
        chunk_size = len(users) // len(questions) + 1
        user_groups = [users[i:i + chunk_size] for i in range(0, len(users), chunk_size)]
        messages_to_send = []

        for idx, group in enumerate(user_groups):
            if idx >= len(questions): break
            q_id = questions[idx]['ID']
            mail_title = f"⚡ Thử thách N3H lúc {datetime.now().strftime('%H:%M')}"
            mail_content = "Bạn có <b>4 giờ</b> để trả lời. Mở Chatbot ngay để nhận 50 XP!"

            for user_code in group:
                # Đánh dấu phiên cũ hết hạn
                self.db.execute_non_query("UPDATE TRAINING_DAILY_SESSION SET Status='EXPIRED' WHERE UserCode=? AND Status='PENDING'", (user_code,))
                # Tạo phiên mới (Hạn 4 tiếng)
                expired_at = datetime.now() + timedelta(hours=4)
                self.db.execute_non_query("INSERT INTO TRAINING_DAILY_SESSION (UserCode, QuestionID, Status, ExpiredAt) VALUES (?, ?, 'PENDING', ?)", (user_code, q_id, expired_at))
                # Gửi thông báo
                self.db.execute_non_query("INSERT INTO TitanOS_Game_Mailbox (UserCode, Title, Content, CreatedTime, IsClaimed) VALUES (?, ?, ?, GETDATE(), 0)", (user_code, mail_title, mail_content))
                messages_to_send.append({"user_code": user_code})
        return messages_to_send

    # 3. LẤY TRẠNG THÁI CHALLENGE (Cho Frontend hiển thị)
    def get_current_challenge_status(self, user_code):
        """Kiểm tra: Đã làm chưa? Còn hạn không? Hay phải chờ?"""
        # 1. Check đã làm hôm nay chưa
        sql_check = """
            SELECT TOP 1 AIScore 
            FROM TRAINING_DAILY_SESSION 
            WHERE UserCode = ? 
            AND CAST(BatchTime AS DATE) = CAST(GETDATE() AS DATE) 
            AND Status IN ('ANSWERED', 'DONE')
        """
        check = self.db.get_data(sql_check, (user_code,))
        if check:
            return {'status': 'DONE', 'score': check[0]['AIScore']}

        # 2. Check đang chờ (Pending)
        sql_pending = """
            SELECT TOP 1 S.SessionID, S.ExpiredAt, Q.Content 
            FROM TRAINING_DAILY_SESSION S
            JOIN TRAINING_QUESTION_BANK Q ON S.QuestionID = Q.ID
            WHERE S.UserCode = ? AND S.Status = 'PENDING'
        """
        pending = self.db.get_data(sql_pending, (user_code,))
        
        if pending:
            row = pending[0]
            now = datetime.now()
            if row['ExpiredAt'] > now:
                seconds_left = (row['ExpiredAt'] - now).total_seconds()
                return {
                    'status': 'AVAILABLE',
                    'session_id': row['SessionID'],
                    'question': row['Content'],
                    'seconds_left': int(seconds_left)
                }
            else:
                self.db.execute_non_query("UPDATE TRAINING_DAILY_SESSION SET Status='EXPIRED' WHERE SessionID=?", (row['SessionID'],))
        
        # 3. Trạng thái Waiting
        next_slot = "09:00"
        h = datetime.now().hour
        if h < 9: next_slot = "09:00"
        elif h < 13: next_slot = "13:00"
        elif h < 17: next_slot = "17:00"
        else: next_slot = "09:00 (Sáng mai)"

        return {'status': 'WAITING', 'next_slot': next_slot}

    # 4. CHẤM ĐIỂM DAILY (Khi user submit)
    def submit_answer(self, user_code, session_id, user_answer):
        # Lấy thông tin câu hỏi và đáp án
        # [FIX]: Dùng LEFT JOIN hoặc check Keywords cẩn thận
        sql = """
            SELECT S.SessionID, Q.CorrectAnswer, Q.Keywords, Q.Content
            FROM TRAINING_DAILY_SESSION S
            JOIN TRAINING_QUESTION_BANK Q ON S.QuestionID = Q.ID
            WHERE S.SessionID = ? AND S.UserCode = ?
        """
        data = self.db.get_data(sql, (session_id, user_code))
        if not data: return {'success': False, 'msg': 'Phiên không hợp lệ'}
        
        row = data[0]
        score = 0
        feedback = ""
        
        # Chấm Keyword (Nếu có cột Keywords)
        if row.get('Keywords'):
            kws = [k.strip().lower() for k in row['Keywords'].split(',') if k.strip()]
            user_text = user_answer.lower()
            match_count = sum(1 for k in kws if k in user_text)
            if kws and (match_count / len(kws) >= 0.7):
                score = 10
                feedback = "Tuyệt vời! Bạn nắm ý chính rất tốt."
        
        # Nếu chưa max điểm, dùng AI chấm
        if score < 10:
            ai_res = self._ai_grade_answer(row['Content'], row['CorrectAnswer'], user_answer)
            score = ai_res.get('score', 5)
            feedback = ai_res.get('feedback', 'Ghi nhận nỗ lực.')

        # Lưu kết quả
        xp = 50 if score >= 8 else (25 if score >= 5 else 5)
        self.db.execute_non_query("""
            UPDATE TRAINING_DAILY_SESSION 
            SET Status='ANSWERED', UserAnswerContent=?, AIScore=?, AIFeedback=?, IsCorrect=1
            WHERE SessionID=?
        """, (user_answer, score, feedback, session_id))
        
        if xp > 0:
            self.gamification.log_activity(user_code, self.ACTIVITY_CODE_WIN, xp)
        
        return {'success': True, 'score': score, 'feedback': feedback, 'xp': xp, 'correct_answer': row['CorrectAnswer']}

    # 5. HÀM PHỤ TRỢ AI CHẤM
    def _ai_grade_answer(self, question, standard, user_ans):
        try:
            model = genai.GenerativeModel('gemini-2.5-flash')
            prompt = f"""
            Chấm điểm tự luận (0-10) và nhận xét ngắn.
            Câu hỏi: {question}
            Đáp án chuẩn: {standard}
            User trả lời: {user_ans}
            Output JSON: {{ "score": number, "feedback": "string" }}
            """
            res = model.generate_content(prompt)
            return json.loads(res.text.replace('```json', '').replace('```', '').strip())
        except:
            return {"score": 5, "feedback": "Hệ thống bận, chấm điểm khuyến khích."}

    # 6. LẤY CHALLENGE CHO CHATBOT (Legacy)
    def get_pending_challenge(self, user_code):
        status = self.get_current_challenge_status(user_code)
        if status['status'] == 'AVAILABLE':
            return f"🔥 **THỬ THÁCH ĐANG CHỜ**\n{status['question']}\n\n👉 Vào 'Đấu Trường' để chiến ngay!"
        return None

    # =========================================================================
    # PHẦN 2: DASHBOARD & COURSE (LOGIC MỚI)
    # =========================================================================

    # 7. LẤY DASHBOARD THEO CATEGORY (V2)
    def get_training_dashboard_v2(self, user_code):
        # 1. Cố gắng lấy dữ liệu cấu trúc MỚI (Có SubCategory)
        # [FIX] Thêm cột C.IsMandatory vào Query
        sql = """
            SELECT 
                C.CourseID, C.Title, C.Description, C.Category, C.ThumbnailUrl, C.XP_Reward,
                C.SubCategory, C.IsMandatory, -- Lấy thêm cột này
                COUNT(DISTINCT M.MaterialID) as TotalLessons, -- Thêm DISTINCT để tránh đếm trùng
                SUM(CASE WHEN P.Status = 'COMPLETED' THEN 1 ELSE 0 END) as CompletedLessons
            FROM TRAINING_COURSES C
            LEFT JOIN TRAINING_MATERIALS M ON C.CourseID = M.CourseID
            LEFT JOIN TRAINING_USER_PROGRESS P ON M.MaterialID = P.MaterialID AND P.UserCode = ?
            GROUP BY C.CourseID, C.Title, C.Description, C.Category, C.ThumbnailUrl, C.XP_Reward, C.SubCategory, C.IsMandatory
        """
        
        try:
            rows = self.db.get_data(sql, (user_code,))
        except Exception as e:
            print(f"Warning: Đang dùng Query dự phòng do lỗi DB: {e}")
            # 2. Fallback: Nếu lỗi (do chưa chạy SQL update DB), dùng Query CŨ
            sql_fallback = """
                SELECT 
                    C.CourseID, C.Title, C.Description, C.Category, C.ThumbnailUrl, C.XP_Reward,
                    COUNT(M.MaterialID) as TotalLessons,
                    SUM(CASE WHEN P.Status = 'COMPLETED' THEN 1 ELSE 0 END) as CompletedLessons
                FROM TRAINING_COURSES C
                LEFT JOIN TRAINING_MATERIALS M ON C.CourseID = M.CourseID
                LEFT JOIN TRAINING_USER_PROGRESS P ON M.MaterialID = P.MaterialID AND P.UserCode = ?
                GROUP BY C.CourseID, C.Title, C.Description, C.Category, C.ThumbnailUrl, C.XP_Reward
            """
            rows = self.db.get_data(sql_fallback, (user_code,))

        grouped = {}
        def_img = 'https://cdn3d.iconscout.com/3d/premium/thumb/folder-5206733-4352249.png'

        # Từ khóa để tự động phân loại nếu DB chưa có dữ liệu chuẩn
        keywords_map = {
            'Vòng bi & Truyền động': ['vòng bi', 'bạc đạn', 'bôi trơn', 'truyền động', 'skf', 'timken'],
            'Hệ thống Cơ khí': ['bơm', 'quạt', 'thủy lực', 'đường ống', 'băng tải', 'khí nén'],
            'Bảo trì & MRO': ['mro', 'bảo trì', 'sửa chữa', 'vận hành', 'cmms'],
            'Công nghệ 4.0': ['số hóa', 'iot', '4.0', 'thông minh', 'phần mềm', 'condasset'],
            'Kinh doanh & Chiến lược': ['bán hàng', 'khách hàng', 'thị trường', 'chiến lược', 'doanh số'],
            'Kỹ năng & Văn hóa': ['lãnh đạo', 'giao tiếp', 'tư duy', 'văn hóa', 'nhân viên mới']
        }

        for r in rows:
            # [AN TOÀN] Dùng .get() để tránh lỗi KeyError nếu cột không tồn tại
            cat_raw = r.get('Category') or 'Khác'
            cat = cat_raw.strip().replace('[', '').replace(']', '').replace('1.', '').replace('5.', '').strip()
            
            if cat not in grouped: grouped[cat] = {}

            # [AN TOÀN] Kiểm tra xem cột SubCategory có tồn tại trong row không
            sub_cat = 'Chung'
            db_sub = r.get('SubCategory') # Lấy giá trị an toàn
            
            if db_sub and str(db_sub).strip():
                sub_cat = str(db_sub).strip()
            else:
                # Logic tự động phân loại bằng từ khóa (Auto-tagging)
                title_lower = r['Title'].lower()
                for key, kws in keywords_map.items():
                    if any(w in title_lower for w in kws):
                        sub_cat = key
                        break
            
            if sub_cat not in grouped[cat]: grouped[cat][sub_cat] = []

            # Tính toán tiến độ
            total = r['TotalLessons'] or 0
            done = r['CompletedLessons'] or 0
            percent = int((done / total) * 100) if total > 0 else 0
            
            is_mandatory_val = r.get('IsMandatory', 0)
            is_mandatory = True if is_mandatory_val == 1 or is_mandatory_val == -1 else False

            course = {
                'id': r['CourseID'],
                'title': r['Title'],
                'desc': r.get('Description', 'Chưa có mô tả.'),
                'thumbnail': r.get('ThumbnailUrl') or def_img,
                'xp': r.get('XP_Reward', 0),
                'lessons': total,
                'is_mandatory': is_mandatory,  # Truyền flag này ra API
                'progress': percent,
                'sub_cat_display': sub_cat
            }
            grouped[cat][sub_cat].append(course)
            
        return grouped
    
    def search_courses_and_materials(self, query):
        term = f"%{query}%"
        sql = """
            SELECT DISTINCT TOP 10 C.CourseID, C.Title, C.Category, C.ThumbnailUrl
            FROM TRAINING_COURSES C
            LEFT JOIN TRAINING_MATERIALS M ON C.CourseID = M.CourseID
            WHERE C.Title LIKE ? OR C.Description LIKE ? OR M.FileName LIKE ? OR M.Summary LIKE ?
        """
        rows = self.db.get_data(sql, (term, term, term, term))

        results = []
        for r in rows:
            results.append({
                'id': r['CourseID'],
                'title': r['Title'],
                'category': r['Category'],
                'thumbnail': r['ThumbnailUrl']
            })
        return results
    
    # 8. LẤY CHI TIẾT KHÓA HỌC & BÀI HỌC
    def get_course_detail(self, course_id, user_code):
        # Info
        c_sql = "SELECT * FROM TRAINING_COURSES WHERE CourseID = ?"
        course = self.db.get_data(c_sql, (course_id,))
        if not course: return None
        
        # Materials List
        m_sql = """
            SELECT 
                M.MaterialID, M.FileName, M.TotalPages, M.Summary,
                ISNULL(P.Status, 'NOT_STARTED') as Status,
                ISNULL(P.LastPageRead, 0) as LastPage
            FROM TRAINING_MATERIALS M
            LEFT JOIN TRAINING_USER_PROGRESS P ON M.MaterialID = P.MaterialID AND P.UserCode = ?
            WHERE M.CourseID = ?
            ORDER BY M.MaterialID
        """
        materials = self.db.get_data(m_sql, (user_code, course_id))
        
        return {'info': course[0], 'materials': materials}

    # =========================================================================
    # PHẦN 3: HỌC TẬP & KIỂM TRA (STUDY & QUIZ)
    # =========================================================================

    # 9. LẤY NỘI DUNG BÀI HỌC (Study Room)
    def get_material_content(self, material_id, user_code):
        sql = "SELECT * FROM TRAINING_MATERIALS WHERE MaterialID = ?"
        data = self.db.get_data(sql, (material_id,))
        if not data: return None
        material = data[0]
        
        # Get Progress
        prog = self.db.get_data("SELECT LastPageRead FROM TRAINING_USER_PROGRESS WHERE UserCode=? AND MaterialID=?", (user_code, material_id))
        material['last_page'] = prog[0]['LastPageRead'] if prog else 1
        
        # Fix path
        if material['FilePath'] and 'static' in material['FilePath']:
            material['WebPath'] = '/static' + material['FilePath'].split('static')[1].replace('\\', '/')
        else:
            material['WebPath'] = material['FilePath']
            
        return material

    # 10. AI TUTOR (Chatbot học tập)
    def chat_with_document(self, material_id, user_question):
        sql = "SELECT FilePath FROM TRAINING_MATERIALS WHERE MaterialID = ?"
        data = self.db.get_data(sql, (material_id,))
        if not data: return {"text": "Tài liệu không tồn tại.", "page": None}
        
        file_path = data[0]['FilePath']
        real_path = file_path
        if file_path.startswith('/'): 
            real_path = os.path.join(current_app.root_path, file_path.lstrip('/'))

        if not os.path.exists(real_path):
             return {"text": f"Không tìm thấy file gốc: {file_path}", "page": None}

        # Live Read PDF
        pdf_text = ""
        try:
            reader = PyPDF2.PdfReader(real_path)
            for i, page in enumerate(reader.pages[:10]): # Đọc 10 trang đầu
                text = page.extract_text()
                if text: pdf_text += f"\n--- TRANG {i+1} ---\n{text}"
        except Exception as e:
            return {"text": f"Lỗi đọc PDF: {str(e)}", "page": None}

        if not pdf_text.strip():
            return {"text": "Tài liệu này là file ảnh scan, AI chưa đọc được chữ.", "page": None}

        try:
            model = genai.GenerativeModel('gemini-2.5-flash')
            prompt = f"Trả lời câu hỏi dựa trên tài liệu. Nếu thấy thông tin ở trang nào, ghi [[PAGE:số_trang]]. Câu hỏi: {user_question}. Dữ liệu: {pdf_text[:15000]}"
            res = model.generate_content(prompt)
            reply = res.text
            
            target_page = None
            match = re.search(r'\[\[PAGE:(\d+)\]\]', reply)
            if match:
                target_page = int(match.group(1))
                reply = reply.replace(match.group(0), f"(Xem trang {target_page})")
            return {"text": reply, "page": target_page}
        except Exception as e:
            return {"text": f"Lỗi AI: {e}", "page": None}

    # 11. CẬP NHẬT TRANG ĐANG ĐỌC
    def update_reading_progress(self, user_code, material_id, page_num):
        check = self.db.get_data("SELECT ProgressID FROM TRAINING_USER_PROGRESS WHERE UserCode=? AND MaterialID=?", (user_code, material_id))
        if check:
            self.db.execute_non_query("UPDATE TRAINING_USER_PROGRESS SET LastPageRead=?, LastAccessDate=GETDATE() WHERE UserCode=? AND MaterialID=?", (page_num, user_code, material_id))
        else:
            self.db.execute_non_query("INSERT INTO TRAINING_USER_PROGRESS (UserCode, MaterialID, Status, LastPageRead, LastAccessDate) VALUES (?, ?, 'IN_PROGRESS', ?, GETDATE())", (user_code, material_id, page_num))
        return True

    # 12. LẤY ĐỀ THI (CƠ CHẾ: GIỮ 4 CŨ - ĐỔI 1 MỚI)
    def get_material_quiz(self, material_id, user_code):
        # 1. Tìm xem user đã thi bài này lần nào chưa
        sql_history = """
            SELECT TOP 5 QuestionID 
            FROM TRAINING_QUIZ_SUBMISSIONS 
            WHERE UserCode = ? AND MaterialID = ?
            ORDER BY AttemptNumber DESC, SubmissionID ASC
        """
        last_questions = self.db.get_data(sql_history, (user_code, material_id))
        
        final_questions = []

        # TRƯỜNG HỢP 1: THI LẦN ĐẦU (Chưa có lịch sử) -> Lấy 5 câu ngẫu nhiên
        if not last_questions or len(last_questions) < 5:
            sql_random = """
                SELECT TOP 5 ID, Content, OptionA, OptionB, OptionC, OptionD 
                FROM TRAINING_QUESTION_BANK 
                WHERE SourceMaterialID = ? 
                ORDER BY NEWID()
            """
            final_questions = self.db.get_data(sql_random, (material_id,))
        
        # TRƯỜNG HỢP 2: THI LẠI (Đã có đề cũ) -> Giữ 4, Đổi 1
        else:
            old_ids = [row['QuestionID'] for row in last_questions]
            
            # Chọn ngẫu nhiên 4 câu từ đề cũ để giữ lại
            keep_ids = random.sample(old_ids, 4)
            
            # Lấy 1 câu MỚI TOANH (không nằm trong đề cũ)
            placeholders = ','.join(['?'] * len(old_ids))
            sql_new = f"""
                SELECT TOP 1 ID, Content, OptionA, OptionB, OptionC, OptionD 
                FROM TRAINING_QUESTION_BANK 
                WHERE SourceMaterialID = ? 
                AND ID NOT IN ({placeholders})
                ORDER BY NEWID()
            """
            params = [material_id] + old_ids
            new_question = self.db.get_data(sql_new, tuple(params))
            
            # Nếu hết câu hỏi mới trong kho -> Đành lấy lại 1 câu cũ còn lại
            if not new_question:
                missing_id = [x for x in old_ids if x not in keep_ids][0]
                sql_fallback = "SELECT ID, Content, OptionA, OptionB, OptionC, OptionD FROM TRAINING_QUESTION_BANK WHERE ID = ?"
                new_question = self.db.get_data(sql_fallback, (missing_id,))

            # Lấy thông tin chi tiết 4 câu giữ lại
            keep_placeholders = ','.join(['?'] * len(keep_ids))
            sql_keep = f"SELECT ID, Content, OptionA, OptionB, OptionC, OptionD FROM TRAINING_QUESTION_BANK WHERE ID IN ({keep_placeholders})"
            kept_questions = self.db.get_data(sql_keep, tuple(keep_ids))
            
            # Gộp lại thành 5 câu
            final_questions = kept_questions + new_question
            random.shuffle(final_questions) # Trộn thứ tự lại cho mới mẻ

        return final_questions

    # 13. NỘP BÀI (AI CHẤM KHẮT KHE + LƯU LỊCH SỬ NHIỀU LẦN)
    def submit_material_quiz(self, user_code, material_id, answers):
        score = 0
        total = len(answers)
        ai_feedback_summary = []
        
        if total == 0: return {'score': 0, 'passed': False}

        # 1. Xác định AttemptNumber (Lần thi thứ mấy)
        sql_att = "SELECT ISNULL(MAX(AttemptNumber), 0) as MaxAtt FROM TRAINING_QUIZ_SUBMISSIONS WHERE UserCode=? AND MaterialID=?"
        att_data = self.db.get_data(sql_att, (user_code, material_id))
        current_attempt = (att_data[0]['MaxAtt'] + 1) if att_data else 1

        for q_id, user_ans in answers.items():
            # Lấy đáp án chuẩn từ DB
            q_sql = "SELECT Content, OptionA, CorrectAnswer FROM TRAINING_QUESTION_BANK WHERE ID=?"
            q_data = self.db.get_data(q_sql, (q_id,))
            if not q_data: continue
            row = q_data[0]
            
            is_correct = 0
            feedback = ""
            
            # Phân loại câu hỏi
            is_mcq = row['OptionA'] and row['OptionA'].strip() != ""
            
            if is_mcq:
                # --- CHẤM TRẮC NGHIỆM ---
                correct_char = row['CorrectAnswer'].strip()[0].upper()
                user_char = user_ans.strip()[0].upper() if user_ans else ""
                if correct_char == user_char:
                    score += 1
                    is_correct = 1
            else:
                # --- CHẤM TỰ LUẬN (AI) ---
                # Gọi hàm AI chấm điểm (Logic mới: >= 70/100 là Đạt)
                ai_res = self._ai_grade_essay(row['Content'], row['CorrectAnswer'], user_ans)
                grade_percent = ai_res.get('score', 0) # Thang 100
                feedback = ai_res.get('feedback', '')
                
                # Logic: Đúng trên 70% nội dung -> Tính điểm
                if grade_percent >= 70:
                    score += 1
                    is_correct = 1
                else:
                    ai_feedback_summary.append(f"- Câu '{row['Content'][:30]}...': {feedback} (Độ khớp: {grade_percent}%)")

            # LƯU VÀO DB (Kèm AttemptNumber)
            self.db.execute_non_query("""
                INSERT INTO TRAINING_QUIZ_SUBMISSIONS 
                (UserCode, MaterialID, QuestionID, UserAnswer, IsCorrect, AIFeedback, AttemptNumber, SubmittedDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
            """, (user_code, material_id, q_id, user_ans, is_correct, feedback, current_attempt))

        # 2. Tính kết quả chung cuộc
        pass_rate = (score / total) * 100
        passed = pass_rate >= 80
        
        # 3. Cập nhật tiến độ (QUAN TRỌNG: Không làm mất trạng thái COMPLETED cũ)
        check = self.db.get_data("SELECT Status FROM TRAINING_USER_PROGRESS WHERE UserCode=? AND MaterialID=?", (user_code, material_id))
        
        new_status = 'COMPLETED' if passed else 'IN_PROGRESS'
        
        if check:
            old_status = check[0]['Status']
            # Chỉ update trạng thái nếu:
            # 1. Trước đó chưa xong (IN_PROGRESS) và giờ làm xong (COMPLETED)
            # 2. Hoặc giữ nguyên trạng thái cũ, chỉ update LastInteraction
            # TUYỆT ĐỐI KHÔNG downgrade từ COMPLETED về IN_PROGRESS
            final_status = 'COMPLETED' if old_status == 'COMPLETED' else new_status

            self.db.execute_non_query("""
                UPDATE TRAINING_USER_PROGRESS 
                SET Status = ?, LastInteraction = GETDATE() 
                WHERE UserCode=? AND MaterialID=?""", (final_status, user_code, material_id))
        else:
            self.db.execute_non_query("INSERT INTO TRAINING_USER_PROGRESS (UserCode, MaterialID, Status, LastPageRead, LastInteraction) VALUES (?, ?, ?, 1, GETDATE())", (user_code, material_id, new_status))
            
        feedback_msg = "<br>".join(ai_feedback_summary) if ai_feedback_summary else "Xuất sắc! Bạn nắm bài rất tốt."
            
        return {
            'score': score, 
            'total': total, 
            'passed': passed, 
            'attempt': current_attempt,
            'feedback': feedback_msg
        }
    
    # HÀM CHẤM TỰ LUẬN NÂNG CAO
    def _ai_grade_essay(self, question, standard_ans, user_ans):
        # Nếu user không trả lời -> 0 điểm ngay
        if not user_ans or len(user_ans.strip()) < 5:
            return {"score": 0, "feedback": "Chưa trả lời hoặc quá ngắn."}

        try:
            model = genai.GenerativeModel(self.ai_model_name)
            
            prompt = f"""
            Bạn là Giám khảo chấm thi Tự luận kỹ thuật.
            
            CÂU HỎI: {question}
            ĐÁP ÁN CHUẨN (Ý chính): {standard_ans}
            
            TRẢ LỜI CỦA HỌC VIÊN: "{user_ans}"
            
            NHIỆM VỤ:
            So sánh ý nghĩa (Semantic Matching) của câu trả lời học viên với đáp án chuẩn.
            - Không bắt bẻ chính tả.
            - Chú trọng vào các từ khóa kỹ thuật và logic.
            - Nếu trả lời lan man, sai trọng tâm -> Điểm thấp.
            - Nếu trả lời đúng ý nhưng khác văn phong -> Điểm cao.
            
            OUTPUT JSON (Bắt buộc):
            {{
                "score": 0-100,  // Điểm số (Interger)
                "feedback": "..." // Nhận xét ngắn gọn (dưới 15 từ) tại sao sai/đúng.
            }}
            """
            
            res = model.generate_content(prompt)
            text = res.text.replace('```json', '').replace('```', '').strip()
            return json.loads(text)
            
        except Exception as e:
            print(f"❌ Lỗi AI Grading: {e}")
            # [QUAN TRỌNG] Lỗi AI -> Trả về 0 điểm để tránh gian lận, yêu cầu user làm lại
            return {"score": 0, "feedback": "Lỗi kết nối AI chấm điểm. Vui lòng thử lại sau giây lát."}
    
    