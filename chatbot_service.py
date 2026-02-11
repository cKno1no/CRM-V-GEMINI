# services/chatbot_service.py

from flask import current_app, session
import google.generativeai as genai
from google.generativeai.types import FunctionDeclaration, Tool
import json
from datetime import datetime
import traceback
import config
from db_manager import safe_float
from services.training_service import TrainingService
from services.gamification_service import GamificationService
import logging # [FIX] Import logging chuẩn để dùng trong __init__

# [FIX] Cấu hình logger cho module này
logger = logging.getLogger(__name__)

class ChatbotService:
    def __init__(self, sales_lookup_service, customer_service, delivery_service, task_service, app_config, db_manager):
        # [UX ONLY] Từ điển này CHỈ DÙNG ĐỂ HIỂN THỊ (Formatter), không dùng bắt logic SQL.
        self.TAG_TRANSLATIONS = {
            'LEADERSHIP': 'Lãnh đạo', 'DEDICATION': 'Tận tâm', 'FUNNY': 'Hài hước',
            'MENTOR': 'Người thầy', 'MENTORSHIP': 'Cố vấn', 'TECHNICAL': 'Kỹ thuật',
            'RESILIENCE': 'Kiên cường', 'SALES': 'Bán hàng', 'VISION': 'Tầm nhìn',
            'TEAMWORK': 'Đồng đội', 'INNOVATION': 'Đổi mới', 'STRATEGY': 'Chiến lược',
            'SUPPORT': 'Hỗ trợ', 'DISCIPLINE': 'Kỷ luật', 'BUSINESSSKILLS': 'Kỹ năng KD',
            'PARETOPRINCIPLE': 'Nguyên lý 80/20', 'PRIORITIZATION': 'Ưu tiên',
            'GUIDANCE': 'Dẫn dắt', 'EXPERIENCE': 'Kinh nghiệm', 'CUSTOMERFOCUS': 'Khách hàng trọng tâm',
            'TRUST': 'Tin cậy', 'HARDWORKING': 'Chăm chỉ', 'DATA': 'Dữ liệu',
            'PROBLEM SOLVING': 'Giải quyết vấn đề', 'CREATIVE': 'Sáng tạo'
        }
        
        self.lookup_service = sales_lookup_service
        self.customer_service = customer_service
        self.delivery_service = delivery_service
        self.task_service = task_service
        self.db = db_manager
        self.app_config = app_config
        # --- [FIX LỖI TẠI ĐÂY] ---
        # Phải khởi tạo Gamification trước vì Training cần dùng nó
        self.gamification = GamificationService(db_manager)
        
        # Khởi tạo TrainingService và gán vào biến self.training_service
        self.training_service = TrainingService(db_manager, self.gamification)

        # [DEPENDENCY] Khởi tạo CustomerAnalysisService
        from services.customer_analysis_service import CustomerAnalysisService
        self.analysis_service = CustomerAnalysisService(db_manager) 

        # 1. Cấu hình API
        api_key = "xxxx"
        if not api_key:
            # [FIX] Dùng logger chuẩn thay vì current_app.logger
            logger.error("⚠️ CRITICAL: GEMINI_API_KEY not found in config!")
        else:
            genai.configure(api_key=api_key)

        # 2. ĐỊNH NGHĨA SKILL MAP (QUAN TRỌNG: Map tên hàm với ItemCode trong DB)
        # Hàm check_product_info KHÔNG có trong này nghĩa là MIỄN PHÍ
        self.skill_mapping = {
            'check_delivery_status': 'skill_delivery',
            'check_replenishment': 'skill_replenishment',
            'check_customer_overview': 'skill_overview',
            'check_daily_briefing': 'skill_briefing',
            'summarize_customer_report': 'skill_report',
            'lookup_sales_flow' : 'skill_Salesflow',
            'analyze_customer_deep_dive': 'skill_deepdive',
            'get_titan_stories': 'skill_stories'
        }

        # 2. DEFINITIONS (Tools cho AI)
        self.tools_definitions = [
            FunctionDeclaration(
                name="check_product_info",
                description="Tra cứu thông tin sản phẩm (Giá, Tồn kho, Lịch sử mua). Phân biệt rõ Tên Hàng và Tên Khách.",
                parameters={
                    "type": "object",
                    "properties": {
                        "product_keywords": {"type": "string", "description": "Mã hoặc tên sản phẩm (VD: '22210 NSK')"},
                        "customer_name": {"type": "string", "description": "Tên khách hàng (VD: 'Kraft', 'Hoa Sen')"},
                        "selection_index": {"type": "integer", "description": "Số thứ tự nếu user chọn từ danh sách trước đó"}
                    },
                    "required": ["product_keywords"]
                }
            ),
            # 1. Nâng cấp Tool Kiểm tra Giao hàng (Type A - Delivery Weekly)
            FunctionDeclaration(
                name="check_delivery_status",
                description="Kiểm tra tình trạng giao hàng THỰC TẾ (Xe chạy chưa, đã giao xong chưa). Dùng bảng Delivery Weekly. Sử dụng khi hỏi: 'Giao chưa?', 'Xe đi chưa?', 'Đang ở đâu?'.",
                parameters={
                    "type": "object",
                    "properties": {
                        "customer_name": {"type": "string", "description": "Tên khách hàng"},
                        "product_keywords": {"type": "string", "description": "Mã hàng cụ thể cần kiểm tra (Nếu có)."},
                        "selection_index": {"type": "integer", "description": "Số thứ tự user chọn"}
                    },
                    "required": ["customer_name"]
                }
            ),
            FunctionDeclaration(
                name="check_replenishment",
                description="Kiểm tra nhu cầu đặt hàng dự phòng (Safety Stock/ROP/BackOrder).",
                parameters={
                    "type": "object",
                    "properties": {
                        "customer_name": {"type": "string", "description": "Tên khách hàng"},
                        "i02id_filter": {"type": "string", "description": "Mã lọc phụ (VD: 'AB' hoặc mã I02ID cụ thể)"},
                        "selection_index": {"type": "integer", "description": "Số thứ tự user chọn"}
                    },
                    "required": ["customer_name"]
                }
            ),
            FunctionDeclaration(
                name="check_customer_overview",
                description="Xem tổng quan về khách hàng (Doanh số, Công nợ cơ bản).",
                parameters={
                    "type": "object",
                    "properties": {
                        "customer_name": {"type": "string", "description": "Tên khách hàng"},
                        "selection_index": {"type": "integer", "description": "Số thứ tự user chọn"}
                    }
                }
            ),
            FunctionDeclaration(
                name="check_daily_briefing",
                description="Tổng hợp công việc hôm nay (Task, Approval, Report).",
                parameters={
                    "type": "object",
                    "properties": {
                        "scope": {"type": "string", "enum": ["today", "week"]}
                    }
                }
            ),
            FunctionDeclaration(
                name="summarize_customer_report",
                description="Đọc và tóm tắt báo cáo (Notes/Activities) của khách hàng.",
                parameters={
                    "type": "object",
                    "properties": {
                        "customer_name": {"type": "string", "description": "Tên khách hàng"},
                        "months": {"type": "integer", "description": "Số tháng (mặc định 6)"},
                        "selection_index": {"type": "integer", "description": "Số thứ tự user chọn"}
                    },
                    "required": ["customer_name"]
                }
            ),
            # [NEW] Tool Phân Tích Sâu
            FunctionDeclaration(
                name="analyze_customer_deep_dive",
                description="Phân tích chuyên sâu 360 độ (KPIs, Top SP, Cơ hội bỏ lỡ, Lãi biên...). Dùng cho câu hỏi 'Phân tích', 'Báo cáo chi tiết'.",
                parameters={
                    "type": "object",
                    "properties": {
                        "customer_name": {"type": "string", "description": "Tên khách hàng"},
                        "selection_index": {"type": "integer", "description": "Số thứ tự user chọn nếu có danh sách"}
                    },
                    "required": ["customer_name"]
                }
            ),

            # 2. Tinh chỉnh Tool Dòng chảy Kinh doanh (Type B - View Summary)
            FunctionDeclaration(
                name="lookup_sales_flow",
                description="Tra cứu dữ liệu Dòng chảy Kinh doanh (PXK, Hóa đơn, Lịch sử). Dùng View Tổng hợp. Sử dụng khi hỏi: 'Xuất kho ngày nào?', 'Số hóa đơn?', 'Giá bán bao nhiêu?', 'Lịch sử mua hàng'.",
                parameters={
                    "type": "object",
                    "properties": {
                        "intent": {
                            "type": "string", 
                            "enum": ["check_export_invoice", "check_price_history", "customer_list"],
                            "description": "Mục đích: check_export_invoice (Ngày xuất kho/HĐ), check_price_history (Lịch sử giá/SL), customer_list (Ai mua mã này)"
                        },
                        "product_keywords": {"type": "string", "description": "Mã hoặc tên sản phẩm"},
                        "customer_name": {"type": "string", "description": "Tên khách hàng"},
                        "order_ref": {"type": "string", "description": "Số đơn hàng (SO), Số PXK hoặc Số Hóa đơn"},
                        "months": {"type": "integer", "description": "Số tháng tra cứu (Mặc định 6)."}
                    },
                    "required": ["intent"]
                }
            ),

            FunctionDeclaration(
                name="lookup_internal_knowledge",
                # [QUAN TRỌNG] Dạy AI: Nếu user chọn câu hỏi gợi ý, hãy gửi nội dung câu đó vào đây
                description="Tra cứu Kiến thức Nội bộ (N3H). Dùng khi user hỏi quy trình, kỹ thuật HOẶC khi user chọn một câu hỏi từ danh sách gợi ý (VD: 'Chọn câu 1').",
                parameters={
                    "type": "object",
                    "properties": {
                        "search_query": {
                            "type": "string", 
                            "description": "Từ khóa tìm kiếm HOẶC nội dung câu hỏi user vừa chọn (VD: 'Miền nhiệt độ làm việc...')."
                        }
                    },
                    "required": ["search_query"]
                }
            ),
        
            FunctionDeclaration(
                name="get_titan_stories",
                # [FIX] Dùng từ khóa mạnh để ép AI hiểu STDD là đối tượng hợp lệ
                description="Kể chuyện Hall of Fame. Đối tượng hợp lệ bao gồm: 1. Các nhân sự (Titan). 2. CÔNG TY STDD (Ngôi nhà chung). Nếu hỏi về STDD, BẮT BUỘC dùng tool này.",
                parameters={
                    "type": "object",
                    "properties": {
                        "titan_name": {"type": "string", "description": "Tên nhân sự hoặc tên công ty (VD: 'STDD', 'Ngôi nhà chung')."},
                        "tag_filter": {"type": "string", "description": "Chủ đề (Tag) muốn lọc."}
                    },
                    "required": ["titan_name"]
                }
            )
        ]
            
        # 3. Khởi tạo Model
        # Ưu tiên các model mới và nhanh
        self.model = genai.GenerativeModel(
            model_name='gemini-2.5-flash',
            tools=[self.tools_definitions]
        )
        
        
        # Fallback cuối cùng
        if not self.model:
            # [FIX] Dùng logger chuẩn
            logger.error("❌ ALL GEMINI MODELS FAILED. Using default 1.5-flash without check.")
            self.model = genai.GenerativeModel('gemini-1.5-flash', tools=[self.tools_definitions])

        # 4. Map Functions
        self.functions_map = {
            'check_product_info': self._wrapper_product_info,
            'check_delivery_status': self._wrapper_delivery_status,
            'check_replenishment': self._wrapper_replenishment,
            'check_customer_overview': self._wrapper_customer_overview,
            'check_daily_briefing': self._wrapper_daily_briefing,
            'summarize_customer_report': self._wrapper_summarize_report,
            'analyze_customer_deep_dive': self._wrapper_analyze_deep_dive,
            'lookup_sales_flow' : self._wrapper_lookup_sales_flow,
            'lookup_internal_knowledge': self._wrapper_lookup_knowledge,
            'get_titan_stories': self._wrapper_titan_stories
        }
    
    # [HELPER 1] Dùng AI để dịch từ khóa User -> Standard DB Tag
    def _ai_translate_tag(self, user_input_tag):
        if not user_input_tag: return ""
        clean_input = user_input_tag.upper().replace("#", "").strip()
        
        # [TỐI ƯU] Tìm trực tiếp trong từ điển trước để tránh gọi AI lần 2
        if clean_input in self.TAG_TRANSLATIONS:
            return clean_input
        
        # Tìm kiếm mờ (Fuzzy) bằng cách check giá trị tiếng Việt
        for en_key, vn_val in self.TAG_TRANSLATIONS.items():
            if vn_val.upper() in clean_input:
                return en_key

        # Chỉ khi không tìm thấy mới gọi AI (hoặc trả về nguyên gốc để giảm trễ)
        try:
            # Rút gọn Prompt cực ngắn để AI trả lời nhanh
            prompt = f"Translate to 1 English keyword: {user_input_tag}"
            response = self.model.generate_content(prompt)
            return response.text.strip().upper()
        except:
            return clean_input

    # [HELPER 2] Format tag hiển thị song ngữ
    def _format_tags_bilingual(self, tag_string):
        if not tag_string: return ""
        raw_tags = [t.strip().replace('#', '') for t in tag_string.replace(',', ' ').split() if t.strip()]
        formatted_tags = []
        seen = set()
        for t in raw_tags:
            upper_t = t.upper()
            if upper_t in seen: continue
            seen.add(upper_t)
            vn = self.TAG_TRANSLATIONS.get(upper_t)
            formatted_tags.append(f"#{t} ({vn})" if vn else f"#{t}")
        return ", ".join(formatted_tags)

    # [HELPER 3] Auto-tagging (Giữ nguyên logic cũ để làm giàu DB)
    def _auto_generate_tags_if_missing(self, story_id, content):
        try:
            prompt = f"""Đọc câu chuyện và đưa ra tối đa 3 Hashtag tiếng Anh (#Leadership, #Dedication...). Nội dung: "{content[:1000]}" """
            response = self.model.generate_content(prompt)
            tags = response.text.strip().replace('\n', '')
            if tags:
                self.db.execute_non_query("UPDATE HR_HALL_OF_FAME SET Tags = ? WHERE StoryID = ?", (tags, story_id))
                return tags
            return ""
        except: return ""

    # [HELPER 4] Render HTML Card (Private method để tái sử dụng)
    def _build_titan_html_card(self, title, subtitle, image_url, content_md):
        """Hàm bọc nội dung vào thẻ HTML Titan Card"""
        img_html = ""
        if image_url:
            # Xử lý đường dẫn ảnh (giả sử ảnh lưu trong folder static/uploads)
            final_url = image_url if image_url.startswith('http') else f"/static/uploads/{image_url}"
            img_html = f'<div class="titan-card-img"><img src="{final_url}" onerror="this.style.display=\'none\'" /></div>'
        
        return f"""
        <div class="titan-card-wrapper">
            <div class="titan-card-header">
                <h3>📜 {title}</h3>
                <span class="titan-badge">{subtitle}</span>
            </div>
            {img_html}
            <div class="titan-card-body">
                {content_md}
            </div>
        </div>
        """
    
    def _get_formal_target_name(self, user_data):
        """Lấy tên đầy đủ và thêm danh xưng trang trọng."""
        # Ưu tiên lấy FullName (userName) từ DB
        full_name = user_data.get('userName') or user_data.get('shortname') or "Titan"
        
        # Nếu tên chưa có danh xưng, tự động thêm "Anh/Chị" (Sếp có thể sửa logic dựa trên giới tính nếu có)
        honorifics = ('ANH', 'CHỊ', 'CHI', 'SẾP', 'SEP', 'CO', 'CHU', 'CÔ', 'CHÚ')
        if not full_name.upper().startswith(honorifics):
            return f"Anh {full_name}"
        return full_name
    
    # --- HÀM KIỂM TRA QUYỀN SỞ HỮU SKILL ---
    def _check_user_has_skill(self, user_code, func_name):
        # 1. Nếu hàm không nằm trong danh sách map -> Miễn phí
        if func_name not in self.skill_mapping:
            return True, None
            
        required_item_code = self.skill_mapping[func_name]
        
        # 2. Kiểm tra DB xem User đã mua và kích hoạt item này chưa
        sql = """
            SELECT TOP 1 ID FROM TitanOS_UserInventory 
            WHERE UserCode = ? AND ItemCode = ? AND IsActive = 1
        """
        check = self.db.get_data(sql, (user_code, required_item_code))
        
        if check:
            return True, None
        else:
            # Lấy tên skill để báo lỗi đẹp hơn
            skill_name_sql = "SELECT ItemName FROM TitanOS_SystemItems WHERE ItemCode = ?"
            skill_info = self.db.get_data(skill_name_sql, (required_item_code,))
            skill_name = skill_info[0]['ItemName'] if skill_info else required_item_code
            return False, skill_name
        
    # --- [NEW] HÀM LẤY TÊN PET ĐANG TRANG BỊ ---
    def _get_equipped_pet_info(self, user_code):
        """Lấy tên Pet và mã Pet đang trang bị để AI xưng hô."""
        sql = """
            SELECT T2.ItemName, T2.ItemCode 
            FROM TitanOS_UserProfile T1
            JOIN TitanOS_SystemItems T2 ON T1.EquippedPet = T2.ItemCode
            WHERE T1.UserCode = ?
        """
        data = self.db.get_data(sql, (user_code,))
        if data:
            item_name = data[0]['ItemName']
            # Gợi ý tên gọi thân mật cho AI dựa trên ItemName hoặc ItemCode
            # Bạn có thể cập nhật ItemName trong DB TitanOS_SystemItems cho hay
            nicknames = {
                'fox': 'Bé Cáo AI',
                'bear': 'Bé Gấu Mặp',
                'dragon': 'Bé Rồng Bự',
                'monkey': 'Bé Khỉ Thiền',
                'cat': 'Bé Mèo Béo',
                'deer': 'Bé Nai Ngơ'
            }
            # Ưu tiên lấy nickname hardcode cho cute, nếu không có thì lấy tên trong DB
            pet_name = nicknames.get(data[0]['ItemCode'], item_name)
            return pet_name
        return "Bé Titan" # Mặc định    
    # =========================================================================
    # MAIN PROCESS (Ở đây app đã chạy, dùng current_app được)
    # =========================================================================
    def process_message(self, message_text, user_code, user_role, theme='light'):
        try:
            # 1. Lấy thông tin User Profile để biết tên gọi
            user_profile = self.db.get_data("SELECT Nickname, SHORTNAME FROM TitanOS_UserProfile P JOIN [GD - NGUOI DUNG] U ON P.UserCode = U.USERCODE WHERE P.UserCode = ?", (user_code,))
            
            user_name = "Sếp" # Mặc định
            if user_profile:
                # Ưu tiên Nickname, nếu không có thì dùng Shortname
                user_name = user_profile[0].get('Nickname') or user_profile[0].get('SHORTNAME')
            # [LOGIC MỚI] Xử lý Persona động theo Pet
            pet_name = "AI"
            if theme == 'adorable':
                pet_name = self._get_equipped_pet_info(user_code)
            # 1. Định nghĩa Persona dựa trên Theme
            base_personas = {
                'light': "Bạn là Trợ lý Kinh doanh Titan (Business Style). Trả lời rành mạch, tập trung vào số liệu.",
                'dark': "Bạn là Hệ thống Titan OS (Formal). Xưng hô: Tôi - Bạn. Phong cách trang trọng, chính xác, khách quan.",
                'fantasy': "Bạn là AI từ tương lai (Sci-Fi). Xưng hô: Commander - System. Giọng điệu máy móc, hào hứng.",
                'adorable': f"Bạn là {pet_name} (Gen Z). Người dùng tên là {user_name}. Xưng hô: Em ({pet_name}) - Hãy gọi người dùng là {user_name} hoặc Sếp {user_name}. Dùng emoji 🦊🐻💖✨. Giọng cute, năng động, hỗ trợ nhiệt tình."
            }
            
            # [FIX QUAN TRỌNG] Thêm luật đặc biệt cho Hall of Fame vào mọi Persona
            hall_of_fame_rule = """
            QUY TẮC HALL OF FAME:
            - 'Titan' bao gồm cả CON NGƯỜI và TẬP THỂ CÔNG TY (STDD).
            - Nếu user hỏi 'kể về STDD', 'ngôi nhà chung', 'công ty', HÃY DÙNG TOOL `get_titan_stories` để kể chuyện.
            - KHÔNG ĐƯỢC TỪ CHỐI kể chuyện về STDD với lý do 'nó là công ty'. Hãy nhân cách hóa nó.
            """

            selected_persona = base_personas.get(theme, base_personas['light'])
            system_instruction = f"{selected_persona}\n{hall_of_fame_rule}"
            
            
            # 2. Context History (Lấy từ Session)
            history = session.get('chat_history', [])
            gemini_history = []
            for h in history:
                gemini_history.append({"role": "user", "parts": [h['user']]})
                gemini_history.append({"role": "model", "parts": [h['bot']]})

            # 3. Tạo Chat Session
            chat = self.model.start_chat(history=gemini_history, enable_automatic_function_calling=False)
            
            self.current_user_code = user_code
            self.current_user_role = user_role

            full_prompt = f"[System Instruction: {system_instruction}]\nUser Query: {message_text}"
            
            # 4. Gửi tin nhắn đi
            response = chat.send_message(full_prompt)
            
            final_text = ""
            # -----------------------------------------------------------
            # [LOGIC 1] CHECK DAILY CHALLENGE ANSWER (Ưu tiên số 1)
            # -----------------------------------------------------------
            # 1. [GIỮ NGUYÊN] ƯU TIÊN SỐ 1: Check trả lời Quiz (A, B, C, D)
            # Vì cái này cần chính xác tuyệt đối, không cần AI suy luận
            clean_msg = message_text.strip().upper()
            if len(clean_msg) == 1 and clean_msg in ['A', 'B', 'C', 'D']:
                res = self.training_service.check_daily_answer(user_code, clean_msg)
                if res: return res

            
            # 5. Xử lý Function Call
            function_call_part = None
            if response.candidates and response.candidates[0].content.parts:
                for part in response.candidates[0].content.parts:
                    if part.function_call:
                        function_call_part = part.function_call
                        break
            
            if function_call_part:
                fc = function_call_part
                func_name = fc.name
                func_args = dict(fc.args)
                
                # [OK] Dùng current_app ở đây được vì đang trong request
                current_app.logger.info(f"🤖 AI Calling Tool: {func_name} | Args: {func_args}")

                # --- [LOGIC CHẶN TÍNH NĂNG Ở ĐÂY] ---
                has_permission, skill_name = self._check_user_has_skill(user_code, func_name)

                if not has_permission:
                    # Nếu chưa mua -> Trả về kết quả lỗi giả lập cho AI
                    api_result = (
                        f"SYSTEM_ALERT: Người dùng CHƯA sở hữu kỹ năng '{skill_name}'. "
                        f"Hãy từ chối thực hiện và yêu cầu họ vào 'Cửa hàng' (Shop) để mở khóa kỹ năng này. "
                        f"Đừng thực hiện lệnh."
                    )
                else:
                    
                    if func_name in self.functions_map:
                        try:
                            api_result = self.functions_map[func_name](**func_args)
                        except Exception as e:
                            error_msg = f"Lỗi thực thi hàm {func_name}: {str(e)}"
                            current_app.logger.error(f"❌ Function Error: {error_msg}")
                            api_result = error_msg
                    else:
                        api_result = "Hàm không tồn tại trong hệ thống."
                # -------------------------------------    
                # --- ĐOẠN ĐIỀU CHỈNH QUAN TRỌNG NHẤT Ở ĐÂY ---
                # Nếu api_result là một HTML Card (chứa class titan-card-wrapper)
                # Chúng ta RETURN LUÔN, không cho AI "nói leo" thêm nữa.
                # =============================================================
                # CƠ CHẾ FAST-RESPONSE: PHÂN LUỒNG TRẢ VỀ
                # =============================================================
                
                # Nhóm 1: Trả về trực tiếp (Không qua AI tóm tắt lần 2)
                # Dùng cho: HTML Cards, Bảng giá tra nhanh, Delivery status
                if isinstance(api_result, str) and (
                    'titan-card-wrapper' in api_result or 
                    '### 📦 Kết quả tra cứu' in api_result or
                    '🚚 **Tình trạng Vận chuyển' in api_result or
                    '🔍 Tìm thấy' in api_result or
                    '📚 **Kiến thức N3H' in api_result or   # <--- THÊM DÒNG NÀY (Để hiện đáp án)
                    '🤔 **Có phải ý Sếp' in api_result or   # <--- THÊM DÒNG NÀY (Để hiện gợi ý)
                    '⚠️' in api_result                       # <--- THÊM DÒNG NÀY (Để hiện cảnh báo)
                ):
                    final_text = api_result
                
                # Nhóm 2: Dữ liệu thô cần AI tóm tắt (Phân tích sâu, Báo cáo công việc)
                else:
                    final_res = chat.send_message({
                        "function_response": {
                            "name": func_name,
                            "response": {"result": api_result}
                        }
                    })
                    final_text = final_res.text
            
            else:
                final_text = response.text

            # 6. Lưu lịch sử
            history.append({'user': message_text, 'bot': final_text})
            if len(history) > 10: history = history[-10:]
            session['chat_history'] = history
            
            return final_text

        except Exception as e:
            traceback.print_exc()
            return f"Hệ thống đang bận hoặc gặp lỗi kết nối AI. Vui lòng thử lại sau. (Error: {str(e)})"

    # =========================================================================
    # CÁC HÀM WRAPPER
    # =========================================================================

    def _resolve_customer(self, customer_name, selection_index):
        context_list = session.get('customer_search_results')
        if selection_index is not None and context_list:
            try:
                idx = int(selection_index) - 1
                if 0 <= idx < len(context_list):
                    selected = context_list[idx]
                    session.pop('customer_search_results', None)
                    return [selected] 
            except: pass

        if not customer_name: return None
        
        customers = self.customer_service.get_customer_by_name(customer_name)
        if not customers: return "NOT_FOUND"
        
        if len(customers) > 1:
            session['customer_search_results'] = customers 
            return "MULTIPLE"
            
        return customers
    
    # --- [HELPER] XỬ LÝ NGÀY THÁNG AN TOÀN (TRÁNH LỖI NaT) ---
    def _safe_format_date(self, date_obj, fmt='%d/%m/%y'):
        """Chuyển đổi ngày tháng an toàn, xử lý cả None và NaT."""
        if date_obj is None: 
            return None
        # Kiểm tra nếu là NaT (Not a Time) của Pandas
        if str(date_obj) == 'NaT': 
            return None
        try:
            return date_obj.strftime(fmt)
        except:
            return None
        
    def _wrapper_product_info(self, product_keywords, customer_name=None, selection_index=None):
        if not customer_name and not selection_index:
            return self._handle_quick_lookup(product_keywords)

        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND":
            return f"Không tìm thấy khách hàng '{customer_name}'.\nĐang tra nhanh mã '{product_keywords}'...\n" + \
                   self._handle_quick_lookup(product_keywords)
                   
        if cust_result == "MULTIPLE":
            return self._format_customer_options(session['customer_search_results'], customer_name)
        
        customer_obj = cust_result[0]
        
        price_info_str = self._handle_price_check_final(product_keywords, customer_obj)
        history_info_str = self._handle_check_history_final(product_keywords, customer_obj)
        
        return f"""
### 📦 Kết quả tra cứu: {customer_obj['FullName']}
---
{price_info_str}

{history_info_str}
"""

    def _wrapper_delivery_status(self, customer_name, product_keywords=None, selection_index=None):
        """
        [TYPE A] Kiểm tra thực tế giao hàng (Delivery Weekly).
        [FIXED] Đã xử lý lỗi NaTType cho ngày thực giao.
        """
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"❌ Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)
        
        customer_id = cust_result[0]['ID']
        customer_full_name = cust_result[0]['FullName']
        
        # SQL (Giữ nguyên)
        sql = f"""
            SELECT TOP 5 
                M.VoucherNo, M.ActualDeliveryDate, M.DeliveryStatus, 
                M.Planned_Day,
                O.RefNo02, D.Notes, D.InventoryID,
                ISNULL(D.ActualQuantity, 0) as Quantity, -- [FIX] Alias về 'Quantity'
                ISNULL(I.InventoryName, D.InventoryID) as InventoryName
            FROM [CRM_STDD].[dbo].[Delivery_Weekly] M
            LEFT JOIN {config.ERP_DELIVERY_MASTER} O ON M.VoucherID = O.VoucherID
            LEFT JOIN {config.ERP_DELIVERY_DETAIL} D ON M.VoucherID = D.VoucherID
            LEFT JOIN {config.ERP_IT1302} I ON D.InventoryID = I.InventoryID
            WHERE M.ObjectID = ?
        """
        params = [customer_id]

        if product_keywords:
            sql += " AND (D.InventoryID LIKE ? OR I.InventoryName LIKE ?)"
            kw = f"%{product_keywords}%"
            params.extend([kw, kw])
        
        sql += " AND M.VoucherDate >= DATEADD(month, -3, GETDATE())"
        sql += " ORDER BY M.VoucherDate DESC"
        
        try:
            data = self.db.get_data(sql, tuple(params))
            
            if not data:
                return f"ℹ️ Không tìm thấy Lệnh Xuất Hàng (Delivery) nào cho **{customer_full_name}** trong 3 tháng qua (khớp yêu cầu)."

            res = f"🚚 **Tình trạng Vận chuyển Thực tế (Delivery Weekly):**\n"
            
            processed_vouchers = []
            count = 0
            
            for item in data:
                status = str(item.get('DeliveryStatus', '')).strip().upper()
                icon = "🟢" if status in ['DONE', 'DA GIAO'] else "🟠"
                
                # [FIX] Xử lý ngày thực giao an toàn
                actual_date_str = self._safe_format_date(item.get('ActualDeliveryDate'), '%d/%m')
                
                if actual_date_str:
                    date_info = f"Đã giao: **{actual_date_str}**"
                else:
                    plan = item.get('Planned_Day', 'POOL')
                    date_info = f"KH: {plan}"

                # [UPDATED] Hiển thị Mã - Tên Hàng
                item_info = ""
                if item.get('InventoryID'):
                    qty = safe_float(item.get('Quantity', 0))
                    inv_id = item['InventoryID']
                    inv_name = item.get('InventoryName', '')
                    
                    # Logic hiển thị: Nếu có tên và tên khác mã -> hiển thị cả hai
                    if inv_name and inv_name != inv_id:
                        # Cắt ngắn tên nếu quá dài để hiển thị đẹp trên chat
                        if len(inv_name) > 30: inv_name = inv_name[:27] + "..."
                        display_str = f"{inv_id} - {inv_name}"
                    else:
                        display_str = inv_id
                        
                    item_info = f"📦 **{display_str}**: {qty:,.0f}"

                ref_info = item.get('RefNo02')
                note_info = item.get('Notes')
                extra_details = []
                if ref_info: extra_details.append(f"Ref: {ref_info}")
                if note_info: extra_details.append(f"Note: {note_info}")
                
                detail_str = f" _({', '.join(extra_details)})_" if extra_details else ""
                
                res += f"- {icon} **{item['VoucherNo']}**: {status} | {date_info} | {item_info}{detail_str}\n"
                
                count += 1
                if count >= 5: 
                    res += "... (còn thêm kết quả)"
                    break 
            
            return res

        except Exception as e:
            logger.error(f"Error in wrapper_delivery_status: {e}")
            return f"Lỗi tra cứu Delivery Weekly: {str(e)}"

    def _wrapper_replenishment(self, customer_name, i02id_filter=None, selection_index=None):
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)
        
        customer_obj = cust_result[0]
        if i02id_filter: 
            customer_obj['i02id_filter'] = i02id_filter
        
        return self._handle_replenishment_check_final(customer_obj)

    def _wrapper_customer_overview(self, customer_name, selection_index=None):
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"❌ Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)
        
        return self._get_customer_detail(cust_result[0]['ID'])

    def _wrapper_daily_briefing(self, scope='today'):
        user_code = getattr(self, 'current_user_code', '')
        res = f"📅 **Tổng quan công việc ({scope}):**\n"
        
        sql_task = "SELECT Subject, Priority FROM Task_Master WHERE AssignedTo = ? AND Status != 'Done' AND DueDate <= GETDATE()"
        tasks = self.db.get_data(sql_task, (user_code,))
        
        if tasks:
            res += "\n📌 **Việc cần làm ngay:**\n" + "\n".join([f"- {t['Subject']} ({t['Priority']})" for t in tasks])
        else:
            res += "\n📌 **Việc cần làm:** Tuyệt vời! Bạn không có task quá hạn."

        sql_approval = "SELECT COUNT(*) as Cnt FROM OT2101 WHERE OrderStatus = 0" 
        approval = self.db.get_data(sql_approval)
        if approval and approval[0]['Cnt'] > 0:
            res += f"\n\n💰 **Phê duyệt:** Hệ thống có {approval[0]['Cnt']} Báo giá đang chờ duyệt."

        return res

    def _wrapper_summarize_report(self, customer_name, months=6, selection_index=None):
        try: months = int(float(months)) if months else 6
        except: months = 6

        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"❌ Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)

        customer_obj = cust_result[0]
        customer_id = customer_obj['ID']
        customer_full_name = customer_obj['FullName']
        
        search_keyword = customer_name if len(customer_name) > 3 else customer_full_name 

        sql = f"""
            SELECT TOP 60 
                [Ngay] as CreatedDate, 
                [Nguoi] as CreateUser,
                CAST([Noi dung 1] AS NVARCHAR(MAX)) as Content1, 
                CAST([Noi dung 2] AS NVARCHAR(MAX)) as Content2_Added,
                CAST([Danh gia 2] AS NVARCHAR(MAX)) as Content3,
                [Khach hang] as TaggedCustomerID
            FROM {config.TEN_BANG_BAO_CAO}
            WHERE 
                ([Ngay] >= DATEADD(month, -?, GETDATE()))
                AND (
                    [Khach hang] = ?  
                    OR (CAST([Noi dung 1] AS NVARCHAR(MAX)) LIKE N'%{search_keyword}%')
                    OR (CAST([Noi dung 2] AS NVARCHAR(MAX)) LIKE N'%{search_keyword}%')
                )
            ORDER BY [Ngay] DESC
        """ 

        try:
            reports = self.db.get_data(sql, (months, customer_id))
        except Exception as e:
            current_app.logger.error(f"SQL Report Error: {e}")
            return f"Lỗi hệ thống khi truy xuất báo cáo: {str(e)}"
            
        if not reports:
            return f"ℹ️ Không tìm thấy báo cáo nào liên quan đến **{customer_full_name}** trong {months} tháng qua."

        context_text_raw = ""
        related_count = 0
        direct_count = 0
        
        for r in reports:
            date_val = r.get('CreatedDate')
            date_str = date_val.strftime('%d/%m/%Y') if date_val else 'N/A'
            
            c1 = str(r.get('Content1', '')).strip()
            c2 = str(r.get('Content2_Added', '')).strip()
            c3 = str(r.get('Content3', '')).strip()
            content = ". ".join([p for p in [c1, c2, c3] if p])
            
            if not content or content == '.': continue 
            
            tagged_id = str(r.get('TaggedCustomerID', '')).strip()
            if tagged_id == str(customer_id):
                source_type = "TRỰC TIẾP"
                direct_count += 1
            else:
                source_type = "LIÊN QUAN"
                related_count += 1
                
            context_text_raw += f"- [{date_str}] [{source_type}] {r['CreateUser']}: {content}\n"
        
        system_prompt = (
            f"Bạn là trợ lý Kinh doanh. Nhiệm vụ: Tóm tắt tình hình khách hàng {customer_full_name} trong 20-25 dòng.\n"
            "Dữ liệu được cung cấp gồm báo cáo TRỰC TIẾP và LIÊN QUAN (nhắc tên).\n"
            "----------------\n"
            "YÊU CẦU:\n"
            f"- Lọc thông tin liên quan đến '{search_keyword}' hoặc '{customer_full_name}'.\n"
            "- Tổng hợp thành 3 phần: \n"
            "   + 1. Tổng quan\n"
            "   + 2. Điểm Tốt & Thành Tựu (QUAN TRỌNG: Tìm kỹ các từ khóa: SKF, FAG, NTN, Chuyển đổi mã, Thành công).\n"
            "   + 3. Rủi ro & Cần Cải Thiện.\n"
            "- Trình bày Markdown rõ ràng."
        )
        
        summary_header = f"### 📊 DỮ LIỆU: {direct_count} Trực tiếp | {related_count} Liên quan\n---"
        full_input = summary_header + context_text_raw

        generation_config = {"temperature": 0.2, "top_p": 0.8, "top_k": 40}

        try:
            summary_model = genai.GenerativeModel(
                model_name=self.model.model_name,
                system_instruction=system_prompt,
                generation_config=generation_config
            )
            response = summary_model.generate_content(contents=[full_input])
            return response.text
        except Exception as e:
            return f"Lỗi AI xử lý tóm tắt: {str(e)}"

    def _wrapper_analyze_deep_dive(self, customer_name, selection_index=None):
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"❌ Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)
        
        customer_obj = cust_result[0]
        cust_id = customer_obj['ID']
        cust_name = customer_obj['FullName']
        
        try:
            metrics = self.analysis_service.get_header_metrics(cust_id)
            top_products = self.analysis_service.get_top_products(cust_id)[:10]
            missed_opps = self.analysis_service.get_missed_opportunities_quotes(cust_id)[:10]
            category_data = self.analysis_service.get_category_analysis(cust_id)
            
        except Exception as e:
            current_app.logger.error(f"Deep Dive Error: {e}")
            return f"Gặp lỗi khi trích xuất dữ liệu phân tích: {str(e)}"

        res = f"### 📊 BÁO CÁO PHÂN TÍCH SÂU: {cust_name} ({cust_id})\n"
        
        res += "**1. Sức khỏe Tài chính & Vận hành (YTD):**\n"
        res += f"- **Doanh số:** {metrics.get('SalesYTD', 0):,.0f} (Target: {metrics.get('TargetYear', 0):,.0f})\n"
        res += f"- **Đơn hàng:** {metrics.get('OrderCount', 0)} | **Báo giá:** {metrics.get('QuoteCount', 0)}\n"
        res += f"- **Công nợ:** Hiện tại {metrics.get('DebtCurrent', 0):,.0f} | Quá hạn **{metrics.get('DebtOverdue', 0):,.0f}**\n"
        res += f"- **Hiệu suất Giao hàng (OTIF):** {metrics.get('OTIF', 0)}%\n"
        res += f"- **Tương tác (Báo cáo):** {metrics.get('ReportCount', 0)} lần\n\n"
        
        res += "**2. Top 10 Sản phẩm Bán chạy (2 năm qua):**\n"
        if top_products:
            for i, p in enumerate(top_products):
                name = p.get('InventoryName', p['InventoryID'])
                rev = safe_float(p.get('TotalRevenue', 0))
                qty_ytd = safe_float(p.get('Qty_YTD', 0))
                res += f"{i+1}. **{name}**: {rev:,.0f} đ (SL năm nay: {qty_ytd:,.0f})\n"
        else:
            res += "_Chưa có dữ liệu bán hàng._\n"
        res += "\n"

        res += "**3. Top 10 Cơ hội Bỏ lỡ (Báo giá trượt 5 năm):**\n"
        if missed_opps:
            for i, m in enumerate(missed_opps):
                name = m.get('InventoryName', m['InventoryID'])
                val = safe_float(m.get('MissedValue', 0))
                count = m.get('QuoteCount', 0)
                res += f"{i+1}. **{name}**: Trượt {val:,.0f} đ ({count} lần báo)\n"
        else:
            res += "_Không có cơ hội bỏ lỡ đáng kể._\n"
        res += "\n"
        
        res += "**4. Cơ cấu Nhóm hàng & Hiệu quả (Top 5):**\n"
        if category_data and 'details' in category_data:
            details = category_data['details']
            for i, item in enumerate(details[:5]):
                name = item['name']
                rev = item['revenue']
                profit = item.get('profit', 0)
                margin = item.get('margin_pct', 0)
                
                icon = "🟢" if margin >= 15 else ("🟠" if margin >= 5 else "🔴")
                res += f"- **{name}**: {rev:,.0f} đ | Lãi: {profit:,.0f} ({icon} **{margin}%**)\n"
        
        elif category_data and 'labels' in category_data:
            for i, label in enumerate(category_data['labels'][:5]):
                val = category_data['series'][i]
                res += f"- **{label}**: {val:,.0f} đ\n"
        else:
            res += "_Chưa có dữ liệu phân tích nhóm hàng._\n"

        res += "\n💡 **Gợi ý từ Titan AI:**\n"
        if safe_float(metrics.get('DebtOverdue', 0)) > 10000000:
            res += "- ⚠️ Cảnh báo: Nợ quá hạn cao, cần nhắc nhở khách.\n"
        if safe_float(metrics.get('OrderCount', 0)) == 0 and safe_float(metrics.get('QuoteCount', 0)) > 5:
            res += "- ⚠️ Tỷ lệ chốt đơn thấp. Cần xem lại giá hoặc đối thủ cạnh tranh.\n"
        if missed_opps:
            top_miss = missed_opps[0].get('InventoryName', 'N/A')
            res += f"- 🎯 Cơ hội: Nên chào lại mã **{top_miss}** vì khách đã hỏi nhiều lần.\n"

        return res
    
    def _wrapper_lookup_sales_flow(self, intent, product_keywords=None, customer_name=None, order_ref=None, months=None):
        """
        [TYPE B] Tra cứu Dòng chảy Kinh doanh.
        [UPDATED] Customer List: Hiển thị chi tiết Mã hàng + Tên hàng theo yêu cầu.
        """
        # 1. Xử lý Khách hàng
        customer_id = None
        customer_display = "Tất cả KH"
        if customer_name:
            cust_result = self._resolve_customer(customer_name, None)
            if cust_result == "NOT_FOUND": return f"❌ Không tìm thấy khách hàng '{customer_name}'."
            if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)
            customer_id = cust_result[0]['ID']
            customer_display = cust_result[0]['FullName']

        try: months = int(months) if months else 24 
        except: months = 24
            
        product_filter = f"%{product_keywords}%" if product_keywords else "%"
        order_filter = f"%{order_ref}%" if order_ref else "%"

        # 2. Query View
        base_sql = f"SELECT TOP 50 * FROM {config.VIEW_CHATBOT_SALES_FLOW} WHERE 1=1"
        params = []

        if customer_id:
            base_sql += " AND CustomerCode = ?"
            params.append(customer_id)
        
        if product_keywords:
            base_sql += " AND (InventoryID LIKE ? OR InventoryName LIKE ?)"
            params.extend([product_filter, product_filter])

        if order_ref:
            base_sql += " AND (OrderNo LIKE ? OR InvoiceNo LIKE ? OR DeliveryVoucherNos LIKE ?)"
            params.extend([order_filter, order_filter, order_filter])
        
        if not order_ref:
            base_sql += " AND OrderDate >= DATEADD(month, -?, GETDATE())"
            params.append(months)

        base_sql += " ORDER BY OrderDate DESC"

        try:
            data = self.db.get_data(base_sql, tuple(params))
        except Exception as e:
            return f"Lỗi truy xuất View Sales Flow: {str(e)}"

        if not data:
            return f"ℹ️ Không tìm thấy dữ liệu phù hợp cho **{customer_display}** trong {months} tháng qua."

        res_lines = []
        
        # --- LOGIC HIỂN THỊ ---

        if intent == 'customer_list':
            # [FIX] Gom nhóm theo (Khách, Mã Hàng, Tên Hàng) để không bị mất chi tiết
            # Key: (CustomerName, InventoryID, InventoryName) -> Value: Total Qty
            detail_summary = {}
            
            for d in data:
                c_name = d.get('CustomerName', 'Khách lẻ')
                inv_id = d.get('InventoryID', '')
                inv_name = d.get('InventoryName', '')
                
                # Tạo key duy nhất
                key = (c_name, inv_id, inv_name)
                
                # Cộng dồn số lượng
                detail_summary[key] = detail_summary.get(key, 0) + d['Qty_Ordered']
            
            # Sắp xếp theo số lượng giảm dần
            sorted_items = sorted(detail_summary.items(), key=lambda x: x[1], reverse=True)
            
            res_lines.append(f"👥 **Khách mua '{product_keywords}' ({months} tháng):**")
            
            for (c_name, inv_id, inv_name), qty in sorted_items[:7]: # Hiển thị top 7 dòng
                # Format: SUNSCO: AB1108... , NSK, mua 24 cái
                row = f"- **{c_name}**: {inv_id} - {inv_name}, mua **{qty:,.0f}** cái"
                res_lines.append(row)
            
            remaining = len(sorted_items) - 7
            if remaining > 0:
                res_lines.append(f"... và {remaining} mã/khách khác.")

        else: 
            # (Logic Lịch sử giá/đơn hàng - Giữ nguyên như cũ)
            first_item = data[0]
            c_name = first_item.get('CustomerName', customer_display)
            c_code = first_item.get('CustomerCode', '')
            inv_id = first_item.get('InventoryID', '')
            inv_name = first_item.get('InventoryName', '')
            years_txt = f"{months//12} năm" if months >= 12 else f"{months} tháng"
            
            header = f"Khách hàng **{c_name}** ({c_code}) đã mua **{len(data)}** lần **{inv_id}** - {inv_name} trong {years_txt} qua:"
            res_lines.append(header)
            res_lines.append("")

            count = 0
            for i, item in enumerate(data):
                if count >= 5: break 
                
                so_no = item.get('OrderNo', 'N/A')
                price = item.get('UnitPrice', 0)
                qty = item.get('Qty_Ordered', 0)
                
                inv_no = item.get('InvoiceNo')
                inv_str = f", hóa đơn {inv_no}" if inv_no else ""
                
                export_date = self._safe_format_date(item.get('LastExportDate'), '%d/%m/%Y')
                if export_date:
                    date_str = f"giao ngày {export_date}"
                else:
                    order_date = self._safe_format_date(item.get('OrderDate'), '%d/%m/%Y')
                    date_str = f"đặt ngày {order_date} (Chưa giao)"

                row = f"{i+1}/ Đơn hàng ({so_no}): giá **{price:,.0f}**, mua {qty:,.0f} cái{inv_str}, {date_str}."
                res_lines.append(row)
                count += 1
            
            remaining = len(data) - count
            if remaining > 0: 
                res_lines.append(f"... và {remaining} lần mua khác.")

        return "\n".join(res_lines)
    
    # =========================================================================
    # HÀM WRAPPER MỚI (Cầu nối giữa AI và Database)
    # =========================================================================
    def _wrapper_lookup_knowledge(self, search_query):
        """
        AI gọi hàm này khi thấy user hỏi kiến thức.
        """
        # Gọi sang TrainingService (Hàm search thông minh sếp đã có)
        result = self.training_service.search_knowledge(search_query)
        
        if result:
            return result
        else:
            # Trả về thông báo để AI biết mà tự chém gió hoặc xin lỗi
            return "NOT_FOUND_IN_DB: Không tìm thấy kiến thức này trong Ngân hàng câu hỏi nội bộ (N3H)."
    # =========================================================================
    # [NEW] TITAN HALL OF FAME HANDLERS
    # =========================================================================

    def _wrapper_titan_stories(self, titan_name, tag_filter=None):
        """
        Hàm xử lý kể chuyện Hall of Fame - Version 11 (Blogger Memoir Style).
        """
        try:
            target_code = None
            target_name = None
            job_title = "Nhân sự Titan"
            department = "STDD"
            personal_tags = ""
            is_stdd_entity = False
            
            raw_input = titan_name.strip()
            clean_name_upper = raw_input.upper()
            stdd_keywords = ['STDD', 'CÔNG TY', 'CONG TY', 'NGÔI NHÀ', 'NGOI NHA', 'TẬP THỂ']
            
            # --- [BƯỚC 1] XÁC ĐỊNH ĐỐI TƯỢNG ---
            if any(k in clean_name_upper for k in stdd_keywords) and len(clean_name_upper) < 20: 
                target_code = 'STDD'
                target_name = 'NGÔI NHÀ CHUNG STDD'
                is_stdd_entity = True
            else:
                # Làm sạch danh xưng để search DB chính xác
                honorifics = ['SẾP', 'SEP', 'BOSS', 'ANH', 'CHỊ', 'CHI', 'EM', 'CÔ', 'CHÚ', 'BÁC', 'MR', 'MS', 'MRS']
                search_term = raw_input
                for prefix in honorifics:
                    if clean_name_upper.startswith(prefix + " "): 
                        search_term = raw_input[len(prefix):].strip()
                        break
                
                sql_find_user = """
                    SELECT TOP 1 U.UserCode, U.shortname, U.userName,
                        ISNULL(P.JobTitle, 'Titan Member') as JobTitle,
                        ISNULL(P.Department, 'STDD') as Department,
                        P.PersonalTags 
                    FROM [GD - NGUOI DUNG] U
                    LEFT JOIN TitanOS_UserProfile P ON U.UserCode = P.UserCode
                    WHERE (U.shortname LIKE N'%{0}%') OR (U.userName LIKE N'%{0}%') OR (U.UserCode = '{0}')
                """.format(search_term)

                user_data_list = self.db.get_data(sql_find_user)
                if not user_data_list:
                    if 'STDD' in clean_name_upper:
                        target_code, target_name, is_stdd_entity = 'STDD', 'NGÔI NHÀ CHUNG STDD', True
                    else:
                        return f"⚠️ Không tìm thấy đồng nghiệp tên '{search_term}' trong hệ thống."
                else:
                    u = user_data_list[0]
                    target_code = u['UserCode']
                    # Sử dụng hàm helper để lấy tên trang trọng
                    target_name = self._get_formal_target_name(u)
                    job_title = u['JobTitle']
                    department = u['Department']
                    personal_tags = u.get('PersonalTags', '')

            # --- [BƯỚC 2] TRUY VẤN CÂU CHUYỆN ---
            sql_stories = """
                SELECT StoryID, StoryTitle, StoryContent, AuthorUserCode, Tags, ImagePaths 
                FROM HR_HALL_OF_FAME WHERE TargetUserCode = ? AND IsPublic = 1
            """
            params = [target_code]
            display_tag_text = tag_filter
            
            if tag_filter:
                normalized_tag = self._ai_translate_tag(tag_filter) # AI Translator
                sql_stories += " AND Tags LIKE ?"
                params.append(f"%{normalized_tag}%")
                vn = self.TAG_TRANSLATIONS.get(normalized_tag)
                display_tag_text = f"{vn} ({normalized_tag})" if vn else normalized_tag

            stories = self.db.get_data(sql_stories, tuple(params))

            # --- [BƯỚC 3] XỬ LÝ NỘI DUNG (AI STORYTELLING) ---
            cover_image = None
            
            # TRƯỜNG HỢP A: KHÔNG CÓ TRUYỆN (PORTRAIT TỪ HASHTAGS)
            if not stories:
                if is_stdd_entity: return "Chưa có dữ liệu về STDD."
                
                tags_display = self._format_tags_bilingual(personal_tags) if personal_tags else "Chiến binh thầm lặng"
                prompt = f"""
                Bạn là một cây bút phóng sự chân dung. Hãy phác họa về **{target_name}** ({job_title}).
                Dữ liệu: Các từ khóa đặc trưng: {tags_display}.
                NHIỆM VỤ: Viết 150-200 từ. KHÔNG dùng từ phủ định. 
                Hãy bắt đầu bằng: "Trong dòng chảy công việc tại STDD, bản sắc của {target_name} hiện lên vô cùng sắc nét qua..."
                """
                generated_text = self.model.generate_content(prompt).text
                return self._build_titan_html_card(f"HỒ SƠ: {target_name.upper()}", job_title, None, generated_text)

            # TRƯỜNG HỢP B: CÓ TRUYỆN (RETELLING)
            context_data = ""
            all_tags = []
            img_gallery = []
            
            for idx, s in enumerate(stories[:10]):
                if not s['Tags']: s['Tags'] = self._auto_generate_tags_if_missing(s['StoryID'], s['StoryContent'])
                if s['Tags']: all_tags.extend([t.strip().replace('#','') for t in s['Tags'].replace(',', ' ').split() if t.strip()])
                if s['ImagePaths']: img_gallery.extend([i.strip() for i in s['ImagePaths'].split(',') if i.strip()])
                context_data += f"\n[DỮ LIỆU GỐC #{idx+1}]: {s['StoryContent']}"

            cover_image = img_gallery[0] if img_gallery else None

            if not tag_filter:
                # MODE: TỔNG QUAN (MENU)
                from collections import Counter
                top_tags = [t[0] for t in Counter(all_tags).most_common(10)]
                tags_menu = self._format_tags_bilingual(", ".join(top_tags))
                
                prompt = f"""
                [MODE: BLOGGER PORTRAIT]
                Đối tượng: **{target_name}**. 
                NHIỆM VỤ: Viết đoạn tóm tắt chân dung 200-300 từ từ tư liệu. 
                - Ép AI chia đoạn, dùng tiêu đề phụ trong thẻ <strong>.
                - Cuối bài mời chọn: "👉 Các chủ đề nổi bật: {tags_menu}"
                CẤM: Không đếm số lượng câu chuyện.
                DỮ LIỆU: {context_data}
                """
            else:
                # MODE: CHI TIẾT (STORYTELLING)
                prompt = f"""
                🔴 [STRICT BLOGGER STORYTELLING MODE]
                Bạn là cây bút ký sự hàng đầu. Hãy kể về **{target_name}** qua chủ đề **{display_tag_text}**.
                
                YÊU CẦU BẮT BUỘC:
                1. Phân đoạn: Ít nhất 3 đoạn văn sâu sắc (300-500 từ).
                2. Tiêu đề phụ: Mỗi đoạn bắt đầu bằng tiêu đề phụ trong thẻ <strong>.
                3. Trích dẫn: Chọn 1 chi tiết đắt giá nhất để đưa vào thẻ <blockquote>.
                4. Phong cách: Hào hùng, trân trọng, giàu cảm xúc. TRUNG THỰC với tư liệu gốc.
                DỮ LIỆU: {context_data}
                """

            response = self.model.generate_content(prompt)
            # Render toàn bộ vào card
            return self._build_titan_html_card(
                title=f"HỒI KÝ TITAN: {target_name.upper()}" if not is_stdd_entity else "BIÊN NIÊN SỬ STDD",
                subtitle=job_title,
                image_url=cover_image,
                content_md=response.text
            )

        except Exception as e:
            current_app.logger.error(f"Titan Story Error: {e}")
            return f"Lỗi hệ thống: {str(e)}"

    def _auto_generate_tags_if_missing(self, story_id, content):
        """
        Hàm phụ trợ: Dùng AI tạo tag nếu bài viết chưa có, và update ngược vào DB.
        """
        try:
            # 1. Gọi AI tạo tag (Dùng model 'flash' cho nhanh)
            prompt = f"""
            Đọc câu chuyện sau về nhân sự và đưa ra tối đa 3 Hashtag (#) mô tả đúng nhất (VD: #Leadership, #Funny, #Dedication, #Technical).
            Chỉ trả về các hashtag cách nhau bằng dấu phẩy. Không giải thích gì thêm.
            
            Nội dung: "{content[:1000]}"
            """
            response = self.model.generate_content(prompt)
            tags = response.text.strip().replace('\n', '')
            
            # 2. Update vào DB để lần sau không phải tạo lại
            if tags:
                sql_update = "UPDATE HR_HALL_OF_FAME SET Tags = ? WHERE StoryID = ?"
                self.db.execute_non_query(sql_update, (tags, story_id)) # Giả sử db_manager có hàm execute_non_query
                current_app.logger.info(f"✅ Auto-tagged Story {story_id}: {tags}")
                return tags
            return ""
        except Exception as e:
            current_app.logger.warning(f"⚠️ Auto-tag failed for Story {story_id}: {e}")
            return ""

    def _format_customer_options(self, customers, term, limit=5):
        response = f"🔍 Tìm thấy **{len(customers)}** khách hàng tên '{term}'. Sếp chọn số mấy?\n"
        for i, c in enumerate(customers[:limit]):
            response += f"**{i+1}**. {c['FullName']} (Mã: {c['ID']})\n"
        return response

    def _get_customer_detail(self, cust_id):
        sql = """
            SELECT TOP 1 ObjectName, O05ID, Address, 
            (SELECT SUM(ConLai) FROM AR_AgingDetail WHERE ObjectID = T1.ObjectID) as Debt
            FROM IT1202 T1 WHERE ObjectID = ?
        """
        data = self.db.get_data(sql, (cust_id,))
        if data:
            c = data[0]
            return (f"🏢 **{c['ObjectName']}** ({cust_id})\n"
                    f"- Phân loại: {c['O05ID']}\n"
                    f"- Công nợ: {c['Debt'] or 0:,.0f} VND\n"
                    f"- Địa chỉ: {c['Address']}")
        return "Lỗi lấy dữ liệu chi tiết."

    def _handle_quick_lookup(self, item_codes, limit=5):
        try:
            data = self.lookup_service.get_quick_lookup_data(item_codes)
            if not data: return f"Không tìm thấy thông tin cho mã: '{item_codes}'."
            
            response_lines = [f"**Kết quả tra nhanh Tồn kho ('{item_codes}'):**"]
            for item in data[:limit]:
                inv_id = item['InventoryID']
                inv_name = item.get('InventoryName', 'N/A') 
                ton = item.get('Ton', 0)
                bo = item.get('BackOrder', 0)
                gbqd = item.get('GiaBanQuyDinh', 0)
                
                line = f"- **{inv_name}** ({inv_id}):\n"
                line += f"  Tồn: **{ton:,.0f}** | BO: **{bo:,.0f}** | Giá QĐ: **{gbqd:,.0f}**"
                if bo > 0: line += f"\n  -> *Gợi ý: Mã này đang BackOrder.*"
                response_lines.append(line)
            
            return "\n".join(response_lines)
        except Exception as e: return f"Lỗi tra cứu nhanh: {e}"

    def _handle_price_check_final(self, item_term, customer_object, limit=5):
        try:
            block1 = self.lookup_service._get_block1_data(item_term, customer_object['ID'])
        except Exception as e: return f"Lỗi lấy giá: {e}"
        
        if not block1: return f"Không tìm thấy mặt hàng '{item_term}' cho KH {customer_object['FullName']}."
            
        response_lines = [f"**Kết quả giá cho '{item_term}' (KH: {customer_object['FullName']}):**"]
        for item in block1[:limit]:
            gbqd = safe_float(item.get('GiaBanQuyDinh', 0))
            gia_hd = safe_float(item.get('GiaBanGanNhat_HD', 0))
            ngay_hd = item.get('NgayGanNhat_HD', '—') 
            
            line = f"- **{item.get('InventoryName', 'N/A')}** ({item.get('InventoryID')}):\n"
            line += f"  Giá Bán QĐ: **{gbqd:,.0f}**"
            
            if gia_hd > 0 and ngay_hd != '—':
                percent_diff = ((gia_hd / gbqd) - 1) * 100 if gbqd > 0 else 0
                symbol = "+" if percent_diff >= 0 else ""
                line += f"\n  Giá HĐ gần nhất: **{gia_hd:,.0f}** (Ngày: {ngay_hd}) ({symbol}{percent_diff:.1f}%)"
            else:
                line += "\n  *(Chưa có lịch sử HĐ)*"
            response_lines.append(line)
            
        return "\n".join(response_lines)

    def _handle_check_history_final(self, item_term, customer_object, limit=5):
        items_found = self.lookup_service.get_quick_lookup_data(item_term)
        if not items_found: return ""

        response_lines = [f"**Lịch sử mua hàng:**"]
        found_history = False

        for item in items_found[:limit]:
            item_id = item['InventoryID']
            last_invoice_date = self.lookup_service.check_purchase_history(customer_object['ID'], item_id)
            
            line = f"- **{item_id}**: "
            if last_invoice_date:
                found_history = True
                line += f"**Đã mua** (Gần nhất: {last_invoice_date})"
            else:
                line += "**Chưa mua**"
            response_lines.append(line)

        if not found_history: return f"**Chưa.** KH chưa mua mặt hàng nào khớp với '{item_term}'."
        return "\n".join(response_lines)

    def _handle_replenishment_check_final(self, customer_object, limit=10):
        data = self.lookup_service.get_replenishment_needs(customer_object['ID'])
        if not data: return f"KH **{customer_object['FullName']}** không có nhu cầu dự phòng."

        deficit_items = [i for i in data if safe_float(i.get('LuongThieuDu')) > 1]
        
        filter_note = ""
        filtered_items = deficit_items
        if customer_object.get('i02id_filter'):
            target = customer_object['i02id_filter'].upper()
            if target != 'AB':
                filtered_items = [i for i in deficit_items if (i.get('I02ID') == target) or (i.get('NhomHang', '').upper().startswith(f'{target}_'))]
                filter_note = f" theo mã **{target}**"

        if not filtered_items: return f"KH **{customer_object['FullName']}** đủ hàng dự phòng{filter_note}."

        response_lines = [f"KH **{customer_object['FullName']}** cần đặt **{len(filtered_items)}** nhóm hàng{filter_note}:"]
        for i, item in enumerate(filtered_items[:limit]):
            thieu = safe_float(item.get('LuongThieuDu', 0))
            rop = safe_float(item.get('DiemTaiDatROP', 0))
            ton_bo = safe_float(item.get('TonBO', 0))
            line = f"**{i+1}. {item.get('NhomHang')}**\n  - Thiếu: **{thieu:,.0f}** | ROP: {rop:,.0f} | Tồn-BO: {ton_bo:,.0f}"
            response_lines.append(line)
            
        return "\n".join(response_lines)
    

    
