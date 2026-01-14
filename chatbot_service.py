# services/chatbot_service.py

from flask import current_app, session
import google.generativeai as genai
from google.generativeai.types import FunctionDeclaration, Tool
import json
from datetime import datetime
import traceback
import config
from db_manager import safe_float
import logging # [FIX] Import logging chuẩn để dùng trong __init__

# [FIX] Cấu hình logger cho module này
logger = logging.getLogger(__name__)

class ChatbotService:
    def __init__(self, sales_lookup_service, customer_service, delivery_service, task_service, app_config, db_manager):
        self.lookup_service = sales_lookup_service
        self.customer_service = customer_service
        self.delivery_service = delivery_service
        self.task_service = task_service
        self.db = db_manager
        self.app_config = app_config
        
        # [DEPENDENCY] Khởi tạo CustomerAnalysisService
        from services.customer_analysis_service import CustomerAnalysisService
        self.analysis_service = CustomerAnalysisService(db_manager) 

        # 1. Cấu hình API
        api_key = ""
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
            'analyze_customer_deep_dive': 'skill_deepdive'
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
            FunctionDeclaration(
                name="check_delivery_status",
                description="Kiểm tra tình trạng giao hàng, các phiếu xuất kho (LXH).",
                parameters={
                    "type": "object",
                    "properties": {
                        "customer_name": {"type": "string", "description": "Tên khách hàng"},
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
        valid_models = ['gemini-2.5-flash', 'gemini-2.0-flash-exp', 'gemini-1.5-flash']
        self.model = None
        for m in valid_models:
            try:
                # Test connection
                genai.GenerativeModel(m).generate_content("Hi")
                self.model = genai.GenerativeModel(m, tools=[self.tools_definitions])
                # [FIX] Dùng logger chuẩn
                logger.info(f"✅ Chatbot Init Success with Model: {m}")
                break
            except Exception as e: 
                # [FIX] Dùng logger chuẩn
                logger.warning(f"⚠️ Model {m} failed: {e}")
                continue
        
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
            'get_titan_stories': self._wrapper_titan_stories
        }
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
                'light': "Bạn là Trợ lý Kinh doanh Titan (Business Style). Trả lời gãy gọn, súc tích, tập trung vào số liệu.",
                'dark': "Bạn là Hệ thống Titan OS (Formal). Phong cách trang trọng, chính xác, khách quan.",
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
                final_res = chat.send_message({
                    "function_response": {
                        "name": func_name,
                        "response": {"result": api_result}
                    }
                })
                final_text = final_res.text
                
            else:
                try:
                    final_text = response.text
                except Exception as e:
                    final_text = "Em đã nhận được thông tin nhưng gặp lỗi hiển thị phản hồi. Sếp thử lại nhé! 🦊"
                    current_app.logger.error(f"⚠️ Text Response Error: {e}")

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

    def _wrapper_delivery_status(self, customer_name, selection_index=None):
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"❌ Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)
        
        customer_obj = cust_result[0]
        customer_id = customer_obj['ID']
        customer_full_name = customer_obj['FullName']
        
        try:
            recent_deliveries = self.delivery_service.get_recent_delivery_status(customer_id, days_ago=7)
            
            if not recent_deliveries:
                return f"ℹ️ Khách hàng **{customer_full_name}** không có Lệnh Xuất Hàng nào trong **7 ngày qua**."

            res = f"### 🚚 Tình trạng giao hàng (7 ngày) - {customer_full_name}\n"
            res += f"*Tổng cộng: {len(recent_deliveries)} đơn hàng*\n\n"
            
            for item in recent_deliveries:
                status = str(item.get('DeliveryStatus', 'CHỜ')).strip().upper()
                icon = "🟢" if status == 'DA GIAO' else "🟠"
                date_str = item.get('VoucherDate', 'N/A')
                v_no = item.get('VoucherNo', 'N/A')
                
                res += f"**{icon} {v_no}** `({date_str})`\n"
                res += f"- **SL mặt hàng:** {item.get('ItemCount', 0)}\n"
                
                if status == 'DA GIAO':
                    res += f"- **Thực tế:** Đã giao ngày {item.get('ActualDeliveryDate', 'N/A')}\n"
                else:
                    plan = item.get('Planned_Day', 'POOL')
                    plan_txt = "Chưa xếp lịch" if plan == 'POOL' else plan
                    res += f"- **Kế hoạch:** {plan_txt}\n"
                res += "\n"
                
            return res

        except Exception as e:
            traceback.print_exc() 
            return f"Lỗi tra cứu giao hàng: {str(e)}"

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
    
    # =========================================================================
    # [NEW] TITAN HALL OF FAME HANDLERS
    # =========================================================================

    def _wrapper_titan_stories(self, titan_name, tag_filter=None):
        """
        Hàm xử lý kể chuyện Hall of Fame - Version 5 (Unified Flow for Humans & STDD).
        Logic: STDD đi chung luồng với Human: Tổng quan -> Chọn Tag -> Chi tiết.
        """
        try:
            target_code = None
            target_name = None
            is_stdd_entity = False
            
            # Chuẩn hóa input để bắt mọi cách gọi tên công ty
            clean_name = titan_name.strip().upper()
            stdd_keywords = ['STDD', 'CÔNG TY', 'CONG TY', 'NGÔI NHÀ', 'NGOI NHA', 'TẬP THỂ']
            
            # --- [LOGIC 1] CHECK STDD (ĐỊNH DANH) ---
            if any(k in clean_name for k in stdd_keywords) and len(clean_name) < 20: 
                target_code = 'STDD'
                target_name = 'Ngôi nhà chung STDD'
                is_stdd_entity = True
            else:
                # --- LOGIC TÌM KIẾM NHÂN SỰ ---
                sql_find_user = """
                    SELECT TOP 1 UserCode, shortname, userName 
                    FROM [GD - NGUOI DUNG]
                    WHERE (shortname LIKE N'%{0}%') OR (userName LIKE N'%{0}%') OR (UserCode = '{0}')
                """.format(titan_name)

                
                user_data = self.db.get_data(sql_find_user)
                if not user_data:
                    # Fallback: Check lại STDD lần cuối
                    if 'STDD' in clean_name:
                        target_code = 'STDD'
                        target_name = 'Ngôi nhà chung STDD'
                        is_stdd_entity = True
                    else:
                        return f"⚠️ Không tìm thấy đồng nghiệp nào tên là '{titan_name}' trong hệ thống Titan."
                else:
                    target_user = user_data[0]
                    target_code = target_user['UserCode']
                    target_name = target_user.get('Nickname') or target_user.get('FullName')

            # --- [LOGIC 2] LẤY STORIES ---
            sql_stories = """
                SELECT StoryID, StoryTitle, StoryContent, AuthorUserCode, Tags 
                FROM HR_HALL_OF_FAME 
                WHERE TargetUserCode = ? AND IsPublic = 1
            """
            params = [target_code]

            if tag_filter:
                sql_stories += " AND Tags LIKE ?"
                params.append(f"%{tag_filter}%")
            
            stories = self.db.get_data(sql_stories, tuple(params))

            if not stories:
                if is_stdd_entity:
                     return "Hệ thống đã nhận diện yêu cầu về STDD nhưng chưa tìm thấy dữ liệu câu chuyện trong bảng Hall of Fame (TargetUserCode='STDD')."
                return f"Hiện tại chưa có giai thoại nào về **{target_name}**."

            # --- [LOGIC 3] XỬ LÝ DỮ LIỆU ĐẦU VÀO ---
            context_data = ""
            # Nếu là Tổng quan (chưa có tag), lấy nhiều chuyện để AI tóm tắt tốt hơn
            limit = 3 if tag_filter else 15 
            
            # Biến phụ trợ để gom Tags cho menu
            all_tags_list = []

            for idx, s in enumerate(stories[:limit]):
                # Auto-tag logic
                if not s['Tags']:
                    s['Tags'] = self._auto_generate_tags_if_missing(s['StoryID'], s['StoryContent'])
                
                if s['Tags']:
                    # Gom tag để hiển thị menu
                    t_list = [t.strip() for t in s['Tags'].split(',') if t.strip()]
                    all_tags_list.extend(t_list)

                context_data += f"""
---
[TƯ LIỆU #{idx+1}]
- Tiêu đề: {s['StoryTitle']}
- Tags: {s['Tags']}
- Nội dung: "{s['StoryContent']}"
"""

            # --- [LOGIC 4] PHÂN LUỒNG TRẢ LỜI (CHUNG CHO CẢ STDD VÀ NGƯỜI) ---

            # === MODE A: PORTRAIT (TỔNG QUAN & MENU) ===
            if not tag_filter:
                # Xử lý hiển thị Tags (Menu chọn)
                from collections import Counter
                # Đếm tần suất tag để hiển thị tag phổ biến nhất lên đầu
                tag_counts = Counter(all_tags_list).most_common(10) 
                tags_display = ", ".join([t[0] for t in tag_counts]) if tag_counts else "(Chưa phân loại)"
                
                # Tùy chỉnh Prompt cho STDD để nó "Nhân cách hóa"
                if is_stdd_entity:
                    role_instruction = """
                    🔴 [LƯU Ý QUAN TRỌNG: NHÂN CÁCH HÓA]
                    Đối tượng là **CÔNG TY/TẬP THỂ STDD**. Hãy viết về nó như viết tiểu sử của một **Vĩ Nhân** hoặc một **Người Mẹ Lớn**.
                    Tập trung vào: Văn hóa, Lịch sử, Tinh thần đoàn kết.
                    KHÔNG liệt kê khô khan. Hãy viết đầy cảm xúc và tự hào.
                    """
                else:
                    role_instruction = f"Đối tượng là đồng nghiệp: **{target_name}**. Hãy viết tiểu sử tóm tắt về tính cách và đóng góp của họ."

                return f"""
                [MODE: PORTRAIT SUMMARY]
                {role_instruction}
                
                NHIỆM VỤ: 
                1. **Viết Tổng Quan (Portrait):** Tóm tắt chân dung của đối tượng dựa trên các tư liệu dưới đây. Độ dài: **200 - 300 từ**.
                2. **Tạo Menu:** Cuối bài, BẮT BUỘC phải mời người dùng chọn một trong các chủ đề (Tags) sau để nghe kể chi tiết: 
                   👉 {tags_display}
                
                DỮ LIỆU TƯ LIỆU:
                {context_data}
                """

            # === MODE B: NARRATIVE (CHI TIẾT THEO TAG) ===
            else:
                # Tùy chỉnh Prompt cho STDD
                if is_stdd_entity:
                    style_instruction = """
                    🔴 [STDD STORYTELLING MODE]
                    Hãy kể câu chuyện về Ngôi nhà chung STDD xoay quanh chủ đề này.
                    Hãy coi STDD là một thực thể sống động, có ký ức và tình cảm.
                    """
                else:
                    style_instruction = f"Hãy kể câu chuyện về **{target_name}** xoay quanh chủ đề này."

                return f"""
                🔴 [SYSTEM ALERT: OVERRIDE PERSONA SETTINGS]
                Mục đích: **STORYTELLING (KỂ CHUYỆN VĂN HỌC)**.
                
                {style_instruction}
                Chủ đề được chọn: **{tag_filter}**.
                
                YÊU CẦU BẮT BUỘC:
                1. **BỎ QUA** sự ngắn gọn. Hãy viết dài và sâu sắc.
                2. **ĐỘ DÀI**: Tối thiểu 300 từ, tối đa 600 từ.
                3. **PHONG CÁCH**: Hào hùng, truyền cảm hứng, xúc động.
                
                DỮ LIỆU CỐT TRUYỆN:
                {context_data}
                
                Hãy bắt đầu kể ngay bây giờ:
                """

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
    
    