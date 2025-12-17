# services/chatbot_service.py

from flask import current_app
import google.generativeai as genai
from google.generativeai.types import FunctionDeclaration, Tool
from flask import session
import json
from datetime import datetime
from db_manager import safe_float
import traceback 
import config 

class ChatbotService:
    def __init__(self, sales_lookup_service, customer_service, delivery_service, task_service, app_config, db_manager):
        self.lookup_service = sales_lookup_service
        self.customer_service = customer_service
        self.delivery_service = delivery_service
        self.task_service = task_service
        self.db = db_manager
        
        # 1. Cấu hình API
        # [QUAN TRỌNG: SỬ DỤNG BIẾN MÔI TRƯỜNG TỪ CONFIG.PY]
        api_key = "AIzaSyAWQcf-gTqydDhhER-X4I2O-Et-mBxAiJA"
        genai.configure(api_key=api_key) 

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
                description="Kiểm tra tình trạng giao hàng, các phiếu xuất kho.",
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
                description="Kiểm tra nhu cầu đặt hàng dự phòng (Safety Stock/ROP).",
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
                description="Xem tổng quan về khách hàng (Doanh số, Công nợ).",
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
            # 6. Đọc báo cáo
            FunctionDeclaration(
                name="summarize_customer_report",
                description="Đọc và tóm tắt báo cáo khách hàng.",
                parameters={
                    "type": "object",
                    "properties": {
                        "customer_name": {"type": "string", "description": "Tên khách hàng"},
                        "months": {"type": "integer", "description": "Số tháng (mặc định 6)"},
                        "selection_index": {"type": "integer", "description": "Số thứ tự user chọn"}
                    },
                    "required": ["customer_name"]
                }
            )
        ]
            
        # 3. Khởi tạo Model
        valid_models = ['gemini-2.5-flash', 'gemini-2.0-flash-exp', 'gemini-flash-latest']
        self.model = None
        for m in valid_models:
            try:
                genai.GenerativeModel(m).generate_content("Hi")
                self.model = genai.GenerativeModel(m, tools=[self.tools_definitions])
                current_app.logger.info(f"✅ Chatbot Model: {m}")
                break
            except: continue
        
        if not self.model:
            self.model = genai.GenerativeModel('gemini-1.5-flash', tools=[self.tools_definitions])

        # 4. Map Functions
        self.functions_map = {
            'check_product_info': self._wrapper_product_info,
            'check_delivery_status': self._wrapper_delivery_status,
            'check_replenishment': self._wrapper_replenishment,
            'check_customer_overview': self._wrapper_customer_overview,
            'check_daily_briefing': self._wrapper_daily_briefing,
            'summarize_customer_report': self._wrapper_summarize_report
        }

    # --- MAIN PROCESS ---
    # --- MAIN PROCESS WITH DYNAMIC PERSONA ---
    def process_message(self, message_text, user_code, user_role, theme='light'):
        try:
            # 1. Định nghĩa Persona
            personas = {
                'light': "Bạn là Trợ lý Kinh doanh chuyên nghiệp (Business Style). Trả lời ngắn gọn, tập trung vào số liệu.",
                'dark': "Bạn là Hệ thống Titan OS (Formal). Phong cách trang trọng, lạnh lùng, chính xác.",
                'fantasy': "Bạn là AI từ tương lai (Cyberpunk). Xưng hô Commander - System. Giọng hào hứng.",
                'adorable': "Bạn là Bé Cáo AI (Gen Z). Xưng hô Em - Sếp. Dùng emoji 🦊💖✨. Giọng cute, năng động."
            }
            system_instruction = personas.get(theme, personas['light'])
            
            # 2. Context History
            history = session.get('chat_history', [])
            gemini_history = []
            for h in history:
                gemini_history.append({"role": "user", "parts": [h['user']]})
                gemini_history.append({"role": "model", "parts": [h['bot']]})

            # 3. Tạo Chat Session
            chat = self.model.start_chat(history=gemini_history, enable_automatic_function_calling=False)
            
            self.current_user_code = user_code
            self.current_user_role = user_role

            full_prompt = f"[System Instruction: {system_instruction}]\nUser says: {message_text}"
            
            # Gửi tin nhắn đi
            response = chat.send_message(full_prompt)
            
            final_text = ""
            
            # [FIX QUAN TRỌNG] KIỂM TRA FUNCTION CALL AN TOÀN TUYỆT ĐỐI
            # Thay vì gọi response.text ngay (gây lỗi), ta kiểm tra từng phần (part)
            
            function_call_part = None
            if response.candidates and response.candidates[0].content.parts:
                for part in response.candidates[0].content.parts:
                    if part.function_call:
                        function_call_part = part.function_call
                        break
            
            if function_call_part:
                # === XỬ LÝ NẾU AI MUỐN GỌI HÀM ===
                fc = function_call_part
                func_name = fc.name
                func_args = dict(fc.args)
                
                current_app.logger.info(f"🤖 AI Calling: {func_name} | Args: {func_args}")
                
                if func_name in self.functions_map:
                    try:
                        api_result = self.functions_map[func_name](**func_args)
                    except Exception as e:
                        api_result = f"Lỗi thực thi hàm: {str(e)}"
                        current_app.logger.error(f"❌ Function Error: {e}")
                else:
                    api_result = "Hàm không tồn tại."

                # Gửi kết quả hàm lại cho AI để nó tổng hợp thành văn bản
                final_res = chat.send_message({
                    "function_response": {
                        "name": func_name,
                        "response": {"result": api_result}
                    }
                })
                final_text = final_res.text
                
            else:
                # === TRƯỜNG HỢP TRẢ LỜI BÌNH THƯỜNG ===
                # Lúc này chắc chắn là text, gọi .text sẽ an toàn
                try:
                    final_text = response.text
                except Exception as e:
                    # Fallback nếu vẫn lỗi (hiếm gặp)
                    final_text = "Em đã nhận được thông tin nhưng gặp chút lỗi hiển thị. Sếp hỏi lại giúp em nhé! 🦊"
                    current_app.logger.error(f"⚠️ Lỗi đọc text: {e}")

            # 5. Lưu lịch sử
            history.append({'user': message_text, 'bot': final_text})
            if len(history) > 10: history = history[-10:]
            session['chat_history'] = history
            
            return final_text

        except Exception as e:
            import traceback
            traceback.print_exc()
            return f"Hệ thống đang bận, vui lòng thử lại sau. (Lỗi: {str(e)})"

    # =========================================================================
    # CÁC HÀM WRAPPER (Cầu nối giữa AI và Logic Gốc)
    # =========================================================================

    def _resolve_customer(self, customer_name, selection_index):
        """Hàm tìm khách hàng, hỗ trợ chọn số thứ tự từ ngữ cảnh"""
        # 1. Ưu tiên chọn từ Session nếu có Index (Context "Số 5")
        context_list = session.get('customer_search_results')
        if selection_index is not None and context_list:
            try:
                idx = int(selection_index) - 1
                if 0 <= idx < len(context_list):
                    selected = context_list[idx]
                    session.pop('customer_search_results', None)
                    return [selected] 
            except: pass

        # 2. Nếu không có index, tìm theo tên
        if not customer_name: return None
        
        customers = self.customer_service.get_customer_by_name(customer_name)
        if not customers: return "NOT_FOUND"
        
        # 3. Tìm thấy nhiều -> Lưu Session
        if len(customers) > 1:
            session['customer_search_results'] = customers 
            return "MULTIPLE"
            
        # 4. Tìm thấy 1
        return customers

    # --- WRAPPER 1: TRA CỨU SẢN PHẨM ---
    def _wrapper_product_info(self, product_keywords, customer_name=None, selection_index=None):
        # A. Nếu KHÔNG có tên khách -> Tra cứu nhanh (Gọi hàm logic gốc)
        if not customer_name and not selection_index:
            return self._handle_quick_lookup(product_keywords)

        # B. Nếu CÓ tên khách -> Giải quyết khách hàng
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND":
            return f"Không tìm thấy khách hàng '{customer_name}'. Đang tra nhanh mã '{product_keywords}'...\n" + \
                   self._handle_quick_lookup(product_keywords)
                   
        if cust_result == "MULTIPLE":
            return self._format_customer_options(session['customer_search_results'], customer_name)
        
        # C. Có khách hàng -> Gọi logic GIÁ và LỊCH SỬ từ file gốc
        customer_obj = cust_result[0]
        
        # Gọi logic lấy dữ liệu
        price_info_str = self._handle_price_check_final(product_keywords, customer_obj)
        history_info_str = self._handle_check_history_final(product_keywords, customer_obj)
        
        # FORMAT MARKDOWN ĐẸP
        return f"""
### 📦 Kết quả tra cứu: {customer_obj['FullName']}
---
{price_info_str}

{history_info_str}
"""

    # --- WRAPPER 2: GIAO HÀNG ---
    def _wrapper_delivery_status(self, customer_name, selection_index=None):
        current_app.logger.info(f"\n>>> DEBUG CHATBOT: Tìm khách '{customer_name}'")
        
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"❌ Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)
        
        # Lấy đối tượng khách hàng
        customer_obj = cust_result[0]
        customer_id = customer_obj['ID']
        customer_full_name = customer_obj['FullName']
        
        current_app.logger.info(f">>> DEBUG CHATBOT: Đã chọn khách {customer_full_name} ({customer_id})")

        try:
            # Gọi service (Tăng lên 7 ngày để chắc chắn bắt được dữ liệu cũ)
            recent_deliveries = self.delivery_service.get_recent_delivery_status(customer_id, days_ago=7)
            
            if not recent_deliveries:
                # [FIX LỖI CŨ]: Đảm bảo dùng đúng tên biến customer_full_name đã khai báo ở trên
                return f"ℹ️ Khách hàng **{customer_full_name}** không có Lệnh Xuất Hàng nào trong **7 ngày qua**."

            # Format kết quả
            res = f"### 🚚 Tình trạng giao hàng (7 ngày) - {customer_full_name}\n"
            res += f"*Tổng cộng: {len(recent_deliveries)} đơn hàng*\n\n"
            
            for item in recent_deliveries:
                status = str(item.get('DeliveryStatus', 'CHỜ')).strip().upper()
                icon = "🟢" if status == 'DA GIAO' else "🟠"
                date_str = item.get('VoucherDate', 'N/A')
                v_no = item.get('VoucherNo', 'N/A')
                
                # Format dòng
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
            # [QUAN TRỌNG]: In lỗi chi tiết ra CMD để bạn nhìn thấy
            import traceback
            traceback.print_exc() 
            current_app.logger.error(f"❌ LỖI NGHIÊM TRỌNG TRONG WRAPPER DELIVERY: {e}")
            return f"Lỗi hệ thống chi tiết: {str(e)}"

    # --- WRAPPER 3: DỰ PHÒNG ---
    def _wrapper_replenishment(self, customer_name, i02id_filter=None, selection_index=None):
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)
        
        customer_obj = cust_result[0]
        if i02id_filter: customer_obj['i02id_filter'] = i02id_filter
        
        # [QUAN TRỌNG] Gọi hàm logic sử dụng LookupService (SP_CROSS_SELL_GAP)
        return self._handle_replenishment_check_final(customer_obj)

    # --- WRAPPER 4: TỔNG QUAN KHÁCH HÀNG ---
    def _wrapper_customer_overview(self, customer_name, selection_index=None):
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"❌ Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)
        
        return self._get_customer_detail(cust_result[0]['ID'])

    # --- WRAPPER 5: DAILY BRIEFING (Giữ nguyên logic SQL Task đơn giản vì chưa có Service) ---
    def _wrapper_daily_briefing(self, scope='today'):
        user_code = self.current_user_code
        res = f"📅 **Tổng quan công việc hôm nay:**\n"
        
        # 1. Tasks
        sql_task = "SELECT Subject, Priority FROM Task_Master WHERE AssignedTo = ? AND Status != 'Done' AND DueDate <= GETDATE()"
        tasks = self.db.get_data(sql_task, (user_code,))
        if tasks:
            res += "\n📌 **Việc cần làm:**\n" + "\n".join([f"- {t['Subject']} ({t['Priority']})" for t in tasks])
        else:
            res += "\n📌 **Việc cần làm:** Không có task quá hạn."

        # 2. Approval (Đếm số lượng báo giá chờ duyệt)
        sql_approval = "SELECT COUNT(*) as Cnt FROM OT2101 WHERE OrderStatus = 0" 
        approval = self.db.get_data(sql_approval)
        if approval and approval[0]['Cnt'] > 0:
            res += f"\n\n💰 **Phê duyệt:** {approval[0]['Cnt']} Báo giá chờ duyệt."

        return res

    # --- WRAPPER 6: TÓM TẮT BÁO CÁO (RAG) ---
    def _wrapper_summarize_report(self, customer_name, months=6, selection_index=None):
        import traceback
        
        # Ép kiểu tháng
        try: months = int(float(months)) if months else 6
        except: months = 6
            
        current_app.logger.info(f"\n>>> DEBUG REPORT: Đang tìm báo cáo cho '{customer_name}' trong {months} tháng...")

        # 1. Tìm ID và Tên chuẩn của khách hàng
        cust_result = self._resolve_customer(customer_name, selection_index)
        
        if cust_result == "NOT_FOUND": return f"❌ Không tìm thấy khách hàng '{customer_name}'."
        if cust_result == "MULTIPLE": return self._format_customer_options(session['customer_search_results'], customer_name)

        customer_obj = cust_result[0]
        customer_id = customer_obj['ID']
        customer_full_name = customer_obj['FullName']
        
        # Tạo từ khóa tìm kiếm (Lấy tên rút gọn hoặc tên đầy đủ để quét nội dung)
        # Ví dụ: Nếu tên là "CÔNG TY TNHH SUNSCO", ta nên tìm "SUNSCO"
        # Logic đơn giản: Lấy phần tên chính (Đây là logic giả định, bạn có thể tùy chỉnh)
        search_keyword = customer_full_name.split(' ')[0] if len(customer_full_name.split(' ')) > 1 else customer_full_name
        # Tuy nhiên, để an toàn, ta tìm chính xác tên user nhập vào hoặc tên trong DB
        search_keyword = customer_name if len(customer_name) > 3 else customer_full_name 

        current_app.logger.info(f">>> DEBUG REPORT: ID={customer_id} | Keyword quét nội dung='%{search_keyword}%'")

        # 2. Query SQL Nâng cấp (Tìm ObjectID HOẶC Nội dung chứa tên khách)
        # Sử dụng OR để lấy cả báo cáo trực tiếp lẫn báo cáo tuần có nhắc tên
        sql = f"""
            SELECT TOP 30 
                [Ngay] as CreatedDate, 
                [Nguoi] as CreateUser,
                CAST([Noi dung 1] AS NVARCHAR(MAX)) as Content1, 
                CAST([Noi dung 2] AS NVARCHAR(MAX)) as Content2_Added,
                CAST([Danh gia 2] AS NVARCHAR(MAX)) as Content3,
                [Khach hang] as TaggedCustomerID -- Lấy thêm cột này để AI biết là báo cáo trực tiếp hay gián tiếp
            FROM {config.TEN_BANG_BAO_CAO}
            WHERE 
                ([Ngay] >= DATEADD(month, -?, GETDATE()))
                AND (
                    [Khach hang] = ?  -- Điều kiện 1: Đúng ID khách hàng
                    OR 
                    (CAST([Noi dung 1] AS NVARCHAR(MAX)) LIKE N'%{search_keyword}%') -- Điều kiện 2: Nội dung nhắc đến tên
                    OR 
                    (CAST([Noi dung 2] AS NVARCHAR(MAX)) LIKE N'%{search_keyword}%')
                )
            ORDER BY [Ngay] DESC
        """ 

        try:
            reports = self.db.get_data(sql, (months, customer_id))
        except Exception as e:
            current_app.logger.error("❌❌❌ LỖI SQL REPORT:")
            traceback.print_exc()
            return f"Lỗi hệ thống khi truy xuất dữ liệu mở rộng: {str(e)}"
            
        if not reports:
            return f"ℹ️ Không tìm thấy báo cáo nào liên quan đến **{customer_full_name}** (kể cả trong báo cáo tuần) trong {months} tháng qua."

        # 3. Tạo Context Text thông minh
        context_text_raw = ""
        related_count = 0
        direct_count = 0
        
        for r in reports:
            date_val = r.get('CreatedDate')
            date_str = date_val.strftime('%d/%m/%Y') if date_val else 'N/A'
            
            # Ghép nội dung
            c1 = str(r.get('Content1', '')).strip()
            c2 = str(r.get('Content2_Added', '')).strip()
            c3 = str(r.get('Content3', '')).strip()
            content = ". ".join([p for p in [c1, c2, c3] if p])
            
            if not content or content == '.': continue 
            
            # Phân loại nguồn báo cáo để AI hiểu
            tagged_id = str(r.get('TaggedCustomerID', '')).strip()
            if tagged_id == str(customer_id):
                source_type = "BÁO CÁO TRỰC TIẾP"
                direct_count += 1
            else:
                source_type = "BÁO CÁO CHUNG/TUẦN (Có nhắc đến)"
                related_count += 1
                
            context_text_raw += f"- [{date_str}] [{source_type}] {r['CreateUser']}: {content}\n"
        
        # 4. Prompt "Thông minh" (Smart Filtering)
        system_prompt = (
            f"Bạn là trợ lý Kinh doanh AI. Nhiệm vụ: Tóm tắt tình hình khách hàng {customer_full_name}.\n"
            "Dữ liệu được cung cấp bao gồm:\n"
            "1. Báo cáo trực tiếp: Dành riêng cho khách này.\n"
            "2. Báo cáo chung (Báo cáo tuần): Có thể chứa thông tin của NHIỀU khách hàng khác nhau (Sunsco, C2, CSVC...).\n"
            "----------------\n"
            "YÊU CẦU QUAN TRỌNG:\n"
            f"- Đối với 'Báo cáo chung', bạn phải LỌC CHÍNH XÁC thông tin liên quan đến '{search_keyword}' hoặc '{customer_full_name}'.\n"
            "- BỎ QUA hoàn toàn thông tin của các khách hàng khác (như C2, CSVC...) nằm trong cùng dòng báo cáo.\n"
            "- Tổng hợp lại thành: Tổng quan, Điểm Tốt, và Điểm Cần Cải Thiện.\n"
            "- Trình bày Markdown rõ ràng."
        )
        
        # Thống kê
        summary_header = f"""
### 📊 DỮ LIỆU TÌM THẤY
- **Báo cáo trực tiếp:** {direct_count}
- **Báo cáo chung (được nhắc tên):** {related_count}
---
"""
        full_input = summary_header + context_text_raw

        try:
            summary_model = genai.GenerativeModel(
                model_name=self.model.model_name,
                system_instruction=system_prompt 
            )
            response = summary_model.generate_content(contents=[full_input])
            return response.text
        except Exception as e:
            return f"Lỗi AI xử lý: {str(e)}"

    # =========================================================================
    # LOGIC CỐT LÕI (SỬ DỤNG SERVICE - KHÔNG VIẾT SQL TRỰC TIẾP)
    # ... (Các hàm helper khác giữ nguyên)
    # =========================================================================
    # 1. TRA CỨU NHANH
    def _handle_quick_lookup(self, item_codes, limit=5):
        try:
            # Gọi Service SalesLookupService -> get_quick_lookup_data
            data = self.lookup_service.get_quick_lookup_data(item_codes)
            
            if not data:
                return f"Không tìm thấy thông tin cho mã: '{item_codes}'."
            
            response_lines = [f"**Kết quả tra nhanh Tồn kho ('{item_codes}'):**"]
            
            for item in data[:limit]:
                inv_id = item['InventoryID']
                inv_name = item.get('InventoryName', 'N/A') 
                ton = item.get('Ton', 0)
                bo = item.get('BackOrder', 0)
                gbqd = item.get('GiaBanQuyDinh', 0)
                
                line = f"- **{inv_name}** ({inv_id}):\n"
                line += f"  Tồn: **{ton:,.0f}** | BO: **{bo:,.0f}** | Giá QĐ: **{gbqd:,.0f}**"
                if bo > 0:
                    line += f"\n  -> *Gợi ý: Mã này đang BackOrder.*"
                response_lines.append(line)
            
            return "\n".join(response_lines)
            
        except Exception as e:
            return f"Lỗi tra cứu nhanh: {e}"

    # 2. KIỂM TRA GIÁ & BLOCK 1
    def _handle_price_check_final(self, item_term, customer_object, limit=5):
        customer_id = customer_object['ID']
        customer_display_name = customer_object['FullName']
        
        try:
            # Gọi Service SalesLookupService -> _get_block1_data (Đã dùng SP_GET_SALES_LOOKUP)
            block1 = self.lookup_service._get_block1_data(item_term, customer_id)
        except Exception as e:
            return f"Lỗi khi gọi SP Block1: {e}"
        
        if not block1:
            return f"Không tìm thấy mặt hàng '{item_term}' cho KH {customer_display_name}."
            
        response_lines = [f"**Kết quả giá cho '{item_term}' (KH: {customer_display_name}):**"]
        
        for item in block1[:limit]:
            gbqd = item.get('GiaBanQuyDinh', 0)
            gia_hd = item.get('GiaBanGanNhat_HD', 0)
            ngay_hd = item.get('NgayGanNhat_HD', '—') 
            
            line = f"- **{item.get('InventoryName', 'N/A')}** ({item.get('InventoryID')}):\n"
            line += f"  Giá Bán QĐ: **{gbqd:,.0f}**"
            
            if gia_hd > 0 and ngay_hd != '—':
                percent_diff = ((gia_hd / gbqd) - 1) * 100 if gbqd > 0 else 0
                symbol = "+" if percent_diff >= 0 else ""
                line += f"\n  Giá HĐ gần nhất: **{gia_hd:,.0f}** (Ngày: {ngay_hd}) ({symbol}{percent_diff:.1f}%)"
            else:
                line += "\n  *(Chưa có lịch sử HĐ cho KH này)*"
            
            response_lines.append(line)
            
        return "\n".join(response_lines)

    # 3. LỊCH SỬ MUA HÀNG
    def _handle_check_history_final(self, item_term, customer_object, limit=5):
        customer_id = customer_object['ID']
        
        # Dùng lại quick_lookup để tìm danh sách mã hàng trước
        items_found = self.lookup_service.get_quick_lookup_data(item_term)
        if not items_found:
            return ""

        response_lines = [f"**Lịch sử mua hàng:**"]
        found_history = False

        for item in items_found[:limit]:
            item_id = item['InventoryID']
            item_name = item['InventoryName']
            
            # Gọi Service SalesLookupService -> check_purchase_history
            last_invoice_date = self.lookup_service.check_purchase_history(customer_id, item_id)
            
            line = f"- **{item_id}**: "
            if last_invoice_date:
                found_history = True
                line += f"**Đã mua** (Gần nhất: {last_invoice_date})"
            else:
                line += "**Chưa mua**"
            response_lines.append(line)

        if not found_history:
             return f"**Chưa.** KH chưa mua mặt hàng nào khớp với '{item_term}'."
            
        return "\n".join(response_lines)

    # 4. GIAO HÀNG
    def _handle_check_delivery_final(self, customer_object):
        customer_id = customer_object['ID']
        customer_display_name = customer_object['FullName']
        
        # Gọi Service DeliveryService -> get_recent_delivery_status
        # Hàm này đã query vào VIEW_DELIVERY chuẩn
        recent_deliveries = self.delivery_service.get_recent_delivery_status(customer_id, days_ago=7)

        if not recent_deliveries:
            return f"Khách hàng **{customer_display_name}** không có Lệnh Xuất Hàng nào trong 7 ngày qua."

        # [FIX]: Format Markdown bảng/list đẹp
        res = f"### 🚚 Tình trạng giao hàng (7 ngày) - {customer_obj['FullName']}\n"
        res += f"*Tổng cộng: {len(recent_deliveries)} đơn hàng*\n\n"
        
        for item in recent_deliveries:
            status = item.get('DeliveryStatus', 'CHỜ').strip().upper()
            icon = "🟢" if status == 'DA GIAO' else "🟠"
            date_str = item.get('VoucherDate', 'N/A')
            
            # Dòng tiêu đề đậm
            res += f"**{icon} LXH {item['VoucherNo']}** `({date_str})`\n"
            
            # Chi tiết thụt dòng
            res += f"- **SL mặt hàng:** {item.get('ItemCount', 0)}\n"
            if status == 'DA GIAO':
                res += f"- **Thực tế:** Đã giao ngày {item.get('ActualDeliveryDate', 'N/A')}\n"
            else:
                plan = item.get('Planned_Day', 'POOL')
                plan_txt = "Chưa xếp lịch" if plan == 'POOL' else plan
                res += f"- **Kế hoạch:** {plan_txt}\n"
            
            res += "\n" # Xuống dòng giữa các item
            
        return res

    # 5. DỰ PHÒNG (REPLENISHMENT)
    def _handle_replenishment_check_final(self, customer_object, limit=10):
        customer_id = customer_object['ID']
        customer_display_name = customer_object['FullName']
        i02id_filter = customer_object.get('i02id_filter')
        
        # Gọi Service SalesLookupService -> get_replenishment_needs (Dùng SP_CROSS_SELL_GAP)
        data = self.lookup_service.get_replenishment_needs(customer_id)
        if not data: return f"KH **{customer_display_name}** không có nhu cầu dự phòng."

        deficit_items = [i for i in data if safe_float(i.get('LuongThieuDu')) > 1]
        
        filter_note = ""
        filtered_items = deficit_items
        if i02id_filter:
            target = i02id_filter.upper()
            if target != 'AB':
                filtered_items = [
                    i for i in deficit_items 
                    if (i.get('I02ID') == target) or (i.get('NhomHang', '').upper().startswith(f'{target}_'))
                ]
                filter_note = f" theo mã **{target}**"

        if not filtered_items: return f"KH **{customer_display_name}** đủ hàng dự phòng{filter_note}."

        response_lines = [f"KH **{customer_display_name}** cần đặt **{len(filtered_items)}** nhóm hàng{filter_note}:"]
        
        for i, item in enumerate(filtered_items[:limit]):
            thieu = safe_float(item.get('LuongThieuDu', 0))
            rop = safe_float(item.get('DiemTaiDatROP', 0))
            ton_bo = safe_float(item.get('TonBO', 0))
            line = f"**{i+1}. {item.get('NhomHang')}**\n  - Thiếu: **{thieu:,.0f}** | ROP: {rop:,.0f} | Tồn-BO: {ton_bo:,.0f}"
            response_lines.append(line)
            
        return "\n".join(response_lines)

    # --- HELPERS ---
    def _format_customer_options(self, customers, term, limit=5):
        response = f"🔍 Tìm thấy **{len(customers)}** khách hàng tên '{term}'. Sếp chọn số mấy?\n"
        for i, c in enumerate(customers[:limit]):
            response += f"**{i+1}**. {c['FullName']} (Mã: {c['ID']})\n"
        return response

    def _get_customer_detail(self, cust_id):
        # Hàm này vẫn dùng SQL trực tiếp vì nó đơn giản và dùng bảng chuẩn IT1202,
        # nhưng nếu muốn an toàn tuyệt đối, bạn nên move nó sang CustomerService.
        # Tạm thời giữ nguyên vì IT1202 là bảng chuẩn ERP.
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