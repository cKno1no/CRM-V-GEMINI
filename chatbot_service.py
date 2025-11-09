# services/chatbot_service.py
# (Bản vá 11 - Sửa Lỗi 1 (NoneType 'strip') khi Tra cứu nhanh)

import re
import pandas as pd
from db_manager import safe_float
from services.sales_lookup_service import SalesLookupService 

class ChatbotService:
    def __init__(self, sales_lookup_service, customer_service):
        self.lookup_service = sales_lookup_service
        self.customer_service = customer_service
        
        self.user_context = {} 
        
        self.intents = {
            'CHECK_HISTORY': re.compile(r'(.+?)\s*(?:đã|có)?\s*mua\s+([\w\d\-,./]+)(?:\s*(?:chưa|không))?\??', re.IGNORECASE),
            
            'PRICE_CHECK': re.compile(
                r'(giá|% giá)\s+([\w\d\-,./\s]+)\s+(?:cho|với)\s+(.+)|' +
                r'([\w\d\-,./\s]+)\s+bán cho\s+(.+?)\s+giá nào\??|' +
                r'(.+?)\s+mua\s+([\w\d\-,./\s]+)\s+giá nào\??', 
                re.IGNORECASE
            ),
            
            'CANCEL_INTENT': re.compile(r'^(bỏ qua|hủy|tra mới|cancel|skip|thoát)$', re.IGNORECASE),
            
            'HELP': re.compile(r'(giúp|help|\?|bạn có thể làm gì|hỗ trợ|menu)', re.IGNORECASE), 
            
            'SELECT_OPTION': re.compile(r'^(?:số\s*)?([1-5])$'), 
            
            # SỬA LỖI 1: Regex này có 2 group. Text nằm ở group(2)
            'QUICK_LOOKUP': re.compile(r'^(?!.*\b(giá|mua|cho|với|help|giúp|hỗ trợ)\b)([\w\d\-,./\s]+)$', re.IGNORECASE), 
        }

    # --- HÀM QUẢN LÝ NGỮ CẢNH (Không đổi) ---
    
    def _get_user_context(self, user_code):
        if user_code not in self.user_context:
            self.user_context[user_code] = {'intent': None, 'data': {}}
        return self.user_context[user_code]

    def _clear_user_context(self, user_code):
        if user_code in self.user_context:
            self.user_context[user_code] = {'intent': None, 'data': {}}
            
    def _set_user_context(self, user_code, intent, data):
        self.user_context[user_code] = {'intent': intent, 'data': data}

    # --- HÀM XỬ LÝ CHÍNH (ĐÃ CẬP NHẬT LOGIC) ---

    def process_message(self, message_text, user_code, user_role):
        message_text = message_text.strip() if message_text else "" 
        context = self._get_user_context(user_code)
        
        try:
            # --- 1. KIỂM TRA LỆNH HỦY (ƯU TIÊN CAO NHẤT) ---
            cancel_match = self.intents['CANCEL_INTENT'].match(message_text)
            if cancel_match:
                self._clear_user_context(user_code)
                return "OK, đã hủy. Bạn muốn hỏi gì tiếp?"

            # --- 2. KIỂM TRA NGỮ CẢNH (NẾU CÓ) ---
            if context.get('intent'):
                response = self._handle_clarification(message_text, user_code, context)
                
                if response is not None:
                    # User đã trả lời đúng (ví dụ: "1" hoặc "Hoa Sen Nghe An")
                    self._clear_user_context(user_code)
                    return response
                else:
                    # User KHÔNG trả lời câu hỏi ngữ cảnh
                    # (Họ gõ "22214" hoặc "abc")
                    # -> Hủy ngữ cảnh cũ VÀ xử lý tin nhắn mới
                    self._clear_user_context(user_code)
                    pass 
            
            # --- 3. XỬ LÝ NHƯ MỘT INTENT MỚI ---
            
            # (Hỏi trợ giúp)
            help_match = self.intents['HELP'].search(message_text)
            if help_match:
                return self._handle_help()

            # (Kiểm tra lịch sử)
            history_match = self.intents['CHECK_HISTORY'].match(message_text)
            if history_match:
                customer_name = history_match.group(1).strip()
                item_term = history_match.group(2).strip()
                return self._process_multi_step_query(
                    user_code, 'CHECK_HISTORY', item_term, customer_name
                )

            # (Kiểm tra giá cho KH)
            price_match = self.intents['PRICE_CHECK'].match(message_text)
            if price_match:
                if price_match.group(1): 
                    item_term = price_match.group(2).strip()
                    customer_name = price_match.group(3).strip()
                elif price_match.group(4):
                    item_term = price_match.group(4).strip()
                    customer_name = price_match.group(5).strip()
                else: 
                    customer_name = price_match.group(6).strip()
                    item_term = price_match.group(7).strip()
                
                return self._process_multi_step_query(
                    user_code, 'PRICE_CHECK', item_term, customer_name
                )

            # (Tra cứu nhanh)
            lookup_match = self.intents['QUICK_LOOKUP'].match(message_text)
            if lookup_match:
                # --- SỬA LỖI 1: 'NoneType' object has no attribute 'strip' ---
                # Regex (?!...)(...) có 2 group. group(1) là None, group(2) là text.
                item_codes = lookup_match.group(2).strip() if lookup_match.group(2) else None
                # --- KẾT THÚC SỬA LỖI 1 ---
                
                if item_codes:
                    return self._handle_quick_lookup(item_codes)
            
            if not message_text:
                return None 

            return "Xin lỗi, tôi chưa hiểu ý định của bạn. Hãy thử gõ 'giúp'."
        
        except Exception as e:
            if "'user_code'" in str(e): 
                 print(f"LỖI FATAL CONTEXT: {e}")
                 self._clear_user_context(user_code)
                 return "Lỗi ngữ cảnh (user_code), vui lòng hỏi lại."
                 
            print(f"LỖI CHATBOT PROCESS: {e}")
            return f"Lỗi hệ thống: {e}"

    # (Hàm _process_multi_step_query không đổi)
    def _process_multi_step_query(self, user_code, intent, item_term, customer_name):
        customers_found = self._find_customer(customer_name)
        
        if isinstance(customers_found, str):
            self._set_user_context(user_code, 'ASK_CUSTOMER', {
                'original_intent': intent,
                'item_term': item_term, 
                'customer_list': self.customer_service.get_customer_by_name(customer_name),
                'last_question': customers_found 
            })
            return customers_found 
        
        customer_obj = customers_found[0]
        
        if intent == 'PRICE_CHECK':
            return self._handle_price_check_final(item_term, customer_obj)
        elif intent == 'CHECK_HISTORY':
            return self._handle_check_history_final(item_term, customer_obj)

    # (Hàm _handle_clarification không đổi)
    def _handle_clarification(self, message_text, user_code, context):
        intent = context.get('intent')
        data = context.get('data', {})
        
        chosen_customer = None
        
        if intent == 'ASK_CUSTOMER':
            customer_list = data.get('customer_list', [])
            chosen_customer = self._find_choice(message_text, customer_list, 'FullName')
            
            if not chosen_customer:
                return None 
            
            item_term = data.get('item_term') 
            original_intent = data.get('original_intent')
            
            if original_intent == 'PRICE_CHECK':
                return self._handle_price_check_final(item_term, chosen_customer)
            elif original_intent == 'CHECK_HISTORY':
                return self._handle_check_history_final(item_term, chosen_customer)
        
        return None 
            
    # (Hàm _find_customer không đổi)
    def _find_customer(self, customer_name):
        try:
            customers = self.customer_service.get_customer_by_name(customer_name)
            if not customers:
                return f"Không tìm thấy khách hàng nào có tên giống '{customer_name}'."
            if len(customers) > 1:
                return self._format_customer_options(customers, customer_name)
            return customers 
        except Exception as e:
            return f"Lỗi khi tìm khách hàng: {e}"

    # (Hàm _find_choice không đổi)
    def _find_choice(self, text, options_list, field_name_to_match):
        match_num = re.match(r'^(?:số\s*)?([1-5])$', text)
        if match_num:
            index = int(match_num.group(1)) - 1
            if 0 <= index < len(options_list):
                return options_list[index]
        
        text_lower = text.lower()
        found_options = []
        for item in options_list:
            full_name = item.get(field_name_to_match, '').lower()
            item_id_key = 'ID' if 'ID' in item else 'InventoryID'
            item_id = item.get(item_id_key, '').lower()
            
            if text_lower == full_name or text_lower == item_id:
                return item 
            
            if text_lower in full_name or (item_id and text_lower in item_id):
                found_options.append(item)
        
        if len(found_options) == 1:
            return found_options[0]
            
        return None


    # --- HÀM ĐỊNH DẠNG PHẢN HỒI (FORMATTING) ---
    
    # (Hàm _format_customer_options không đổi)
    def _format_customer_options(self, customers, term, limit=5):
        response = f"Tôi tìm thấy **{len(customers)}** khách hàng khớp với '{term}'. Vui lòng gõ số hoặc tên để chọn 1:\n"
        for i, c in enumerate(customers[:limit]):
            response += f"**{i+1}**. {c['FullName']} ({c['ID']})\n"
        return response

    # (Hàm _format_item_options không đổi)
    def _format_item_options(self, items, term, limit=5):
        response = f"Tôi tìm thấy **{len(items)}** mặt hàng khớp với '{term}'. Vui lòng gõ số hoặc tên/mã để chọn 1 (Hiển thị 5/{len(items)}):\n"
        for i, item in enumerate(items[:limit]):
            response += f"**{i+1}**. **{item.get('InventoryName', 'N/A')}** ({item.get('InventoryID')})\n"
        return response

    # (Hàm _handle_help không đổi)
    def _handle_help(self):
        response = "**Tôi có thể giúp bạn:**\n"
        response += "1. **Tra cứu nhanh Tồn kho/Giá QĐ**\n   (Gõ: `6210zzc3` hoặc `nsk 6210zz`)\n"
        response += "2. **Kiểm tra Giá bán & Lịch sử**\n   (Gõ: `giá 6214 cho Vina Kraft`)\n"
        response += "3. **Kiểm tra Lịch sử mua hàng**\n   (Gõ: `Vina Kraft có mua 6214 không?`)\n"
        response += "4. **Gợi ý Đặt hàng dự phòng**\n   (Tự động khi tra cứu nhanh nếu có BackOrder)"
        return response

    # (Hàm _handle_check_history_final không đổi)
    def _handle_check_history_final(self, item_term, customer_object, limit=5):
        customer_id = customer_object['ID']
        customer_display_name = customer_object['FullName']
        
        items_found = self.lookup_service.get_quick_lookup_data(item_term)
        if not items_found:
            return f"Không tìm thấy mặt hàng nào khớp với '{item_term}'."

        response_lines = [f"**Kết quả lịch sử mua '{item_term}' (KH: {customer_display_name}):**"]
        items_to_show = items_found[:limit]
        found_history = False

        for item in items_to_show:
            item_id = item['InventoryID']
            item_name = item['InventoryName']
            
            last_invoice_date = self.lookup_service.check_purchase_history(customer_id, item_id)
            
            line = f"- **{item_name}** ({item_id}): "
            if last_invoice_date:
                found_history = True
                line += f"**Đã mua** (Gần nhất: {last_invoice_date})"
            else:
                line += "**Chưa mua**"
            response_lines.append(line)

        if not found_history:
             response_lines = [f"**Chưa.** Khách hàng **{customer_display_name}** chưa có lịch sử mua (hoặc chưa xuất HĐ) cho bất kỳ mặt hàng nào khớp với '{item_term}'."]
            
        if len(items_found) > limit:
            response_lines.append(f"\n*(Hiển thị {limit} / {len(items_found)} mặt hàng khớp...)*")
            
        return "\n".join(response_lines)

    # (Hàm _handle_price_check_final không đổi)
    def _handle_price_check_final(self, item_term, customer_object, limit=5):
        
        customer_id = customer_object['ID']
        customer_display_name = customer_object['FullName']
        
        try:
            block1 = self.lookup_service._get_block1_data(item_term, customer_id)
        except Exception as e:
            return f"Lỗi khi gọi SP (Block 1): {e}"
        
        if not block1:
            return f"Không tìm thấy mặt hàng nào khớp với '{item_term}' cho KH {customer_display_name}."
            
        response_lines = [f"**Kết quả giá cho '{item_term}' (KH: {customer_display_name}):**"]
        
        items_to_show = block1[:limit]

        for item in items_to_show:
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
            
        if len(block1) > limit:
            response_lines.append(f"\n*(Hiển thị {limit} / {len(block1)} kết quả tìm thấy...)*")
            
        return "\n".join(response_lines)

    # (Hàm _handle_quick_lookup không đổi)
    def _handle_quick_lookup(self, item_codes, limit=5):
        
        try:
            data = self.lookup_service.get_quick_lookup_data(item_codes)
            
            if not data:
                return f"Không tìm thấy thông tin cho mã: '{item_codes}'."
            
            response_lines = ["**Kết quả tra nhanh Tồn kho:**"]
            
            items_to_show = data[:limit]
            
            for item in items_to_show:
                inv_id = item['InventoryID']
                inv_name = item.get('InventoryName', 'N/A') 
                ton = item.get('Ton', 0)
                bo = item.get('BackOrder', 0)
                gbqd = item.get('GiaBanQuyDinh', 0)
                
                line = f"- **{inv_name}** ({inv_id}):\n"
                line += f"  Tồn: **{ton:,.0f}** | BO: **{bo:,.0f}** | Giá QĐ: **{gbqd:,.0f}**"
                
                if bo > 0:
                    line += f"\n  -> *Gợi ý: Mã này đang BackOrder. Anh nên đề xuất khách đặt dự phòng.*"
                    
                response_lines.append(line)
            
            if len(data) > limit:
                response_lines.append(f"\n*(Hiển thị {limit} / {len(data)} kết quả tìm thấy...)*")
            
            return "\n".join(response_lines)
            
        except Exception as e:
            print(f"Lỗi _handle_quick_lookup: {e}")
            return f"Lỗi hệ thống khi tra cứu nhanh: {e}"