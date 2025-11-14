import re
import pandas as pd
from db_manager import safe_float
from services.sales_lookup_service import SalesLookupService 
from services.delivery_service import DeliveryService # Cần import cho Type Hint/Best Practice

class ChatbotService:
    # ĐÃ SỬA: Thêm delivery_service vào signature
    def __init__(self, sales_lookup_service, customer_service, delivery_service, redis_client=None):
        self.lookup_service = sales_lookup_service
        self.customer_service = customer_service
        self.delivery_service = delivery_service # Gán Delivery Service
        self.redis_client = redis_client
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
            
            'QUICK_LOOKUP': re.compile(r'^(?!.*\b(giá|mua|cho|với|help|giúp|hỗ trợ)\b)([\w\d\-,./\s]+)$', re.IGNORECASE), 
            
            # Cấu trúc FIX mới: 3 mẫu câu cố định
            'CHECK_REPLENISHMENT': re.compile(
                # P1/P2: (Group 1: KH, Group 2: I02ID) - Dùng chung 2 mẫu
                r'^(?:đặt|dự)\s*dự\s*phòng\s*cho\s*(.+?)\s*theo\s*mã\s*(.+?)\??$' + r'|' + 
                r'^dự\s*phòng\s*cho\s*(.+?)\s*theo\s*mã\s*(.+?)\??$' + r'|' + 
                # P3: (Group 5: KH, Group 6: I02ID)
                r'^dự\s*phòng\s*(.+?)\s*mã\s*(.+?)\??$',
                re.IGNORECASE
            ),
            'CHECK_DELIVERY': re.compile(
                # Mẫu cuối cùng: Dùng nhóm không trích xuất (?:...) để cố định phần đầu
                r'^(?:tình trạng|tình hình)?\s*(giao hàng|vận chuyển)\s+cho\s+(.+?)\s*(chưa|không)?\??$', 
                re.IGNORECASE
            ),
            
        
        }

    # --- HÀM QUẢN LÝ NGỮ CẢNH ---
    def _get_user_context(self, user_code):
        if user_code not in self.user_context:
            self.user_context[user_code] = {'intent': None, 'data': {}}
        return self.user_context[user_code]

    def _clear_user_context(self, user_code):
        if user_code in self.user_context:
            self.user_context[user_code] = {'intent': None, 'data': {}}
            
    def _set_user_context(self, user_code, intent, data):
        self.user_context[user_code] = {'intent': intent, 'data': data}

    # --- HÀM XỬ LÝ CHÍNH ---

    def process_message(self, message_text, user_code, user_role):
        message_text = message_text.strip() if message_text else "" 
        context = self._get_user_context(user_code)
        
        try:
            # --- 1. KIỂM TRA LỆNH HỦY ---
            cancel_match = self.intents['CANCEL_INTENT'].match(message_text)
            if cancel_match:
                self._clear_user_context(user_code)
                return "OK, đã hủy. Bạn muốn hỏi gì tiếp."

            # --- 2. KIỂM TRA NGỮ CẢNH (NẾU CÓ) ---
            if context.get('intent'):
                response = self._handle_clarification(message_text, user_code, context)
                if response is not None:
                    self._clear_user_context(user_code)
                    return response
                else:
                    self._clear_user_context(user_code)
                    pass 
            
            # --- 3. XỬ LÝ NHƯ MỘT INTENT MỚI ---
            
            # (Hỏi trợ giúp)
            help_match = self.intents['HELP'].search(message_text)
            if help_match:
                return self._handle_help() 
            
            # (Kiểm tra giao hàng)
            delivery_match = self.intents['CHECK_DELIVERY'].search(message_text)
            if delivery_match:
                # Group 2 là Tên Khách hàng trong mẫu mới
                if delivery_match.group(2): 
                    customer_name = delivery_match.group(2).strip()
                else:
                    customer_name = None
                
                # Thêm kiểm tra chính xác tên KH
                if not customer_name or len(customer_name) < 2:
                    # Lỗi này có thể xảy ra nếu KH nhập cú pháp sai
                    return "Xin lỗi, tôi không thể trích xuất tên khách hàng chính xác. (Cú pháp: 'Tình trạng giao hàng cho Tên_KH')"
                
                return self._process_multi_step_query(
                    user_code, 'CHECK_DELIVERY', 'LXH_STATUS', customer_name
                )
            # (Kiểm tra nhu cầu Dự phòng)
            replenishment_match = self.intents['CHECK_REPLENISHMENT'].match(message_text)
            if replenishment_match:
                
                customer_name = None
                i02id_filter = None

                # Logic Phân tích Mẫu Cố Định
                if replenishment_match.group(1): # Pattern 1: Đặt/Dự phòng cho X theo mã Y
                    customer_name = replenishment_match.group(1).strip()
                    i02id_filter = replenishment_match.group(2)
                elif replenishment_match.group(3): # Pattern 2: Dự phòng cho X theo mã Y
                    customer_name = replenishment_match.group(3).strip()
                    i02id_filter = replenishment_match.group(4)
                elif replenishment_match.group(5): # Pattern 3: Dự phòng X mã Y
                    customer_name = replenishment_match.group(5).strip()
                    i02id_filter = replenishment_match.group(6)
                
                # --- KIỂM TRA KHÁCH HÀNG ---
                if not customer_name:
                    return "Xin lỗi, tôi không thể trích xuất tên khách hàng theo mẫu câu chuẩn. (Thử: 'Dự phòng cho Vina Kraft theo mã AB')" 

                context_data = {'i02id_filter': i02id_filter.upper() if i02id_filter else None}
                
                return self._process_multi_step_query(
                    user_code, 'CHECK_REPLENISHMENT', 'REPLENISH', customer_name, context_data=context_data
                )

            # (Kiểm tra các intent còn lại)
            
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
                item_codes = lookup_match.group(2).strip() if lookup_match.group(2) else None
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

    # --- CÁC HÀM XỬ LÝ MULTI-STEP VÀ CLARIFICATION ---

    def _process_multi_step_query(self, user_code, intent, item_term, customer_name, context_data=None):
        customers_found = self._find_customer(customer_name)
        
        if isinstance(customers_found, str):
            context_to_save = {
                'original_intent': intent,
                'item_term': item_term, 
                'customer_list': self.customer_service.get_customer_by_name(customer_name),
                'last_question': customers_found 
            }
            if context_data:
                 context_to_save.update(context_data)
                 
            self._set_user_context(user_code, 'ASK_CUSTOMER', context_to_save)
            return customers_found 
        
        customer_obj = customers_found[0]
        
        if intent == 'CHECK_REPLENISHMENT' and context_data:
             customer_obj.update(context_data)
        
        # Xử lý các Intent sau khi xác định KH
        if intent == 'PRICE_CHECK':
            return self._handle_price_check_final(item_term, customer_obj)
        elif intent == 'CHECK_HISTORY':
            return self._handle_check_history_final(item_term, customer_obj)
        elif intent == 'CHECK_DELIVERY':
            return self._handle_check_delivery_final(customer_obj) # <-- HÀM MỚI
        elif intent == 'CHECK_REPLENISHMENT':
            return self._handle_replenishment_check_final(customer_obj)


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
            
            if data.get('i02id_filter'):
                 chosen_customer['i02id_filter'] = data['i02id_filter']
            
            if original_intent == 'PRICE_CHECK':
                return self._handle_price_check_final(item_term, chosen_customer)
            elif original_intent == 'CHECK_HISTORY':
                return self._handle_check_history_final(item_term, chosen_customer)
            elif original_intent == 'CHECK_DELIVERY':
                 return self._handle_check_delivery_final(chosen_customer) # <-- HÀM MỚI
            elif original_intent == 'CHECK_REPLENISHMENT':
                return self._handle_replenishment_check_final(chosen_customer)
        
        return None 
            
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

    # ... (Các hàm helper khác: _find_choice, _format_customer_options, _handle_help) ...

    # --- HÀM XỬ LÝ GIAO HÀNG (DELIVERY) MỚI ---
    def _handle_check_delivery_final(self, customer_object):
        customer_id = customer_object['ID']
        customer_display_name = customer_object['FullName']
        
        # Gọi hàm service với days_ago=7
        recent_deliveries = self.delivery_service.get_recent_delivery_status(customer_id, days_ago=7)

        if not recent_deliveries:
            return f"Khách hàng **{customer_display_name}** không có Lệnh Xuất Hàng nào trong 7 ngày qua."

        response_lines = [f"**Tình trạng giao hàng (7 ngày qua) cho {customer_display_name}:**"]
        
        all_delivered = True
        
        for item in recent_deliveries:
            status = item.get('DeliveryStatus', 'CHỜ').strip().upper()
            planned_day = item.get('Planned_Day', 'POOL').strip().upper()
            
            if status != 'DA GIAO':
                 all_delivered = False
            
            # Xử lý màu sắc và hiển thị
            line = f"- **LXH {item['VoucherNo']}** (Ngày: {item['VoucherDate']}):\n"
            
            # --- LOGIC DÙNG HTML/CSS CƠ BẢN ĐỂ TÔ MÀU ---
            
            # 1. Hiển thị Kế hoạch (Vàng cam/Orange)
            if planned_day == 'POOL':
                planned_display = f'<span style="color: #F9AA33;">Chưa xếp lịch giao</span>'
            else:
                planned_display = planned_day
            
            # 2. Tô màu Trạng thái (Xanh lá/Green)
            if status == 'DA GIAO':
                 status_display = f'<span style="color: #34A853;">✅ DA GIAO</span>' 
            else:
                 status_display = f'**{status}**' # Giữ nguyên xanh dương/đậm cho trạng thái khác
            
            line += f"  > Kế hoạch: {planned_display} | Tình trạng: {status_display}\n"
            # --- END LOGIC DÙNG HTML/CSS CƠ BẢN ---

            if status == 'DA GIAO':
                 line += f"  > Ngày giao: {item['ActualDeliveryDate']}"
            elif item['EarliestRequestDate'] != '—':
                 line += f"  > Y/C sớm nhất: {item['EarliestRequestDate']}"
                 
            response_lines.append(line)
        
        if all_delivered and len(recent_deliveries) > 0:
             response_lines.insert(0, f"✅ **ĐÃ XUẤT HÀNG/GIAO TẤT CẢ** ({len(recent_deliveries)} LXH) trong 7 ngày gần nhất.")
            
        return "\n".join(response_lines)

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

    def _format_customer_options(self, customers, term, limit=5):
        response = f"Tôi tìm thấy **{len(customers)}** khách hàng khớp với '{term}'. Vui lòng gõ số hoặc tên để chọn 1:\n"
        for i, c in enumerate(customers[:limit]):
            response += f"**{i+1}**. {c['FullName']} ({c['ID']})\n"
        return response
    
    def _handle_help(self):
        response = "**Tôi có thể giúp bạn với các cú pháp chuẩn nhất sau:**\n"
        response += "1. **Tra cứu Tồn kho/Giá QĐ**\n   (Cú pháp: `Mã_hàng` hoặc `Hãng Mã_hàng`)\n   (Ví dụ: `22214` hoặc `nsk 6210zz`)\n"
        response += "2. **Kiểm tra Giá bán & Lịch sử**\n   (Cú pháp: `giá Mã_hàng cho Tên_KH`)\n   (Ví dụ: `giá 22214 cho Vina Kraft`)\n"
        response += "3. **Kiểm tra Đặt hàng Dự phòng**\n   (Cú pháp: `Dự phòng cho Tên_KH theo mã AB`)\n   (Ví dụ: `Dự phòng cho Vina Kraft theo mã AB`)\n"
        response += "4. **Kiểm tra Lịch sử mua hàng**\n   (Cú pháp: `Tên_KH có mua Mã_hàng chưa`)\n (Ví dụ: `Hoa Sen mua 6320 chưa`)\n"
        response += "5. **Kiểm tra Tình trạng giao hàng**\n   (Cú pháp: `Giao hàng cho Tên_KH chưa`)\n (Ví dụ: `Giao hàng cho VMS chưa`)\n"
        return response

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
                line += f"\n  Giá HĐ gần nhất: **{gia_hd:,.0f}** (Ngày: {ngay_hd}) ({symbol}{percent_diff:.1}%)"
            else:
                line += "\n  *(Chưa có lịch sử HĐ cho KH này)*"
            
            response_lines.append(line)
            
        if len(block1) > limit:
            response_lines.append(f"\n*(Hiển thị {limit} / {len(block1)} kết quả tìm thấy...)*")
            
        return "\n".join(response_lines)

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
            
    # --- HÀM XỬ LÝ REPLENISHMENT MỚI VÀ ĐÃ SỬA ĐỔI ---
    def _handle_replenishment_check_final(self, customer_object, limit=10):
        customer_id = customer_object['ID']
        customer_display_name = customer_object['FullName']
        
        # 1. Lấy I02ID Filter từ customer_object
        i02id_filter = customer_object.get('i02id_filter')
        
        # 2. Gọi hàm mới từ SalesLookupService
        data = self.lookup_service.get_replenishment_needs(customer_id)
        
        if not data:
            return f"Khách hàng **{customer_display_name}** hiện không có dữ liệu nhu cầu dự phòng."

        # 3. Lọc: Chỉ lấy các nhóm có Lượng Thiếu/Dư > 0 VÀ Lọc theo I02ID
        deficit_items = [
            item for item in data 
            if safe_float(item.get('LuongThieuDu')) > 1 
        ]
        
        # --- START FIX LOGIC LỌC I02ID ---
        if i02id_filter:
            target_code = i02id_filter.upper()
            
            # Nếu filter là 'AB', ta coi đó là chỉ dẫn chung và bỏ qua kiểm tra I02ID
            if target_code == 'AB':
                filtered_items = deficit_items
                filter_note = f" theo mã **AB**"
            else:
                # Nếu filter là mã cụ thể khác 'AB', áp dụng kiểm tra nghiêm ngặt (I02ID) và dự phòng (NhomHang)
                filtered_items = [
                    item for item in deficit_items 
                    if (item.get('I02ID') and item['I02ID'].upper() == target_code) or
                       (item.get('NhomHang') and item['NhomHang'].upper().startswith(f'{target_code}_')) 
                ]
                filter_note = f" theo mã **{target_code}**"
        else:
            filtered_items = deficit_items
            filter_note = ""
        # --- END FIX LOGIC LỌC I02ID ---


        if not filtered_items:
            return f"Khách hàng **{customer_display_name}** hiện không có nhu cầu đặt hàng dự phòng nổi bật{filter_note}."
            
        # 4. Giới hạn Top 10 và Định dạng
        top_items = filtered_items[:limit]
        
        response_lines = [
            f"Khách hàng **{customer_display_name}** cần đặt dự phòng cho **{len(filtered_items)}** nhóm hàng{filter_note}."
        ]
        
        if len(filtered_items) > limit:
             response_lines[0] += f" (Top {limit} hiển thị):"
        else:
             response_lines[0] += ":"

        for i, item in enumerate(top_items):
            nhom_hang = item.get('NhomHang', 'N/A')
            thieu_du = safe_float(item.get('LuongThieuDu', 0))
            rop = safe_float(item.get('DiemTaiDatROP', 0)) 
            ton_bo = safe_float(item.get('TonBO', 0))
            
            # FIX 2a: Bỏ phần (Mã AB: N/A)
            line = f"**{i+1}. {nhom_hang}**\n" 
            # FIX 2b: Đổi nhãn 'Dự phòng' thành 'Tồn-BO'
            line += f"  - Thiếu: **{thieu_du:,.0f}** | ROP: {rop:,.0f} | Tồn-BO: {ton_bo:,.0f}" 
            response_lines.append(line)
            
        if len(filtered_items) > limit:
            response_lines.append(f"\n*(Và {len(filtered_items) - limit} nhóm khác...)*")
            
        response_lines.append("\n-> *Xem chi tiết tại Dashboard Dự phòng Khách hàng.*")
        
        return "\n".join(response_lines)