# services/delivery_service.py
# (Bản vá 13 - Logic CUỐI: Tab 1 GỘP TOÀN BỘ, Tab 2 DÙNG LẺ)

from db_manager import DBManager, safe_float
import datetime as dt # Import thư viện gốc với alias
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict 
import locale # <-- THÊM THƯ VIỆN LOCALE
import config
# Cài đặt locale Tiếng Việt để lấy đúng tên Thứ
try:
    locale.setlocale(locale.LC_TIME, 'vi_VN.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, 'Vietnamese_Vietnam.1258')
    except locale.Error:
        print("Warning: Locale 'vi_VN' or 'Vietnamese' not found. Day names might be in English.")

class DeliveryService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def _format_date_safe(self, date_val):
        """Kiểm tra giá trị an toàn (cho cả None và Pandas NaT)."""
        if pd.isna(date_val) or not isinstance(date_val, (datetime, pd.Timestamp)):
            try:
                date_val = datetime.strptime(str(date_val), '%Y-%m-%d %H:%M:%S')
            except (ValueError, TypeError):
                 try:
                     date_val = datetime.strptime(str(date_val), '%Y-%m-%d')
                 except (ValueError, TypeError):
                     return '—'
        
        if isinstance(date_val, datetime):
            date_val = date_val.date()
            
        try:
            return date_val.strftime('%d/%m/%Y')
        except AttributeError: 
             return str(date_val) 
        
    # --- HÀM MỚI: TÍNH TOÁN NGÀY GIAO HÀNG ---
    def _get_planned_date_info(self, planned_day_str):
        """
        Tính toán ngày ISO và ngày hiển thị (Thứ, dd/mm) cho tuần này/tuần sau.
        """
        if not planned_day_str or planned_day_str in ['POOL', 'URGENT', 'WITHIN_WEEK', 'COMPLETED']:
            return '9999-12-31', None # Sort về cuối, không hiển thị

        weekdays_map = {
            'MONDAY': 0, 'TUESDAY': 1, 'WEDNESDAY': 2, 
            'THURSDAY': 3, 'FRIDAY': 4, 'SATURDAY': 5
        }
        
        target_weekday = weekdays_map.get(planned_day_str.upper())
        if target_weekday is None:
            return '9999-12-31', None # Không phải ngày trong tuần

        today = datetime.now()
        today_weekday = today.weekday() # Monday is 0, Sunday is 6
        
        # Tính toán ngày mục tiêu
        days_ahead = target_weekday - today_weekday
        
        # Nếu ngày mục tiêu đã qua trong tuần này (hoặc là hôm nay nhưng đã qua giờ),
        # thì dời sang tuần sau. (Giả định Thứ 7 (5) là ngày cuối tuần làm việc)
        if days_ahead < 0:
             days_ahead += 7 # Lấy ngày đó của tuần sau

        target_date = today + timedelta(days=days_ahead)
        
        iso_date = target_date.strftime('%Y-%m-%d')
        
        # Thử lấy tên Thứ theo locale
        try:
            day_name = target_date.strftime('%A')
            # Chuẩn hóa nếu locale không trả về đúng "Thứ X"
            if "Monday" in day_name: day_name = "Thứ 2"
            elif "Tuesday" in day_name: day_name = "Thứ 3"
            elif "Wednesday" in day_name: day_name = "Thứ 4"
            elif "Thursday" in day_name: day_name = "Thứ 5"
            elif "Friday" in day_name: day_name = "Thứ 6"
            elif "Saturday" in day_name: day_name = "Thứ 7"
            
            display_str = f"{day_name}, {target_date.strftime('%d/%m')}"
        except Exception:
            # Fallback nếu locale lỗi
            day_names_vn = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"]
            display_str = f"{day_names_vn[target_date.weekday()]}, {target_date.strftime('%d/%m')}"

        return iso_date, display_str
    # --- KẾT THÚC HÀM MỚI ---

    def get_planning_board_data(self):
        """
        (LOGIC CUỐI) 
        1. grouped_tasks: Gộp nhóm TẤT CẢ LXH (Chưa giao) theo KH (cho Tab 1)
        2. ungrouped_tasks: TẤT CẢ LXH (Chưa giao + Đã giao) (cho Tab 2)
        """
        
        seven_days_ago = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        
        query = f"""
            SELECT 
                VoucherID, VoucherNo, VoucherDate, RefNo02, ObjectID, ObjectName, 
                TotalValue, ItemCount, EarliestRequestDate, Planned_Day, DeliveryStatus,
                ActualDeliveryDate
            FROM {config.DELIVERY_WEEKLY_VIEW}
            WHERE 
                (DeliveryStatus <> 'Da Giao') 
                OR (DeliveryStatus = 'Da Giao' AND ActualDeliveryDate >= '{seven_days_ago}')
            ORDER BY EarliestRequestDate
        """
        data = self.db.get_data(query)
        
        if not data:
            return [], [] 
        
        # --- LOGIC GỘP NHÓM TOÀN CỤC (YÊU CẦU CUỐI) ---
        customer_groups = defaultdict(lambda: {
            'ObjectID': None, 'ObjectName': None, 
            'LXH_Count': 0, 'TotalValue': 0, 
            'EarliestRequestDate_str': '9999-12-31',
            'RefNo02_str': '', 
            'RefNo02_latest_date': datetime(1900, 1, 1).date(), 
            'Planned_Day': 'POOL', 
            'Status_Summary': '', 
            'StatusCounts': defaultdict(int),
            'VoucherIDs': [],
        })
        
        ungrouped_tasks_list = [] 

        for row in data:
            # Chuẩn bị dữ liệu
            voucher_date_obj = row.get('VoucherDate')
            earliest_date_obj = row.get('EarliestRequestDate')
            
            if pd.isna(row.get('ActualDeliveryDate')): row['ActualDeliveryDate'] = None
            row['VoucherDate_str'] = self._format_date_safe(voucher_date_obj)
            row['EarliestRequestDate_str'] = self._format_date_safe(earliest_date_obj)

            row['VoucherDate_str'] = self._format_date_safe(voucher_date_obj)
            row['EarliestRequestDate_str'] = self._format_date_safe(earliest_date_obj)

             # === BỔ SUNG LOGIC NGÀY GIAO THỰC TẾ ===
            actual_delivery_date_obj = row.get('ActualDeliveryDate')
            row['ActualDeliveryDate_str'] = self._format_date_safe(actual_delivery_date_obj)
            if isinstance(actual_delivery_date_obj, (dt.datetime, dt.date)):
                row['ActualDeliveryDate_ISO'] = actual_delivery_date_obj.strftime('%Y-%m-%d')
            else:
                row['ActualDeliveryDate_ISO'] = '1900-01-01'
            # === KẾT THÚC BỔ SUNG ===

            # === YÊU CẦU MỚI: BỔ SUNG THÔNG TIN NGÀY GIAO ===
            planned_day = row.get('Planned_Day', 'POOL')
            iso_date, display_day = self._get_planned_date_info(planned_day)
            row['Planned_Date_ISO'] = iso_date
            row['Planned_Day_Display'] = display_day
            # === KẾT THÚC BỔ SUNG ===

            # Thêm vào list lẻ (cho Tab 2)
            ungrouped_tasks_list.append(row)

            # --- GỘP NHÓM TẤT CẢ PHIẾU CHƯA GIAO (CHO TAB 1) ---
            if row['DeliveryStatus'] == 'Da Giao':
                continue
            
            group_key = row['ObjectID']
            if not group_key: continue
                
            group = customer_groups[group_key]
            
            group['ObjectID'] = row['ObjectID']
            group['ObjectName'] = row['ObjectName']
            group['LXH_Count'] += 1
            group['TotalValue'] += safe_float(row.get('TotalValue'))
            group['StatusCounts'][row['DeliveryStatus']] += 1
            group['VoucherIDs'].append(row['VoucherID'])
            
            # Logic: Gán Planned_Day của NHÓM = Cột được gán GẦN NHẤT
            if row['Planned_Day'] != 'POOL':
                 group['Planned_Day'] = row['Planned_Day'] 
                 # Gán luôn ngày đã tính toán cho nhóm
                 group['Planned_Date_ISO'] = iso_date
                 group['Planned_Day_Display'] = display_day

            # Logic lấy RefNo02 gần nhất
            if isinstance(voucher_date_obj, str): 
                try: voucher_date_obj = datetime.strptime(voucher_date_obj, '%Y-%m-%d').date()
                except (ValueError, TypeError): voucher_date_obj = None

            if row['RefNo02'] and voucher_date_obj:
                if voucher_date_obj > group['RefNo02_latest_date']:
                    group['RefNo02_latest_date'] = voucher_date_obj
                    group['RefNo02_str'] = row['RefNo02'] 
            
            # Lấy ngày yêu cầu sớm nhất
            earliest_date_str = row['EarliestRequestDate_str']
            if earliest_date_str != '—' and earliest_date_str < group['EarliestRequestDate_str']:
                group['EarliestRequestDate_str'] = earliest_date_str

        grouped_tasks_list = list(customer_groups.values())
        
        # Tạo chuỗi tóm tắt Status (VD: "5 Open, 2 Da Soan")
        for group in grouped_tasks_list:
            summary = []
            if group['StatusCounts']['Open'] > 0:
                summary.append(f"{group['StatusCounts']['Open']} Open")
            if group['StatusCounts']['Da Soan'] > 0:
                summary.append(f"{group['StatusCounts']['Da Soan']} Đã Soạn")
            group['Status_Summary'] = ", ".join(summary)
            if not group['Status_Summary']:
                group['Status_Summary'] = "N/A"
            
            # --- YÊU CẦU MỚI: Đảm bảo các nhóm POOL/URGENT cũng có key để sort ---
            if 'Planned_Date_ISO' not in group:
                group['Planned_Date_ISO'] = '9999-12-31'
                group['Planned_Day_Display'] = None
            # --- KẾT THÚC BỔ SUNG ---

        
            
            # Xóa các key tạm thời trước khi trả về JSON
            del group['StatusCounts']
            del group['RefNo02_latest_date']
            del group['VoucherIDs']

        return grouped_tasks_list, ungrouped_tasks_list # Trả về (Gộp) và (Lẻ TẤT CẢ)

    def set_planned_day(self, voucher_id, object_id, new_day, user_code, old_day):
        """
        Cập nhật cột Kanban (Planned_Day)
        - Nếu kéo Thẻ Khách hàng (gộp nhóm), cập nhật TẤT CẢ LXH của KH đó.
        - Nếu kéo 1 LXH lẻ, cập nhật LXH đó.
        """
        # [FIX]: Sử dụng f-string đúng cách để thay thế tên bảng từ config
        if object_id:
            # Kéo-thả Thẻ Khách hàng (Gộp nhóm) -> Cập nhật TẤT CẢ LXH của KH
            query = f"""
                UPDATE {config.DELIVERY_WEEKLY_VIEW}
                SET Planned_Day = ?, LastUpdated = GETDATE()
                WHERE 
                    ObjectID = ? 
                    AND DeliveryStatus IN ('Open', 'Da Soan')
            """
            params = (new_day, object_id) 
        elif voucher_id:
            # Kéo-thả 1 LXH lẻ
            query = f"""
                UPDATE {config.DELIVERY_WEEKLY_VIEW}
                SET Planned_Day = ?, LastUpdated = GETDATE()
                WHERE VoucherID = ?
            """
            params = (new_day, voucher_id)
        else:
            return False 
            
        return self.db.execute_non_query(query, params)

    def set_delivery_status(self, voucher_id, new_status, user_code):
        # [FIX]: Sử dụng f-string đúng cách
        if new_status == config.DELIVERY_STATUS_DONE:
            query = f"""
                UPDATE {config.DELIVERY_WEEKLY_VIEW}
                SET DeliveryStatus = ?, ActualDeliveryDate = GETDATE(), DispatcherUser = ?
                WHERE VoucherID = ?
            """
            params = (new_status, user_code, voucher_id)
        else:
            query = f"""
                UPDATE {config.DELIVERY_WEEKLY_VIEW}
                SET DeliveryStatus = ?, LastUpdated = GETDATE()
                WHERE VoucherID = ?
            """
            params = (new_status, voucher_id)
            
        return self.db.execute_non_query(query, params)

    def get_delivery_items(self, voucher_id):
        query = """
            SELECT 
                d.TransactionID, d.InventoryID, i.InventoryName, d.ActualQuantity
            FROM {config.ERP_DELIVERY_DETAIL} d
            LEFT JOIN {config.ERP_IT1302} i ON d.InventoryID = i.InventoryID
            WHERE d.VoucherID = ?
            ORDER BY d.Orders
        """
        data = self.db.get_data(query, (voucher_id,))
        
        if not data:
            return []
            
        for row in data:
            row['ActualQuantity'] = safe_float(row.get('ActualQuantity'))
            
        return data

        # Thêm phương thức này vào class DeliveryService
    def get_recent_delivery_status(self, object_id, days_ago=7):
        """
        Lấy chi tiết LXH và trạng thái giao hàng cho một khách hàng trong N ngày gần nhất.
        """
        date_limit = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        query = f"""
            SELECT TOP 20
                VoucherNo, VoucherDate, Planned_Day, DeliveryStatus, 
                EarliestRequestDate, ActualDeliveryDate,
                ItemCount  -- <<< BỔ SUNG ITEMCOUNT
            FROM {config.DELIVERY_WEEKLY_VIEW}
            WHERE 
                ObjectID = ? 
                AND VoucherDate >= '{date_limit}'
            ORDER BY VoucherDate DESC
        """
        data = self.db.get_data(query, (object_id,))
        
        if not data:
            return []
            
        for row in data:
            # Sử dụng hàm định dạng ngày tháng an toàn (từ self._format_date_safe)
            row['VoucherDate'] = self._format_date_safe(row.get('VoucherDate'))
            row['EarliestRequestDate'] = self._format_date_safe(row.get('EarliestRequestDate'))
            row['ActualDeliveryDate'] = self._format_date_safe(row.get('ActualDeliveryDate'))
            row['ItemCount'] = int(safe_float(row.get('ItemCount', 0))) # <<< ĐẢM BẢO LÀ SỐ NGUYÊN
            
        return data