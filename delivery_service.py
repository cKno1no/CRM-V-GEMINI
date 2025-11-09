# services/delivery_service.py
# (Bản vá 13 - Logic CUỐI: Tab 1 GỘP TOÀN BỘ, Tab 2 DÙNG LẺ)

from db_manager import DBManager, safe_float
from datetime import datetime, timedelta
import pandas as pd
from collections import defaultdict 

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
            FROM dbo.Delivery_Weekly
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
        
        if object_id:
            # Kéo-thả Thẻ Khách hàng (Gộp nhóm) -> Cập nhật TẤT CẢ LXH của KH
            query = """
                UPDATE dbo.Delivery_Weekly
                SET Planned_Day = ?, LastUpdated = GETDATE()
                WHERE 
                    ObjectID = ? 
                    AND DeliveryStatus IN ('Open', 'Da Soan')
            """
            params = (new_day, object_id) # Bỏ old_day trong WHERE vì đã gộp nhóm
        elif voucher_id:
            # Kéo-thả 1 LXH lẻ
            query = """
                UPDATE dbo.Delivery_Weekly
                SET Planned_Day = ?, LastUpdated = GETDATE()
                WHERE VoucherID = ?
            """
            params = (new_day, voucher_id)
        else:
            return False 
            
        return self.db.execute_non_query(query, params)

    def set_delivery_status(self, voucher_id, new_status, user_code):
        if new_status == 'Da Giao':
            query = """
                UPDATE dbo.Delivery_Weekly
                SET DeliveryStatus = ?, ActualDeliveryDate = GETDATE(), DispatcherUser = ?
                WHERE VoucherID = ?
            """
            params = (new_status, user_code, voucher_id)
        else:
            query = """
                UPDATE dbo.Delivery_Weekly
                SET DeliveryStatus = ?, LastUpdated = GETDATE()
                WHERE VoucherID = ?
            """
            params = (new_status, voucher_id)
            
        return self.db.execute_non_query(query, params)

    def get_delivery_items(self, voucher_id):
        query = """
            SELECT 
                d.TransactionID, d.InventoryID, i.InventoryName, d.ActualQuantity
            FROM [OMEGA_STDD].[dbo].[OT2302] d
            LEFT JOIN [OMEGA_STDD].[dbo].[IT1302] i ON d.InventoryID = i.InventoryID
            WHERE d.VoucherID = ?
            ORDER BY d.Orders
        """
        data = self.db.get_data(query, (voucher_id,))
        
        if not data:
            return []
            
        for row in data:
            row['ActualQuantity'] = safe_float(row.get('ActualQuantity'))
            
        return data