# services/ar_aging_service.py
# (ĐÃ CẬP NHẬT: Thêm ReDueDays và TotalOverdueDebt)

from db_manager import DBManager, safe_float
from datetime import datetime
import config

class ARAgingService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_ar_aging_summary(self, user_code, user_role, customer_name=""):
        """
        Lấy dữ liệu công nợ đã được tính toán trước từ Bảng Tổng hợp.
        (ĐÃ CẬP NHẬT CÁC CỘT MỚI)
        """
        
        where_conditions = []
        params = []

        if user_role.upper() not in ['ADMIN', 'GM', 'MANAGER']:
            where_conditions.append("SalesManID = ?")
            params.append(user_code)
            
        if customer_name:
            where_conditions.append("ObjectName LIKE ?")
            params.append(f"%{customer_name}%")
            
        where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

        # --- SỬA CÂU SELECT ---
        query = f"""
            SELECT 
                ObjectID, ObjectName, SalesManName,
                ReDueDays, TotalDebt, TotalOverdueDebt, -- <-- CỘT MỚI
                Debt_Current, Debt_Range_1_30, Debt_Range_31_90, Debt_Range_91_180, Debt_Over_180
            FROM dbo.CRM_AR_AGING_SUMMARY
            WHERE 
                {where_clause}
                AND TotalDebt > 1 
            ORDER BY 
                TotalOverdueDebt DESC, TotalDebt DESC -- <-- SẮP XẾP THEO NỢ QUÁ HẠN
        """
        # --- KẾT THÚC SỬA ---
        
        data = self.db.get_data(query, tuple(params))
        
        if not data:
            return []
            
        # --- SỬA VÒNG LẶP SAFE_FLOAT ---
        for row in data:
            row['ReDueDays'] = int(safe_float(row.get('ReDueDays'))) # Hạn nợ là số nguyên
            row['TotalDebt'] = safe_float(row.get('TotalDebt'))
            row['TotalOverdueDebt'] = safe_float(row.get('TotalOverdueDebt'))
            
            row['Debt_Current'] = safe_float(row.get('Debt_Current'))
            row['Debt_Range_1_30'] = safe_float(row.get('Debt_Range_1_30'))
            row['Debt_Range_31_90'] = safe_float(row.get('Debt_Range_31_90'))
            row['Debt_Range_91_180'] = safe_float(row.get('Debt_Range_91_180'))
            row['Debt_Over_180'] = safe_float(row.get('Debt_Over_180'))
        # --- KẾT THÚC SỬA ---
            
        return data
    
    def get_ar_aging_details_by_voucher(self, user_code, user_role, customer_id=None, customer_name=None, filter_salesman_id=None):
    
        # Lấy năm hiện tại cho bộ lọc DTCL (Yêu cầu 2)
        current_year = datetime.now().year
        
        # 1. Xác định tham số Salesman (PHU TRACH DS)
        salesman_param = None
        if user_role.upper() in ['ADMIN', 'GM', 'MANAGER']:
            # Nếu là Admin/Manager, ta dùng filter_salesman_id từ form (Yêu cầu 4)
            salesman_param = filter_salesman_id
        else:
            # Nếu là NVKD, ta chỉ xem KH mình phụ trách (từ DTCL)
            salesman_param = user_code
            
        # 2. Xử lý tham số Khách hàng
        object_id_param = customer_id
        
        # 3. Gọi SP (Thêm tham số CurrentYear)
        sp_results = self.db.execute_sp_multi(
            'dbo.sp_GetARAgingDetail', 
            (salesman_param, object_id_param, current_year)
        )
        
        data = sp_results[0] if sp_results and len(sp_results) > 0 else []

        # 4. Lọc Tên Khách hàng ở Python (nếu cần)
        if customer_name:
            data = [
                row for row in data 
                if customer_name.lower() in row.get('ShortObjectName', '').lower()
            ]

        # 5. Định dạng dữ liệu (Đã thay đổi tên cột)
        for row in data:
            # RemainingBalance là cột trung gian tổng giá trị chưa thanh toán
            row['RemainingBalance'] = safe_float(row.get('RemainingBalance'))
            row['Debt_In_Term'] = safe_float(row.get('Debt_In_Term'))
            row['Debt_Total_Overdue'] = safe_float(row.get('Debt_Total_Overdue'))
            row['TotalInvoiceAmount'] = safe_float(row.get('TotalInvoiceAmount'))
            
            # Định dạng ngày
            for key in ['VoucherDate', 'DueDate']:
                if row.get(key):
                    try:
                        row[key] = row[key].strftime('%d/%m/%Y')
                    except AttributeError:
                        pass
                        
            row['OverdueDays'] = int(max(0, safe_float(row.get('OverdueDays'))))
                
        return data
    # Thêm phương thức này vào class ARAgingService
    def get_single_customer_aging_summary(self, object_id, user_code, user_role):
        """
        Lấy 1 hàng tổng hợp công nợ từ CRM_AR_AGING_SUMMARY cho một khách hàng cụ thể (dùng cho KPI blocks).
        """
        
        where_conditions = ["ObjectID = ?"]
        params = [object_id]

        if user_role.upper() not in ['ADMIN', 'GM', 'MANAGER']:
            # Áp dụng bộ lọc cho NVKD thường (dựa trên SalesManID trên bảng tổng hợp)
            where_conditions.append("SalesManID = ?")
            params.append(user_code) 
            
        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT TOP 1
                ObjectID, ObjectName, SalesManName,
                TotalDebt, TotalOverdueDebt, Debt_Over_180
            FROM dbo.CRM_AR_AGING_SUMMARY
            WHERE 
                {where_clause}
        """
        
        data = self.db.get_data(query, tuple(params))
        
        if not data:
            return None
            
        row = data[0]
        # Đảm bảo các cột là float an toàn
        for key in ['TotalDebt', 'TotalOverdueDebt', 'Debt_Over_180']:
            row[key] = safe_float(row.get(key))
            
        return row
