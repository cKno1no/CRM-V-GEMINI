# services/ar_aging_service.py
# (ĐÃ CẬP NHẬT: Thêm ReDueDays và TotalOverdueDebt)

from db_manager import DBManager, safe_float
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