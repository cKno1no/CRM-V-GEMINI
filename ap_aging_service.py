from db_manager import DBManager, safe_float
import config

class APAgingService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_ap_aging_summary(self, vendor_name_filter=None, debt_type_filter='ALL'):
        """
        Lấy tổng hợp công nợ phải trả (Hỗ trợ lọc theo Loại).
        """
        where_conditions = ["1=1"]
        params = []

        # Lọc theo Tên/Mã
        if vendor_name_filter:
            where_conditions.append("(ObjectID LIKE ? OR ObjectName LIKE ?)")
            params.extend([f"%{vendor_name_filter}%", f"%{vendor_name_filter}%"])

        # Lọc theo Loại Nợ (SUPPLIER, BANK, OTHER)
        if debt_type_filter and debt_type_filter != 'ALL':
            where_conditions.append("DebtType = ?")
            params.append(debt_type_filter)

        where_clause = " AND ".join(where_conditions)

        # Cập nhật SELECT list để lấy thêm cột mới
        query = f"""
            SELECT 
                DebtType, ObjectID, ObjectName, ReDueDays, 
                TotalDebt, TotalOverdueDebt, Debt_Current, 
                Debt_Range_1_30, Debt_Range_31_90, 
                Debt_Range_91_180, Debt_Over_180  -- Cột mới
            FROM {config.CRM_AP_AGING_SUMMARY}
            WHERE {where_clause}
            ORDER BY 
                -- Ưu tiên GTG, VLP lên đầu nếu cần, hoặc giữ nguyên logic cũ
                CASE WHEN DebtType IN ('GTG', 'VLP') THEN 1 
                     WHEN DebtType = 'BANK' THEN 2 
                     ELSE 3 END,
                TotalDebt DESC
        """
        
        data = self.db.get_data(query, tuple(params))

        if data:
            for row in data:
                d_type = row.get('DebtType', 'UNKNOWN')
                
                # --- SỬA 1: Logic hiển thị tên rõ ràng hơn ---
                if d_type == 'BANK': 
                    row['TypeDisplay'] = 'Ngân hàng'
                    # Thêm hậu tố vào tên để tránh nhầm lẫn với dòng Supplier
                    row['ObjectName'] = f"{row['ObjectName']} (Vay/Khế ước)"
                    
                elif d_type == 'SUPPLIER': 
                    row['TypeDisplay'] = 'Nhà Cung Cấp'
                elif d_type == 'GTG': row['TypeDisplay'] = 'Gia công (GTG)'
                elif d_type == 'VLP': row['TypeDisplay'] = 'Vật liệu phụ (VLP)'
                else: row['TypeDisplay'] = 'Khác'
                
                # Format số
                for key in ['TotalDebt', 'TotalOverdueDebt', 'Debt_Current', 
                            'Debt_Range_1_30', 'Debt_Range_31_90', 
                            'Debt_Range_91_180', 'Debt_Over_180']:
                    row[key] = safe_float(row.get(key))
        return data

    def get_ap_details(self, vendor_id):
        """
        Lấy chi tiết hóa đơn của 1 NCC (Dùng cho Modal hoặc Drilldown).
        """
       # Lưu ý: Bạn cần sửa cả Stored Procedure SP_AP_AGING_DETAIL trong SQL 
        # để nhận thêm tham số @DebtType, hoặc xử lý lọc python ở đây.
        # Ví dụ giả định SP đã sửa để nhận 2 tham số:
        results = self.db.execute_sp_multi(config.SP_AP_AGING_DETAIL, (vendor_id, debt_type))
        return results[0] if results else []