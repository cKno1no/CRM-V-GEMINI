from db_manager import DBManager
import config

class UserService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_all_users(self, division=None): 
        params = []
        query = f"""
            SELECT USERCODE, USERNAME, SHORTNAME, ROLE, [CAP TREN], [BO PHAN], [CHUC VU], [Division]
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE 1=1
        """
        
        # Bây giờ biến division đã hợp lệ để sử dụng
        if division:
            query += " AND [Division] = ?"
            params.append(division)
            
        query += " ORDER BY USERCODE"
        return self.db.get_data(query, tuple(params))

    def get_user_detail(self, user_code):
        query = f"""
            SELECT USERCODE, USERNAME, SHORTNAME, ROLE, [CAP TREN], [BO PHAN], [CHUC VU], [PASSWORD]
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE USERCODE = ?
        """
        data = self.db.get_data(query, (user_code,))
        return data[0] if data else None

    def update_user(self, user_data):
        # Chỉ cập nhật Password nếu người dùng nhập mới
        password_clause = ", [PASSWORD] = ?" if user_data.get('password') else ""
        
        query = f"""
            UPDATE {config.TEN_BANG_NGUOI_DUNG}
            SET SHORTNAME = ?, ROLE = ?, [CAP TREN] = ?, 
                [BO PHAN] = ?, [CHUC VU] = ? {password_clause}
            WHERE USERCODE = ?
        """
        params = [
            user_data.get('shortname'),
            user_data.get('role'),
            user_data.get('cap_tren'),
            user_data.get('bo_phan'),
            user_data.get('chuc_vu')
        ]
        if user_data.get('password'):
            params.append(user_data.get('password'))
        
        params.append(user_data.get('user_code'))
        
        return self.db.execute_non_query(query, tuple(params))

    def get_all_roles(self):
        """Lấy danh sách các Role duy nhất đang có trong hệ thống."""
        query = f"SELECT DISTINCT ROLE FROM {config.TEN_BANG_NGUOI_DUNG} WHERE ROLE IS NOT NULL AND ROLE <> ''"
        data = self.db.get_data(query)
        return sorted([r['ROLE'].strip().upper() for r in data])

    def get_permissions_matrix(self):
        """Trả về Dictionary: { 'ROLE_NAME': ['VIEW_DASHBOARD', 'APPROVE_ORDER', ...] }"""
        query = f"SELECT RoleID, FeatureCode FROM {config.TABLE_SYS_PERMISSIONS}"
        data = self.db.get_data(query)
        
        matrix = {}
        for row in data:
            role = row['RoleID'].strip().upper()
            if role not in matrix:
                matrix[role] = []
            matrix[role].append(row['FeatureCode'])
        return matrix

    def update_permissions(self, role_id, features):
        conn = None
        try:
            conn = self.db.get_transaction_connection()
            cursor = conn.cursor()
            
            # 1. Xóa quyền cũ
            del_query = f"DELETE FROM {config.TABLE_SYS_PERMISSIONS} WHERE RoleID = ?"
            cursor.execute(del_query, (role_id,))
            
            # 2. Thêm quyền mới
            if features:
                insert_query = f"INSERT INTO {config.TABLE_SYS_PERMISSIONS} (RoleID, FeatureCode) VALUES (?, ?)"
                params = [(role_id, feat) for feat in features]
                cursor.executemany(insert_query, params)
            
            conn.commit()
            return True
        except Exception as e:
            print(f"Lỗi lưu phân quyền: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()

    # Thêm hàm mới vào Class UserService
    def update_user_theme_preference(self, user_code, theme_code):
        """Cập nhật theme mặc định cho User."""
        query = f"UPDATE {config.TEN_BANG_NGUOI_DUNG} SET [THEME] = ? WHERE USERCODE = ?"
        return self.db.execute_non_query(query, (theme_code, user_code))