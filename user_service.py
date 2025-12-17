from flask import current_app
from db_manager import DBManager
import config
import os
from werkzeug.utils import secure_filename
from datetime import datetime

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
            current_app.logger.error(f"Lỗi lưu phân quyền: {e}")
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()

    # Thêm hàm mới vào Class UserService
    def update_user_theme_preference(self, user_code, theme_code):
        """Cập nhật theme mặc định cho User."""
        query = f"UPDATE {config.TEN_BANG_NGUOI_DUNG} SET [THEME] = ? WHERE USERCODE = ?"
        return self.db.execute_non_query(query, (theme_code, user_code))
    
    # --- 1. LẤY THÔNG TIN PROFILE (Tự động tạo nếu chưa có) ---
    def get_user_stats(self, user_code):
        query = f"""
            SELECT Level, CurrentXP, TitanCoins, AvatarUrl, EquippedTheme, EquippedPet
            FROM {config.TABLE_TITAN_PROFILE}
            WHERE UserCode = ?
        """
        data = self.db.get_data(query, (user_code,))
        
        if not data:
            # Nếu user chưa có trong bảng Profile -> Tạo mới (Level 1)
            self._init_new_user(user_code)
            return self.get_user_stats(user_code) # Gọi lại đệ quy
            
        stats = data[0]
        
        # Tính toán XP cho Level tiếp theo (Công thức: Level * 1000)
        # VD: Lv1 -> 1000, Lv2 -> 2000
        next_level_xp = stats['Level'] * 1000
        xp_percent = (stats['CurrentXP'] / next_level_xp) * 100 if next_level_xp > 0 else 0
        
        stats['next_level_xp'] = next_level_xp
        stats['xp_percent'] = round(xp_percent, 1)
        
        return stats

    def _init_new_user(self, user_code):
        """Tạo dòng dữ liệu mặc định cho user mới."""
        insert_sql = f"""
            INSERT INTO {config.TABLE_TITAN_PROFILE} 
            (UserCode, Level, CurrentXP, TitanCoins, EquippedTheme, EquippedPet)
            VALUES (?, 1, 0, 0, 'light', 'fox')
        """
        self.db.execute_non_query(insert_sql, (user_code,))
        
        # Tặng vật phẩm mặc định vào kho
        default_items = ['light', 'dark', 'fox']
        for item in default_items:
            self.db.execute_non_query(
                f"INSERT INTO {config.TABLE_TITAN_INVENTORY} (UserCode, ItemCode) VALUES (?, ?)", 
                (user_code, item)
            )

    # --- 2. LẤY KHO ĐỒ (Inventory) ---
    def get_user_inventory(self, user_code):
        """Lấy danh sách các món đồ user ĐÃ SỞ HỮU."""
        query = f"""
            SELECT 
                T1.ItemCode, T2.ItemName, T2.Icon, T2.ItemType, T2.Description,
                CASE WHEN (T3.EquippedTheme = T1.ItemCode OR T3.EquippedPet = T1.ItemCode) 
                     THEN 1 ELSE 0 END as IsEquipped
            FROM {config.TABLE_TITAN_INVENTORY} T1
            JOIN {config.TABLE_TITAN_ITEMS} T2 ON T1.ItemCode = T2.ItemCode
            LEFT JOIN {config.TABLE_TITAN_PROFILE} T3 ON T1.UserCode = T3.UserCode
            WHERE T1.UserCode = ? AND T1.IsActive = 1
        """
        return self.db.get_data(query, (user_code,))

    # --- 3. LẤY CỬA HÀNG (Shop) ---
    def get_shop_items(self, user_code):
        """Lấy danh sách vật phẩm trong Shop (Kèm trạng thái Mua/Khóa)."""
        query = f"""
            SELECT 
                T1.*,
                CASE WHEN T2.ItemCode IS NOT NULL THEN 1 ELSE 0 END as IsOwned
            FROM {config.TABLE_TITAN_ITEMS} T1
            LEFT JOIN {config.TABLE_TITAN_INVENTORY} T2 
                ON T1.ItemCode = T2.ItemCode AND T2.UserCode = ?
            WHERE T1.IsActive = 1
            ORDER BY T1.ItemType, T1.Price
        """
        return self.db.get_data(query, (user_code,))

    # --- 4. MUA VẬT PHẨM (Transaction) ---
    def buy_item(self, user_code, item_code):
        # A. Kiểm tra điều kiện
        profile = self.get_user_stats(user_code)
        item_query = f"SELECT Price, MinLevel, ItemName FROM {config.TABLE_TITAN_ITEMS} WHERE ItemCode = ?"
        item_data = self.db.get_data(item_query, (item_code,))
        
        if not item_data: return {'success': False, 'message': 'Vật phẩm không tồn tại!'}
        item = item_data[0]
        
        # Check sở hữu
        check_own = self.db.get_data(f"SELECT 1 FROM {config.TABLE_TITAN_INVENTORY} WHERE UserCode=? AND ItemCode=?", (user_code, item_code))
        if check_own: return {'success': False, 'message': 'Bạn đã có món này rồi!'}

        # Check Level & Tiền
        if profile['Level'] < item['MinLevel']:
            return {'success': False, 'message': f"Yêu cầu Level {item['MinLevel']}!"}
        if profile['TitanCoins'] < item['Price']:
            return {'success': False, 'message': 'Không đủ Titan Coins!'}

        # B. Giao dịch (Trừ tiền + Thêm đồ)
        conn = self.db.get_transaction_connection()
        try:
            # 1. Trừ tiền
            self.db.execute_query_in_transaction(conn, 
                f"UPDATE {config.TABLE_TITAN_PROFILE} SET TitanCoins = TitanCoins - ? WHERE UserCode = ?", 
                (item['Price'], user_code))
            
            # 2. Thêm vào kho
            self.db.execute_query_in_transaction(conn,
                f"INSERT INTO {config.TABLE_TITAN_INVENTORY} (UserCode, ItemCode) VALUES (?, ?)",
                (user_code, item_code))
            
            self.db.commit(conn)
            return {'success': True, 'message': f"Đã mua {item['ItemName']} thành công!"}
        except Exception as e:
            self.db.rollback(conn)
            return {'success': False, 'message': f'Lỗi hệ thống: {str(e)}'}
        finally:
            conn.close()

    # --- 5. TRANG BỊ (Đổi Theme/Pet) ---
    def equip_item(self, user_code, item_code):
        # Xác định loại vật phẩm để update đúng cột
        type_query = f"SELECT ItemType FROM {config.TABLE_TITAN_ITEMS} WHERE ItemCode = ?"
        data = self.db.get_data(type_query, (item_code,))
        
        if not data: return {'success': False, 'message': 'Vật phẩm lỗi.'}
        
        item_type = data[0]['ItemType']
        col_update = "EquippedTheme" if item_type == 'THEME' else "EquippedPet" if item_type == 'PET' else None
        
        if not col_update:
            return {'success': False, 'message': 'Vật phẩm này không thể trang bị (Skill/Frame).'}
            
        sql = f"UPDATE {config.TABLE_TITAN_PROFILE} SET {col_update} = ? WHERE UserCode = ?"
        
        if self.db.execute_non_query(sql, (item_code, user_code)):
            return {'success': True, 'message': 'Đã trang bị thành công!'}
        return {'success': False, 'message': 'Lỗi cập nhật DB.'}

    # --- 6. UPLOAD AVATAR ---
    def update_avatar(self, user_code, file):
        try:
            filename = secure_filename(f"{user_code}_avatar_{int(datetime.now().timestamp())}.png") 
            # Thêm timestamp để tránh cache trình duyệt
            
            save_folder = os.path.join(config.UPLOAD_FOLDER_PATH, 'avatars')
            if not os.path.exists(save_folder): os.makedirs(save_folder)
                
            full_path = os.path.join(save_folder, filename)
            file.save(full_path)
            
            # Đường dẫn tương đối để lưu DB
            db_url = f"/attachments/avatars/{filename}"
            
            sql = f"UPDATE {config.TABLE_TITAN_PROFILE} SET AvatarUrl = ? WHERE UserCode = ?"
            self.db.execute_non_query(sql, (db_url, user_code))
            
            return {'success': True, 'url': db_url}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    # --- 7. ĐỔI MẬT KHẨU ---
    def change_password(self, user_code, old_pass, new_pass):
        # [QUAN TRỌNG] Logic này cần khớp với cách bạn hash pass trong login
        # Ở đây tôi giả định bạn lưu pass thô (plaintext) hoặc hàm check đơn giản
        # Nếu dùng hash (bcrypt/pbkdf2), hãy import thư viện tương ứng
        
        # 1. Kiểm tra pass cũ
        sql_check = f"SELECT PASSWORD FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
        user = self.db.get_data(sql_check, (user_code,))
        
        if not user or user[0]['PASSWORD'] != old_pass:
            return {'success': False, 'message': 'Mật khẩu cũ không đúng!'}
            
        # 2. Cập nhật pass mới
        sql_update = f"UPDATE {config.TEN_BANG_NGUOI_DUNG} SET PASSWORD = ? WHERE USERCODE = ?"
        if self.db.execute_non_query(sql_update, (new_pass, user_code)):
            return {'success': True, 'message': 'Đổi mật khẩu thành công!'}
            
        return {'success': False, 'message': 'Lỗi hệ thống.'}