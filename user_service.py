from flask import current_app
import config
import os
from werkzeug.utils import secure_filename
from datetime import datetime

class UserService:
    def __init__(self, db_manager):
        self.db = db_manager

    # =========================================================================
    # 1. QUẢN LÝ NGƯỜI DÙNG (ADMIN CRUD)
    # =========================================================================
    
    def get_all_users(self, division=None): 
        params = []
        query = f"""
            SELECT USERCODE, USERNAME, SHORTNAME, ROLE, [CAP TREN], [BO PHAN], [CHUC VU], [Division], [CreatedDate]
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE 1=1
        """
        if division:
            query += " AND [Division] = ?"
            params.append(division)
            
        query += " ORDER BY USERCODE"
        return self.db.get_data(query, tuple(params))

    def get_user_detail(self, user_code):
        query = f"""
            SELECT USERCODE, USERNAME, SHORTNAME, ROLE, [CAP TREN], [BO PHAN], [CHUC VU], [Division], [EMAIL], [THEME]
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE USERCODE = ?
        """
        data = self.db.get_data(query, (user_code,))
        return data[0] if data else None

    def create_user(self, user_data):
        try:
            check = self.get_user_detail(user_data['user_code'])
            if check: return {'success': False, 'message': 'Mã nhân viên đã tồn tại!'}

            sql = f"""
                INSERT INTO {config.TEN_BANG_NGUOI_DUNG} 
                (USERCODE, PASSWORD, USERNAME, SHORTNAME, ROLE, [CAP TREN], [BO PHAN], [CHUC VU], [Division], [EMAIL], CreatedDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """
            params = (
                user_data['user_code'], user_data['password'], user_data['username'], user_data['shortname'],
                user_data['role'], user_data['manager_code'], user_data['department'], user_data['position'],
                user_data['division'], user_data.get('email', '')
            )
            self.db.execute_non_query(sql, params)
            
            # Init Stats & Profile (Dùng cột chuẩn EquippedTheme)
            self.db.execute_non_query("INSERT INTO TitanOS_UserStats (UserCode, Level, CurrentXP, TotalCoins) VALUES (?, 1, 0, 0)", (user_data['user_code'],))
            self.db.execute_non_query("INSERT INTO TitanOS_UserProfile (UserCode, EquippedTheme, EquippedPet) VALUES (?, 'light', 'fox')", (user_data['user_code'],))
            
            return {'success': True, 'message': 'Tạo nhân viên thành công!'}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    def update_user(self, user_data):
        try:
            sql = f"""
                UPDATE {config.TEN_BANG_NGUOI_DUNG}
                SET USERNAME=?, SHORTNAME=?, ROLE=?, [CAP TREN]=?, [BO PHAN]=?, [CHUC VU]=?, [Division]=?, [EMAIL]=?
                WHERE USERCODE=?
            """
            params = (
                user_data['username'], user_data['shortname'], user_data['role'], user_data['manager_code'],
                user_data['department'], user_data['position'], user_data['division'], user_data.get('email', ''),
                user_data['user_code']
            )
            self.db.execute_non_query(sql, params)
            return {'success': True, 'message': 'Cập nhật thành công!'}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    def delete_user(self, user_code):
        try:
            tables = ['TitanOS_UserStats', 'TitanOS_UserProfile', 'TitanOS_UserInventory', 'TitanOS_UserPermissions', 'TitanOS_Game_Mailbox']
            for t in tables:
                self.db.execute_non_query(f"DELETE FROM {t} WHERE UserCode=?", (user_code,))
            
            self.db.execute_non_query(f"DELETE FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?", (user_code,))
            return {'success': True, 'message': 'Đã xóa nhân viên.'}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    def admin_reset_password(self, user_code, new_pass):
        try:
            self.db.execute_non_query(f"UPDATE {config.TEN_BANG_NGUOI_DUNG} SET PASSWORD = ? WHERE USERCODE = ?", (new_pass, user_code))
            return {'success': True, 'message': 'Đã reset mật khẩu.'}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    # =========================================================================
    # 2. QUẢN LÝ PHÂN QUYỀN
    # =========================================================================
    
    def get_all_roles(self):
        """Lấy danh sách các Role duy nhất đang có trong hệ thống."""
        query = f"SELECT DISTINCT ROLE FROM {config.TEN_BANG_NGUOI_DUNG} WHERE ROLE IS NOT NULL AND ROLE <> ''"
        data = self.db.get_data(query)
        return sorted([r['ROLE'].strip().upper() for r in data])
        
    def get_all_permissions(self):
        return self.db.get_data(f"SELECT * FROM {config.TABLE_SYS_PERMISSIONS_DEF} ORDER BY GroupName, FeatureCode")

    def get_permissions_matrix(self):
        query = f"SELECT RoleID, FeatureCode FROM {config.TABLE_SYS_PERMISSIONS}"
        data = self.db.get_data(query)
        matrix = {}
        for row in data:
            role = row['RoleID'].strip().upper()
            if role not in matrix: matrix[role] = []
            matrix[role].append(row['FeatureCode'])
        return matrix

    def update_permissions(self, role_id, features):
        conn = None
        try:
            conn = self.db.get_transaction_connection()
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {config.TABLE_SYS_PERMISSIONS} WHERE RoleID = ?", (role_id,))
            if features:
                insert_query = f"INSERT INTO {config.TABLE_SYS_PERMISSIONS} (RoleID, FeatureCode) VALUES (?, ?)"
                params = [(role_id, feat) for feat in features]
                cursor.executemany(insert_query, params)
            conn.commit()
            return True
        except Exception as e:
            if conn: conn.rollback()
            return False
        finally:
            if conn: conn.close()

    def get_user_permissions(self, user_code):
        data = self.db.get_data("SELECT PermissionCode FROM TitanOS_UserPermissions WHERE UserCode = ?", (user_code,))
        return [row['PermissionCode'] for row in data]

    def update_user_permissions(self, user_code, permission_codes):
        try:
            self.db.execute_non_query("DELETE FROM TitanOS_UserPermissions WHERE UserCode = ?", (user_code,))
            if permission_codes:
                for code in permission_codes:
                    self.db.execute_non_query("INSERT INTO TitanOS_UserPermissions (UserCode, PermissionCode) VALUES (?, ?)", (user_code, code))
            return {'success': True, 'message': 'Cập nhật thành công!'}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    def get_all_divisions(self):
        return self.db.get_data(f"SELECT DISTINCT [Division] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE [Division] IS NOT NULL")

    # =========================================================================
    # 3. GAMIFICATION & PROFILE (USER FRONTEND) - [ĐÃ SỬA CỘT DB]
    # =========================================================================
    
    def get_user_profile(self, user_code):
        """
        Lấy thông tin profile.
        [FIX]: Map đúng cột EquippedTheme của DB thành ThemeColor cho Frontend.
        """
        query = f"""
            SELECT 
                T1.USERCODE, T1.USERNAME, T1.SHORTNAME, T1.[CHUC VU], T1.[BO PHAN], T1.EMAIL,
                S.Level, ISNULL(S.CurrentXP, 0) as CurrentXP, ISNULL(S.TotalCoins, 0) as TotalCoins,
                ISNULL(P.AvatarFrame, '') as AvatarFrame,
                ISNULL(P.Title, '') as Title,
                ISNULL(P.NameEffect, '') as NameEffect,
                ISNULL(P.EquippedTheme, 'light') as ThemeColor,
                ISNULL(P.EquippedPet, '') as EquippedPet,
                ISNULL(P.IsFlexing, 0) as IsFlexing,
                P.AvatarUrl,
                ISNULL(P.Nickname, '') as Nickname -- <--- THÊM DÒNG NÀY
            FROM {config.TEN_BANG_NGUOI_DUNG} AS T1
            LEFT JOIN TitanOS_UserStats AS S ON T1.USERCODE = S.UserCode
            LEFT JOIN TitanOS_UserProfile AS P ON T1.USERCODE = P.UserCode
            WHERE T1.USERCODE = ?
        """
        data = self.db.get_data(query, (user_code,))
        if not data: return None
        user_profile = data[0]

        # Self-healing Stats
        if user_profile['Level'] is None:
            try:
                self.db.execute_non_query("INSERT INTO TitanOS_UserStats (UserCode, Level, CurrentXP, TotalCoins) VALUES (?, 1, 0, 0)", (user_code,))
                user_profile.update({'Level': 1, 'CurrentXP': 0, 'TotalCoins': 0})
            except: pass
            
        # Self-healing Profile
        if 'ThemeColor' not in user_profile or not user_profile['ThemeColor']:
             try:
                self.db.execute_non_query("INSERT INTO TitanOS_UserProfile (UserCode, EquippedTheme) VALUES (?, 'light')", (user_code,))
                user_profile['ThemeColor'] = 'light'
             except: pass

        # XP Calculation
        current_lvl = user_profile['Level']
        current_xp = user_profile['CurrentXP']
        xp_data = self.db.get_data("SELECT XP_Required FROM TitanOS_Game_Levels WHERE Level = ?", (current_lvl,))
        next_level_xp = xp_data[0]['XP_Required'] if xp_data else 2000
        
        progress_percent = int((current_xp / next_level_xp) * 100) if next_level_xp > 0 else 100
        user_profile['NextLevelXP'] = next_level_xp
        user_profile['ProgressPercent'] = min(progress_percent, 100)

        return user_profile

    def update_user_theme_preference(self, user_code, theme_code):
        """Cập nhật theme vào bảng UserProfile (cột EquippedTheme)."""
        # Kiểm tra tồn tại
        if not self.db.get_data("SELECT UserCode FROM TitanOS_UserProfile WHERE UserCode=?", (user_code,)):
             self.db.execute_non_query("INSERT INTO TitanOS_UserProfile (UserCode, EquippedTheme) VALUES (?, ?)", (user_code, theme_code))
        else:
             self.db.execute_non_query("UPDATE TitanOS_UserProfile SET EquippedTheme = ? WHERE UserCode = ?", (theme_code, user_code))
        return True

    def update_avatar(self, user_code, file):
        try:
            if not file: return {'success': False, 'message': 'Chưa chọn file'}
            filename = secure_filename(f"{user_code}_{int(datetime.now().timestamp())}_{file.filename}")
            upload_folder = os.path.join(current_app.root_path, 'static', 'attachments', 'avatars')
            if not os.path.exists(upload_folder): os.makedirs(upload_folder)
            
            file.save(os.path.join(upload_folder, filename))
            db_url = f"/static/attachments/avatars/{filename}"
            
            # Upsert Profile
            check = self.db.get_data("SELECT UserCode FROM TitanOS_UserProfile WHERE UserCode=?", (user_code,))
            if check:
                self.db.execute_non_query("UPDATE TitanOS_UserProfile SET AvatarUrl=? WHERE UserCode=?", (db_url, user_code))
            else:
                self.db.execute_non_query("INSERT INTO TitanOS_UserProfile (UserCode, AvatarUrl, EquippedTheme) VALUES (?, ?, 'light')", (user_code, db_url))
            return {'success': True, 'url': db_url}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    # =========================================================================
    # 4. SHOP & INVENTORY
    # =========================================================================

    def buy_item(self, user_code, item_code):
        item = self.db.get_data("SELECT Price, ItemName FROM TitanOS_SystemItems WHERE ItemCode = ?", (item_code,))
        if not item: return {'success': False, 'message': 'Vật phẩm không tồn tại.'}
        
        price = item[0]['Price']
        item_name = item[0]['ItemName']

        if self.db.get_data("SELECT ID FROM TitanOS_UserInventory WHERE UserCode=? AND ItemCode=?", (user_code, item_code)):
             return {'success': False, 'message': 'Đã sở hữu vật phẩm này!'}

        stats = self.db.get_data("SELECT TotalCoins FROM TitanOS_UserStats WHERE UserCode = ?", (user_code,))
        current_coins = stats[0]['TotalCoins'] if stats else 0
        
        if current_coins < price:
            return {'success': False, 'message': f'Thiếu {price - current_coins} Coins.'}

        try:
            self.db.execute_non_query("UPDATE TitanOS_UserStats SET TotalCoins = TotalCoins - ? WHERE UserCode = ?", (price, user_code))
            self.db.execute_non_query("INSERT INTO TitanOS_UserInventory (UserCode, ItemCode, AcquiredDate, IsActive) VALUES (?, ?, GETDATE(), 1)", (user_code, item_code))
            return {'success': True, 'message': f'Mua thành công "{item_name}"!'}
        except Exception as e:
            return {'success': False, 'message': str(e)}

    def equip_item(self, user_code, item_code):
        if not self.db.get_data("SELECT ID FROM TitanOS_UserInventory WHERE UserCode=? AND ItemCode=?", (user_code, item_code)):
            return {'success': False, 'message': 'Bạn chưa có vật phẩm này.'}

        item_info = self.db.get_data("SELECT ItemType FROM TitanOS_SystemItems WHERE ItemCode=?", (item_code,))
        if not item_info: return {'success': False, 'message': 'Lỗi vật phẩm.'}
        
        item_type = item_info[0]['ItemType']
        
        # [FIX]: Map cột DB cho đúng tên (EquippedTheme, EquippedPet)
        column_map = {
            'THEME': 'EquippedTheme', 
            'FRAME': 'AvatarFrame', 
            'TITLE': 'Title', 
            'EFFECT': 'NameEffect', 
            'PET': 'EquippedPet'
        }
        target_col = column_map.get(item_type)

        if target_col:
            if not self.db.get_data("SELECT UserCode FROM TitanOS_UserProfile WHERE UserCode=?", (user_code,)):
                self.db.execute_non_query("INSERT INTO TitanOS_UserProfile (UserCode, IsFlexing, EquippedTheme) VALUES (?, 1, 'light')", (user_code,))
            
            self.db.execute_non_query(f"UPDATE TitanOS_UserProfile SET {target_col} = ?, IsFlexing = 1 WHERE UserCode = ?", (item_code, user_code))
            return {'success': True, 'message': 'Đã trang bị!'}
        
        return {'success': True, 'message': 'Đã kích hoạt!'}
    
    def use_rename_card(self, user_code, new_nickname):
        """Sử dụng thẻ đổi tên: Update Nickname và Xóa thẻ khỏi túi."""
        # 1. Kiểm tra có thẻ trong túi không
        check_item = self.db.get_data(
            "SELECT ID FROM TitanOS_UserInventory WHERE UserCode=? AND ItemCode='rename_card' AND IsActive=1", 
            (user_code,)
        )
        if not check_item:
            return {'success': False, 'message': 'Bạn không có Thẻ Đổi Tên!'}
        
        item_id = check_item[0]['ID']
        
        # 2. Thực hiện đổi tên (Dùng Transaction)
        conn = None
        try:
            conn = self.db.get_transaction_connection()
            cursor = conn.cursor()
            
            # Update Nickname
            cursor.execute("UPDATE TitanOS_UserProfile SET Nickname = ? WHERE UserCode = ?", (new_nickname, user_code))
            
            # Xóa vật phẩm đã dùng (Consumable)
            cursor.execute("DELETE FROM TitanOS_UserInventory WHERE ID = ?", (item_id,))
            
            conn.commit()
            return {'success': True, 'message': f'Đã đổi tên thành công sang: "{new_nickname}"'}
        except Exception as e:
            if conn: conn.rollback()
            return {'success': False, 'message': str(e)}
        finally:
            if conn: conn.close()

    # =========================================================================
    # 5. BẢO MẬT CÁ NHÂN
    # =========================================================================

    def change_password(self, user_code, old_pass, new_pass):
        sql_check = f"SELECT PASSWORD FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?"
        user = self.db.get_data(sql_check, (user_code,))
        if not user or user[0]['PASSWORD'] != old_pass:
            return {'success': False, 'message': 'Mật khẩu cũ không đúng!'}
        try:
            self.db.execute_non_query(f"UPDATE {config.TEN_BANG_NGUOI_DUNG} SET PASSWORD = ? WHERE USERCODE = ?", (new_pass, user_code))
            return {'success': True, 'message': 'Đổi mật khẩu thành công!'}
        except Exception as e:
            return {'success': False, 'message': str(e)}