# services/gamification_service.py
from datetime import datetime
import config

class GamificationService:
    def __init__(self, db_manager):
        self.db = db_manager
        self.MAX_DAILY_XP = 2607 # Giới hạn cứng theo yêu cầu

    def log_activity(self, user_code, activity_code):
        """
        Ghi nhận hành động của user vào log.
        Hàm này chạy Real-time khi user thao tác.
        """
        try:
            # Chỉ ghi log, chưa tính toán gì để đảm bảo tốc độ app
            query = "INSERT INTO TitanOS_Game_DailyLogs (UserCode, ActivityCode) VALUES (?, ?)"
            self.db.execute_non_query(query, (user_code, activity_code))
        except Exception as e:
            print(f"Lỗi log gamification: {e}")

    def process_daily_rewards(self):
        """
        [CRON JOB 20:00] Tổng kết và gửi quà.
        ĐÃ CẬP NHẬT: Chống gửi trùng lặp (Idempotency Check).
        """
        print(f">>> Bắt đầu quét thưởng ngày {datetime.now().strftime('%d/%m/%Y')}...")
        
        # 1. Lấy danh sách User có hoạt động chưa xử lý
        users_query = "SELECT DISTINCT UserCode FROM TitanOS_Game_DailyLogs WHERE IsProcessed = 0"
        users = self.db.get_data(users_query)

        if not users:
            print(">>> Không có hoạt động nào mới.")
            return

        today_str = datetime.now().strftime('%d/%m')
        mail_title_prefix = f"🎁 Tổng kết hoạt động ngày {today_str}"

        count_sent = 0

        for u in users:
            user_code = u['UserCode']
            
            # --- [LOGIC MỚI] CHECK TRÙNG LẶP ---
            # Kiểm tra xem user này đã nhận thư tổng kết hôm nay chưa
            check_mail_sql = """
                SELECT MailID FROM TitanOS_Game_Mailbox 
                WHERE UserCode = ? AND Title LIKE ?
            """
            # Dùng LIKE để tìm tiêu đề chứa ngày hôm nay
            is_rewarded = self.db.get_data(check_mail_sql, (user_code, f"{mail_title_prefix}%"))
            
            if is_rewarded:
                print(f"⚠️ User {user_code} đã nhận quà hôm nay rồi -> Bỏ qua.")
                # Tùy chọn: Có thể update luôn các log còn sót thành đã xử lý để dọn dẹp
                self.db.execute_non_query(
                    "UPDATE TitanOS_Game_DailyLogs SET IsProcessed = 1 WHERE UserCode = ? AND IsProcessed = 0", 
                    (user_code,)
                )
                continue
            # -----------------------------------

            # 2. Tính toán điểm thưởng
            # Lấy chi tiết log
            log_sql = """
                SELECT L.ActivityCode, COUNT(*) as Count, A.XP_Reward, A.Coin_Reward, A.Description, A.Daily_Limit
                FROM TitanOS_Game_DailyLogs L
                JOIN TitanOS_Game_Activities A ON L.ActivityCode = A.ActivityCode
                WHERE L.UserCode = ? AND L.IsProcessed = 0
                GROUP BY L.ActivityCode, A.XP_Reward, A.Coin_Reward, A.Description, A.Daily_Limit
            """
            logs = self.db.get_data(log_sql, (user_code,))
            
            if not logs: continue

            total_xp = 0
            total_coins = 0
            details_html = "<ul>"

            for log in logs:
                count = log['Count']
                limit = log['Daily_Limit']
                
                # Logic giới hạn số lần (Capping per activity)
                valid_count = count if (limit == 0 or count <= limit) else limit
                
                xp_earn = valid_count * log['XP_Reward']
                coin_earn = valid_count * log['Coin_Reward']
                
                total_xp += xp_earn
                total_coins += coin_earn
                
                details_html += f"<li>{log['Description']}: {valid_count} lần (+{xp_earn} XP)</li>"

            details_html += "</ul>"
            
            # Logic giới hạn tổng XP ngày (Global Cap)
            if total_xp > self.MAX_DAILY_XP:
                total_xp = self.MAX_DAILY_XP
                details_html += f"<p class='text-danger small'>*(Đã đạt giới hạn {self.MAX_DAILY_XP} XP/ngày)</p>"

            # 3. Gửi thư (Insert Mailbox)
            if total_xp > 0 or total_coins > 0:
                mail_sql = """
                    INSERT INTO TitanOS_Game_Mailbox 
                    (UserCode, Title, Content, Total_XP, Total_Coins, CreatedTime, IsClaimed)
                    VALUES (?, ?, ?, ?, ?, GETDATE(), 0)
                """
                self.db.execute_non_query(mail_sql, (user_code, mail_title_prefix, details_html, total_xp, total_coins))
                count_sent += 1

            # 4. Đánh dấu Log đã xử lý
            self.db.execute_non_query(
                "UPDATE TitanOS_Game_DailyLogs SET IsProcessed = 1 WHERE UserCode = ? AND IsProcessed = 0", 
                (user_code,)
            )

        print(f">>> Hoàn tất. Đã gửi quà cho {count_sent} user.")

    def _generate_daily_mail_for_user(self, user_code):
        # 2. Lấy chi tiết hoạt động và cấu hình điểm
        sql = """
            SELECT 
                L.ActivityCode, 
                A.Description, 
                A.XP_Reward, 
                A.Daily_Limit,
                COUNT(L.LogID) as ActionCount
            FROM TitanOS_Game_DailyLogs L
            JOIN TitanOS_Game_Activities A ON L.ActivityCode = A.ActivityCode
            WHERE L.UserCode = ? AND L.IsProcessed = 0
            GROUP BY L.ActivityCode, A.Description, A.XP_Reward, A.Daily_Limit
        """
        activities = self.db.get_data(sql, (user_code,))
        
        if not activities: return

        total_xp = 0
        total_coins = 0 # (Nếu có activity nào thưởng coin trực tiếp)
        
        detail_html = "<ul>"
        
        for act in activities:
            count = act['ActionCount']
            limit = act['Daily_Limit']
            xp_unit = act['XP_Reward']
            
            # Tính số lần hợp lệ (không vượt quá limit ngày)
            valid_count = count if (limit == 0 or count <= limit) else limit
            
            earned_xp = valid_count * xp_unit
            total_xp += earned_xp
            
            detail_html += f"<li>{act['Description']}: {valid_count} lần (+{earned_xp} XP)</li>"

        # 3. Áp dụng giới hạn 2607 XP
        final_xp = min(total_xp, self.MAX_DAILY_XP)
        if total_xp > self.MAX_DAILY_XP:
            detail_html += f"<li style='color:red'><i>Đã đạt giới hạn ngày. XP thực nhận: {self.MAX_DAILY_XP}</i></li>"
        
        detail_html += "</ul>"

        # 4. Gửi thư (Tạo bản ghi trong Mailbox)
        title = f"🎁 Tổng kết hoạt động ngày {datetime.now().strftime('%d/%m')}"
        mail_sql = """
            INSERT INTO TitanOS_Game_Mailbox (UserCode, Title, Content, Total_XP, Total_Coins)
            VALUES (?, ?, ?, ?, ?)
        """
        self.db.execute_non_query(mail_sql, (user_code, title, detail_html, final_xp, total_coins))

        # 5. Đánh dấu log đã xử lý
        self.db.execute_non_query("UPDATE TitanOS_Game_DailyLogs SET IsProcessed=1 WHERE UserCode = ? AND IsProcessed=0", (user_code,))

    def create_hall_of_fame_story(self, author_code, target_code, title, content, tags, images_str=None, is_public=True):
        """
        Tạo story kèm danh sách ảnh.
        """
        try:
            sql = """
                INSERT INTO [dbo].[HR_HALL_OF_FAME] 
                (TargetUserCode, AuthorUserCode, StoryTitle, StoryContent, Tags, ImagePaths, IsPublic, CreatedDate)
                VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
            """
            public_bit = 1 if is_public else 0
            # images_str là chuỗi đường dẫn ảnh ngăn cách bởi dấu phẩy hoặc chấm phẩy
            self.db.execute_non_query(sql, (target_code, author_code, title, content, tags, images_str, public_bit))
            return True, "Câu chuyện đã được lưu trữ mãi mãi!"
        except Exception as e:
            print(f"Error creating story: {e}")
            return False, str(e)

    def get_all_users_for_select(self):
        """
        Lấy danh sách nhân viên chi tiết (Kèm Chức vụ, Bộ phận) để gợi nhớ.
        [FIX]: Dùng cột USERNAME thay vì FULLNAME.
        """
        try:
            sql = """
                SELECT USERCODE, SHORTNAME, USERNAME, [BO PHAN], [CHUC VU]
                FROM [GD - NGUOI DUNG] 
                WHERE [BO PHAN] IS NOT NULL  
                ORDER BY SHORTNAME ASC
            """
            return self.db.get_data(sql)
        except Exception as e:
            print(f"Error fetching users: {e}")
            return []
