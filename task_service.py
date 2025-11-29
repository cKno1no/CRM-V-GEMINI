from db_manager import DBManager, safe_float
import datetime as dt # Import thư viện gốc với alias
from datetime import datetime, timedelta # Import các đối tượng phổ biến
import config
import math
import pandas as pd # <-- FIX: THÊM DÒNG NÀY ĐỂ KHẮC PHỤC LỖI "pd is not defined"
class TaskService:
    """Xử lý toàn bộ logic nghiệp vụ liên quan đến quản lý đầu việc (Task Management)."""
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager
        self.TASK_TABLE = config.TASK_TABLE if hasattr(config, 'TASK_TABLE') else 'dbo.Task_Master'
        self.TASK_LOG_TABLE = config.TASK_LOG_TABLE # <-- Đảm bảo dòng này có
        # FIX: Khai báo rõ ràng tất cả các constants là thuộc tính instance (self.)
        self.STATUS_BLOCKED = 'BLOCKED'
        self.LOG_TYPE_PROGRESS = 'PROGRESS'
        self.LOG_TYPE_BLOCKED = 'BLOCKED'
        self.LOG_TYPE_HELP_CALL = 'HELP_CALL'
        self.LOG_TYPE_REQUEST_CLOSE = 'REQUEST_CLOSE'
        self.LOG_TYPE_SUPERVISOR_NOTE = 'SUPERVISOR_NOTE'

    # SỬA LỖI: THÊM 'self' VÀO ĐỊNH NGHĨA PHƯƠNG THỨC
    def _standardize_task_data(self, tasks): 
        """
        Chuẩn hóa DATETIME và giá trị NULL sang định dạng an toàn cho JSON/Jinja2, 
        và tạo trường hiển thị ngày tháng.
        """
        if not tasks:
            return []
        
        standardized_tasks = []
        for task in tasks:
            
            # 1. CHUẨN HÓA DATETIME VÀ TẠO DISPLAY FIELD
            task_date = task.get('TaskDate')
            
            # FIX: Chuyển đổi an toàn ngày tháng
            if isinstance(task_date, (dt.datetime, dt.date)): 
                task['TaskDateDisplay'] = task_date.strftime('%d/%m')
            else:
                task['TaskDateDisplay'] = task.get('TaskDate')
            
            # Chuẩn hóa các cột DATETIME khác sang chuỗi ISO hoặc None
            for key in ['CompletedDate', 'NoteTimestamp']:
                value = task.get(key)
                if isinstance(value, (dt.datetime, dt.date)):
                    task[key] = value.isoformat()
                elif value is None or value == 'nan':
                    task[key] = None
            
            # 2. CHUẨN HÓA CỘT NULLABLE
            for key in ['ObjectID', 'DetailContent', 'NoteCapTren', 'SupervisorCode', 'Attachments']:
                if task.get(key) is None or str(task.get(key)).strip().upper() == 'NAN':
                     task[key] = None
                
            standardized_tasks.append(task)
        return standardized_tasks
    
    def _is_admin_user(self, user_code):
        """Kiểm tra xem UserCode có vai trò ADMIN hay không."""
        query = f"""
            SELECT [ROLE] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ? AND RTRIM([ROLE]) = config.ROLE_ADMIN
        """
        return bool(self.db.get_data(query, (user_code,)))
    
    # THÊM: Helper để kiểm tra mối quan hệ Cấp trên (Req 2)
    def _is_helper_subordinate(self, helper_code, supervisor_code):
        """Kiểm tra helper có phải là nhân viên cấp dưới của supervisor_code hay không. (Yêu cầu 1)"""
        if not helper_code or not supervisor_code:
            return False

        # 1. Admin luôn là cấp trên (Yêu cầu 1)
        if self._is_admin_user(supervisor_code):
            return True
        
        # 2. Kiểm tra cấp trên trực tiếp (Logic cũ)
        query = f"""
            SELECT [CAP TREN]
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE USERCODE = ?
        """
        data = self.db.get_data(query, (helper_code,))
        
        if data and data[0].get('CAP TREN'):
            return data[0]['CAP TREN'].strip().upper() == supervisor_code.strip().upper()
        return False
    
    # THÊM: Helper để lấy tên KH theo ObjectID (Req 1)
    def _enrich_tasks_with_client_name(self, tasks):
        object_ids = [t['ObjectID'] for t in tasks if t.get('ObjectID') and t['ObjectID'].strip()]
        if not object_ids:
            for task in tasks:
                task['ClientName'] = None
            return tasks

        object_ids_str = ", ".join(f"'{o.strip()}'" for o in set(object_ids))

        # IT1202 là bảng Khách hàng ERP (ShortObjectName, ObjectID)
        query = f"""
            SELECT RTRIM(ObjectID) AS ObjectID, ShortObjectName AS ClientName
            FROM {config.ERP_IT1202} 
            WHERE ObjectID IN ({object_ids_str})
        """
        name_data = self.db.get_data(query)
        name_dict = {row['ObjectID']: row['ClientName'] for row in name_data}

        for task in tasks:
            task['ClientName'] = name_dict.get(task.get('ObjectID', '').strip(), None)
        return tasks
    
    def _enrich_tasks_with_user_info(self, tasks):
        """Lấy ShortName của UserCode (Người Gán) cho bảng Lịch sử. (Yêu cầu 3)"""
        user_codes = [t['UserCode'] for t in tasks if t.get('UserCode') and t['UserCode'].strip()]
        if not user_codes:
            for task in tasks:
                task['AssigneeShortName'] = None
            return tasks

        user_codes_str = ", ".join(f"'{u.strip()}'" for u in set(user_codes))

        query = f"""
            SELECT [USERCODE], [SHORTNAME] AS AssigneeShortName
            FROM {config.TEN_BANG_NGUOI_DUNG} 
            WHERE USERCODE IN ({user_codes_str})
        """
        name_data = self.db.get_data(query)
        name_dict = {row['USERCODE']: row['AssigneeShortName'] for row in name_data}

        for task in tasks:
            task['AssigneeShortName'] = name_dict.get(task.get('UserCode', '').strip(), task.get('UserCode'))
        return tasks

    def _get_time_filter_params(self, days_ago=30):
        """Tạo tham số ngày lọc: ngày bắt đầu và ngày hôm nay."""
        date_limit = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        today_date = datetime.now().strftime('%Y-%m-%d')
        return date_limit, today_date
        
    def create_new_task(self, user_code, title, supervisor_code, task_type, attachments=None, object_id=None): # <-- Bổ sung task_type
        """Tạo một Task mới, gán ngày hiện tại, CapTren và Attachments."""
        
        insert_query = f"""
            INSERT INTO {self.TASK_TABLE} (UserCode, TaskDate, Status, Title, CapTren, Attachments, TaskType, ObjectID, LastUpdated, ProgressPercentage)
            VALUES (?, GETDATE(), 'OPEN', ?, ?, ?, ?, ?, GETDATE(), 0)
        """
        # Đảm bảo TaskType được truyền chính xác vào params
        params = (user_code, title, supervisor_code, attachments, task_type.upper(), object_id)
        
        try:
            self.db.execute_non_query(insert_query, params)
            return True
        except Exception as e:
            print(f"LỖI TẠO TASK: {e}")
            return False

    
    # --- KHỐI 1: TASK CẦN XỬ LÝ GẤP (HÔM NAY VÀ HÔM QUA) ---
    def get_kanban_tasks(self, user_code, is_admin=False, days_ago=3, view_mode='USER'):
        """Lấy Task cho Kanban Board (3 ngày), hỗ trợ View Quản lý. (Yêu cầu 2)"""
        date_limit = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
        
        # Lọc UserCode/CapTren (Base filter)
        base_conditions = []
        if view_mode == 'SUPERVISOR':
            base_conditions.append(f"CapTren = '{user_code}'") 
        elif not is_admin: 
            base_conditions.append(f"UserCode = '{user_code}'") 
        
        user_filter = " AND ".join(base_conditions) if base_conditions else "1=1"

        query = f"""
            SELECT T1.*,
                   (SELECT COUNT(LogID) FROM {self.TASK_LOG_TABLE} T2 WHERE T2.TaskID = T1.TaskID) AS LogCount
            FROM {self.TASK_TABLE} AS T1
            WHERE 
                ({user_filter}) 
                AND  
                (
                    (TaskDate >= '{date_limit}') -- (2a) Task tạo trong 3 ngày gần nhất (bao gồm cả COMPLETED)
                    OR 
                    (Status IN ('OPEN', 'PENDING', 'BLOCKED', 'HELP_NEEDED')) -- (2b) Task vẫn còn active (ALL TIME)
                )
            ORDER BY LastUpdated DESC, TaskDate DESC
        """
        data = self.db.get_data(query)
        data = self._enrich_tasks_with_client_name(data)
        data = self._enrich_tasks_with_user_info(data) 
        return self._standardize_task_data(data)
    
    # --- KHỐI 2: TASK LỊCH SỬ VÀ RỦI RO (30 NGÀY) ---
    def get_filtered_tasks(self, user_code, filter_type='RISK', is_admin=False, days_ago=30, view_mode='USER', text_search_term=None): 
        """Lấy Task cho bảng Lịch sử (30 ngày), hỗ trợ View Quản lý và lọc văn bản."""
        """
        Lấy danh sách Task theo loại bộ lọc (All/Pending/Completed/Risk/Help) trong 30 ngày qua.
        """
        date_limit, today_date = self._get_time_filter_params(days_ago)
        
        where_conditions = [f"TaskDate BETWEEN '{date_limit}' AND '{today_date}'"]
        
        if view_mode == 'SUPERVISOR':
            where_conditions.append(f"CapTren = '{user_code}'") # Lọc theo người GIAO VIỆC
        elif not is_admin: 
            where_conditions.append(f"UserCode = '{user_code}'") # Lọc theo người NHẬN VIỆC

        if filter_type == 'COMPLETED':
            where_conditions.append("Status = 'COMPLETED'")
        elif filter_type == 'RISK':
            where_conditions.append("Status IN ('PENDING', 'HELP_NEEDED', 'OPEN')")
        elif filter_type == 'HELP':
            where_conditions.append("Status = 'HELP_NEEDED'")
        elif filter_type == 'PENDING':
             where_conditions.append("Status IN ('PENDING', 'OPEN')")
        elif filter_type == 'ALL':
             pass
        
        # APPLY TEXT SEARCH FILTER (Yêu cầu 5)
        if text_search_term and text_search_term.strip():
            # NOTE: Không dùng tham số hóa vì tôi đang truyền chuỗi f-string
            terms = [t.strip() for t in text_search_term.split(';') if t.strip()]
            if terms:
                search_conditions = []
                for term in terms:
                    # Search logic là OR trên Title, DetailContent, và ObjectID
                    search_conditions.append(f"(Title LIKE '%{term}%' OR DetailContent LIKE '%{term}%' OR ObjectID LIKE '%{term}%')")
                where_conditions.append("(" + " OR ".join(search_conditions) + ")")

        query = f"""
            SELECT T1.*,
                   (SELECT COUNT(LogID) FROM {self.TASK_LOG_TABLE} T2 WHERE T2.TaskID = T1.TaskID) AS LogCount
            FROM {self.TASK_TABLE} AS T1
            WHERE {' AND '.join(where_conditions)}
            ORDER BY TaskDate DESC, LastUpdated DESC
        """
        data = self.db.get_data(query)
        data = self._enrich_tasks_with_client_name(data) # Gán tên KH
        data = self._enrich_tasks_with_user_info(data)
        return self._standardize_task_data(data)

    def get_kpi_summary(self, user_code, is_admin=False, days_ago=30):
        """Tính toán tổng Task trong 30 ngày qua."""
        
        date_limit, today_date = self._get_time_filter_params(days_ago)
        where_conditions = [f"TaskDate BETWEEN '{date_limit}' AND '{today_date}'"]
        
        if not is_admin:
            where_conditions.append(f"UserCode = '{user_code}'")

        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            SELECT 
                COUNT(TaskID) AS TotalTasks,
                SUM(CASE WHEN Status = 'COMPLETED' THEN 1 ELSE 0 END) AS Completed,
                SUM(CASE WHEN Status IN ('OPEN', 'PENDING') THEN 1 ELSE 0 END) AS Pending,
                SUM(CASE WHEN Status = 'HELP_NEEDED' THEN 1 ELSE 0 END) AS HelpNeeded
            FROM {self.TASK_TABLE}
            WHERE {where_clause}
        """
        
        data = self.db.get_data(query)
        summary = data[0] if data else {'TotalTasks': 0, 'Completed': 0, 'Pending': 0, 'HelpNeeded': 0}
        
        total = summary['TotalTasks'] or 0
        completed = summary['Completed'] or 0
        
        summary['CompletedPercent'] = round((completed / total) * 100) if total > 0 else 0
        
        return summary

    def get_user_tasks(self, user_code, month=None, is_admin=False):
        """Lấy danh sách Task của User hoặc tất cả Task (nếu là Admin) trong tháng."""
        
        where_conditions = ["1 = 1"]
        
        if not is_admin:
            where_conditions.append(f"UserCode = '{user_code}'")
        
        # Thêm logic lọc tháng nếu cần (tương tự như KPI summary)
        
        query = f"""
            SELECT 
                TaskID, UserCode, TaskDate, Status, Priority, Title, 
                ObjectID, DetailContent, NoteCapTren, NoteTimestamp, LastUpdated, CompletedDate
            FROM {self.TASK_TABLE}
            WHERE {' AND '.join(where_conditions)}
            ORDER BY TaskDate DESC, Priority DESC
        """
        return self.db.get_data(query)

        # BỔ SUNG: CHUẨN HÓA DỮ LIỆU NGAY TRƯỚC KHI TRẢ VỀ TEMPLATE
        if data:
            for task in data:
                # Kiểm tra nếu TaskDate là đối tượng ngày (date hoặc datetime)
                if isinstance(task.get('TaskDate'), (datetime, datetime.date)): 
                    task['TaskDateDisplay'] = task['TaskDate'].strftime('%d/%m')
                else:
                    # Nếu nó đã là chuỗi (trường hợp này là lỗi) hoặc không tồn tại, ta dùng giá trị thô
                    task['TaskDateDisplay'] = task.get('TaskDate')
                    
        return data # Trả về dữ liệu đã được bổ sung field TaskDateDisplay

    def update_task_progress(self, task_id, object_id, content, status, helper_code=None, completed_date=None):
        """
        [DEPRECATED WRAPPER] Hỗ trợ các API cũ. Chuyển hướng sang Log mới.
        """
        from flask import session # Đảm bảo session có sẵn nếu API cũ gọi

        # 1. Xác định Log Type và Progress dựa trên Status cũ
        if status.upper() == 'COMPLETED':
            log_type = self.LOG_TYPE_REQUEST_CLOSE
            progress_percent = 100
        elif status.upper() == 'HELP_NEEDED':
            log_type = self.LOG_TYPE_HELP_CALL
            progress_percent = 50 # Giả định 50% khi kêu gọi giúp đỡ
        else:
            log_type = self.LOG_TYPE_PROGRESS
            progress_percent = 50 

        # 2. Ghi Log Tiến độ (Dùng user hiện tại)
        log_id = self.log_task_progress(
            task_id=task_id, 
            user_code=session.get('user_code', 'SYSTEM'), 
            progress_percent=progress_percent, 
            content=content, 
            log_type=log_type, 
            helper_code=helper_code
        )
        
        # 3. Cập nhật ObjectID trên Task Master (Log không làm điều này)
        update_object_query = f"""
            UPDATE {self.TASK_TABLE}
            SET ObjectID = ?
            WHERE TaskID = ?
        """
        self.db.execute_non_query(update_object_query, (object_id, task_id))
        
        return log_id is not None

    def add_supervisor_note(self, task_id, supervisor_code, note):
        """Cấp trên note lên Task."""
        
        update_query = f"""
            UPDATE {self.TASK_TABLE} 
            SET NoteCapTren = ?, 
                NoteTimestamp = GETDATE(),
                SupervisorCode = ?
            WHERE TaskID = ?
        """
        params = (note, supervisor_code, task_id)
        
        try:
            return self.db.execute_non_query(update_query, params)
        except Exception as e:
            print(f"LỖI NOTE CẤP TRÊN: {e}")
            return False

    def get_task_by_id(self, task_id):
        """Hàm helper để lấy dữ liệu Task Master theo ID (bao gồm ProgressPercentage)."""
        query = f"SELECT * FROM {self.TASK_TABLE} WHERE TaskID = ?"
        data = self.db.get_data(query, (task_id,))
        
        # Bổ sung ProgressPercentage vào chuẩn hóa nếu nó chưa có
        standardized_data = self._standardize_task_data(data)
        
        return standardized_data[0] if standardized_data else None

    def update_task_priority(self, task_id, new_priority):
        """Cập nhật Priority Task."""
        update_query = f"""
            UPDATE {self.TASK_TABLE} 
            SET Priority = ?, LastUpdated = GETDATE() 
            WHERE TaskID = ?
        """
        params = (new_priority.upper(), task_id)
        try:
            return self.db.execute_non_query(update_query, params)
        except Exception as e:
            print(f"LỖI CẬP NHẬT PRIORITY: {e}")
            return False
    
    # THÊM: Hàm lấy danh sách Helper đủ điều kiện (Req 2)
    def get_eligible_helpers(self):
        """
        Lấy danh sách nhân viên hỗ trợ.
        YÊU CẦU 4: Lọc [PHONG BAN] IS NOT NULL và <> '9. DU HOC'.
        """
        query = f"""
            SELECT [USERCODE], [SHORTNAME] 
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE 
                [PHONG BAN] IS NOT NULL 
                AND RTRIM([PHONG BAN]) <> '9. DU HOC'
            ORDER BY [SHORTNAME]
        """
        return self.db.get_data(query)


    # THÊM: Hàm tạo Task mới cho Helper (Req 3)
    def create_help_request_task(self, helper_code, original_task_id, current_user_code, original_title, original_object_id, original_detail_content, new_task_type):
        
        # 1. KIỂM TRA MỐI QUAN HỆ (KD004 giao việc cho KD021)
        is_delegated_task = self._is_helper_subordinate(helper_code, current_user_code)

        # 2. XỬ LÝ NỘI DUNG VÀ ƯU TIÊN
        if is_delegated_task:
            # Logic Giao việc (Priority HIGH)
            new_priority = 'HIGH' 
            new_title = f"Y/c từ cấp trên - {current_user_code} - {original_title}"
            new_detail_content = f"Bạn vừa nhận được y/c: {original_detail_content}"
        else:
            # Logic Hỗ trợ (Priority ALERT)
            new_priority = 'ALERT' 
            new_title = f"HELP - [{current_user_code}] - {original_title}"
            new_detail_content = f"[Hãy giúp tôi:] {original_detail_content}"

        # 3. CHÈN TASK MỚI (Đã thêm TaskType)
        insert_query = f"""
            INSERT INTO {self.TASK_TABLE} (UserCode, TaskDate, Status, Priority, Title, CapTren, ObjectID, DetailContent, LastUpdated, SupervisorCode, TaskType)
            VALUES (?, GETDATE(), 'HELP_NEEDED', ?, ?, ?, ?, ?, GETDATE(), 'KD000', ?)
        """
        params = (
            helper_code, 
            new_priority,
            new_title, 
            current_user_code, 
            original_object_id, 
            new_detail_content,
            new_task_type  # Gán TaskType của task mới
        )
        
        try:
            return self.db.execute_non_query(insert_query, params)
        except Exception as e:
            print(f"LỖI TẠO TASK YÊU CẦU HỖ TRỢ/GIAO VIỆC: {e}")
            return False
    # --- PHẦN MỚI: Lấy chi tiết Log History ---
    def get_task_history_logs(self, task_id):
        """Lấy tất cả Log tiến độ cho một Task."""
        query = f"""
            SELECT 
                T1.LogID, T1.UpdateDate, T1.UserCode, T1.SupervisorCode,
                T1.ProgressPercentage, T1.UpdateContent, T1.TaskLogType, 
                T1.SupervisorFeedback, T1.FeedbackDate, T1.HelperRequestCode,
                T2.SHORTNAME AS UserShortName
            FROM {self.TASK_LOG_TABLE} AS T1
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T2 ON T1.UserCode = T2.USERCODE
            WHERE T1.TaskID = ?
            ORDER BY T1.UpdateDate DESC
        """
        data = self.db.get_data(query, (task_id,))
        # Cần chuẩn hóa Date/Time cho các cột LogID

        if not data:
            return []

        # FIX NaTType: Chuyển đổi NaT/None thành None để JSON hóa an toàn
        def safe_serialize_datetime(dt_obj):
            if pd.isna(dt_obj) or dt_obj is None:
                return None
            try:
                # Trả về chuỗi ISO format để JS dễ xử lý
                return dt_obj.isoformat() 
            except AttributeError:
                return None

        # Áp dụng serialization an toàn cho các cột DATETIME
        for log in data:
            log['UpdateDate'] = safe_serialize_datetime(log.get('UpdateDate'))
            log['FeedbackDate'] = safe_serialize_datetime(log.get('FeedbackDate'))
        return data
    
    # --- SỬA ĐỔI: CẬP NHẬT TIẾN ĐỘ THÀNH GHI LOG MỚI ---
    def log_task_progress(self, task_id, user_code, progress_percent, content, log_type, helper_code=None):
        """
        Ghi Log Tiến độ mới vào TPL. Cập nhật Status và Progress trên Task_Master.
        [SỬA ĐỔI]: Tự động tạo Task cho Helper nếu là Help Call.
        """
        
        # 1. Ghi Log vào TPL (Trả về LogID)
        log_id = self.db.log_progress_entry(
            task_id, user_code, progress_percent, content, log_type, helper_code
        )
        
        if not log_id: return False
        
        # 2. Xác định Status mới cho Master & Tạo Task Hỗ trợ
        if log_type == self.LOG_TYPE_BLOCKED:
             new_status = self.STATUS_BLOCKED
        elif log_type == self.LOG_TYPE_REQUEST_CLOSE:
             new_status = 'COMPLETED' if progress_percent == 100 else 'PENDING'
        elif log_type == self.LOG_TYPE_HELP_CALL:
             new_status = 'HELP_NEEDED'
             
             # [FIX] Tạo Task mới cho Helper ngay tại đây
             if helper_code:
                 original_task = self.get_task_by_id(task_id)
                 if original_task:
                     self.create_help_request_task(
                         helper_code=helper_code,
                         original_task_id=task_id,
                         current_user_code=user_code,
                         original_title=original_task.get('Title', 'Yêu cầu hỗ trợ'),
                         original_object_id=original_task.get('ObjectID'),
                         # Quan trọng: Truyền nội dung log hiện tại vào Task mới
                         original_detail_content=content, 
                         new_task_type=original_task.get('TaskType', 'KHAC')
                     )

        else:
             new_status = 'PENDING' 
        
        # 3. Cập nhật Master Task (Task_Master)
        update_master_query = f"""
            UPDATE {self.TASK_TABLE}
            SET LastUpdated = GETDATE(),
                ProgressPercentage = ?, 
                Status = ?,
                DetailContent = ?
            WHERE TaskID = ?
        """
        self.db.execute_non_query(update_master_query, (progress_percent, new_status, content, task_id))

        return log_id

        # --- SỬA ĐỔI: Phản hồi Cấp trên trên Log cụ thể ---
    def add_supervisor_feedback(self, log_id, supervisor_code, feedback):
        """Cấp trên note/feedback trên một LogID cụ thể."""
        
        # Đồng thời ghi một Log Type riêng cho phản hồi của cấp trên nếu cần
        # Ở đây ta chỉ cập nhật vào LogID đang được phản hồi.
        return self.db.execute_update_log_feedback(log_id, supervisor_code, feedback)

    def get_recently_updated_tasks(self, user_code, is_admin=False, view_mode='USER', minutes_ago=15):
        """Lấy danh sách Task có cập nhật (LastUpdated) trong N phút gần nhất."""
        
        minutes_ago_str = (datetime.now() - timedelta(minutes=minutes_ago)).strftime('%Y-%m-%d %H:%M:%S')
        
        where_conditions = [
            f"LastUpdated >= '{minutes_ago_str}'",
            f"Status IN ('OPEN', 'PENDING', 'HELP_NEEDED', 'COMPLETED')" 
        ]
        
        # Áp dụng bộ lọc quyền tương tự như get_kanban_tasks
        if view_mode == 'SUPERVISOR':
            where_conditions.append(f"CapTren = '{user_code}'") 
        elif not is_admin: 
            where_conditions.append(f"UserCode = '{user_code}'") 
        
        query = f"""
            SELECT TaskID, LastUpdated
            FROM {self.TASK_TABLE}
            WHERE {' AND '.join(where_conditions)}
            ORDER BY LastUpdated DESC
        """
        data = self.db.get_data(query)
        # Chỉ trả về TaskID và LastUpdated để giảm tải
        return [{'TaskID': task['TaskID'], 'LastUpdated': task['LastUpdated']} for task in data]
    
    def get_users_by_department(self, dept_code):
        """Lấy danh sách UserCode thuộc một bộ phận."""
        query = f"SELECT USERCODE FROM {config.TEN_BANG_NGUOI_DUNG} WHERE [BO PHAN] = ?"
        data = self.db.get_data(query, (dept_code,))
        return [row['USERCODE'] for row in data] if data else []

    def process_help_request_multicast(self, helper_codes_list, original_task_id, current_user_code, detail_content):
        """
        Xử lý yêu cầu hỗ trợ cho NHIỀU người (hoặc Bộ phận).
        """
        original_task = self.get_task_by_id(original_task_id)
        if not original_task: return False
        
        # 1. Làm phẳng danh sách (Xử lý nếu có mã Bộ phận)
        final_helpers = set()
        for code in helper_codes_list:
            if code.startswith('DEPT_'): # Quy ước mã bộ phận bắt đầu bằng DEPT_
                real_dept = code.replace('DEPT_', '')
                dept_users = self.get_users_by_department(real_dept)
                final_helpers.update(dept_users)
            else:
                final_helpers.add(code)
        
        # Loại bỏ chính mình (nếu lỡ chọn)
        if current_user_code in final_helpers:
            final_helpers.remove(current_user_code)

        # 2. Vòng lặp tạo Task cho từng người
        count = 0
        for helper in final_helpers:
            self.create_help_request_task(
                helper_code=helper,
                original_task_id=original_task_id,
                current_user_code=current_user_code,
                original_title=original_task.get('Title', 'N/A'),
                original_object_id=original_task.get('ObjectID', None),
                original_detail_content=detail_content,
                new_task_type='NOI_BO' # Hoặc giữ nguyên loại cũ
            )
            count += 1
            
        return count # Trả về số lượng task đã tạo    
    