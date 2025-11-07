from db_manager import DBManager, safe_float
import datetime as dt # Import thư viện gốc với alias
from datetime import datetime, timedelta # Import các đối tượng phổ biến
import config
import math

class TaskService:
    """Xử lý toàn bộ logic nghiệp vụ liên quan đến quản lý đầu việc (Task Management)."""
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager
        self.TASK_TABLE = config.TASK_TABLE if hasattr(config, 'TASK_TABLE') else 'dbo.Task_Master'

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
            SELECT [ROLE] FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ? AND RTRIM([ROLE]) = 'ADMIN'
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
            INSERT INTO {self.TASK_TABLE} (UserCode, TaskDate, Status, Title, CapTren, Attachments, TaskType, ObjectID, LastUpdated)
            VALUES (?, GETDATE(), 'OPEN', ?, ?, ?, ?, ?, GETDATE())
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
        date_today = datetime.now().strftime('%Y-%m-%d')
        
        where_conditions = [
            f"TaskDate BETWEEN '{date_limit}' AND '{date_today}'",
            f"Status IN ('OPEN', 'PENDING', 'HELP_NEEDED', 'COMPLETED')" 
        ]
        
        if view_mode == 'SUPERVISOR':
            where_conditions.append(f"CapTren = '{user_code}'") # Lọc theo người GIAO VIỆC
        elif not is_admin: 
            where_conditions.append(f"UserCode = '{user_code}'") # Lọc theo người NHẬN VIỆC
        
        query = f"""
            SELECT *
            FROM {self.TASK_TABLE}
            WHERE {' AND '.join(where_conditions)}
            ORDER BY TaskDate DESC, LastUpdated DESC
        """
        data = self.db.get_data(query)
        data = self._enrich_tasks_with_client_name(data)
        data = self._enrich_tasks_with_user_info(data) # Lấy Assignee ShortName
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
            SELECT *
            FROM {self.TASK_TABLE}
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
        """Cập nhật tiến độ hoàn thành Task cuối ngày."""
        
        # 1. Xây dựng Set Clauses
        set_clauses = ["ObjectID = ?", "DetailContent = ?", "Status = ?", "LastUpdated = GETDATE()"]
        params = [object_id, content, status.upper()]
        
        # 2. Xử lý trạng thái hoàn thành
        if status.upper() == 'COMPLETED' and not completed_date:
            set_clauses.append("CompletedDate = GETDATE()")
        
        # 3. Xử lý gán người hỗ trợ/giao việc (YÊU CẦU 3)
        if status.upper() == 'HELP_NEEDED' and helper_code:
            set_clauses.append("SupervisorCode = ?") # SupervisorCode = Assignee ID
            params.append(helper_code) 
            
        update_query = f"""
            UPDATE {self.TASK_TABLE} 
            SET {', '.join(set_clauses)}
            WHERE TaskID = ?
        """
        params.append(task_id)
        
        try:
            return self.db.execute_non_query(update_query, tuple(params))
        except Exception as e:
            print(f"LỖI CẬP NHẬT TASK: {e}")
            return False

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
        """Hàm helper để lấy dữ liệu Task theo ID."""
        query = f"SELECT * FROM {self.TASK_TABLE} WHERE TaskID = ?"
        data = self.db.get_data(query, (task_id,))
        return self._standardize_task_data(data)[0] if data else None

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
        query = f"""
            SELECT [USERCODE], [SHORTNAME] 
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE 
                [BO PHAN] IS NOT NULL 
                AND RTRIM([BO PHAN]) NOT LIKE '9. DU HOC%'
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
    