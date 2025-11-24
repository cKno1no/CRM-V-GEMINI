# db_manager.py

import pyodbc
import pandas as pd
import re
import config 

# =========================================================================
# HÀM HELPER XỬ LÝ DỮ LIỆU
# =========================================================================

def safe_float(value):
    """Xử lý an toàn giá trị None, chuỗi rỗng hoặc chuỗi 'None' thành 0.0 float."""
    if value is None or str(value).strip() == '' or str(value).strip().lower() == 'none':
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0

def parse_filter_string(filter_str):
    """Phân tích chuỗi điều kiện lọc (Ví dụ: '>100' -> ('>', 100))."""
    filter_str = filter_str.replace(' ', '')
    match = re.match(r"([<>=!]+)([0-9,.]+)", filter_str)
    if match:
        operator = match.group(1)
        threshold = safe_float(match.group(2).replace(',', '').replace('.', '')) 
        return operator, threshold
    return None, None

def evaluate_condition(value, operator, threshold):
    """Đánh giá điều kiện (> < =)."""
    if operator == '>': return value > threshold
    elif operator == '<': return value < threshold
    elif operator == '=' or operator == '==': return value == threshold
    elif operator == '>=': return value >= threshold
    elif operator == '<=': return value <= threshold
    elif operator == '!=': return value != threshold
    return True

# =========================================================================
# DATA ACCESS LAYER (DAL)
# =========================================================================

class DBManager:
    """
    Lớp Quản lý Truy cập Dữ liệu (DAL).
    Xử lý tất cả các tương tác CSDL.
    """
    def __init__(self):
        self.conn_str = config.CONNECTION_STRING
        
    def _get_connection(self):
        return pyodbc.connect(self.conn_str)

    def get_data(self, query, params=None):
        """
        Thực thi truy vấn SELECT và trả về danh sách dict.
        [FIX]: Đã thêm xử lý UnicodeDecodeError cho dữ liệu bẩn.
        """
        conn = None
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
            if params:
                 cursor.execute(query, params)
                 columns = [column[0] for column in cursor.description]
                 data = cursor.fetchall()
                 df = pd.DataFrame.from_records(data, columns=columns)
            else:
                 df = pd.read_sql(query, conn)
            
            # Xử lý cột CAP TREN (Tránh lỗi NoneType đặc thù)
            if config.TEN_BANG_NGUOI_DUNG.strip('[]') in query and 'CAP TREN' in df.columns:
                df['CAP TREN'] = df['CAP TREN'].fillna('').astype(str) 

            # LÀM SẠCH CHUỖI VÀ CHUYỂN VỀ DICT (FIX LỖI UNICODE TẠI ĐÂY)
            for col in df.select_dtypes(include=['object']).columns:
                 
                 # Hàm xử lý từng ô dữ liệu an toàn
                 def clean_cell_data(x):
                     if x is None: return ''
                     if isinstance(x, bytes):
                         # Thử giải mã với các bảng mã phổ biến ở VN
                         for encoding in ['utf-8', 'cp1252', 'cp1258', 'latin1']:
                             try:
                                 return x.decode(encoding)
                             except UnicodeDecodeError:
                                 continue
                         # Nếu tất cả thất bại, ép giải mã và bỏ qua ký tự lỗi
                         return x.decode('utf-8', errors='ignore')
                     return str(x).strip()

                 # Áp dụng hàm xử lý thay vì dùng astype(str)
                 df[col] = df[col].apply(clean_cell_data).replace(['nan', 'None'], '')
                 
            return df.to_dict('records')

        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"LỖI SQL - CODE: {sqlstate}, QUERY: {query}")
            return None
        except Exception as e:
            print(f"LỖI HỆ THỐNG (get_data): {e}")
            return None
        finally:
            if conn:
                conn.close()

    def execute_non_query(self, query, params=None):
        """Thực thi INSERT/UPDATE/DELETE."""
        conn = None
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            conn.commit()
            return True
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"LỖI SQL - CODE: {sqlstate}, QUERY: {query}")
            return False
        finally:
            if conn:
                conn.close()

    def get_khachhang_by_ma(self, ma_doi_tuong):
        """Hàm helper lấy tên khách hàng."""
        query = f"""
            SELECT TOP 1 [TEN DOI TUONG] AS FullName
            FROM dbo.{config.TEN_BANG_KHACH_HANG}
            WHERE [MA DOI TUONG] = ?
        """
        data = self.get_data(query, (ma_doi_tuong,))
        if data:
            return data[0]['FullName']
        return None
        
    def execute_sp_multi(self, sp_name, params=None):
        """Thực thi SP và trả về tất cả các Result Set."""
        conn = None
        results = [] 
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            
            param_placeholders = ', '.join(['?' for _ in params]) if params else ''
            sql_command = f"EXEC {sp_name} {param_placeholders}"
            
            if params:
                cursor.execute(sql_command, params)
            else:
                cursor.execute(sql_command)
                
            while True: 
                if cursor.description: 
                    columns = [column[0] for column in cursor.description]
                    data = cursor.fetchall()
                    if data:
                        df = pd.DataFrame.from_records(data, columns=columns)
                        
                        # Áp dụng logic làm sạch tương tự (FIX LỖI UNICODE CHO SP)
                        for col in df.select_dtypes(include=['object']).columns:
                             df[col] = df[col].apply(lambda x: str(x).strip() if x is not None else '').replace(['nan', 'None'], '')
                             
                        results.append(df.to_dict('records'))
                    else:
                         results.append([])
                
                if not cursor.nextset():
                    break 

            return results
            
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"LỖI SQL SP - CODE: {sqlstate}, SP: {sp_name}, Params: {params}")
            return [[]] 
        finally:
            if conn:
                conn.close()
    
    def get_transaction_connection(self):
        try:
            return pyodbc.connect(self.conn_str)
        except pyodbc.Error as ex:
            print(f"LỖI KẾT NỐI CSDL: {ex.args[0]}") 
            raise 
            
    def commit(self, conn):
        if conn:
            conn.commit()

    def rollback(self, conn):
        if conn:
            conn.rollback()

    def execute_query_in_transaction(self, conn, query, params=None):
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.rowcount
        except pyodbc.Error as ex:
            print(f"LỖI SQL TRADING: {ex.args[0]}")
            raise 
        
    def write_audit_log(self, user_code, action_type, severity, details, ip_address):
        query = """
            INSERT INTO dbo.AUDIT_LOGS 
                (UserCode, ActionType, Severity, Details, IPAddress)
            VALUES (?, ?, ?, ?, ?)
        """
        conn = None
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            cursor.execute(query, (user_code, action_type, severity, details, ip_address))
            conn.commit()
        except Exception as e:
            print(f"LỖI GHI AUDIT LOG (Bỏ qua): {e}")
        finally:
            if conn:
                conn.close()

    def log_progress_entry(self, task_id, user_code, progress_percent, content, log_type, helper_code=None):
        query = f"""
            INSERT INTO {config.TASK_LOG_TABLE} (
                TaskID, UserCode, UpdateDate, ProgressPercentage, UpdateContent, TaskLogType, HelperRequestCode
            )
            OUTPUT INSERTED.LogID
            VALUES (?, ?, GETDATE(), ?, ?, ?, ?);
        """
        conn = None
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor()
            cursor.execute(query, (task_id, user_code, progress_percent, content, log_type, helper_code))
            log_id = cursor.fetchone()[0] 
            conn.commit()
            return int(log_id)
        except pyodbc.Error as ex:
            print(f"LỖI SQL - TASK LOG: {ex.args[0]}")
            return None
        finally:
            if conn:
                conn.close()
                
    def execute_update_log_feedback(self, log_id, supervisor_code, feedback):
        query = f"""
            UPDATE {config.TASK_LOG_TABLE}
            SET SupervisorFeedback = ?,
                SupervisorCode = ?,
                FeedbackDate = GETDATE()
            WHERE LogID = ?
        """
        return self.execute_non_query(query, (feedback, supervisor_code, log_id))