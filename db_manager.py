# db_manager.py

from flask import current_app
import pandas as pd
from sqlalchemy import create_engine, text
import config
import time
import math
import logging  # <--- Thêm dòng này
# =========================================================================
# HÀM HELPER XỬ LÝ DỮ LIỆU
# =========================================================================

def safe_float(value):
    """Xử lý an toàn giá trị None, chuỗi rỗng, 'None' hoặc 'nan' thành 0.0 float."""
    if value is None:
        return 0.0
    
    # Chuyển về chuỗi và xử lý chữ thường để bắt 'nan', 'none', ''
    str_val = str(value).strip().lower()
    
    if str_val in ['', 'none', 'nan']:
        return 0.0
        
    try:
        f_val = float(value)
        # Kiểm tra thêm nếu giá trị là vô cực hoặc NaN của Python math
        if math.isnan(f_val) or math.isinf(f_val):
            return 0.0
        return f_val
    except (ValueError, TypeError):
        return 0.0

def parse_filter_string(filter_str):
    """Phân tích chuỗi điều kiện lọc (Ví dụ: '>100' -> ('>', 100))."""
    import re
    if not filter_str:
        return None, None
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

# --- Lớp DBManager Chính ---
class DBManager:
    def __init__(self):
        # KHỞI TẠO SQLALCHEMY ENGINE VỚI CONNECTION POOL
        logging.info(f"--- Init DB Connection Pool to {config.DB_SERVER} ---")
        self.engine = create_engine(
            config.SQLALCHEMY_DATABASE_URI,
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=1800,
            fast_executemany=True 
        )
        
    # 1. PHƯƠNG THỨC TỐI ƯU (Dùng cho Dashboard/Report - Chỉ đọc)
    def get_data(self, query, params=None):
        """
        Thực thi SELECT dùng SQLAlchemy Pool + Pandas.
        """
        try:
            # Tự động lấy kết nối từ Pool
            with self.engine.connect() as conn:
                if params:
                    # Pandas read_sql hỗ trợ tốt việc truyền params qua SQLAlchemy driver
                    df = pd.read_sql(query, conn, params=params)
                else:
                    df = pd.read_sql(query, conn)
                
                # Logic làm sạch dữ liệu
                for col in df.select_dtypes(include=['object']).columns:
                    def clean_cell(x):
                        if x is None: return ''
                        if isinstance(x, bytes):
                            return x.decode('utf-8', errors='ignore')
                        return str(x).strip()
                    
                    df[col] = df[col].apply(clean_cell)
                
                return df.to_dict('records')

        except Exception as e:
            # Chỉ in mã lỗi dạng ASCII an toàn hoặc encode/replace
            try:
                error_msg = str(e).encode('utf-8', 'replace').decode('utf-8')
                current_app.logger.error(f"Lỗi get_data (Hybrid): {error_msg}")
            except:
                current_app.logger.error("Lỗi get_data (Hybrid): (Lỗi Unicode khi in log)")
                
            return []

    # 2. PHƯƠNG THỨC THỰC THI (QUAN TRỌNG: ĐÃ SỬA ĐỂ DÙNG RAW CONNECTION)
    def execute_non_query(self, query, params=None):
        """
        Thực thi INSERT/UPDATE/DELETE.
        Sử dụng Raw Connection để hỗ trợ cú pháp '?' của code cũ.
        """
        conn = None
        try:
            # Lấy kết nối thô từ Pool
            conn = self.engine.raw_connection()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            conn.commit()
            return True
        except Exception as e:
            current_app.logger.error(f"Lỗi execute_non_query: {e}")
            return False
        finally:
            if conn: conn.close()

    # 3. PHƯƠNG THỨC TƯƠNG THÍCH NGƯỢC (LEGACY SUPPORT)

    def get_transaction_connection(self):
        """Trả về kết nối thô (Raw PyODBC Connection) từ Pool."""
        return self.engine.raw_connection()
    
    def commit(self, conn):
        conn.commit()

    def rollback(self, conn):
        conn.rollback()

    def execute_query_in_transaction(self, conn, query, params=None):
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        return cursor

    def execute_sp_multi(self, sp_name, params=None):
        """Thực thi Stored Procedure trả về NHIỀU bảng (Multi-ResultSet)."""
        conn = None
        results = []
        try:
            conn = self.engine.raw_connection() # Lấy kết nối từ Pool
            cursor = conn.cursor()
            
            # Xây dựng câu lệnh EXEC
            param_placeholders = ', '.join(['?' for _ in params]) if params else ''
            sql = f"EXEC {sp_name} {param_placeholders}"
            
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            
            # Loop qua từng Result Set
            while True:
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    data = cursor.fetchall()
                    if data:
                        df = pd.DataFrame.from_records(data, columns=columns)
                        # Clean data
                        for col in df.select_dtypes(include=['object']).columns:
                             df[col] = df[col].fillna('').astype(str).str.strip()
                        results.append(df.to_dict('records'))
                    else:
                        results.append([])
                
                if not cursor.nextset():
                    break
            
            conn.commit() # Commit nếu SP có ghi dữ liệu tạm
            return results

        except Exception as e:
            current_app.logger.error(f"Lỗi execute_sp_multi: {e}")
            return [[]] # Trả về list rỗng an toàn
        finally:
            if conn:
                conn.close() # Trả kết nối về Pool

    # --- CÁC HÀM CỤ THỂ KHÁC ---

    def write_audit_log(self, user_code, action_type, severity, details, ip_address):
        """Ghi log hệ thống (Dùng raw_connection để an toàn)."""
        try:
            conn = self.engine.raw_connection()
            cursor = conn.cursor()
            query = "INSERT INTO dbo.AUDIT_LOGS (UserCode, ActionType, Severity, Details, IPAddress) VALUES (?, ?, ?, ?, ?)"
            cursor.execute(query, (user_code, action_type, severity, details, ip_address))
            conn.commit()
            conn.close()
        except Exception:
            pass # Log lỗi không nên làm crash app

    def log_progress_entry(self, task_id, user_code, progress_percent, content, log_type, helper_code=None):
        """Ghi log Task (Dùng OUTPUT INSERTED -> Bắt buộc Raw Connection)."""
        conn = None
        try:
            conn = self.engine.raw_connection()
            cursor = conn.cursor()
            query = f"""
                INSERT INTO {config.TASK_LOG_TABLE} (
                    TaskID, UserCode, UpdateDate, ProgressPercentage, UpdateContent, TaskLogType, HelperRequestCode
                )
                OUTPUT INSERTED.LogID
                VALUES (?, ?, GETDATE(), ?, ?, ?, ?);
            """
            cursor.execute(query, (task_id, user_code, progress_percent, content, log_type, helper_code))
            row = cursor.fetchone()
            conn.commit()
            return int(row[0]) if row else None
        except Exception as e:
            current_app.logger.error(f"Lỗi log_progress_entry: {e}")
            return None
        finally:
            if conn: conn.close()

    def execute_update_log_feedback(self, log_id, supervisor_code, feedback):
        query = f"""
            UPDATE {config.TASK_LOG_TABLE}
            SET SupervisorFeedback = ?, SupervisorCode = ?, FeedbackDate = GETDATE()
            WHERE LogID = ?
        """
        # Tái sử dụng execute_non_query đã sửa
        return self.execute_non_query(query, (feedback, supervisor_code, log_id))
    
    def get_khachhang_by_ma(self, ma_doi_tuong):
        """Helper lấy tên khách hàng."""
        query = f"SELECT TOP 1 [TEN DOI TUONG] AS FullName FROM dbo.{config.TEN_BANG_KHACH_HANG} WHERE [MA DOI TUONG] = ?"
        data = self.get_data(query, (ma_doi_tuong,))
        return data[0]['FullName'] if data else None