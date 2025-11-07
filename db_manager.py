# db_manager.py

import pyodbc
import pandas as pd
import re
import config # Import config để lấy CONNECTION_STRING

# =========================================================================
# HÀM HELPER XỬ LÝ DỮ LIỆU (Utility Functions)
# =========================================================================

def safe_float(value):
    """Xử lý an toàn giá trị None, chuỗi rỗng hoặc chuỗi 'None' thành 0.0 float."""
    if value is None or str(value).strip() == '' or str(value).strip().lower() == 'none':
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0
# app.py (Thêm hàm helper mới)

def truncate_content(text, max_lines=5):
    """
    Cắt nội dung văn bản dài thành tối đa N dòng, giữ định dạng xuống dòng và thêm '...'.
    """
    if not text:
        return ""
        
    lines = text.split('\n')
    
    if len(lines) <= max_lines:
        return text # Trả về toàn bộ nếu nội dung ngắn hơn 5 dòng

    # Cắt 5 dòng đầu tiên
    truncated_lines = lines[:max_lines]
    
    # Hợp nhất và thêm dấu ba chấm
    return '\n'.join(truncated_lines) + '...'
    
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
        
    # THÊM HÀM HELPER NÀY (YÊU CẦU BỞI upsert_cost_override)
    def _get_connection(self):
        """Tạo và trả về một đối tượng kết nối thô (raw connection object)."""
        import pyodbc # Đảm bảo pyodbc đã được import ở đầu file
        return pyodbc.connect(self.conn_str)

    def get_data(self, query, params=None):
        """
        Thực thi truy vấn SELECT và trả về danh sách dict.
        FIX: Chuẩn hóa việc sử dụng cursor.execute và fetchall() để tránh blocking 
        trên pandas.read_sql khi không có tham số (Source of UserWarning/Concurrency Issue).
        """
        conn = None
        try:
            conn = pyodbc.connect(self.conn_str)
            cursor = conn.cursor() # Luôn khởi tạo cursor
            
            if params:
                 cursor.execute(query, params)
            else:
                 cursor.execute(query) # Vẫn sử dụng execute ngay cả khi không có params
                 
            # Lấy dữ liệu chung cho cả hai trường hợp
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()
            df = pd.DataFrame.from_records(data, columns=columns)
            
            # Xử lý các cột đặc biệt (ví dụ: CAP TREN)
            if config.TEN_BANG_NGUOI_DUNG.strip('[]') in query and 'CAP TREN' in df.columns:
                df['CAP TREN'] = df['CAP TREN'].fillna('').astype(str) 

            # LÀM SẠCH CHUỖI VÀ CHUYỂN VỀ DICT
            for col in df.select_dtypes(include=['object']).columns:
                 df[col] = df[col].astype(str).str.strip().replace('nan', '').replace('None', '')
                 
            return df.to_dict('records')
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"LỖI SQL - CODE: {sqlstate}, QUERY: {query}")
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
        """Hàm helper lấy tên khách hàng cho form NSLH - DÙNG THAM SỐ HÓA."""
        query = f"""
            SELECT TOP 1 [TEN DOI TUONG] AS FullName
            FROM dbo.{config.TEN_BANG_KHACH_HANG}
            WHERE [MA DOI TUONG] = ?
        """
        # Sử dụng phương thức get_data của chính lớp này
        data = self.get_data(query, (ma_doi_tuong,))
        if data:
            return data[0]['FullName']
        return None
        
    def execute_sp_multi(self, sp_name, params=None):
        """Thực thi SP và trả về nhiều Result Set (đã fix lỗi NoneType)."""
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
                if cursor.description is None:
                    if cursor.nextset() == True:
                        continue
                    else:
                        break

                columns = [column[0] for column in cursor.description]
                data = cursor.fetchall()
                
                if data:
                    df = pd.DataFrame.from_records(data, columns=columns)
                    for col in df.select_dtypes(include=['object']).columns:
                         df[col] = df[col].astype(str).str.strip().replace('nan', '').replace('None', '')
                    results.append(df.to_dict('records'))
                else:
                     results.append([])

                if not cursor.nextset():
                    break

            while len(results) < 5:
                 results.append([])
                 
            return results
            
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"LỖI SQL SP - CODE: {sqlstate}, SP: {sp_name}, Params: {params}")
            return [[]] * 5
        finally:
            if conn:
                conn.close()
    
    def get_transaction_connection(self):
        """Tạo và trả về kết nối thô (raw connection) cho Service Layer quản lý."""
        try:
            # Giả định self.conn_str đã được định nghĩa trong __init__
            return pyodbc.connect(self.conn_str)
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"LỖI KẾT NỐI CSDL: {sqlstate}") 
            raise 
            
    def commit(self, conn):
        """Xác nhận các thay đổi đang chờ trên kết nối được cung cấp."""
        if conn:
            conn.commit()

    def rollback(self, conn):
        """Hủy bỏ các thay đổi đang chờ trên kết nối được cung cấp."""
        if conn:
            conn.rollback()

    def execute_query_in_transaction(self, conn, query, params=None):
        """
        Thực thi INSERT/UPDATE/DELETE trong một giao dịch đang mở (KHÔNG COMMIT HOẶC CLOSE).
        """
        try:
            cursor = conn.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            return cursor.rowcount
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            print(f"LỖI SQL TRADING - CODE: {sqlstate}, QUERY: {query}")
            raise # Re-raise để Service Layer bắt và rollback