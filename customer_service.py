# CRM STDD/customer_service.py

from datetime import datetime, timedelta
# Import thư viện cần thiết
import pandas as pd 
from db_manager import DBManager, safe_float
import config

class CustomerService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager
    # Hằng số cấu hình KPI (Có thể đưa vào config.py sau này)
    RISK_CONFIG = {
        'DELAY_DAYS_THRESHOLD': 10, # Cảnh báo nếu trạng thái CHỜ/DELAY > 10 ngày
        'NO_ACTION_DAYS_THRESHOLD': 5, # Cảnh báo nếu không có hành động trong 5 ngày
        'AVG_VALUE_THRESHOLD': 30000000.0, # Giá trị trung bình để phân loại (Ví dụ: 50 triệu)
    }

    def _calculate_quote_risk(self, quote, current_status):
        """Tính toán điểm rủi ro (Risk Score) cho từng báo giá."""
        
        score = 100 # Bắt đầu với điểm tuyệt đối 100
        risk_level = 'HEALTHY'
        notes = []
        
        quote_date = quote.get('QuoteDate')
        quote_value = safe_float(quote.get('QuoteValue'))
        
        # 1. PHÂN TÍCH TUỔI BG (AGING & STATUS)
        if current_status in ['CHỜ', 'DELAY']:
            if self._is_datetime_valid(quote_date):
                age_days = (datetime.now() - quote_date).days
                
                if age_days > self.RISK_CONFIG['DELAY_DAYS_THRESHOLD']:
                    score -= 25
                    notes.append(f"BG đã quá {self.RISK_CONFIG['DELAY_DAYS_THRESHOLD']} ngày ({age_days} ngày).")
                else:
                    score -= age_days * 1.5 # Giảm 1.5 điểm mỗi ngày
        
        # 2. PHÂN TÍCH GIÁ TRỊ (VALUE)
        if quote_value > self.RISK_CONFIG['AVG_VALUE_THRESHOLD']:
            score += 10 # BG giá trị lớn được ưu tiên điểm base cao hơn
            notes.append("Giá trị BG Cao (Ưu tiên theo dõi).")
        else:
            score -= 5

        # 3. PHÂN TÍCH HÀNH ĐỘNG (ACTION) - Giả định NGAY_CAP_NHAT là hành động cuối
        last_update = quote.get('LastUpdateDate') # Lấy từ CRM (NGAY_CAP_NHAT)
        if self._is_datetime_valid(last_update):
            days_since_action = (datetime.now() - last_update).days
            if days_since_action > self.RISK_CONFIG['NO_ACTION_DAYS_THRESHOLD']:
                score -= 30
                notes.append(f"Không có hành động cập nhật trong {days_since_action} ngày.")
            
        # 4. XỬ LÝ TRẠNG THÁI CUỐI CÙNG (FINAL STATUS ADJUSTMENT)
        if current_status in ['WIN', 'LOST', 'CANCEL']:
            score = 0 # Đã hoàn tất, không cần rủi ro
            risk_level = 'CLOSED'
        elif current_status in ['LOST', 'DELAY'] or score < 60:
            risk_level = 'HIGH'
        elif score < 80:
            risk_level = 'MEDIUM'

        final_score = max(0, score)
        return final_score, risk_level, notes
        
    def _safe_strftime(self, dt_obj):
        """
        Kiểm tra NaT/None an toàn và định dạng ra chuỗi datetime-local 
        (format: YYYY-MM-DDTHH:MM)
        """
        # 1. Xử lý trường hợp None hoặc Pandas NaT
        if dt_obj is None or pd.isna(dt_obj):
            return ''
        
        # 2. Định dạng chuỗi (đảm bảo là đối tượng datetime)
        try:
            return dt_obj.strftime('%Y-%m-%dT%H:%M')
        except Exception:
            return ''
            
    def _is_datetime_valid(self, dt_obj):
        """Kiểm tra xem đối tượng có phải là datetime hợp lệ (không phải None/NaT)"""
        # Kiểm tra NaT của Pandas và kiểu datetime của Python
        return isinstance(dt_obj, datetime) and not pd.isna(dt_obj)

    def get_quotes_for_input(self, user_code, date_from, date_to):
        """
        Truy vấn danh sách Báo giá (OT2101) của Salesman trong khoảng thời gian xác định 
        và JOIN với trạng thái cập nhật gần nhất từ CRM.
        """
        
        where_conditions = []
        where_params = []
        
        # Thêm điều kiện lọc ngày (Ngày Báo giá)
        where_conditions.append("T1.QuotationDate BETWEEN ? AND ?")
        where_params.extend([date_from, date_to])
        
        # Thêm điều kiện lọc theo Salesman
        where_conditions.append("T1.SalesManID = ?")
        where_params.append(user_code)
        
        where_clause = " AND ".join(where_conditions)
        
        # 1. Lấy thông tin Báo giá (Quote) từ ERP (OT2101)
        quote_query = f"""
            SELECT
                T1.QuotationNo AS QuoteID,
                T1.QuotationDate AS QuoteDate,
                T1.ObjectID AS ClientID,
                T2.ShortObjectName AS ClientName,
                T1.SaleAmount AS QuoteValue
            FROM {config.ERP_QUOTES} AS T1
            LEFT JOIN {config.ERP_IT1202} AS T2 ON T1.ObjectID = T2.ObjectID
            WHERE 
                {where_clause} 
            ORDER BY T1.QuotationDate DESC
        """
        quotes = self.db.get_data(quote_query, tuple(where_params))
        if not quotes:
            return []

        quote_ids = [f"'{q['QuoteID']}'" for q in quotes]
        quote_ids_str = ", ".join(quote_ids)

        # 2. Lấy trạng thái cập nhật gần nhất từ CRM
        status_query = f"""
            SELECT
                T1.MA_BAO_GIA, T1.TINH_TRANG_BG, T1.LY_DO_THUA, T1.NGAY_CAP_NHAT,
                T1.MA_HANH_DONG_1, T1.MA_HANH_DONG_2, 
                T1.THOI_GIAN_PHAT_SINH, T1.THOI_GIAN_HOAN_TAT 
            FROM {config.TEN_BANG_CAP_NHAT_BG} AS T1
            INNER JOIN (
                SELECT MA_BAO_GIA, MAX(NGAY_CAP_NHAT) AS MaxDate
                FROM {config.TEN_BANG_CAP_NHAT_BG}
                WHERE MA_BAO_GIA IN ({quote_ids_str})
                GROUP BY MA_BAO_GIA
            ) AS T2 
            ON T1.MA_BAO_GIA = T2.MA_BAO_GIA AND T1.NGAY_CAP_NHAT = T2.MaxDate
        """
        status_data = self.db.get_data(status_query)
        status_dict = {s['MA_BAO_GIA']: s for s in status_data}
        
        # 3. Hợp nhất dữ liệu và xử lý ngày giờ
        for quote in quotes:
            status = status_dict.get(quote['QuoteID'], {})
            
            time_start = status.get('THOI_GIAN_PHAT_SINH')
            time_completed = status.get('THOI_GIAN_HOAN_TAT')

            # Tính toán Tốc độ Phản ứng (Giờ)
            reaction_time = 'N/A'
            # CHỈ tính toán nếu cả hai trường là datetime hợp lệ (không phải NaT/None)
            if self._is_datetime_valid(time_start) and self._is_datetime_valid(time_completed):
                delta = time_completed - time_start
                reaction_time = f"{delta.total_seconds() / 3600:.1f}h" 

            # Cập nhật các trường mới
            quote['StatusCRM'] = status.get('TINH_TRANG_BG', 'CHỜ')
            quote['LossReason'] = status.get('LY_DO_THUA', '')
            quote['Action1'] = status.get('MA_HANH_DONG_1', '')
            quote['Action2'] = status.get('MA_HANH_DONG_2', '')
            quote['ReactionTime'] = reaction_time
            
            # 4. Trả về giá trị DATETIME/TEXT cho 2 ô nhập liệu bằng hàm an toàn
            # FIX: Sử dụng hàm an toàn để tránh lỗi NaTType does not support strftime
            quote['TimeStartValue'] = self._safe_strftime(time_start)
            quote['TimeCompleteValue'] = self._safe_strftime(time_completed)
            
        return quotes
    
    # --- HÀM MỚI (Hỗ trợ Chatbot) ---
    def get_customer_by_name(self, name_fragment):
        """
        Tìm kiếm khách hàng (giống api/khachhang) nhưng trả về data cho service.
        """
        
        # SỬA ĐỔI: Thêm "OR T1.ObjectName LIKE ?" vào truy vấn
        query = f"""
            SELECT TOP 5 T1.ObjectID AS ID, T1.ShortObjectName AS FullName
            FROM {config.ERP_IT1202} AS T1 
            WHERE 
                T1.ShortObjectName LIKE ? 
                OR T1.ObjectID LIKE ?
                OR T1.ObjectName LIKE ?
            ORDER BY T1.ShortObjectName
        """
        like_param = f'%{name_fragment}%'
        
        # SỬA ĐỔI: Truyền 3 tham số (thay vì 2)
        data = self.db.get_data(query, (like_param, like_param, like_param))
        return data if data else []