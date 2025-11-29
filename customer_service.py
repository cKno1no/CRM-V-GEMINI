# services/customer_service.py

from datetime import datetime, timedelta
import pandas as pd 
from db_manager import DBManager, safe_float
import config

class CustomerService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager
        # Đã chuyển RISK_CONFIG sang config.py

    def _calculate_quote_risk(self, quote, current_status):
        """Tính toán điểm rủi ro (Risk Score) cho từng báo giá."""
        
        score = 100 # Bắt đầu với điểm tuyệt đối 100
        risk_level = 'HEALTHY'
        notes = []
        
        quote_date = quote.get('QuoteDate')
        quote_value = safe_float(quote.get('QuoteValue'))
        
        # 1. PHÂN TÍCH TUỔI BG (AGING & STATUS)
        # [CONFIG]: Sử dụng QUOTE_STATUS_PENDING, QUOTE_STATUS_DELAY
        if current_status in [config.QUOTE_STATUS_PENDING, config.QUOTE_STATUS_DELAY]:
            if self._is_datetime_valid(quote_date):
                age_days = (datetime.now() - quote_date).days
                
                # [CONFIG]: Sử dụng QUOTE_RISK_DELAY_DAYS
                if age_days > config.QUOTE_RISK_DELAY_DAYS:
                    score -= 25
                    notes.append(f"BG đã quá {config.QUOTE_RISK_DELAY_DAYS} ngày ({age_days} ngày).")
                else:
                    score -= age_days * 1.5 
        
        # 2. PHÂN TÍCH GIÁ TRỊ (VALUE)
        # [CONFIG]: Sử dụng QUOTE_RISK_AVG_VALUE
        if quote_value > config.QUOTE_RISK_AVG_VALUE:
            score += 10 
            notes.append("Giá trị BG Cao (Ưu tiên theo dõi).")
        else:
            score -= 5

        # 3. PHÂN TÍCH HÀNH ĐỘNG (ACTION)
        last_update = quote.get('LastUpdateDate')
        if self._is_datetime_valid(last_update):
            days_since_action = (datetime.now() - last_update).days
            # [CONFIG]: Sử dụng QUOTE_RISK_NO_ACTION_DAYS
            if days_since_action > config.QUOTE_RISK_NO_ACTION_DAYS:
                score -= 30
                notes.append(f"Không có hành động cập nhật trong {days_since_action} ngày.")
            
        # 4. XỬ LÝ TRẠNG THÁI CUỐI CÙNG
        # [CONFIG]: Sử dụng các trạng thái kết thúc
        closed_statuses = [config.QUOTE_STATUS_WIN, config.QUOTE_STATUS_LOST, config.QUOTE_STATUS_CANCEL]
        
        if current_status in closed_statuses:
            score = 0 
            risk_level = 'CLOSED'
        elif current_status in [config.QUOTE_STATUS_LOST, config.QUOTE_STATUS_DELAY] or score < 60:
            risk_level = 'HIGH'
        elif score < 80:
            risk_level = 'MEDIUM'

        final_score = max(0, score)
        return final_score, risk_level, notes
        
    def _safe_strftime(self, dt_obj):
        if dt_obj is None or pd.isna(dt_obj): return ''
        try: return dt_obj.strftime('%Y-%m-%dT%H:%M') # Format cho input datetime-local
        except Exception: return ''
            
    def _is_datetime_valid(self, dt_obj):
        return isinstance(dt_obj, datetime) and not pd.isna(dt_obj)

    def get_quotes_for_input(self, user_code, date_from, date_to):
        """
        Truy vấn danh sách Báo giá và JOIN với trạng thái cập nhật.
        """
        where_conditions = ["T1.QuotationDate BETWEEN ? AND ?"]
        where_params = [date_from, date_to]
        
        where_conditions.append("T1.SalesManID = ?")
        where_params.append(user_code)
        
        where_clause = " AND ".join(where_conditions)
        
        # [CONFIG]: ERP_QUOTES, ERP_IT1202
        quote_query = f"""
            SELECT
                T1.QuotationNo AS QuoteID,
                T1.QuotationDate AS QuoteDate,
                T1.ObjectID AS ClientID,
                T2.ShortObjectName AS ClientName,
                T1.SaleAmount AS QuoteValue
            FROM {config.ERP_QUOTES} AS T1
            LEFT JOIN {config.ERP_IT1202} AS T2 ON T1.ObjectID = T2.ObjectID
            WHERE {where_clause} 
            ORDER BY T1.QuotationDate DESC
        """
        quotes = self.db.get_data(quote_query, tuple(where_params))
        if not quotes: return []

        quote_ids = [f"'{q['QuoteID']}'" for q in quotes]
        quote_ids_str = ", ".join(quote_ids)

        # [CONFIG]: TEN_BANG_CAP_NHAT_BG
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
        
        for quote in quotes:
            status = status_dict.get(quote['QuoteID'], {})
            
            # Lấy thông tin cập nhật
            time_start = status.get('THOI_GIAN_PHAT_SINH')
            time_completed = status.get('THOI_GIAN_HOAN_TAT')
            quote['LastUpdateDate'] = status.get('NGAY_CAP_NHAT') # Dùng cho Risk Calc

            reaction_time = 'N/A'
            if self._is_datetime_valid(time_start) and self._is_datetime_valid(time_completed):
                delta = time_completed - time_start
                reaction_time = f"{delta.total_seconds() / 3600:.1f}h" 

            # [CONFIG]: Mặc định trạng thái là PENDING (CHỜ)
            current_status = status.get('TINH_TRANG_BG', config.QUOTE_STATUS_PENDING)
            
            quote['StatusCRM'] = current_status
            quote['LossReason'] = status.get('LY_DO_THUA', '')
            quote['Action1'] = status.get('MA_HANH_DONG_1', '')
            quote['Action2'] = status.get('MA_HANH_DONG_2', '')
            quote['ReactionTime'] = reaction_time
            
            quote['TimeStartValue'] = self._safe_strftime(time_start)
            quote['TimeCompleteValue'] = self._safe_strftime(time_completed)

            # Tính toán Risk
            score, level, notes = self._calculate_quote_risk(quote, current_status)
            quote['RiskScore'] = score
            quote['RiskLevel'] = level
            quote['RiskNotes'] = "\n".join(notes)
            
        return quotes
    
    def get_customer_by_name(self, name_fragment):
        """Tìm kiếm khách hàng (cho Chatbot)."""
        # [CONFIG]: ERP_IT1202
        query = f"""
            SELECT TOP 5 
                T1.ObjectID AS ID, 
                T1.ShortObjectName AS FullName,
                T1.Address AS Address
            FROM {config.ERP_IT1202} AS T1 
            WHERE 
                T1.ShortObjectName LIKE ? 
                OR T1.ObjectID LIKE ?
                OR T1.ObjectName LIKE ?
            ORDER BY T1.ShortObjectName
        """
        search_term = f"%{name_fragment}%"
        return self.db.get_data(query, (search_term, search_term, search_term)) or []