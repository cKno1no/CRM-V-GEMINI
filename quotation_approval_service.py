from datetime import datetime
from db_manager import DBManager, safe_float
import config
import math

class QuotationApprovalService:
    """Xử lý toàn bộ logic nghiệp vụ liên quan đến phê duyệt báo giá."""
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def is_user_admin(self, user_code):
        """Kiểm tra xem user_code có vai trò Admin hay không."""
        query = f"""
            SELECT 1 
            FROM {config.TEN_BANG_NGUOI_DUNG}
            WHERE USERCODE = ? AND Role = 'ADMIN'
        """
        return bool(self.db.get_data(query, (user_code,)))

    
    def get_quotes_for_approval(self, user_code, date_from, date_to):
        """
        1. Truy vấn các chào giá CHỈ được gán duyệt cho current_user TRONG 7 NGÀY.
        2. Áp dụng INNER JOIN Quyền duyệt (OT0006) và Lọc Ngày.
        """
        
        user_code_lower = user_code.lower() 
        is_admin = self.is_user_admin(user_code) 
        where_params = [date_from, date_to]
        join_clause = ""
        
        # 2. XỬ LÝ LỌC QUYỀN DUYỆT GÁN (Nếu KHÔNG phải Admin)
        if not is_admin:
            join_clause = f"""
                INNER JOIN (
                    SELECT VoucherTypeID 
                    FROM {config.ERP_APPROVER_MASTER} 
                    WHERE LOWER(Approver) = '{user_code_lower}'
                ) AS T_APPROVER 
                ON T_APPROVER.VoucherTypeID = T1.VoucherTypeID 
            """

        # 3. TRUY VẤN CHÍNH
        where_conditions = ["T1.OrderStatus = 0", "T1.QuotationDate BETWEEN ? AND ?"] # FIX: Status=0
        where_clause = " AND ".join(where_conditions)
        
        quote_query = f"""
            SELECT 
                T1.QuotationID, T1.QuotationNo, T1.QuotationDate, 
                T1.SaleAmount, T1.SalesManID, T1.EmployeeID,     -- <<< LẤY EmployeeID CHO NGUOILAM
                T1.VoucherTypeID, T1.ObjectID AS ClientID, 
                ISNULL(T2.ShortObjectName, 'N/A') AS ClientName,
                ISNULL(T2.O05ID, 'N/A') AS CustomerClass, 
                ISNULL(T6.SHORTNAME, 'N/A') AS SalesAdminName,   -- Tên TKKD/EmployeeID
                ISNULL(T7.SHORTNAME, 'N/A') AS NVKDName,         -- Tên NVKD/SalesManID
                SUM(T4.ConvertedAmount) AS TotalSaleAmount, 
                
                SUM(T4.QuoQuantity * COALESCE(
                        T8.Cost,                   
                        T5.Recievedprice,          
                        0                          
                    )
                ) AS TotalCost,
                
                MIN(
                    CASE 
                        WHEN (T5.SalePrice01 IS NULL OR T5.SalePrice01 <= 1) OR 
                             (T5.Recievedprice IS NULL OR T5.Recievedprice <= 2)
                        THEN 1 
                        ELSE 0 
                    END
                ) AS NeedsCostOverride,
                
                MAX(CASE WHEN T8.Cost IS NOT NULL AND T8.Cost > 0 THEN 1 ELSE 0 END) AS HasCostOverrideData
                
            FROM {config.ERP_QUOTES} AS T1                        
            {join_clause} 
            
            LEFT JOIN {config.ERP_IT1202} AS T2 ON T1.ObjectID = T2.ObjectID   
            LEFT JOIN {config.ERP_QUOTE_DETAILS} AS T4 ON T1.QuotationID = T4.QuotationID 
            LEFT JOIN {config.ERP_ITEM_PRICING} AS T5 ON T4.InventoryID = T5.InventoryID        
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T6 ON T1.EmployeeID = T6.USERCODE
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T7 ON T1.SalesManID = T7.USERCODE 
            
            LEFT JOIN {config.BOSUNG_CHAOGIA_TABLE} AS T8 ON T4.TransactionID = T8.TransactionID

            WHERE {where_clause} 
            
            GROUP BY 
                T1.QuotationID, T1.QuotationNo, T1.QuotationDate, T1.SaleAmount, T1.SalesManID, 
                T1.VoucherTypeID, T1.ObjectID, T1.EmployeeID, 
                ISNULL(T2.ShortObjectName, 'N/A'), ISNULL(T2.O05ID, 'N/A'), 
                ISNULL(T6.SHORTNAME, 'N/A'), ISNULL(T7.SHORTNAME, 'N/A')
            
            ORDER BY T1.QuotationDate ASC
        """
        
        quotes = self.db.get_data(quote_query, tuple(where_params))
        if not quotes: return []

        results = []
        for quote in quotes:
            # Gán EmployeeID vào trường EmployeeID để HTML đọc NGUOILAM
            quote['EmployeeID'] = quote.get('EmployeeID')

            if quote.get('NeedsCostOverride') == 1 and quote.get('HasCostOverrideData') != 1:
                quote['ApprovalResult'] = {
                    'Passed': False, 
                    'Reason': 'PENDING: Thiếu Giá QD. Cần Bổ sung!',
                    'ApproverRequired': user_code,
                    'ApproverDisplay': user_code,
                    'ApprovalRatio': 0
                }
                quote['CanOpenCostOverride'] = True
                results.append(quote)
                continue

            # 2. Tính toán Approval Criteria như bình thường
            quote['CanOpenCostOverride'] = quote.get('NeedsCostOverride') == 1 and quote.get('HasCostOverrideData') == 1
            results.append(self._check_approval_criteria(quote, user_code))
            
        return results

    def _check_approval_criteria(self, quote, current_user_code):
        """Kiểm tra các quy tắc nghiệp vụ (tính tỷ số duyệt, xác định người duyệt cuối)."""
        
        # Khởi tạo trạng thái mặc định (có thể là trạng thái sẵn sàng duyệt nếu pass)
        approval_status = {'Passed': True, 'Reason': 'OK', 'ApproverRequired': current_user_code, 'ApproverDisplay': 'TỰ DUYỆT (SELF)', 'ApprovalRatio': 0}
        quote['ApprovalResult'] = approval_status
        
        total_sale = safe_float(quote.get('TotalSaleAmount'))
        total_cost = safe_float(quote.get('TotalCost'))
        customer_class = quote.get('CustomerClass')
        sale_amount = safe_float(quote.get('SaleAmount')) 
        
        # 1. KIỂM TRA ĐẦY ĐỦ TRƯỜNG & GIÁ
        if not quote.get('SalesManID') or total_sale == 0 or total_cost == 0:
            approval_status['Passed'] = False
            approval_status['Reason'] = 'FAILED: Thiếu NVKD hoặc Giá QD.'
            return quote 

        # 2. KIỂM TRA TỶ SỐ DUYỆT
        ratio = 0
        required_ratio = 0
        if total_cost > 0 and total_sale > 0:
            ratio = 30 + 100 * (total_sale / total_cost)
            approval_status['ApprovalRatio'] = min(9999, round(ratio))
            
            if customer_class == 'M':
                required_ratio = 150
            elif customer_class == 'T':
                required_ratio = 138
                
            if required_ratio > 0 and ratio < required_ratio:
                approval_status['Passed'] = False
                # SỬA LỖI: Chuyển FAILED (lỗi cứng) thành PENDING (lỗi lợi nhuận)
                approval_status['Reason'] = f'PENDING: Tỷ số ({round(ratio)}) < Y/C ({required_ratio}).'
                # THÊM CỜ: Cho phép Ghi đè
                approval_status['NeedsOverride'] = True
            
        elif total_cost == 0 or total_sale == 0:
             # Đây vẫn là lỗi cứng
            approval_status['Reason'] = 'FAILED: Không tính được Tỷ số Duyệt (Sale/Cost = 0).'
            approval_status['Passed'] = False


        # 3. XÁC ĐỊNH NGƯỜI DUYỆT (Quyết định hành động cuối cùng)
        if sale_amount >= 20000000.0: # Ngưỡng 20 Triệu: Phải qua logic duyệt phức tạp
            
            approver_data = self.db.get_data(f"SELECT Approver FROM {config.ERP_APPROVER_MASTER} WHERE VoucherTypeID = ?", (quote.get('VoucherTypeID'),))
            
            approvers = [d['Approver'] for d in approver_data] if approver_data else []
            approvers_str = ", ".join(approvers)
            
            if not approvers:
                approvers = ['ADMIN']
                approvers_str = 'ADMIN'
            
            approval_status['ApproverDisplay'] = approvers_str
            approval_status['ApproverRequired'] = approvers_str 
            
            is_current_user_approver = current_user_code in approvers # Dùng current_user_code gốc

            if not approval_status['Passed']:
                # Nếu tỷ số duyệt thất bại
                approval_status['Reason'] = f"PENDING: Không đủ LN."
            
            elif not is_current_user_approver:
                 # Nếu người dùng không phải người duyệt bắt buộc
                 approval_status['Passed'] = False
                 approval_status['Reason'] = f"PENDING: Chờ {approvers_str}."
            
            # Trường hợp còn lại: Tỷ số OK VÀ người dùng là người duyệt bắt buộc -> Passed=True (Sẵn sàng duyệt)
        
        else: # sale_amount < 20000000.0: Tự duyệt
             # Giữ nguyên Passed=True và ApproverDisplay='TỰ DUYỆT (SELF)'
             pass

        quote['ApprovalResult'] = approval_status
        return quote

    def get_quote_details(self, quote_id):
        """Truy vấn chi tiết mặt hàng cho Panel Detail (giữ nguyên logic ban đầu)."""
        
        detail_query = f"""
            SELECT
                T1.InventoryID AS MaHang, T1.QuoQuantity AS SoLuong, T1.UnitPrice AS DonGia,
                T1.ConvertedAmount AS ThanhTien, T1.Notes,
                ISNULL(T1.InventoryCommonName, T2.InventoryName) AS TenHang,  
                T2.SalePrice01 AS DonGiaQuyDinh, T2.Recievedprice AS GiaMuaQuyDinh

            FROM {config.ERP_QUOTE_DETAILS} AS T1 
            LEFT JOIN {config.ERP_ITEM_PRICING} AS T2 ON T1.InventoryID = T2.InventoryID 
            
            WHERE T1.QuotationID = ?
            ORDER BY T1.Orders
        """
        
        try:
            details = self.db.get_data(detail_query, (quote_id,))
        except Exception as e:
            print(f"LỖI SQL Chi tiết BG {quote_id}: {e}")
            return []
            
        if not details: return []
        
        for detail in details:
            detail['SoLuong'] = f"{safe_float(detail.get('SoLuong')):.0f}"
            detail['DonGia'] = f"{safe_float(detail.get('DonGia')):,.0f}"
            detail['DonGiaQuyDinh'] = f"{safe_float(detail.get('DonGiaQuyDinh')):,.0f}"
            detail['ThanhTien'] = f"{safe_float(detail.get('ThanhTien')):,.0f}"
            
        return details

    def get_quote_cost_details(self, quotation_id):
        """
        Truy vấn chi tiết mặt hàng cho Form bổ sung Cost.
        JOIN OT2102, OT2101, IT1302, và BOSUNG_CHAOGIA.
        """
        # QUAN TRỌNG: Chỉ lấy các trường cần thiết theo Yêu cầu 2
        query = f"""
            SELECT
                T3.TransactionID,
                T3.QuotationID,
                T2.QuotationNo,
                T3.InventoryID,
                ISNULL(T3.InventoryCommonName, T5.InventoryName) AS InventoryName,
                T3.QuoQuantity,
                T3.UnitPrice,
                T5.Recievedprice, -- Giá nhập quy định (IT1302)
                T5.SalePrice01,   -- Giá bán quy định (IT1302)
                T6.Cost,          -- Giá Cost đã nhập (BOSUNG_CHAOGIA)
                T6.NOTE           -- Ghi chú đã nhập (BOSUNG_CHAOGIA)
            FROM {config.ERP_QUOTES} AS T2 -- OT2101
            INNER JOIN {config.ERP_QUOTE_DETAILS} AS T3 ON T2.QuotationID = T3.QuotationID -- OT2102
            LEFT JOIN {config.ERP_ITEM_PRICING} AS T5 ON T3.InventoryID = T5.InventoryID -- IT1302
            LEFT JOIN {config.BOSUNG_CHAOGIA_TABLE} AS T6 ON T3.TransactionID = T6.TransactionID -- Bảng bổ sung
            WHERE 
                T2.QuotationID = ?
                -- LỌC: CHỈ HIỂN THỊ CÁC MẶT HÀNG BỊ THIẾU GIÁ HOẶC ĐÃ CÓ OVERRIDE
                AND (
                    T6.TransactionID IS NOT NULL OR -- Đã có override
                    T5.SalePrice01 IS NULL OR T5.SalePrice01 <= 1 OR -- Thiếu SalePrice01
                    T5.Recievedprice IS NULL OR T5.Recievedprice <= 2  -- Thiếu Recievedprice
                )
            ORDER BY T3.Orders
        """
        return self.db.get_data(query, (quotation_id,))

    def upsert_cost_override(self, updates, user_code):
        """
        Xóa các bản ghi Cost Override cũ và INSERT dữ liệu mới trong một Transaction.
        """
        db = self.db
        conn = None
        
        transaction_ids = [u['transaction_id'] for u in updates]
        placeholders = ', '.join(['?' for _ in transaction_ids]) 
        
        delete_query = f"""
            DELETE FROM {config.BOSUNG_CHAOGIA_TABLE} 
            WHERE TransactionID IN ({placeholders})
        """
        
        insert_base = f"""
            INSERT INTO {config.BOSUNG_CHAOGIA_TABLE} 
            (TransactionID, Cost, NOTE, CREATEUSER, CREATEDATE) 
            VALUES (?, ?, ?, ?, GETDATE())
        """
        
        insert_params = []
        for u in updates:
            # Chuyển đổi dữ liệu thành dạng tuple
            insert_params.append((
                u['transaction_id'], 
                u['cost'], 
                u['note'], 
                user_code
            ))
            
        try:
            conn = db.get_transaction_connection()
            cursor = conn.cursor()

            # 1. Xóa dữ liệu cũ (Sử dụng execute_query_in_transaction)
            if transaction_ids:
                 db.execute_query_in_transaction(conn, delete_query, transaction_ids)
            
            # 2. Thực hiện batch INSERT (executemany)
            # Dùng cursor trực tiếp trên conn để thực hiện executemany
            cursor.executemany(insert_base, insert_params)
            
            # 3. COMMIT
            db.commit(conn) 
            return {"success": True, "message": "Bổ sung Cost thành công. Tỷ số duyệt sẽ được tính lại."}
        
        except Exception as e:
            if conn:
                db.rollback(conn) 
            # Đảm bảo bạn thấy LỖI này trong Console/Log file
            print(f"LỖI UPSERT COST OVERRIDE (CRITICAL): {e}") 
            return {"success": False, "message": f"Lỗi hệ thống khi lưu Cost Override: {str(e)}"}
        finally:
            if conn:
                conn.close()
    
    def approve_quotation(self, quotation_no, quotation_id, object_id, employee_id, approval_ratio, current_user):
        """
        Thực hiện phê duyệt Chào Giá (Cập nhật ERP: Status = 1) 
        và lưu log vào bảng DUYETCT trong cùng một Transaction.
        """
        db = self.db
        conn = None
        
        try:
            conn = db.get_transaction_connection()
            
            # 1. Lấy thông tin cần thiết từ ERP
            query_get_data = f"""
                SELECT T1.SaleAmount AS QuotationAmount, T1.QuotationDate
                FROM {config.ERP_QUOTES} AS T1 -- OT2101
                WHERE T1.QuotationID = ? 
            """
            data_detail = db.get_data(query_get_data, (quotation_id,)) 
            
            if not data_detail:
                raise Exception(f"Không tìm thấy dữ liệu Chào Giá {quotation_no} trong ERP.")
            
            detail = data_detail[0]
            sale_amount = safe_float(detail.get('QuotationAmount'))
            quotation_date = detail.get('QuotationDate')

            # 2. Thực hiện phê duyệt trong ERP (Update Status = 1)
            update_query_erp = f"""
                UPDATE {config.ERP_QUOTES} -- OT2101
                SET OrderStatus = 1 
                WHERE QuotationNo = ?
            """
            db.execute_query_in_transaction(conn, update_query_erp, (quotation_no,)) 
            
            # 3. Lưu log vào bảng DUYETCT
            insert_query_log = f"""
                INSERT INTO DUYETCT ( 
                    MACT, NGayCT, TySoDuyetCT, NGUOILAM, Tonggiatri, 
                    MasoCT, MaKH, NguoiDuyet, Ngayduyet, TINHTRANG
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), 0)
            """
            params = (
                quotation_no, quotation_date, approval_ratio, employee_id, 
                sale_amount, quotation_id, object_id, current_user
            )

            db.execute_query_in_transaction(conn, insert_query_log, params)
            
            # 4. COMMIT - Hoàn tất giao dịch
            db.commit(conn) 

            return {"success": True, "message": f"Chào giá {quotation_no} đã duyệt thành công và lưu log."}

        except Exception as e:
            # FIX: BỎ ROLLBACK THEO YÊU CẦU VÀ RE-RAISE ĐỂ HIỂN THỊ LỖI THÔ TRÊN TERMINAL
            raise e
        finally:
            if conn:
                conn.close()
    # CRM STDD/quotation_approval_service.py
# ... (thêm vào cuối class QuotationApprovalService) ...

    def update_quote_salesman(self, quotation_id, new_salesman_id):
        """
        Cập nhật SalesManID (NVKD) cho một Chào giá (OT2101) dựa trên QuotationID.
        """
        db = self.db
        
        # Cập nhật bảng OT2101 (Bảng Header của Chào giá)
        update_query = f"""
            UPDATE {config.ERP_QUOTES} 
            SET SalesManID = ? 
            WHERE QuotationID = ?
        """
        
        try:
            # Sử dụng execute_non_query (tự động commit)
            if db.execute_non_query(update_query, (new_salesman_id, quotation_id)):
                return {"success": True, "message": "Cập nhật NVKD thành công."}
            else:
                return {"success": False, "message": "Lệnh UPDATE không thực thi."}
                
        except Exception as e:
            print(f"LỖI UPDATE SALESMAN (SERVICE): {e}")
            return {"success": False, "message": f"Lỗi hệ thống: {str(e)}"}