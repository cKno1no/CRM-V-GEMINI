# services/sales_order_approval_service.py

from flask import current_app
from datetime import datetime
from db_manager import DBManager, safe_float
import config

class SalesOrderApprovalService:
    """Xử lý toàn bộ logic nghiệp vụ liên quan đến phê duyệt Đơn hàng bán (DHB)."""
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_orders_for_approval(self, user_code, user_role, date_from=None, date_to=None):
        """
        Truy vấn danh sách Đơn hàng bán chờ duyệt (OrderStatus = 0).
        Logic: Admin thấy hết. User thấy phiếu MÌNH TẠO (EmployeeID) VÀ CÓ QUYỀN DUYỆT.
        """
        
        # 1. Xử lý ngày tháng
        if not date_from or not date_to:
            now = datetime.now()
            date_from = datetime(now.year, now.month, 1).strftime('%Y-%m-%d')
            if now.month == 12:
                date_to = datetime(now.year + 1, 1, 1).strftime('%Y-%m-%d')
            else:
                date_to = datetime(now.year, now.month + 1, 1).strftime('%Y-%m-%d')

        # 2. Xây dựng điều kiện WHERE
        # [RULE 4]: Phiếu chưa duyệt có OrderStatus = 0
        where_conditions = ["T1.OrderStatus = 0"] 
        where_params = []
        
        if date_from and date_to:
             where_conditions.append("T1.OrderDate BETWEEN ? AND ?") 
             where_params.extend([date_from, date_to])
        
        # [RULE 1 & 2]: Phân quyền
        is_admin = (user_role in [config.ROLE_ADMIN, config.ROLE_GM])
        
        if not is_admin:
            # Điều kiện A: Người tạo phiếu (Dùng EmployeeID theo yêu cầu)
            where_conditions.append("T1.EmployeeID = ?")
            where_params.append(user_code)
            
            # Điều kiện B: Có quyền duyệt loại phiếu này (Check bảng OT0006)
            # (Logic: Chỉ thấy phiếu mình tạo MÀ mình cũng có quyền duyệt)
            where_conditions.append("""
                EXISTS (
                    SELECT 1 FROM OT0006 
                    WHERE VoucherTypeID = T1.VoucherTypeID 
                    AND Approver = ?
                )
            """)
            where_params.append(user_code)

        where_clause = " AND ".join(where_conditions)
        
        # 3. Câu Query F-string
        # [RULE 3]: Mã số phiếu là VoucherNo (OrderID), ID phiếu là SorderID
        order_query = f"""
            SELECT 
                T1.VoucherNo AS OrderID,    -- Mã hiển thị (DDH/...)
                T1.SOrderID,                -- ID duy nhất (Primary Key) - CHỈ SELECT 1 LẦN
                T1.OrderDate,               
                T1.SaleAmount AS SaleAmount,
                T1.SalesManID,
                T1.EmployeeID,
                T1.VoucherTypeID, 
                T1.ObjectID AS ClientID, 
                ISNULL(T2.ShortObjectName, 'N/A') AS ClientName,
                ISNULL(T2.O05ID, 'N/A') AS CustomerClass,         
                ISNULL(T6.SHORTNAME, 'N/A') AS SalesAdminName,  
                ISNULL(T7.SHORTNAME, 'N/A') AS NVKDName,        
                
                -- Đã xóa dòng T1.SOrderID thừa ở đây --
                
                SUM(T4.ConvertedAmount) AS TotalSaleAmount, 
                SUM(T4.OrderQuantity * ISNULL(T5.Recievedprice, 0)) AS TotalCost,
                
                MIN(CAST(CASE WHEN T4.Date01 IS NULL THEN 0 ELSE 1 END AS INT)) AS HasAllDate01,
                MIN(CAST(CASE WHEN T4.QuotationID IS NULL THEN 0 ELSE 1 END AS INT)) AS IsFullyQuoted

            FROM {config.ERP_OT2001} AS T1                        
            LEFT JOIN {config.ERP_IT1202} AS T2 ON T1.ObjectID = T2.ObjectID   
            LEFT JOIN {config.ERP_SALES_DETAIL} AS T4 ON T1.SOrderID = T4.SOrderID
            LEFT JOIN {config.ERP_ITEM_PRICING} AS T5 ON T4.InventoryID = T5.InventoryID        
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T6 ON T1.EmployeeID = T6.USERCODE 
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T7 ON T1.SalesManID = T7.USERCODE 
            
            WHERE {where_clause}
            
            GROUP BY 
                T1.VoucherNo, T1.SOrderID, T1.OrderDate, T1.SaleAmount, T1.SalesManID, T1.EmployeeID, 
                T1.VoucherTypeID, T1.ObjectID, T2.ShortObjectName, T2.O05ID, 
                T6.SHORTNAME, T7.SHORTNAME
            
            ORDER BY T1.OrderDate DESC
        """
        
        try:
            orders = self.db.get_data(order_query, tuple(where_params)) 
        except Exception as e:
            current_app.logger.error(f"LỖI TRUY VẤN DUYỆT ĐƠN HÀNG BÁN: {e}")
            return [] 
            
        if not orders: return []

        results = []
        for order in orders:
            if order is None or not isinstance(order, dict): continue
            
            # 4. Kiểm tra logic nghiệp vụ để trả về ApprovalResult cho HTML
            processed_order = self._check_approval_criteria(order, user_code)
            
            # Đảm bảo có kết quả duyệt để HTML không bị lỗi
            if processed_order and processed_order.get('ApprovalResult') is not None:
                results.append(processed_order)
            else:
                current_app.logger.warning(f"Bỏ qua đơn hàng {order.get('OrderID')} do lỗi xử lý criteria.")
        
        return results

    def _check_approval_criteria(self, order, current_user_code):
        """
        Kiểm tra các quy tắc nghiệp vụ cho DHB.
        LOGIC ƯU TIÊN: Ratio (Cao nhất) -> Auto-Approve (Hạn mức) -> Quyền duyệt cấp cao.
        """
        
        approval_status = {
            'Passed': True, 
            'Reason': 'OK', 
            'ApproverRequired': current_user_code, 
            'ApproverDisplay': 'TỰ DUYỆT (SELF)', 
            'ApprovalRatio': 0,
            'NeedsOverride': False
        }
        order['ApprovalResult'] = approval_status
        
        # Lấy dữ liệu an toàn
        total_sale = safe_float(order.get('TotalSaleAmount'))
        total_cost = safe_float(order.get('TotalCost'))
        customer_class = str(order.get('CustomerClass', '')).strip().upper()
        sale_amount = safe_float(order.get('SaleAmount')) 
        voucher_type = str(order.get('VoucherTypeID', '')).strip()
        
        has_all_date01 = order.get('HasAllDate01') == 1
        is_fully_quoted = order.get('IsFullyQuoted') == 1

        # --- 1. KIỂM TRA ĐIỀU KIỆN CƠ BẢN (Validation) ---
        if not order.get('SalesManID') or total_sale == 0 or total_cost == 0 or not has_all_date01:
            approval_status['Passed'] = False
            approval_status['Reason'] = 'FAILED: Thiếu SalesmanID/Giá/Chi phí (Cost)/Date01 không đầy đủ.'
            return order
        
        # --- 2. KIỂM TRA KẾ THỪA (Trừ DTK) ---
        if voucher_type != 'DTK' and not is_fully_quoted:
             approval_status['Passed'] = False
             approval_status['Reason'] = 'FAILED: DHB không 100% kế thừa từ Chào giá.'
             return order

        # [NEW RULE]: CHỐNG GIAN LẬN DDH (DDH FRAUD PROTECTION)
        # Logic: DDH là hàng đặt (không có tồn). Nếu check View Tồn kho thấy
        # đang có hàng (đáp ứng > Threshold %) -> Bắt đổi sang SO/SIG.
        # =================================================================
        if voucher_type == 'DDH':
            is_fraud, fraud_msg = self._validate_ddh_stock(order.get('SOrderID'))
            
            if is_fraud:
                approval_status['Passed'] = False
                approval_status['Reason'] = f'VIOLATION: {fraud_msg}'
                # Set NeedsOverride = True nếu muốn cho Sếp ghi đè, 
                # hoặc False nếu muốn chặn tuyệt đối (ở đây tôi để True để linh hoạt)
                approval_status['NeedsOverride'] = True 
                return order  # <--- RETURN NGAY, CHẶN LẠI
        # =================================================================
        # [NEW RULE]: KIỂM TRA PHÂN LOẠI SO vs SIG
        # Logic: SO chỉ dành cho đơn nhỏ (< 20tr). Nếu >= 20tr bắt buộc dùng SIG.
        # Ngăn chặn việc dùng sai loại phiếu để lách quy trình báo cáo.
        # =================================================================
        # Lấy dữ liệu và Hạn mức
        sale_amount = safe_float(order.get('SaleAmount')) 
        voucher_type = str(order.get('VoucherTypeID', '')).strip()
        limit_so = getattr(config, 'LIMIT_AUTO_APPROVE_SO', 20000000)

        if voucher_type == 'SO' and sale_amount >= limit_so:
             approval_status['Passed'] = False
             approval_status['Reason'] = (f"VIOLATION: Đơn hàng {sale_amount/1000000:,.1f}M >= {limit_so/1000000:,.1f}M. "
                                          f"Vui lòng hủy và tạo phiếu SIG.")
             approval_status['NeedsOverride'] = False # Chặn tuyệt đối, không cho sếp override cái sai này
             return order

        # --- 3. KIỂM TRA RATIO (ĐIỀU KIỆN TIÊN QUYẾT) ---
        # Nếu Fail Ratio -> Chặn ngay lập tức, không xét hạn mức nữa.
        
        required_ratio = 0
        ratio = 0
        
        if total_cost > 0:
            ratio = 30 + 100 * (total_sale / total_cost)
            approval_status['ApprovalRatio'] = min(9999, round(ratio))
            
            # Lấy cấu hình tỷ lệ yêu cầu
            required_ratio = config.RATIO_REQ_CLASS_M # Mặc định cao nhất (Safe Default)
            if customer_class == 'T': required_ratio = config.RATIO_REQ_CLASS_T
            # Nếu là M hoặc NULL đều dính 150
            
            
            # [CHỐT CHẶN RATIO]
            if required_ratio > 0 and ratio < required_ratio:
                approval_status['Passed'] = False
                approval_status['Reason'] = f'FAILED: Tỷ số ({round(ratio)}) < Y/C ({required_ratio}). (Không cho phép duyệt lỗ).'
                approval_status['NeedsOverride'] = True
                return order  # <--- RETURN NGAY, CHẶN LẠI HẾT
                
        else:
             approval_status['Passed'] = False
             approval_status['Reason'] = 'FAILED: Cost = 0 (Lỗi dữ liệu).'
             return order

        # --- 4. XÁC ĐỊNH QUYỀN DUYỆT & AUTO-APPROVE ---
        # (Chỉ chạy xuống đây khi Ratio đã OK)

        limit_dtk = config.LIMIT_AUTO_APPROVE_DTK
        limit_so = getattr(config, 'LIMIT_AUTO_APPROVE_SO', 20000000) 

        is_auto_approve = False
        auto_reason = ""

        if voucher_type == 'DTK':
            if sale_amount < limit_dtk:
                is_auto_approve = True
                auto_reason = f"TỰ DUYỆT (DTK < {limit_dtk/1000000:,.0f}M)"
        else:
            if sale_amount < limit_so:
                is_auto_approve = True
                auto_reason = f"TỰ DUYỆT (SO < {limit_so/1000000:,.0f}M)"

        # --- 5. KẾT HỢP LOGIC ---
        
        if is_auto_approve:
            # Ratio OK + Hạn mức nhỏ -> TỰ DUYỆT
            approval_status['Passed'] = True
            approval_status['Reason'] = f"OK: {auto_reason}"
            approval_status['ApproverDisplay'] = auto_reason
            approval_status['ApproverRequired'] = current_user_code 
        
        else:
            # Ratio OK + Hạn mức lớn -> CẦN SẾP DUYỆT
            
            # Lấy danh sách sếp
            approver_query = f"SELECT Approver FROM {config.ERP_APPROVER_MASTER} WHERE VoucherTypeID = ?"
            approver_data = self.db.get_data(approver_query, (voucher_type,))
            approvers = [d['Approver'].strip() for d in approver_data if d.get('Approver')] if approver_data else []
            if not approvers: approvers = [config.ROLE_ADMIN]
            approvers_str = ", ".join(approvers)
            
            approval_status['ApproverDisplay'] = approvers_str
            approval_status['ApproverRequired'] = approvers_str
            
            # Kiểm tra quyền
            is_authorized = current_user_code in approvers

            if not is_authorized:
                approval_status['Passed'] = False
                approval_status['Reason'] = f"PENDING: Vượt hạn mức, chờ duyệt bởi {approvers_str}."
            else:
                approval_status['Passed'] = True
                approval_status['Reason'] = "OK: Đủ điều kiện duyệt."

        return order
    
    def _validate_ddh_stock(self, sorder_id):
        """
        Kiểm tra tỷ lệ đáp ứng tồn kho (Fulfillable Ratio).
        Chỉ tính lượng tồn kho CÓ THỂ dùng cho đơn hàng này, loại bỏ phần tồn kho dư thừa (Outliers).
        """
        try:
            threshold = getattr(config, 'DDH_FRAUD_THRESHOLD', 30.0)

            # [LOGIC MỚI]: Sử dụng CASE WHEN để lấy MIN(OrderQty, Stock)
            # Ý nghĩa: Nếu tồn kho (T2.Ton) lớn hơn số đặt (T1.OrderQuantity), 
            # chỉ tính phần bằng số đặt. Ngược lại lấy số tồn thực tế.
            sql = f"""
                SELECT 
                    SUM(T1.OrderQuantity) AS TotalOrder,
                    
                    SUM(
                        CASE 
                            WHEN ISNULL(T2.Ton, 0) >= T1.OrderQuantity THEN T1.OrderQuantity
                            ELSE ISNULL(T2.Ton, 0)
                        END
                    ) AS TotalFulfillable
                    
                FROM {config.ERP_SALES_DETAIL} AS T1
                LEFT JOIN {config.VIEW_BACK_ORDER} AS T2 ON T1.InventoryID = T2.InventoryID
                WHERE T1.SOrderID = ?
            """
            
            data = self.db.get_data(sql, (sorder_id,))
            
            if not data: return False, "OK"
            
            total_order = safe_float(data[0]['TotalOrder'])
            total_fulfillable = safe_float(data[0]['TotalFulfillable']) # Lượng hàng có thể lấy ngay từ kho
            
            if total_order == 0: return False, "OK"

            # Tính tỷ lệ đáp ứng
            fulfillable_ratio = (total_fulfillable / total_order) * 100
            
            # So sánh với ngưỡng (30%)
            if fulfillable_ratio > threshold:
                return True, (f"Phát hiện {round(fulfillable_ratio)}% đơn hàng vi phạm. "
                              f"DDH chỉ dùng để đặt hàng theo PO của khách. Vui lòng làm đúng chứng từ.")
            
            return False, "OK"

        except Exception as e:
            current_app.logger.error(f"Lỗi check tồn kho DDH (Fulfillable Logic) {sorder_id}: {e}")
            return False, "System Error Check Stock"
        
    def get_order_details(self, sorder_id):
        """
        Truy vấn chi tiết mặt hàng cho Panel Detail DHB.
        """
        try:
            detail_query = f"""
                SELECT
                    T1.InventoryID AS MaHang, 
                    T1.OrderQuantity AS SoLuong,      
                    T1.SalePrice AS DonGia,        
                    T1.ConvertedAmount AS ThanhTien, 
                    T1.Notes, T1.QuotationID AS MaBaoGia, T1.Date01,
                    
                    T2.InventoryName AS TenHang,  
                    
                    T2.SalePrice01 AS DonGiaQuyDinh,     
                    T2.Recievedprice AS GiaMuaQuyDinh

                FROM {config.ERP_SALES_DETAIL} AS T1
                LEFT JOIN {config.ERP_ITEM_PRICING} AS T2 ON T1.InventoryID = T2.InventoryID
                
                WHERE T1.SOrderID = ? 
                ORDER BY T1.Orders
            """
            
            details = self.db.get_data(detail_query, (sorder_id,))
            
        except Exception as e:
            current_app.logger.error(f"LỖI SQL Chi tiết DHB {sorder_id}: {e}")
            return []
            
        if not details: return []
        
        for detail in details:
            detail['SoLuong'] = f"{safe_float(detail.get('SoLuong')):.0f}"
            detail['DonGia'] = f"{safe_float(detail.get('DonGia')):,.0f}"
            detail['DonGiaQuyDinh'] = f"{safe_float(detail.get('DonGiaQuyDinh')):,.0f}"
            detail['ThanhTien'] = f"{safe_float(detail.get('ThanhTien')):,.0f}"
            
            date01_obj = detail.get('Date01')
            if isinstance(date01_obj, datetime):
                 detail['Date01'] = date01_obj.strftime('%d/%m/%Y')
            else:
                 detail['Date01'] = 'N/A'
            
        return details
    
    
    def approve_sales_order(self, sorder_no, sorder_id, object_id, employee_id, approval_ratio, current_user):
        """
        Thực hiện phê duyệt Đơn hàng.
        [UPDATED]: Dùng SOrderID làm Key chính để UPDATE và SELECT.
        sorder_id: SO2026... (Khóa chính)
        sorder_no: DDH/... (Chỉ dùng để ghi log Audit cho dễ đọc)
        """
        db = self.db
        conn = None
        
        # [QUAN TRỌNG] Clean input sorder_id để tránh khoảng trắng thừa gây lỗi
        clean_sorder_id = sorder_id.strip() if sorder_id else ""

        if not clean_sorder_id:
             return {"success": False, "message": "Lỗi: Không nhận được SOrderID (Mã hệ thống)."}

        try:
            conn = db.get_transaction_connection()
            
            # 1. KIỂM TRA TỒN TẠI BẰNG SOrderID (Chính xác tuyệt đối)
            query_get_info = f"""
                SELECT SalesManID, OrderDate, VoucherNo 
                FROM {config.ERP_OT2001} 
                WHERE SOrderID = ? 
            """
            info_data = db.get_data(query_get_info, (clean_sorder_id,))
            
            if not info_data:
                raise Exception(f"Không tìm thấy đơn hàng trong hệ thống với ID: {clean_sorder_id}")
                
            create_user_id = info_data[0]['SalesManID'] 
            order_date = info_data[0]['OrderDate']
            real_voucher_no = info_data[0]['VoucherNo'] # Lấy mã DDH chuẩn từ DB

            # Tính tổng tiền (Dùng SOrderID)
            query_sum = f"SELECT SUM(ConvertedAmount) as Total FROM {config.ERP_SALES_DETAIL} WHERE SorderID = ?"
            sum_data = db.get_data(query_sum, (clean_sorder_id,))
            total_amount = safe_float(sum_data[0]['Total']) if sum_data else 0

            # 2. UPDATE STATUS DÙNG SOrderID
            update_query = f"""
                UPDATE {config.ERP_OT2001} 
                SET OrderStatus = 1 
                WHERE SOrderID = ?
            """
            db.execute_query_in_transaction(conn, update_query, (clean_sorder_id,))
            
            # 3. Ghi Log DUYETCT (Dùng SOrderID cho MasoCT)
            insert_log_duyet = f"""
                INSERT INTO DUYETCT (
                    MACT, NGayCT, TySoDuyetCT, NGUOILAM, Tonggiatri, 
                    MasoCT, MaKH, NguoiDuyet, Ngayduyet, TINHTRANG
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), 0)
            """
            params_duyet = (
                real_voucher_no, # MACT là DDH/...
                order_date, 
                approval_ratio, 
                employee_id, 
                total_amount, 
                clean_sorder_id, # MasoCT là SO...
                object_id, 
                current_user
            )
            db.execute_query_in_transaction(conn, insert_log_duyet, params_duyet)

            # 4. Ghi Log OT6000 (Dùng SOrderID)
            approve_id = f"{current_user}{datetime.now().strftime('%Y%m%d%H%M%S')}"
            
            insert_ot6000 = f"""
                INSERT INTO {config.ERP_DB}.[dbo].[OT6000]
                (ApproveID, SOrderID, Approver, LevelApprove, Approvedate, 
                 CreateUserID, Createdate, LastModifyUserID, LastModifyDate, 
                 StypeID, Notes, ApproveStatus)
                VALUES (?, ?, ?, 1, GETDATE(), ?, GETDATE(), ?, GETDATE(), 'SO', '', 1)
            """
            params_ot6000 = (
                approve_id, clean_sorder_id, current_user, 
                create_user_id, current_user
            )
            db.execute_query_in_transaction(conn, insert_ot6000, params_ot6000)
            
            db.commit(conn)
            
            return {"success": True, "message": f"Duyệt thành công phiếu {real_voucher_no}."}

        except Exception as e:
            raise e
        finally:
            if conn:
                conn.close()