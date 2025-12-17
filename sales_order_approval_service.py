# services/sales_order_approval_service.py

from flask import current_app
from datetime import datetime
from db_manager import DBManager, safe_float
import config

class SalesOrderApprovalService:
    """Xử lý toàn bộ logic nghiệp vụ liên quan đến phê duyệt Đơn hàng bán (DHB)."""
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_orders_for_approval(self, user_code, date_from, date_to):
        """
        Truy vấn tất cả các Đơn hàng bán chưa duyệt (OrderStatus = 0) trong khoảng ngày.
        """
        
        where_conditions = ["T1.OrderStatus = 0"]
        where_params = []
        
        if date_from and date_to:
             where_conditions.append("T1.OrderDate BETWEEN ? AND ?") 
             where_params.extend([date_from, date_to])
        
        where_clause = " AND ".join(where_conditions)
        
        # TRUY VẤN TỔNG HỢP (OT2001 Header)
        # [CONFIG]: Tất cả các bảng đã được lấy từ config
        order_query = f"""
            SELECT 
                T1.VoucherNo AS OrderID,    
                T1.OrderDate,               
                T1.SaleAmount AS SaleAmount,
                T1.SalesManID, 
                T1.VoucherTypeID, T1.ObjectID AS ClientID, 
                T2.ShortObjectName AS ClientName,
                T2.O05ID AS CustomerClass,         
                T6.SHORTNAME AS SalesAdminName,  
                T7.SHORTNAME AS NVKDName,        
                
                T1.SOrderID, 
                
                SUM(T4.ConvertedAmount) AS TotalSaleAmount, 
                SUM(T4.OrderQuantity * T5.Recievedprice) AS TotalCost,
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
                T1.VoucherNo, T1.OrderDate, T1.SaleAmount, T1.SalesManID, T1.VoucherTypeID, 
                T1.ObjectID, T2.ShortObjectName, T2.O05ID, T6.SHORTNAME, T7.SHORTNAME, T1.SOrderID 
            
            ORDER BY T1.OrderDate ASC
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
            processed_order = self._check_approval_criteria(order, user_code)
            if processed_order and processed_order.get('ApprovalResult') is not None:
                results.append(processed_order)
            else:
                current_app.logger.error(f"CẢNH BÁO: Bỏ qua đơn hàng {order.get('OrderID')} do thiếu ApprovalResult.")
        return results

    def _check_approval_criteria(self, order, current_user_code):
        """Kiểm tra các quy tắc nghiệp vụ cho DHB."""
        
        approval_status = {'Passed': True, 'Reason': 'OK', 'ApproverRequired': current_user_code, 'ApproverDisplay': 'TỰ DUYỆT (SELF)', 'ApprovalRatio': 0}
        order['ApprovalResult'] = approval_status
        
        total_sale = safe_float(order.get('TotalSaleAmount'))
        total_cost = safe_float(order.get('TotalCost'))
        customer_class = order.get('CustomerClass')
        sale_amount = safe_float(order.get('SaleAmount')) 
        voucher_type = order.get('VoucherTypeID', '').strip()
        
        has_all_date01 = order.get('HasAllDate01') == 1
        is_fully_quoted = order.get('IsFullyQuoted') == 1

        # --- KIỂM TRA ĐIỀU KIỆN 1 & 2a (Validation & DTK Exception) ---
        
        # 1. Check hợp lệ
        if not order.get('SalesManID') or total_sale == 0 or total_cost == 0 or not has_all_date01:
            approval_status['Passed'] = False
            approval_status['Reason'] = 'FAILED: Thiếu SalesmanID/Giá/Chi phí (Cost)/Date01 không đầy đủ.'
            return order
        
        # 2. Kiểm tra 100% kế thừa từ chào giá (trừ DTK)
        if voucher_type != 'DTK' and not is_fully_quoted:
             approval_status['Passed'] = False
             approval_status['Reason'] = 'FAILED: DHB không 100% kế thừa từ Chào giá.'
             return order

        # --- KIỂM TRA ĐIỀU KIỆN 3 (Tỷ số duyệt) ---
        required_ratio = 0
        ratio = 0
        ratio_failed = False
        
        if total_cost > 0 and total_sale > 0:
            ratio = 30 + 100 * (total_sale / total_cost)
            approval_status['ApprovalRatio'] = min(9999, round(ratio))
            
            # [CONFIG]: Dùng RATIO từ config
            if customer_class == 'M': required_ratio = config.RATIO_REQ_CLASS_M
            elif customer_class == 'T': required_ratio = config.RATIO_REQ_CLASS_T
                
            if required_ratio > 0 and ratio < required_ratio:
                approval_status['Passed'] = False
                ratio_failed = True
                approval_status['Reason'] = f'PENDING: Tỷ số ({round(ratio)}) < Y/C ({required_ratio}).'
                approval_status['NeedsOverride'] = True
        else:
             approval_status['Reason'] = 'FAILED: Không tính được Tỷ số Duyệt (Sale/Cost = 0).'
             approval_status['Passed'] = False
             ratio_failed = True

        # --- KIỂM TRA ĐIỀU KIỆN 2b & 4 (Xác định người duyệt) ---
        
        # [CONFIG]: Dùng LIMIT_AUTO_APPROVE_DTK
        is_dtk_and_small = voucher_type == 'DTK' and sale_amount < config.LIMIT_AUTO_APPROVE_DTK
        
        # 1. TRƯỜNG HỢP TỰ DUYỆT
        if is_dtk_and_small and approval_status['Passed']:
            approval_status['ApproverDisplay'] = 'TỰ DUYỆT (SELF - DTK < 100M)'
            approval_status['ApproverRequired'] = current_user_code
        
        # 2. TRƯỜNG HỢP PHẢI XÉT NGƯỜI DUYỆT CẤP CAO
        else: 
            # [CONFIG]: Dùng bảng ERP_APPROVER_MASTER
            approver_query = f"""
                SELECT Approver 
                FROM {config.ERP_APPROVER_MASTER} 
                WHERE VoucherTypeID = ?
            """
            approver_data = self.db.get_data(approver_query, (voucher_type,))
            
            approvers = [d['Approver'].strip() for d in approver_data if d.get('Approver')] if approver_data else []
            
            if not approvers:
                approvers = [config.ROLE_ADMIN] # Mặc định là ADMIN

            approvers_str = ", ".join(approvers)
            
            approval_status['ApproverDisplay'] = approvers_str
            approval_status['ApproverRequired'] = approvers_str
            
            is_current_user_approver = current_user_code in approvers

            if not is_current_user_approver:
                 approval_status['Passed'] = False
                 if not ratio_failed:
                     approval_status['Reason'] = f"PENDING: Cần sự duyệt của {approvers_str}."
                 else:
                     approval_status['Reason'] = f"FAILED: {approval_status['Reason']} (Cần Sửa lỗi & Duyệt bởi {approvers_str})."
            
            elif is_current_user_approver and not approval_status['Passed']:
                approval_status['Reason'] = f"FAILED: {approval_status['Reason']} (Bạn là người duyệt, Cần SỬA LỖI TRƯỚC KHI DUYỆT)."
            
        return order

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
    
    def approve_sales_order(self, order_id, sorder_id, client_id, salesman_id, approval_ratio, current_user):
        """
        Thực hiện phê duyệt Đơn hàng Bán và lưu log.
        """
        db = self.db
        conn = None
        
        try:
            conn = db.get_transaction_connection()
            
            # 1. Lấy thông tin cần thiết từ ERP
            query_get_data = f"""
                SELECT T1.SaleAmount, T1.OrderDate
                FROM {config.ERP_OT2001} AS T1
                WHERE T1.SOrderID = ? 
            """
            data_detail = db.get_data(query_get_data, (sorder_id,)) 
            
            if not data_detail:
                raise Exception(f"Không tìm thấy dữ liệu DHB {order_id} trong ERP.")
            
            detail = data_detail[0]
            sale_amount = safe_float(detail.get('SaleAmount'))
            order_date = detail.get('OrderDate')

            # 2. Thực hiện phê duyệt trong ERP
            update_query_erp = f"""
                UPDATE {config.ERP_OT2001} 
                SET OrderStatus = 1 
                WHERE VoucherNo = ?
            """
            db.execute_query_in_transaction(conn, update_query_erp, (order_id,)) 
            
            # 3. Lưu log vào bảng DUYETCT
            # [CONFIG]: Dùng config.LOG_DUYETCT_TABLE thay vì chuỗi cứng
            insert_query_log = f"""
                INSERT INTO {config.LOG_DUYETCT_TABLE} (
                    MACT, NGayCT, TySoDuyetCT, NGUOILAM, Tonggiatri, 
                    MasoCT, MaKH, NguoiDuyet, Ngayduyet
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            """
            params = (
                order_id, order_date, approval_ratio, salesman_id, 
                sale_amount, sorder_id, client_id, current_user
            )

            db.execute_query_in_transaction(conn, insert_query_log, params)
            
            # 4. COMMIT
            db.commit(conn) 

            return {"success": True, "message": f"Đơn hàng Bán {order_id} đã duyệt thành công và lưu log vào {config.LOG_DUYETCT_TABLE}."}

        except Exception as e:
            if conn:
                db.rollback(conn) 
            current_app.logger.error(f"LỖI DUYỆT DHB {order_id}: {e}")
            return {"success": False, "message": f"Duyệt thất bại. Lỗi hệ thống: {str(e)}"}
        finally:
            if conn:
                conn.close()