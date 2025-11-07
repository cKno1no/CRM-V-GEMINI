# sales_service.py

from datetime import datetime
from operator import itemgetter

# Import từ các module khác (Import trực tiếp)
from db_manager import DBManager, safe_float, parse_filter_string, evaluate_condition
import config # Import config để lấy tên bảng

class SalesService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_sales_performance_data(self, current_year, user_code, is_admin):
        """
        Tổng hợp 4 chỉ số KPI chính cho Sales Dashboard.
        Thực hiện lọc theo SalesManID nếu người dùng không phải là Admin.
        """
        
        current_month = datetime.now().month
        
        # --- LỌC ĐIỀU KIỆN THEO ROLE ---
        salesman_filter = ""
        ytd_params = [current_year]
        monthly_params = [current_year, current_month]
        pending_params = [current_year]
        reg_params = [current_year]
        
        if not is_admin:
            # Nếu không phải Admin, áp dụng bộ lọc cho TẤT CẢ các query
            user_code_stripped = user_code.strip()
            salesman_filter = " AND RTRIM(T1.SalesManID) = ?"
            
            ytd_params.append(user_code_stripped)
            monthly_params.append(user_code_stripped)
            pending_params.append(user_code_stripped)
            
            # Query đăng ký (CRM_DTCL) có tên cột khác
            reg_params_filter = [user_code_stripped]

        # --- 1. TRUY VẤN CƠ SỞ (YTD Sales & Total Orders) ---
        ytd_query = f"""
            SELECT 
                RTRIM(T1.SalesManID) AS EmployeeID,
                T3.SHORTNAME AS SalesManName,
                SUM(T1.ConvertedAmount) AS TotalSalesAmount,
                COUNT(DISTINCT T1.VoucherNo) AS TotalOrders
            FROM {config.ERP_GIAO_DICH} AS T1
            LEFT JOIN {config.TEN_BANG_NGUOI_DUNG} AS T3 ON T1.SalesManID = T3.USERCODE
            WHERE 
                T1.DebitAccountID = '13111'
                AND T1.CreditAccountID LIKE '5%'
                AND T1.TranYear = ?
                {salesman_filter} -- ÁP DỤNG LỌC
            GROUP BY 
                RTRIM(T1.SalesManID), T3.SHORTNAME
            HAVING 
                RTRIM(T1.SalesManID) IS NOT NULL
        """
        base_summary = self.db.get_data(ytd_query, tuple(ytd_params))
        summary_dict = {row['EmployeeID']: row for row in base_summary} if base_summary else {}
        
        # --- 2. DS ĐĂNG KÝ (Registered Sales) và Merge ---
        # LƯU Ý: Phải sử dụng bộ lọc cho Registered Query nếu không phải Admin
        registered_query = f"""
            SELECT 
                RTRIM([PHU TRACH DS]) AS EmployeeID,
                SUM(ISNULL(DK, 0)) AS RegisteredSales
            FROM {config.CRM_DTCL}
            WHERE 
                [Nam] = ?
                AND [PHU TRACH DS] IS NOT NULL
                { " AND RTRIM([PHU TRACH DS]) = ?" if not is_admin else "" } -- ÁP DỤNG LỌC RIÊNG
            GROUP BY 
                RTRIM([PHU TRACH DS])
        """
        registered_sales = self.db.get_data(registered_query, tuple(reg_params + (reg_params_filter if not is_admin else [])))

        if registered_sales:
            for row in registered_sales:
                emp_id = row['EmployeeID']
                raw_reg_sales = safe_float(row.get('RegisteredSales'))
                if emp_id in summary_dict:
                    summary_dict[emp_id]['RegisteredSales'] = raw_reg_sales
                else:
                    summary_dict[emp_id] = { 'EmployeeID': emp_id, 'SalesManName': 'N/A', 'TotalSalesAmount': 0.0, 'TotalOrders': 0, 'RegisteredSales': raw_reg_sales, 'CurrentMonthSales': 0.0, 'PendingOrdersAmount': 0.0 }
        
        # --- 3. DS THÁNG HIỆN TẠI và Merge ---
        monthly_query = f"""
            SELECT 
                RTRIM(T1.SalesManID) AS EmployeeID,
                SUM(T1.ConvertedAmount) AS CurrentMonthSales
            FROM {config.ERP_GIAO_DICH} AS T1
            WHERE 
                T1.DebitAccountID = '13111' AND T1.CreditAccountID LIKE '5%'
                AND T1.TranYear = ? AND T1.TranMonth = ?
                {salesman_filter} -- ÁP DỤNG LỌC
            GROUP BY 
                RTRIM(T1.SalesManID)
        """
        monthly_sales = self.db.get_data(monthly_query, tuple(monthly_params))
        if monthly_sales:
            for row in monthly_sales:
                emp_id = row['EmployeeID']
                if emp_id in summary_dict:
                    summary_dict[emp_id]['CurrentMonthSales'] = safe_float(row['CurrentMonthSales'])
                    
        # --- 4. Đơn hàng Chờ giao và Merge ---
        pending_query = f"""
            SELECT 
                RTRIM(T1.SalesManID) AS EmployeeID,
                SUM(T1.saleAmount) AS PendingOrdersAmount
            FROM {config.ERP_OT2001} AS T1 
            LEFT JOIN (
                SELECT DISTINCT G.orderID FROM {config.ERP_GIAO_DICH} AS G WHERE G.VoucherTypeID = 'BH' 
            ) AS Delivered ON T1.sorderid = Delivered.orderID
            WHERE 
                T1.orderStatus = 1 AND Delivered.orderID IS NULL AND T1.TranYear = ?
                {salesman_filter} -- ÁP DỤNG LỌC
            GROUP BY 
                RTRIM(T1.SalesManID)
        """
        pending_orders = self.db.get_data(pending_query, tuple(pending_params))
        if pending_orders:
            for row in pending_orders:
                emp_id = row['EmployeeID']
                if emp_id in summary_dict:
                    summary_dict[emp_id]['PendingOrdersAmount'] = safe_float(row['PendingOrdersAmount'])
                
        # --- 5. FINAL CLEANUP, TYPE CONVERSION VÀ SORTING ---
        final_summary_list = []
        
        for emp_id, row in summary_dict.items():
            row['TotalSalesAmount'] = safe_float(row.get('TotalSalesAmount'))
            row['RegisteredSales'] = safe_float(row.get('RegisteredSales', 0.0))
            row['CurrentMonthSales'] = safe_float(row.get('CurrentMonthSales', 0.0))
            row['PendingOrdersAmount'] = safe_float(row.get('PendingOrdersAmount', 0.0))
            row['TotalOrders'] = int(row.get('TotalOrders', 0) or 0)       
            
            if row['SalesManName'] == 'N/A' or row['SalesManName'] == '':
                 name_data = self.db.get_data(f"SELECT SHORTNAME FROM {config.TEN_BANG_NGUOI_DUNG} WHERE USERCODE = ?", (emp_id,))
                 row['SalesManName'] = name_data[0]['SHORTNAME'] if name_data else row['EmployeeID']

            final_summary_list.append(row)
            
        return final_summary_list

    # CRM STDD/sales_service.py (Hàm get_order_detail_drilldown)

    def get_order_detail_drilldown(self, sorder_id):
        """
        Lấy chi tiết mặt hàng theo SOrderID (khóa duy nhất).
        """
        # Truy vấn trực tiếp bằng SOrderID
        query = f"""
            SELECT
                T1.InventoryID,
                ISNULL(T1.InventoryCommonName, T2.InventoryName) AS InventoryName,
                T1.OrderQuantity AS SoLuong,
                T1.ConvertedAmount AS ThanhTien
            FROM {config.ERP_SALES_DETAIL} AS T1
            LEFT JOIN {config.ERP_ITEM_PRICING} AS T2 ON T1.InventoryID = T2.InventoryID
            WHERE T1.SOrderID = ? -- SỬ DỤNG SORDERID
            ORDER BY T1.Orders
        """
        
        try:
            details = self.db.get_data(query, (sorder_id,))
        except Exception as e:
            print(f"LỖI SQL DRILLDOWN DHB {sorder_id}: {e}")
            return []
            
        # Định dạng tiền tệ
        for detail in details:
            detail['SoLuong'] = f"{safe_float(detail.get('SoLuong')):.0f}"
            detail['ThanhTien'] = f"{safe_float(detail.get('ThanhTien')):,.0f}"

        return details


    def get_client_details_for_salesman(self, employee_id, current_year):
        """
        Lấy DS chi tiết theo khách hàng, phân loại thành Đăng ký và Phát sinh mới.
        """
        
        current_month = datetime.now().month
        today_str = datetime.now().strftime('%Y-%m-%d')
        DIVISOR = 1000000.0
        SMALL_CUSTOMER_LIMIT_VN = 20000000.0 
        
        # 1. TRUY VẤN TỔNG DS ĐĂNG KÝ THÔ
        total_registered_query = f"""
            SELECT 
                SUM(ISNULL(DK, 0)) AS TotalRegisteredSalesRaw
            FROM {config.CRM_DTCL}
            WHERE 
                RTRIM([PHU TRACH DS]) = ? AND Nam = ?
        """
        total_reg_data = self.db.get_data(total_registered_query, (employee_id, current_year))
        total_registered_sales_raw = safe_float(total_reg_data[0].get('TotalRegisteredSalesRaw')) if total_reg_data else 0.0

        
        # 2. TRUY VẤN CHI TIẾT THEO KHÁCH HÀNG (BASE DATA)
        base_client_sales_query = f"""
            SELECT 
                RTRIM(T1.ObjectID) AS ClientID,
                T4.ShortObjectName AS ClientName,
                SUM(CASE WHEN T1.TranYear = ? THEN T1.ConvertedAmount ELSE 0 END) AS TotalSalesAmount,
                SUM(CASE WHEN T1.TranMonth = ? AND T1.TranYear = ? THEN T1.ConvertedAmount ELSE 0 END) AS CurrentMonthSales,
                COUNT(DISTINCT T1.VoucherNo) AS TotalOrders
            FROM {config.ERP_GIAO_DICH} AS T1
            LEFT JOIN {config.ERP_IT1202} AS T4 ON T1.ObjectID = T4.ObjectID
            WHERE 
                RTRIM(T1.SalesManID) = ?
                AND T1.DebitAccountID = '13111' AND T1.CreditAccountID LIKE '5%'
                AND T1.TranYear >= ?
            GROUP BY 
                RTRIM(T1.ObjectID), T4.ShortObjectName
        """
        base_client_sales = self.db.get_data(
            base_client_sales_query, 
            (current_year, current_month, current_year, employee_id, current_year - 1)
        )
        
        client_dict = {}
        if base_client_sales:
            for row in base_client_sales:
                client_id = row['ClientID']
                client_dict[client_id] = {
                    'ClientID': client_id,
                    'ClientName': row.get('ClientName') or 'N/A',
                    'TotalSalesAmount': safe_float(row.get('TotalSalesAmount')),
                    'CurrentMonthSales': safe_float(row.get('CurrentMonthSales')),
                    'TotalOrders': int(row.get('TotalOrders') or 0),
                    'RegisteredSales': 0.0,
                    'PendingOrdersAmount': 0.0
                }

        # 3. HỢP NHẤT DS ĐĂNG KÝ
        registered_query = f"""
            SELECT 
                RTRIM(T1.[MA KH]) AS ClientID, 
                SUM(ISNULL(T1.DK, 0)) AS RegisteredSales
            FROM {config.CRM_DTCL} AS T1
            WHERE 
                RTRIM(T1.[PHU TRACH DS]) = ? AND T1.Nam = ? 
            GROUP BY 
                RTRIM(T1.[MA KH])
        """
        registered_data = self.db.get_data(registered_query, (employee_id, current_year))
        if registered_data: 
            for row in registered_data:
                client_id = row['ClientID']
                raw_registered_sales = safe_float(row.get('RegisteredSales'))
                if client_id in client_dict:
                    client_dict[client_id]['RegisteredSales'] = raw_registered_sales
                elif raw_registered_sales > 0:
                     client_dict[client_id] = {'ClientID': client_id, 'RegisteredSales': raw_registered_sales, 'ClientName': 'N/A', 'TotalSalesAmount': 0.0, 'CurrentMonthSales': 0.0, 'TotalOrders': 0, 'PendingOrdersAmount': 0.0}

        # 4. TRUY VẤN ĐƠN CHỜ GIAO (Pending Orders) và HỢP NHẤT
        pending_query = f"""
            SELECT 
                RTRIM(T1.ObjectID) AS ClientID,
                SUM(T1.saleAmount) AS PendingOrdersAmount
            FROM {config.ERP_OT2001} AS T1 
            LEFT JOIN (
                SELECT DISTINCT G.orderID FROM {config.ERP_GIAO_DICH} AS G WHERE G.VoucherTypeID = 'BH' 
            ) AS Delivered ON T1.sorderid = Delivered.orderID
            WHERE 
                RTRIM(T1.SalesManID) = ?
                AND T1.orderStatus = 1 AND Delivered.orderID IS NULL 
                AND T1.orderDate >= DATEADD(YEAR, -1, ?) -- Lọc 1 năm
            GROUP BY 
                RTRIM(T1.ObjectID)
        """
        pending_data = self.db.get_data(pending_query, (employee_id, today_str)) 
        if pending_data:
            for row in pending_data:
                client_id = row['ClientID']
                raw_poa = safe_float(row.get('PendingOrdersAmount'))
                if client_id in client_dict:
                    client_dict[client_id]['PendingOrdersAmount'] = raw_poa
                elif raw_poa > 0:
                     client_dict[client_id] = {'ClientID': client_id, 'RegisteredSales': 0.0, 'ClientName': 'N/A', 'TotalSalesAmount': 0.0, 'CurrentMonthSales': 0.0, 'TotalOrders': 0, 'PendingOrdersAmount': raw_poa}
                 
        # 5. FINAL CLEANUP, PHÂN LOẠI VÀ TÍNH TỔNG ĐỒNG NHẤT
        registered_clients = []
        new_business_clients = []
        total_poa_amount = 0 # Tổng POA thô
        
        small_customer_group = {'RegisteredSales': 0.0, 'CurrentMonthSales': 0.0, 'TotalSalesAmount': 0.0, 'TotalOrders': 0, 'PendingOrdersAmount': 0.0}
        small_customer_count = 0

        for client_id, row in client_dict.items():
            raw_poa = safe_float(row.get('PendingOrdersAmount'))
            raw_current_sales = safe_float(row.get('CurrentMonthSales'))
            raw_total_sales = safe_float(row.get('TotalSalesAmount'))
            raw_registered_sales = safe_float(row.get('RegisteredSales'))
            
            total_poa_amount += raw_poa

            # 1. KHÁCH NHỎ LẺ (DS YTD < 20 TRIỆU)
            if raw_total_sales < SMALL_CUSTOMER_LIMIT_VN and (raw_total_sales > 0 or raw_registered_sales > 0 or raw_poa > 0):
                small_customer_group['RegisteredSales'] += raw_registered_sales
                small_customer_group['CurrentMonthSales'] += raw_current_sales
                small_customer_group['TotalSalesAmount'] += raw_total_sales
                small_customer_group['TotalOrders'] += int(row.get('TotalOrders', 0))
                small_customer_group['PendingOrdersAmount'] += raw_poa
                small_customer_count += 1
                continue

            # 2. ÁP DỤNG ĐỒNG NHẤT LOGIC CHIA 1 TRIỆU
            row['RegisteredSales'] = raw_registered_sales / DIVISOR 
            row['CurrentMonthSales'] = raw_current_sales / DIVISOR
            row['TotalSalesAmount'] = raw_total_sales / DIVISOR
            row['PendingOrdersAmount'] = raw_poa / DIVISOR
            
            # 3. PHÂN LOẠI và ĐỔ VÀO LIST
            if raw_registered_sales > 0:
                row['ClientType'] = row['ClientName']
                registered_clients.append(row)
            elif raw_total_sales > 0:
                row['ClientType'] = f'--- PS mới - {row["ClientName"]}'
                new_business_clients.append(row)
            elif raw_poa > 0:
                 row['ClientType'] = f'--- Chờ giao - {row["ClientName"]}'
                 new_business_clients.append(row)

        if small_customer_count > 0:
            small_customer_row = {
                'ClientID': 'NHÓM',
                'ClientName': f'--- KHÁCH NHỎ LẺ ({small_customer_count} KH) ---',
                'RegisteredSales': small_customer_group['RegisteredSales'] / DIVISOR, 
                'CurrentMonthSales': small_customer_group['CurrentMonthSales'] / DIVISOR,
                'TotalSalesAmount': small_customer_group['TotalSalesAmount'] / DIVISOR,
                'TotalOrders': small_customer_group['TotalOrders'],
                'PendingOrdersAmount': small_customer_group['PendingOrdersAmount'] / DIVISOR
            }
            new_business_clients.insert(0, small_customer_row) 

        registered_clients = sorted(registered_clients, key=itemgetter('RegisteredSales', 'TotalSalesAmount'), reverse=True)
        new_business_clients = sorted(new_business_clients, key=itemgetter('TotalSalesAmount'), reverse=True)

        return registered_clients, new_business_clients, total_poa_amount, total_registered_sales_raw

class InventoryService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    # sales_service.py (Hàm get_inventory_aging_data)

    def get_inventory_aging_data(self, item_filter_term, category_filter, qty_filter, value_filter, i05id_filter):
        """
        Lấy và lọc dữ liệu tuổi hàng tồn kho (Inventory Aging).
        - Sử dụng logic lọc KHÁC (<>) cho Ngành hàng và Tính chất (I05ID).
        - Tính Subtotal cho 4 KPI Tiles.
        """
        
        # Khai báo hằng số
        DIVISOR = 1000000.0
        RISK_THRESHOLD = 5000000.0 # 5 Triệu VNĐ
        
        # 1. Chuẩn bị gọi SP: Luôn truyền NULL để SP trả về toàn bộ dữ liệu
        sp_query = "{CALL dbo.sp_GetInventoryAging (?)}" 
        aging_data = []
        
        try:
            # Gọi hàm lấy dữ liệu (Truyền NULL để khắc phục lỗi STRING_SPLIT và lấy toàn bộ)
            raw_data = self.db.get_data(sp_query, (None,))
            if raw_data is not None:
                aging_data = raw_data
            
        except Exception as e:
            print(f"LỖI KHI GỌI SP INVENTORY AGING: {e}")
            # Trả về danh sách rỗng nếu có lỗi
            return [], {'total_inventory': 0, 'total_quantity': 0, 'total_new_6_months': 0, 'total_over_2_years': 0, 'total_clc_value': 0}


        # --- 2. TÍNH TOÁN KPI VÀ ÁP DỤNG LỌC PYTHON TRÊN TẤT CẢ CÁC ROWS ---

        total_inventory = 0
        total_quantity = 0 
        total_new_6_months = 0 
        total_over_2_years = 0 
        total_clc_value = 0
        
        filtered_and_summed_data = []
        
        # Phân tích điều kiện lọc số trước vòng lặp (Sử dụng hàm helper)
        qty_op, qty_thresh = parse_filter_string(qty_filter)
        val_op, val_thresh = parse_filter_string(value_filter)
        search_terms = [t.strip().lower() for t in item_filter_term.split(';') if t.strip()]

        for row in aging_data:
            # Lấy giá trị thô và ép kiểu an toàn (sử dụng safe_float)
            total_val = safe_float(row.get('TotalCurrentValue'))
            total_qty = safe_float(row.get('TotalCurrentQuantity'))
            range_0_180_val = safe_float(row.get('Range_0_180_V'))
            range_over_720_val = safe_float(row.get('Range_Over_720_V'))
            
            # 1. TÍNH TOÁN CỘT D (RỦI RO CLC) VÀ ÁP DỤNG LOGIC KHÁC
            item_class = row.get('StockClass', '')
            
            # Logic Cột D: IF I05ID != 'D' AND >720_V > 5M THEN >720_V ELSE 0
            risk_clc_val = 0.0
            # Kiểm tra điều kiện 1: I05ID KHÁC 'D' hoặc là NULL/Rỗng
            is_not_d_class = (item_class.upper() != 'D' and item_class.strip() != '') or (item_class is None or item_class.strip() == '')

            if is_not_d_class and range_over_720_val > RISK_THRESHOLD:
                risk_clc_val = range_over_720_val
                
            row['Risk_CLC_Value'] = risk_clc_val # Gán giá trị tính toán vào row (cho hiển thị)

            # 2. LOGIC LỌC PYTHON (Subtotal)
            is_match_text = True
            is_match_category = True
            is_match_class = True
            is_match_qty = True
            is_match_value = True

            # Lấy giá trị chuỗi
            item_id = row.get('InventoryID', '').lower()
            item_name = row.get('InventoryName', '').lower()
            item_category_code = row.get('ItemCategory', '').lower() # I02ID
            item_category_name = row.get('InventoryTypeName', '').lower() # Tên ngành hàng
            item_class = row.get('StockClass', '').lower()

            # Lọc Text (SKU/Tên)
            if search_terms:
                item_id = row.get('InventoryID', '').lower()
                item_name = row.get('InventoryName', '').lower()
                is_match_text = any(term in item_id or term in item_name for term in search_terms)
                
            # FIX 1: LỌC NGÀNH HÀNG (I02ID HOẶC TÊN)
            if category_filter:
                op, val = ('!=', category_filter.replace('!=', '').replace('<>', '')) if category_filter.startswith(('!=', '<>')) else ('=', category_filter)
                val_lower = val.lower()
                
                # Điều kiện khớp: Khớp với Code HOẶC Khớp với Tên
                is_category_match = (val_lower == item_category_code) or (val_lower in item_category_name)
                
                is_match_category = is_category_match if op == '=' else (not is_category_match)

            # Lọc Tính chất (I05ID) - Hỗ trợ lọc KHÁC (!=)
            if i05id_filter:
                item_class_lower = item_class.lower()
                op, val = ('!=', i05id_filter.replace('!=', '').replace('<>', '')) if i05id_filter.startswith(('!=', '<>')) else ('=', i05id_filter)
                is_match_class = (item_class_lower == val.lower()) if op == '=' else (item_class_lower != val.lower())
                
            # Lọc Số lượng & Giá trị
            if qty_thresh is not None:
                is_match_qty = evaluate_condition(total_qty, qty_op, qty_thresh)
            if val_thresh is not None:
                is_match_value = evaluate_condition(total_val, val_op, val_thresh)


            # 3. TÍNH SUBTOTTAL VÀ THÊM VÀO DANH SÁCH
            if is_match_text and is_match_category and is_match_class and is_match_qty and is_match_value:
                # TÍNH SUBTOTTAL CỦA BỘ LỌC HIỆN TẠI
                total_inventory += total_val
                total_quantity += total_qty 
                total_new_6_months += range_0_180_val
                total_over_2_years += range_over_720_val
                total_clc_value += risk_clc_val # Tính tổng KPI Cột D
                
                filtered_and_summed_data.append(row)

        # 3. SẮP XẾP CUỐI CÙNG (FIX: Sort theo Cột D trước, rồi TotalCurrentValue)
        
        # SỬ DỤNG HÀM LAMBDA ĐỂ ÉP KIỂU AN TOÀN TRONG ITEMGETTER
        filtered_and_summed_data = sorted(
            filtered_and_summed_data, 
            key=lambda row: (
                safe_float(row.get('Risk_CLC_Value', 0)),    # Sắp xếp Cột D trước
                safe_float(row.get('TotalCurrentValue', 0)) # Sắp xếp Tổng Tồn sau
            ), 
            reverse=True
        )        

           
                
        # 4. Chuẩn bị dữ liệu tổng
        totals = {
            'total_inventory': total_inventory,
            'total_quantity': total_quantity,
            'total_new_6_months': total_new_6_months,
            'total_over_2_years': total_over_2_years,
            'total_clc_value': total_clc_value
        }
            
        return filtered_and_summed_data, totals