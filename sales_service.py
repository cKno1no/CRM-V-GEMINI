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
        [UPDATED] Tổng hợp KPI Sales sử dụng Stored Procedure tối ưu.
        """
        try:
            # Tham số cho SP: @CurrentYear, @UserCode, @IsAdmin
            # Lưu ý: db_manager.execute_sp_multi trả về list các bảng, ta lấy bảng đầu tiên [0]
            result_sets = self.db.execute_sp_multi(
                'dbo.sp_GetSalesPerformanceSummary', 
                (current_year, user_code, 1 if is_admin else 0)
            )
            
            if not result_sets or not result_sets[0]:
                return []
                
            data = result_sets[0]
            
            # Ép kiểu dữ liệu để đảm bảo an toàn khi tính toán ở view
            for row in data:
                row['TotalSalesAmount'] = float(row.get('TotalSalesAmount') or 0)
                row['CurrentMonthSales'] = float(row.get('CurrentMonthSales') or 0)
                row['RegisteredSales'] = float(row.get('RegisteredSales') or 0)
                row['PendingOrdersAmount'] = float(row.get('PendingOrdersAmount') or 0)
                row['TotalOrders'] = int(row.get('TotalOrders') or 0)
                
            return data

        except Exception as e:
            print(f"Lỗi khi lấy KPI Sales (SP): {e}")
            return []

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
    # Thêm vào class SalesService
    def get_profit_analysis(self, date_from, date_to, user_code, is_admin):
        """Lấy dữ liệu phân tích lợi nhuận gộp (Gom nhóm theo Khách hàng -> Đơn hàng)."""
        try:
            salesman_param = None if is_admin else user_code
            
            result = self.db.execute_sp_multi(
                'dbo.sp_GetSalesGrossProfit_Analysis', 
                (date_from, date_to, salesman_param)
            )
            
            raw_data = result[0] if result and len(result) > 0 else []
            
            # KPI Tổng
            summary = {'Revenue': 0, 'COGS': 0, 'GrossProfit': 0, 'AvgMargin': 0}
            
            # Cấu trúc dữ liệu phân cấp
            hierarchy = {} 

            if raw_data:
                for row in raw_data:
                    # 1. [FIX QUAN TRỌNG] Ép kiểu và GÁN NGƯỢC LẠI vào row
                    # Để template HTML có thể dùng format {:,.0f}
                    row['SoLuong'] = float(row.get('SoLuong') or 0)
                    row['DoanhThu'] = float(row.get('DoanhThu') or 0)
                    row['GiaVon'] = float(row.get('GiaVon') or 0)
                    row['LaiGop'] = float(row.get('LaiGop') or 0)
                    row['TyLeLaiGop'] = float(row.get('TyLeLaiGop') or 0)

                    # 2. Cộng tổng toàn cục
                    summary['Revenue'] += row['DoanhThu']
                    summary['COGS'] += row['GiaVon']
                    summary['GrossProfit'] += row['LaiGop']

                    # 3. Gom nhóm theo KHÁCH HÀNG
                    cust_id = row['MaKhachHang']
                    if cust_id not in hierarchy:
                        hierarchy[cust_id] = {
                            'ID': cust_id,
                            'Name': row['TenKhachHang'],
                            'SalesMan': row['SalesManName'],
                            'Revenue': 0.0,
                            'COGS': 0.0,
                            'Profit': 0.0,
                            'Orders': {} 
                        }
                    
                    hierarchy[cust_id]['Revenue'] += row['DoanhThu']
                    hierarchy[cust_id]['COGS'] += row['GiaVon']
                    hierarchy[cust_id]['Profit'] += row['LaiGop']

                    # 4. Gom nhóm theo ĐƠN HÀNG
                    order_id = row['SoDonHang']
                    if order_id not in hierarchy[cust_id]['Orders']:
                        hierarchy[cust_id]['Orders'][order_id] = {
                            'ID': order_id,
                            'Date': row['NgayHachToan'], 
                            'VoucherNo': row['SoChungTu'],
                            'Revenue': 0.0,
                            'COGS': 0.0,
                            'Profit': 0.0,
                            'Items': [] 
                        }
                    
                    hierarchy[cust_id]['Orders'][order_id]['Revenue'] += row['DoanhThu']
                    hierarchy[cust_id]['Orders'][order_id]['COGS'] += row['GiaVon']
                    hierarchy[cust_id]['Orders'][order_id]['Profit'] += row['LaiGop']
                    
                    # 5. Thêm chi tiết Mã hàng (Đã được ép kiểu float ở bước 1)
                    row['Margin'] = (row['LaiGop'] / row['DoanhThu'] * 100) if row['DoanhThu'] else 0
                    hierarchy[cust_id]['Orders'][order_id]['Items'].append(row)

            # Tính % Margin tổng
            if summary['Revenue'] > 0:
                summary['AvgMargin'] = (summary['GrossProfit'] / summary['Revenue']) * 100
            
            # Chuyển đổi hierarchy từ Dict sang List
            final_list = []
            for cust in hierarchy.values():
                cust['Margin'] = (cust['Profit'] / cust['Revenue'] * 100) if cust['Revenue'] else 0
                
                orders_list = []
                for ord_val in cust['Orders'].values():
                    ord_val['Margin'] = (ord_val['Profit'] / ord_val['Revenue'] * 100) if ord_val['Revenue'] else 0
                    orders_list.append(ord_val)
                
                # Sắp xếp đơn hàng mới nhất lên đầu
                cust['Orders'] = sorted(orders_list, key=lambda x: x['Date'] or '', reverse=True)
                final_list.append(cust)
            
            # Sắp xếp Khách hàng theo Lợi nhuận giảm dần
            final_list.sort(key=lambda x: x['Profit'], reverse=True)

            return final_list, summary
            
        except Exception as e:
            print(f"Lỗi get_profit_analysis: {e}")
            return [], {'Revenue': 0, 'COGS': 0, 'GrossProfit': 0, 'AvgMargin': 0}

class InventoryService:
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    # sales_service.py (Hàm get_inventory_aging_data)

    
    def get_inventory_aging_data(self, item_filter_term, category_filter, qty_filter, value_filter, i05id_filter):
        """
        [UPDATED] Lấy dữ liệu tồn kho, GOM NHÓM THEO I04ID và Sắp xếp theo rủi ro.
        """
        
        RISK_THRESHOLD = 5000000.0 # 5 Triệu VNĐ
        
        # 1. Lấy dữ liệu Aging từ SP
        sp_query = "{CALL dbo.sp_GetInventoryAging (?)}" 
        aging_data = []
        try:
            raw_data = self.db.get_data(sp_query, (None,))
            if raw_data:
                aging_data = raw_data
        except Exception as e:
            print(f"Lỗi SP Inventory Aging: {e}")
            return [], {}

        # 2. [MỚI] Lấy Mapping I04ID từ IT1302
        i04_map = {}
        try:
            query_i04 = f"SELECT InventoryID, I04ID FROM {config.ERP_IT1302}"
            i04_data = self.db.get_data(query_i04)
            if i04_data:
                for row in i04_data:
                    i04_map[row['InventoryID']] = row['I04ID'] if row['I04ID'] and row['I04ID'].strip() else 'KHÁC'
        except Exception as e:
            print(f"Lỗi lấy mapping I04ID: {e}")

        # 3. [MỚI] Lấy Mapping Tên Nhóm từ [NOI DUNG HD]
        # Link: [NOI DUNG HD].[LOAI] = I04ID, Lấy cột [TEN]
        i04_name_map = {}
        try:
            query_name = f"SELECT [LOAI], [TEN] FROM {config.TEN_BANG_NOI_DUNG_HD}"
            name_data = self.db.get_data(query_name)
            if name_data:
                for row in name_data:
                    i04_name_map[row['LOAI']] = row['TEN']
        except Exception as e:
            print(f"Lỗi lấy tên nhóm I04: {e}")

        # 3. Xử lý Lọc, Tính toán & Gom nhóm
        totals = {
            'total_inventory': 0, 'total_quantity': 0, 'total_new_6_months': 0,
            'total_over_2_years': 0, 'total_clc_value': 0
        }
        
        # Dictionary để gom nhóm
        groups = {}

        # Parse điều kiện lọc
        qty_op, qty_thresh = parse_filter_string(qty_filter)
        val_op, val_thresh = parse_filter_string(value_filter)
        search_terms = [t.strip().lower() for t in item_filter_term.split(';') if t.strip()]

        for row in aging_data:
            # -- A. Ép kiểu số an toàn --
            row['TotalCurrentValue'] = safe_float(row.get('TotalCurrentValue'))
            row['TotalCurrentQuantity'] = safe_float(row.get('TotalCurrentQuantity'))
            row['Range_0_180_V'] = safe_float(row.get('Range_0_180_V'))
            row['Range_181_360_V'] = safe_float(row.get('Range_181_360_V'))
            row['Range_361_540_V'] = safe_float(row.get('Range_361_540_V'))
            row['Range_541_720_V'] = safe_float(row.get('Range_541_720_V'))
            row['Range_Over_720_V'] = safe_float(row.get('Range_Over_720_V'))
            
            # -- B. Tính chỉ số CLC (Risk Value) --
            stock_class = str(row.get('StockClass', '')).strip().upper()
            row['Risk_CLC_Value'] = 0.0
            if stock_class != 'D' and row['Range_Over_720_V'] > RISK_THRESHOLD:
                row['Risk_CLC_Value'] = row['Range_Over_720_V']

            # -- C. Kiểm tra điều kiện lọc --
            is_match = True
            
            # Lọc Text
            if search_terms:
                inv_str = str(row.get('InventoryID', '')).lower()
                name_str = str(row.get('InventoryName', '')).lower()
                if not any(term in inv_str or term in name_str for term in search_terms):
                    is_match = False
            
            # Lọc Ngành hàng
            if is_match and category_filter:
                cat_filter_val = category_filter.replace('!=', '').replace('<>', '').strip().lower()
                item_cat = str(row.get('InventoryTypeName', '')).lower()
                item_cat_code = str(row.get('ItemCategory', '')).lower()
                
                is_cat_match = (cat_filter_val in item_cat) or (cat_filter_val == item_cat_code)
                if category_filter.startswith(('!=', '<>')):
                    if is_cat_match: is_match = False
                else:
                    if not is_cat_match: is_match = False

            # Lọc I05
            if is_match and i05id_filter:
                i05_val = i05id_filter.replace('!=', '').replace('<>', '').strip().upper()
                if i05id_filter.startswith(('!=', '<>')):
                    if stock_class == i05_val: is_match = False
                else:
                    if stock_class != i05_val: is_match = False

            # Lọc Số lượng & Giá trị
            if is_match and qty_thresh is not None:
                is_match = evaluate_condition(row['TotalCurrentQuantity'], qty_op, qty_thresh)
            if is_match and val_thresh is not None:
                is_match = evaluate_condition(row['TotalCurrentValue'], val_op, val_thresh)

            # -- D. Cộng tổng & Gom nhóm --
            if is_match:
                # Cộng Totals toàn cục
                totals['total_inventory'] += row['TotalCurrentValue']
                totals['total_quantity'] += row['TotalCurrentQuantity']
                totals['total_new_6_months'] += row['Range_0_180_V']
                totals['total_over_2_years'] += row['Range_Over_720_V']
                totals['total_clc_value'] += row['Risk_CLC_Value']

                # Lấy mã nhóm I04 và Tên nhóm
                i04_code = i04_map.get(row['InventoryID'], 'KHÁC')
                # Lấy tên từ map, nếu không có thì dùng chính mã code
                i04_name = i04_name_map.get(i04_code, i04_code)
                if i04_code == 'KHÁC': i04_name = 'Khác / Chưa phân loại'
                
                # Tạo nhóm
                if i04_code not in groups:
                    groups[i04_code] = {
                        'GroupID': i04_code,
                        'GroupName': i04_name, # <--- Thêm trường Tên Nhóm
                        'Items': [],
                        'Group_TotalVal': 0.0,
                        'Group_TotalQty': 0.0,
                        'Group_Over720': 0.0,
                        'Group_CLC': 0.0
                    }
                
                # Cộng vào nhóm
                groups[i04_code]['Items'].append(row)
                groups[i04_code]['Group_TotalVal'] += row['TotalCurrentValue']
                groups[i04_code]['Group_TotalQty'] += row['TotalCurrentQuantity']
                groups[i04_code]['Group_Over720'] += row['Range_Over_720_V']
                groups[i04_code]['Group_CLC'] += row['Risk_CLC_Value']

        # 5. SẮP XẾP
        sorted_groups = sorted(
            groups.values(),
            key=lambda g: (g['Group_CLC'], g['Group_Over720']), 
            reverse=True
        )

        for group in sorted_groups:
            group['Items'] = sorted(
                group['Items'],
                key=lambda i: (i['Risk_CLC_Value'], i['Range_Over_720_V']),
                reverse=True
            )

        return sorted_groups, totals

    