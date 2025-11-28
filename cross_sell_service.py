# services/cross_sell_service.py

from db_manager import DBManager, safe_float
from datetime import datetime
import config

class CrossSellService:
    """
    Service xử lý logic phân tích bán chéo (Cross-selling) dựa trên nhóm vật tư (I04ID).
    [UPDATED]: Sử dụng Rolling 12 Months (365 ngày gần nhất) thay vì YTD.
    """
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    def get_i04_master_list(self):
        """Lấy danh sách mã I04ID chuẩn."""
        query = f"""
            SELECT DISTINCT I04ID 
            FROM {config.ERP_IT1302} 
            WHERE I04ID IS NOT NULL AND I04ID <> ''
            ORDER BY I04ID
        """
        data = self.db.get_data(query)
        return [row['I04ID'] for row in data] if data else []

    def get_i04_name_map(self):
        """Lấy Mapping Mã -> Tên Nhóm từ bảng [NOI DUNG HD]."""
        try:
            query = f"SELECT [LOAI], [TEN] FROM {config.TEN_BANG_NOI_DUNG_HD}"
            data = self.db.get_data(query)
            return {row['LOAI']: row['TEN'] for row in data} if data else {}
        except Exception as e:
            print(f"Lỗi lấy tên nhóm I04: {e}")
            return {}

    def get_cross_sell_dna(self, year=None): # Tham số 'year' giữ lại để tương thích nhưng không dùng nữa
        """
        Lấy dữ liệu tổng hợp DNA bán chéo cho Dashboard.
        Logic: Lấy dữ liệu trong 365 ngày gần nhất (Rolling Year).
        """
        
        # 1. Lấy danh sách Master & Tên Nhóm
        master_i04_list = self.get_i04_master_list()
        i04_names = self.get_i04_name_map()
        
        # 2. Truy vấn Doanh số (SỬA ĐỔI LOGIC TẠI ĐÂY)
        # Thay vì: T1.TranYear = ? 
        # Thành: T1.VoucherDate >= DATEADD(day, -365, GETDATE())
        
        query = f"""
            SELECT 
                T1.ObjectID AS ClientID,
                ISNULL(T3.ShortObjectName, T3.ObjectName) AS ClientName,
                T2.I04ID,
                SUM(CASE WHEN T1.CreditAccountID LIKE '511%' THEN T1.ConvertedAmount ELSE 0 END) as Revenue,
                SUM(CASE WHEN T1.DebitAccountID LIKE '632%' THEN T1.ConvertedAmount ELSE 0 END) as COGS
            FROM {config.ERP_GIAO_DICH} T1
            INNER JOIN {config.ERP_IT1302} T2 ON T1.InventoryID = T2.InventoryID
            LEFT JOIN {config.ERP_IT1202} T3 ON T1.ObjectID = T3.ObjectID
            WHERE 
                T1.VoucherDate >= DATEADD(day, -365, GETDATE()) -- Lấy 12 tháng gần nhất
                AND T2.I04ID IS NOT NULL AND T2.I04ID <> ''
                AND (T1.CreditAccountID LIKE '511%' OR T1.DebitAccountID LIKE '632%')
            GROUP BY T1.ObjectID, T3.ShortObjectName, T3.ObjectName, T2.I04ID
        """
        
        raw_data = self.db.get_data(query) # Không cần truyền tham số năm nữa
        
        if not raw_data:
            return {'buckets': {'titan': [], 'diamond': [], 'growth': [], 'opp': []}, 
                    'summary': {'titan_count':0, 'diamond_count':0, 'growth_count':0, 'opp_count':0}, 
                    'master_dna': master_i04_list}

        # 3. Xử lý dữ liệu
        customers = {}
        
        for row in raw_data:
            client_id = row['ClientID']
            i04_id = row['I04ID']
            rev = safe_float(row['Revenue'])
            cogs = safe_float(row['COGS'])
            
            if client_id not in customers:
                customers[client_id] = {
                    'ClientID': client_id,
                    'ClientName': row['ClientName'] or client_id,
                    'TotalRevenue': 0.0,
                    'TotalProfit': 0.0,
                    'Purchased_I04': set(),
                    'DNA_Map': {}
                }
            
            cust = customers[client_id]
            cust['TotalRevenue'] += rev
            cust['TotalProfit'] += (rev - cogs)
            
            if rev > 0:
                cust['Purchased_I04'].add(i04_id)
                cust['DNA_Map'][i04_id] = {
                    'Margin': ((rev - cogs) / rev * 100) if rev > 0 else 0
                }

        # 4. Phân loại & Tạo Visual
        buckets = {'titan': [], 'diamond': [], 'growth': [], 'opp': []}
        LOW_MARGIN_THRESHOLD = 10.0 

        for client_id, data in customers.items():
            i04_count = len(data['Purchased_I04'])
            data['I04_Count'] = i04_count
            data['AvgMargin'] = (data['TotalProfit'] / data['TotalRevenue'] * 100) if data['TotalRevenue'] > 0 else 0
            
            dna_visual = []
            for m_code in master_i04_list:
                group_name = i04_names.get(m_code, m_code)
                if m_code in data['DNA_Map']:
                    item_margin = data['DNA_Map'][m_code]['Margin']
                    status = 'active-low-margin' if item_margin < LOW_MARGIN_THRESHOLD else 'active'
                    tooltip_text = f"{group_name}: {item_margin:.1f}%"
                    dna_visual.append({'status': status, 'code': m_code, 'tooltip': tooltip_text})
                else:
                    tooltip_text = f"{group_name}: Chưa mua"
                    dna_visual.append({'status': '', 'code': m_code, 'tooltip': tooltip_text})
            
            data['DNA_Visual'] = dna_visual

            # Logic phân nhóm (Giữ nguyên)
            if i04_count > 15: buckets['titan'].append(data)
            elif 10 <= i04_count <= 15: buckets['diamond'].append(data)
            elif 4 <= i04_count <= 9: buckets['growth'].append(data)
            else: buckets['opp'].append(data)

        # 5. Sort
        for key in buckets:
            buckets[key].sort(key=lambda x: x['TotalRevenue'], reverse=True)

        return {
            'buckets': buckets,
            'master_dna': master_i04_list,
            'summary': {
                'titan_count': len(buckets['titan']),
                'diamond_count': len(buckets['diamond']),
                'growth_count': len(buckets['growth']),
                'opp_count': len(buckets['opp'])
            }
        }

    def get_customer_gap_analysis(self, client_id, year=None):
        """
        API Detail: Trả về chi tiết theo 12 tháng gần nhất.
        """
        master_i04 = self.get_i04_master_list()
        i04_names = self.get_i04_name_map()
        
        # Cập nhật query chi tiết cũng dùng Rolling 12 Months
        query = f"""
            SELECT 
                T2.I04ID,
                SUM(CASE WHEN T1.CreditAccountID LIKE '511%' THEN T1.ConvertedAmount ELSE 0 END) as Revenue,
                SUM(CASE WHEN T1.DebitAccountID LIKE '632%' THEN T1.ConvertedAmount ELSE 0 END) as COGS
            FROM {config.ERP_GIAO_DICH} T1
            INNER JOIN {config.ERP_IT1302} T2 ON T1.InventoryID = T2.InventoryID
            WHERE 
                T1.ObjectID = ? 
                AND T1.VoucherDate >= DATEADD(day, -365, GETDATE()) -- Lọc 365 ngày
                AND T2.I04ID IS NOT NULL
            GROUP BY T2.I04ID
            HAVING SUM(CASE WHEN T1.CreditAccountID LIKE '511%' THEN T1.ConvertedAmount ELSE 0 END) > 0
        """
        purchased_data = self.db.get_data(query, (client_id,))
        
        purchased_map = {}
        if purchased_data:
            for row in purchased_data:
                rev = safe_float(row['Revenue'])
                cogs = safe_float(row['COGS'])
                purchased_map[row['I04ID']] = {
                    'Revenue': rev,
                    'Profit': rev - cogs,
                    'Margin': ((rev - cogs) / rev * 100) if rev > 0 else 0
                }
        
        bought_list = []
        white_space_list = []
        
        for code in master_i04:
            group_name = i04_names.get(code, code)
            if code in purchased_map:
                item = purchased_map[code]
                bought_list.append({
                    'I04ID': code,
                    'GroupName': group_name,
                    'Revenue': item['Revenue'],
                    'Profit': item['Profit'],
                    'Margin': item['Margin']
                })
            else:
                white_space_list.append({
                    'I04ID': code, 
                    'GroupName': group_name,
                    'Potential': 'High'
                })
                
        bought_list.sort(key=lambda x: x['Margin'])
        
        return {
            'bought': bought_list,
            'white_space': white_space_list
        }