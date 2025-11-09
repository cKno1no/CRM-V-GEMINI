# services/sales_lookup_service.py
# (Bản vá 8 - Đã sửa Yêu cầu 2: Logic tra cứu nhanh 'NSK 6210ZZ')

from db_manager import DBManager, safe_float
from datetime import datetime
import config
import pandas as pd 
import re 

class SalesLookupService:
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager

    # (Hàm get_sales_lookup_data không đổi, vẫn dùng SP)
    def get_sales_lookup_data(self, item_search_term, object_id):
        if not item_search_term: return {}
        object_id_param = object_id if object_id else None
        
        # Gửi chuỗi gốc (SP Bản vá 6 đã xử lý)
        sp_item_search_param = item_search_term 
        
        block1_data = self._get_block1_data(sp_item_search_param, object_id_param)
        
        # Lấy like_param cho các khối lịch sử
        like_param = f"%{item_search_term.split(',')[0].strip()}%" 
        
        block2_data = self._get_block2_history(like_param, object_id_param)
        block3_data = self._get_block3_history(like_param)
        
        return {'block1': block1_data, 'block2': block2_data, 'block3': block3_data}

    # --- HÀM TRA NHANH (SỬA YÊU CẦU 2) ---
    def get_quick_lookup_data(self, item_search_term):
        """
        Tra cứu nhanh Tồn kho/BO/Giá QĐ.
        (SỬA YÊU CẦU 2) Dùng logic AND LIKE (lọc nhiều lần bằng khoảng trắng)
        Ví dụ: "NSK 6210ZZ" -> ...LIKE '%NSK%' AND (...LIKE '%6210ZZ%')
        """
        if not item_search_term:
            return []

        # Tách các từ khóa tìm kiếm bằng KHOẢNG TRẮNG
        search_terms_list = [term.strip() for term in item_search_term.split(' ') if term.strip()]
        if not search_terms_list:
            return []
        
        where_conditions = []
        params = []
        
        # Xây dựng mệnh đề WHERE động (AND LIKE)
        for term in search_terms_list:
            like_val = f"%{term}%"
            # Mỗi từ khóa phải khớp VỚI MÃ hoặc TÊN
            where_conditions.append("(T1.InventoryID LIKE ? OR T1.InventoryName LIKE ?)")
            params.extend([like_val, like_val])

        # Nối các điều kiện bằng 'AND'
        where_clause = " AND ".join(where_conditions)

        query = f"""
            SELECT 
                T1.InventoryID, 
                T1.InventoryName,
                ISNULL(T2_Sum.Ton, 0) AS Ton, 
                ISNULL(T2_Sum.BackOrder, 0) AS BackOrder,
                ISNULL(T1.SalePrice01, 0) AS GiaBanQuyDinh
            FROM [OMEGA_STDD].[dbo].[IT1302] AS T1
            LEFT JOIN (
                SELECT 
                    InventoryID, 
                    SUM(Ton) as Ton, 
                    SUM(con) as BackOrder 
                FROM [OMEGA_STDD].[dbo].[CRM_TON KHO BACK ORDER]
                GROUP BY InventoryID
            ) AS T2_Sum ON T1.InventoryID = T2_Sum.InventoryID
            WHERE 
                ({where_clause}) -- Áp dụng bộ lọc AND
            ORDER BY
                T1.InventoryID
        """
        
        data = self.db.get_data(query, tuple(params))
        
        if not data:
            return []
            
        formatted_data = []
        for row in data:
            row['Ton'] = safe_float(row.get('Ton'))
            row['BackOrder'] = safe_float(row.get('BackOrder'))
            row['GiaBanQuyDinh'] = safe_float(row.get('GiaBanQuyDinh'))
            formatted_data.append(row)
        return formatted_data
    # --- KẾT THÚC CẬP NHẬT YÊU CẦU 2 ---

    def _format_date_safe(self, date_val):
        if pd.isna(date_val) or not isinstance(date_val, (datetime, pd.Timestamp)):
            return '—'
        return date_val.strftime('%d/%m/%Y')

    def _get_block1_data(self, sp_item_search_param, object_id_param):
        try:
            sp_params = (sp_item_search_param, object_id_param) 
            data = self.db.execute_sp_multi('dbo.sp_GetSalesLookup_Block1', sp_params)
            
            if data and len(data) > 0:
                formatted_data = []
                for row in data[0]:
                    row['Ton'] = safe_float(row.get('Ton'))
                    row['BackOrder'] = safe_float(row.get('BackOrder'))
                    row['GiaBanQuyDinh'] = safe_float(row.get('GiaBanQuyDinh'))
                    row['GiaBanGanNhat_HD'] = safe_float(row.get('GiaBanGanNhat_HD'))
                    row['GiaChaoGanNhat_BG'] = safe_float(row.get('GiaChaoGanNhat_BG'))
                    row['NgayGanNhat_HD'] = self._format_date_safe(row.get('NgayGanNhat_HD'))
                    row['NgayGanNhat_BG'] = self._format_date_safe(row.get('NgayGanNhat_BG'))
                    formatted_data.append(row)
                return formatted_data
            else:
                return []
        except Exception as e:
            print(f"LỖI SP sp_GetSalesLookup_Block1: {e}")
            return []

    def _get_block2_history(self, like_param, object_id_param):
        
        where_conditions = ["(InventoryID LIKE ? OR InventoryName LIKE ?)"]
        params = [like_param, like_param]
        
        if object_id_param:
            where_conditions.append("ObjectID = ?")
            params.append(object_id_param)

        where_clause = " AND ".join(where_conditions)
        
        query = f"""
            SELECT TOP 20
                VoucherNo, OrderDate, 
                InventoryID, InventoryName, OrderQuantity, SalePrice,
                Description AS SoPXK, VoucherDate AS NgayPXK, ActualQuantity AS SL_PXK, 
                InvoiceNo AS SoHoaDon, InvoiceDate AS NgayHoaDon, Quantity AS SL_HoaDon
            FROM [OMEGA_STDD].[dbo].[CRM_TV_THONG TIN DHB_FULL]
            WHERE {where_clause}
            ORDER BY OrderDate DESC
        """
        
        data = self.db.get_data(query, tuple(params))
        
        if not data:
            return []
        
        for row in data:
            row['OrderDate'] = self._format_date_safe(row.get('OrderDate'))
            row['NgayPXK'] = self._format_date_safe(row.get('NgayPXK'))
            row['NgayHoaDon'] = self._format_date_safe(row.get('NgayHoaDon'))
        
        return data

    def _get_block3_history(self, like_param):
        
        query = f"""
            SELECT TOP 20
                VoucherNo, OrderDate, 
                InventoryID, InventoryName, OrderQuantity, SalePrice,
                PO AS SoPO, ShipDate AS NgayPO, [PO SL] AS SL_PO, 
                Description AS SoPN, VoucherDate AS NgayPN, ActualQuantity AS SL_PN
            FROM [OMEGA_STDD].[dbo].[CRM_TV_THONG TIN DHB_FULL 2]
            WHERE 
                (InventoryID LIKE ? OR InventoryName LIKE ?)
            ORDER BY 
                OrderDate DESC
        """
        params = (like_param, like_param)
        data = self.db.get_data(query, params)
        
        if not data:
            return []

        for row in data:
            row['OrderDate'] = self._format_date_safe(row.get('OrderDate'))
            row['NgayPO'] = self._format_date_safe(row.get('NgayPO'))
            row['NgayPN'] = self._format_date_safe(row.get('NgayPN'))

        return data

    def check_purchase_history(self, customer_id, inventory_id):
        query = f"""
            SELECT TOP 1 InvoiceDate
            FROM [OMEGA_STDD].[dbo].[CRM_TV_THONG TIN DHB_FULL]
            WHERE 
                ObjectID = ? 
                AND InventoryID = ?
                AND InvoiceNo IS NOT NULL
            ORDER BY 
                InvoiceDate DESC
        """
        params = (customer_id, inventory_id)
        data = self.db.get_data(query, params)
        
        if not data:
            return None
        return self._format_date_safe(data[0].get('InvoiceDate'))