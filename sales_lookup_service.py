from db_manager import DBManager, safe_float
from datetime import datetime, timedelta
import config

class SalesLookupService:
    """Cung cấp các thông tin tra cứu quan trọng phục vụ bán hàng."""
    
    def __init__(self, db_manager: DBManager):
        self.db = db_manager
        # Phạm vi thời gian cho lịch sử Khách hàng/Giá (3 năm)
        self.three_years_ago = (datetime.now() - timedelta(days=365*3)).strftime('%Y-%m-%d')
        self.today = datetime.now().strftime('%Y-%m-%d')

    def get_sales_lookup_data(self, inventory_ids, object_id=None, is_admin=False):
        """
        Hàm tổng hợp, chạy các truy vấn con và hợp nhất kết quả cho một danh sách mặt hàng.
        :param inventory_ids: Chuỗi các mã mặt hàng (ví dụ: 'MHA, MHB')
        :param object_id: Mã khách hàng (tùy chọn)
        :param is_admin: Quyền Admin để xem thông tin phức tạp
        :return: Danh sách các dict chứa dữ liệu tổng hợp
        """
        
        inv_list = [f"'{i.strip()}'" for i in inventory_ids.split(',') if i.strip()]
        if not inv_list:
            return []
        inventory_list_str = ",".join(inv_list)
        
        # 1. Lấy dữ liệu cơ bản (Tồn kho và Giá quy định)
        base_data = self._get_inventory_and_price01(inventory_list_str)
        
        # 2. Lấy Giá bán gần nhất (Hóa đơn)
        recent_sale_prices = self._get_recent_sale_price(inventory_list_str, object_id)

        # 3. Lấy Giá chào gần nhất (Báo giá)
        recent_quote_prices = self._get_recent_quote_price(inventory_list_str, object_id)
        
        results = []
        for item in base_data:
            inv_id = item['InventoryID']
            
            # Hợp nhất dữ liệu Giá
            item['RecentSalePrice'] = recent_sale_prices.get(inv_id, {'SalePrice': 'Không có', 'InvoiceDate': ''})
            item['RecentQuotePrice'] = recent_quote_prices.get(inv_id, {'QuotePrice': 'Không có', 'QuoteDate': ''})
            
            # Bổ sung khối dữ liệu CHỈ DÀNH CHO ADMIN
            if is_admin:
                # Khối 1: Lịch sử Báo giá thành công (Top 5 KH)
                item['Top5SuccessfulQuotes'] = self._get_top5_successful_quotes(inv_id)
                # Khối 2: Top 10 lần nhập kho gần nhất
                item['Top10RecentReceipts'] = self._get_top10_recent_receipts(inv_id)
            else:
                item['Top5SuccessfulQuotes'] = []
                item['Top10RecentReceipts'] = []
            
            results.append(item)
            
        return results

    # --- HÀM 1 & 3a: Tồn kho & Giá bán quy định (IT1302) ---
    def _get_inventory_and_price01(self, inventory_list_str):
        """Lấy tồn kho (Ton/Con) và Giá bán quy định (SalePrice01)."""
        query = f"""
            SELECT 
                T1.InventoryID, T1.InventoryName, 
                ISNULL(T2.Ton, 0) AS Ton, 
                ISNULL(T2.con, 0) AS BackOrder,
                ISNULL(T1.SalePrice01, 0) AS SalePrice01
            FROM {config.ERP_ITEM_PRICING} AS T1
            LEFT JOIN {config.CRM_BACK_ORDER_VIEW} AS T2 
                ON T1.InventoryID = T2.InventoryID 
            WHERE T1.InventoryID IN ({inventory_list_str})
        """
        return self.db.get_data(query)

    # --- HÀM 3b: Giá bán gần nhất (GT9000) Dùng Tài khoản Kế toán ---
    def _get_recent_sale_price(self, inventory_list_str, object_id):
        """
        Lấy Giá bán gần nhất trong 2 năm từ GT9000 (Sổ cái) bằng logic tài khoản kế toán.
        NỢ (Debit) Like '5%' (Doanh thu) và CÓ (Credit) = '13111' (Phải thu KH).
        """
        
        where_conditions = [
            f"T1.InventoryID IN ({inventory_list_str})",
            f"T1.VoucherDate BETWEEN '{self.three_years_ago}' AND '{self.today}'",
            # Lọc theo logic Kế toán: Giá bán (Sale Price)
            f"T1.DebitAccountID LIKE '5%'" ,
            f"T1.CreditAccountID = '13111'"
        ]
        
        # Lọc theo Khách hàng nếu có
        if object_id:
            # GT9000 sử dụng ObjectID cho đối tượng giao dịch
            where_conditions.append(f"T1.ObjectID = '{object_id}'")
            
        where_clause = " AND ".join(where_conditions)

        query = f"""
            WITH RankedSales AS (
                SELECT 
                    T1.InventoryID, 
                    T1.UnitPrice AS SalePrice, 
                    T1.VoucherDate,
                    ROW_NUMBER() OVER(
                        PARTITION BY T1.InventoryID 
                        ORDER BY T1.VoucherDate DESC
                    ) AS rn
                FROM {config.ERP_GENERAL_LEDGER} AS T1 -- GT9000
                WHERE {where_clause}
            )
            SELECT InventoryID, SalePrice, VoucherDate 
            FROM RankedSales 
            WHERE rn = 1
        """
        
        data = self.db.get_data(query)
        # Chuyển đổi thành dictionary {InventoryID: {SalePrice, VoucherDate}}
        return {d['InventoryID']: {'SalePrice': safe_float(d['SalePrice']), 'InvoiceDate': d['VoucherDate']} for d in data}

    # --- HÀM 3c: Giá chào gần nhất (OT2102) trong 2 năm ---
    def _get_recent_quote_price(self, inventory_list_str, object_id):
        """Lấy Giá chào gần nhất trong 2 năm từ OT2102."""
        
        where_conditions = [
            f"T1.InventoryID IN ({inventory_list_str})",
            # Yêu cầu là 2 năm, mặc dù phạm vi dữ liệu lịch sử khách hàng là 3 năm
            f"T2.QuotationDate BETWEEN DATEADD(year, -2, GETDATE()) AND GETDATE()"
        ]
        
        if object_id:
            where_conditions.append(f"T2.ObjectID = '{object_id}'")
            
        where_clause = " AND ".join(where_conditions)

        query = f"""
            WITH RankedQuotes AS (
                SELECT 
                    T1.InventoryID, T1.UnitPrice AS QuotePrice, T2.QuotationDate,
                    ROW_NUMBER() OVER(
                        PARTITION BY T1.InventoryID 
                        ORDER BY T2.QuotationDate DESC
                    ) AS rn
                FROM {config.ERP_QUOTE_DETAILS} AS T1
                INNER JOIN {config.ERP_QUOTES} AS T2
                    ON T1.QuotationID = T2.QuotationID
                WHERE {where_clause}
            )
            SELECT InventoryID, QuotePrice, QuotationDate 
            FROM RankedQuotes 
            WHERE rn = 1
        """
        data = self.db.get_data(query)
        return {d['InventoryID']: {'QuotePrice': safe_float(d['QuotePrice']), 'QuoteDate': d['QuotationDate']} for d in data}
    
    def _get_saleprice01(self, inventory_id):
        query = f"""
            SELECT TOP 1 SalePrice01
            FROM {config.ERP_ITEM_PRICING}
            WHERE InventoryID = ?
        """
        data = self.db.get_data(query, (inventory_id,))
        return safe_float(data[0]['SalePrice01']) if data else 0

    # --- KHỐI 1: Top 5 Khách hàng (Báo giá Thành công) trong 3 năm ---
    def _get_top5_successful_quotes(self, inventory_id):
        """
        Truy vấn Top 5 khách hàng có số lượng báo giá thành công lớn nhất (trong 3 năm) 
        và tính toán % GBQD (Giá bán thực tế / GBQD - 1).
        """
        
        gbqd = self._get_saleprice01(inventory_id) # Lấy giá GBQD
        
        # Ngăn chặn lỗi chia cho 0 trong SQL
        gbqd_safe = gbqd if gbqd > 0 else 1 

        query = f"""
            WITH SuccessfulQuotes AS (
                SELECT
                    T2.ObjectID AS ClientID, 
                    T2.QuotationDate,
                    T3.QuoQuantity, 
                    T3.UnitPrice,
                    T4.OrderQuantity AS InheritedQuantity
                    
                FROM {config.ERP_QUOTES} AS T2 -- OT2101
                INNER JOIN {config.ERP_QUOTE_DETAILS} AS T3 -- OT2102 (Chi tiết báo giá)
                    ON T2.QuotationID = T3.QuotationID
                
                LEFT JOIN {config.ERP_SALES_DETAIL} AS T4 -- OT2002
                    ON T3.TransactionID = T4.ReTransactionID 
                    
                WHERE T3.InventoryID = '{inventory_id}' 
                  AND T4.OrderQuantity IS NOT NULL
                  AND T2.QuotationDate BETWEEN '{self.three_years_ago}' AND '{self.today}'
            )
            -- Tính tổng số lượng báo giá thành công và tìm Top 5 KH
            SELECT TOP 5
                T1.ClientID,
                T5.ShortObjectName AS ClientName,
                SUM(T1.QuoQuantity) AS TotalQuoteQuantity,
                SUM(T1.InheritedQuantity) AS TotalSuccessQuantity,
                AVG(T1.UnitPrice) AS AverageQuotePrice,
                -- BỔ SUNG: Tính % GBQD (AVG(Price) / GBQD - 1) * 100
                CAST( (AVG(T1.UnitPrice) / {gbqd_safe} - 1) AS DECIMAL(10, 4)) * 100 AS PercentGBQD 
            FROM SuccessfulQuotes AS T1
            LEFT JOIN {config.ERP_IT1202} AS T5 ON T1.ClientID = T5.ObjectID 
            GROUP BY T1.ClientID, T5.ShortObjectName
            ORDER BY TotalSuccessQuantity DESC;
        """
        data = self.db.get_data(query)
        
        # Định dạng dữ liệu (ví dụ: hiển thị % với 2 chữ số)
        for row in data:
            percent = row.get('PercentGBQD', 0)
            row['PercentGBQD'] = f"{percent:.2f}%"
            
        return data

    # --- KHỐI 2: Top 10 lần Nhập kho gần nhất (WT2006/WT2007) ---
    def _get_top10_recent_receipts(self, inventory_id):
        
        query = f"""
            SELECT TOP 10
                T1.VoucherNo,
                T1.VoucherDate,
                T1.ObjectID AS CustomerID,
                T2.ShortObjectName AS CustomerName,
                T3.InventoryID,
                T3.ActualQuantity AS ReceiptQuantity, -- ĐÃ SỬA TỪ Quantity SANG ActualQuantity
                T3.UnitPrice AS ReceiptPrice
            FROM {config.ERP_GOODS_RECEIPT_MASTER} AS T1 -- WT2006
            INNER JOIN {config.ERP_IT1202} AS T2 ON T1.ObjectID = T2.ObjectID
            INNER JOIN {config.ERP_GOODS_RECEIPT_DETAIL} AS T3 -- WT2007
                ON T1.VoucherID = T3.VoucherID
            WHERE T3.InventoryID = '{inventory_id}' 
              AND T1.WarehouseID = 'Q4'
              AND T1.WarehouseID2 IS NULL 
            ORDER BY T1.VoucherDate DESC, T1.VoucherNo DESC;
        """
        return self.db.get_data(query)