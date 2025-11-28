USE [CRM_STDD]
GO

/****** Object:  StoredProcedure [dbo].[sp_GetRealtimeSalesKPI]    Script Date: 26/10/2025 15:58:16 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_GetRealtimeSalesKPI]
    @SalesmanID NVARCHAR(10) = NULL,
    @CurrentYear INT
AS
BEGIN
    SET NOCOUNT ON;
    SET ANSI_PADDING OFF; 

    -- (Khai báo biến ngày tháng...)
    DECLARE @Today DATE = GETDATE();
    DECLARE @StartOfWeek DATE = DATEADD(wk, DATEDIFF(wk, 0, @Today), 0);
    DECLARE @EndOfWeek DATE = DATEADD(wk, DATEDIFF(wk, 0, @Today), 6);
    DECLARE @StartOfMonth DATE = DATEADD(month, DATEDIFF(month, 0, @Today), 0);
    DECLARE @EndOfMonth DATE = DATEADD(month, DATEDIFF(month, 0, @Today) + 1, -1);
    DECLARE @StartOfLastYear DATE = DATEFROMPARTS(@CurrentYear - 1, 1, 1);
    DECLARE @EndOfLastYear DATE = DATEFROMPARTS(@CurrentYear - 1, 12, 31);
    DECLARE @StartOfTwoYearsAgo DATE = DATEFROMPARTS(@CurrentYear - 2, 1, 1);
    DECLARE @StartOfOneYearAgo DATE = DATEADD(YEAR, -1, @Today); -- Biến 1 năm trước
    DECLARE @NextThreeMonthsStart DATE = DATEADD(month, DATEDIFF(month, 0, @Today) + 1, 0);
    DECLARE @NextThreeMonthsEnd DATE = DATEADD(day, -1, DATEADD(month, 4, @StartOfMonth));
    
    
    -- =========================================================================
    -- 1. DỮ LIỆU DOANH SỐ TỔNG HỢP (KPI) - ĐÃ THÊM TÍNH TOÁN TỔNG PO
    -- =========================================================================
    
    -- CTE: Tính toán Tổng Đơn Hàng Chờ Giao (POA)
    WITH PendingOrderAmount AS (
        SELECT 
            SUM(T1.saleAmount) AS TotalPOA
        FROM [OMEGA_STDD].[dbo].[OT2001] AS T1 
        LEFT JOIN (
            SELECT DISTINCT G.orderID FROM [OMEGA_STDD].dbo.GT9000 AS G WHERE G.VoucherTypeID = 'BH'
        ) AS Delivered ON T1.sorderid = Delivered.orderID
        WHERE 
            T1.orderStatus = 1 AND Delivered.orderID IS NULL -- Đơn hàng chờ giao
            AND T1.orderDate >= @StartOfOneYearAgo -- Trong 1 năm trở lại đây
            AND (@SalesmanID IS NULL OR RTRIM(T1.SalesManID) = RTRIM(@SalesmanID))
    ),
    -- CTE: Lọc dữ liệu Sales để tính YTD/Tháng
    FilteredSales AS (
        SELECT 
            T1.ConvertedAmount, T1.TranYear, T1.VoucherDate, T1.TranMonth
        FROM [OMEGA_STDD].dbo.GT9000 AS T1 
        WHERE T1.DebitAccountID = '13111' AND T1.CreditAccountID LIKE '5%' 
            AND T1.TranYear >= @CurrentYear - 1
            AND (T1.TranYear < @CurrentYear OR T1.VoucherDate <= @Today)
            AND (@SalesmanID IS NULL OR RTRIM(T1.SalesManID) = RTRIM(@SalesmanID))
    )
    -- Câu SELECT cuối cùng cho Bộ 1
    SELECT
        'Sales' AS KPI_Type,
        SUM(CASE WHEN T1.VoucherDate BETWEEN @StartOfWeek AND @EndOfWeek THEN ISNULL(T1.ConvertedAmount, 0) ELSE 0 END) AS Sales_CurrentWeek,
        SUM(CASE WHEN T1.VoucherDate BETWEEN @StartOfMonth AND @EndOfMonth THEN ISNULL(T1.ConvertedAmount, 0) ELSE 0 END) AS Sales_CurrentMonth,
        SUM(CASE WHEN T1.TranYear = @CurrentYear THEN ISNULL(T1.ConvertedAmount, 0) ELSE 0 END) AS Sales_YTD,
        SUM(CASE WHEN T1.VoucherDate BETWEEN @StartOfLastYear AND @EndOfLastYear THEN ISNULL(T1.ConvertedAmount, 0) ELSE 0 END) AS Sales_LastYear,
        
        -- KẾT QUẢ MỚI: Tổng giá trị PO
        (SELECT TotalPOA FROM PendingOrderAmount) AS PendingOrdersAmount 

    FROM FilteredSales AS T1
    HAVING COUNT(*) > 0 OR @SalesmanID IS NULL;

    
    -- =========================================================================
    -- 2. TOP 20 ĐƠN HÀNG CHƯA GIAO CÓ GIÁ TRỊ LỚN NHẤT (Đã giới hạn 1 năm)
    -- =========================================================================
    SELECT TOP 20 
        T1.VoucherNo, 
        CONVERT(VARCHAR(10), T1.orderDate, 120) AS TranDate, 
        T1.ObjectID, 
        T2.ShortObjectName AS ClientName, 
        T1.saleAmount AS TotalConvertedAmount
    FROM [OMEGA_STDD].[dbo].[OT2001] AS T1 
    LEFT JOIN [OMEGA_STDD].[dbo].[IT1202] AS T2 ON T1.ObjectID = T2.ObjectID 
    LEFT JOIN (
        SELECT DISTINCT G.orderID FROM [OMEGA_STDD].dbo.GT9000 AS G WHERE G.VoucherTypeID = 'BH'
    ) AS Delivered ON T1.sorderid = Delivered.orderID
    WHERE T1.orderStatus = 1 AND Delivered.orderID IS NULL
        AND T1.orderDate >= @StartOfOneYearAgo -- Lọc 1 năm
        AND (@SalesmanID IS NULL OR RTRIM(T1.SalesManID) = RTRIM(@SalesmanID))
    ORDER BY T1.saleAmount DESC;

    -- ... (Bộ 3, 4, 5 giữ nguyên) ...

    -- =========================================================================
    -- 3. TOP 10 ĐƠN HÀNG LỚN NHẤT (Tháng hiện tại)
    -- =========================================================================
    SELECT TOP 10 
        T1.VoucherNo, 
        T2.ShortObjectName AS ClientName, 
        T1.saleAmount AS TotalConvertedAmount
    FROM [OMEGA_STDD].[dbo].[OT2001] AS T1 
    LEFT JOIN [OMEGA_STDD].[dbo].[IT1202] AS T2 ON T1.ObjectID = T2.ObjectID 
    WHERE T1.orderDate BETWEEN @StartOfMonth AND @EndOfMonth AND T1.orderStatus = 1
        AND (@SalesmanID IS NULL OR RTRIM(T1.SalesManID) = RTRIM(@SalesmanID))
    ORDER BY T1.SALEAmount DESC;


    -- =========================================================================
    -- 4. TOP 10 BÁO GIÁ LỚN NHẤT CỦA THÁNG HIỆN TẠI (XỬ LÝ TẬP RỖNG)
    -- *ĐỊNH DẠNG NGÀY & TÊN NGẮN*
    -- =========================================================================
    IF EXISTS (
        SELECT 1 FROM [OMEGA_STDD].[dbo].[OT2101] AS T1 
        WHERE T1.QuotationDate BETWEEN @StartOfMonth AND @EndOfMonth AND T1.OrderStatus = 1 
        AND (@SalesmanID IS NULL OR RTRIM(T1.SalesManID) = RTRIM(@SalesmanID))
    )
    BEGIN
        SELECT TOP 10
            T1.QuotationNo AS VoucherNo, 
            CONVERT(VARCHAR(10), T1.QuotationDate, 120) AS QuoteDate, -- ĐỊNH DẠNG NGÀY
            T1.SaleAmount AS QuoteAmount, 
            T2.ShortObjectName AS ClientName 
        FROM [OMEGA_STDD].[dbo].[OT2101] AS T1 
        LEFT JOIN [OMEGA_STDD].[dbo].[IT1202] AS T2 ON T1.ObjectID = T2.ObjectID 
        WHERE T1.QuotationDate BETWEEN @StartOfMonth AND @EndOfMonth AND T1.OrderStatus = 1 
            AND (@SalesmanID IS NULL OR RTRIM(T1.SalesManID) = RTRIM(@SalesmanID))
        ORDER BY T1.SaleAmount DESC;
    END
    ELSE
    BEGIN
        -- KHÔNG CÓ DỮ LIỆU -> TRẢ VỀ CẤU TRÚC CỘT 0 HÀNG
        SELECT TOP 0
            CAST(NULL AS NVARCHAR(10)) AS VoucherNo, 
            CAST(NULL AS DATE) AS QuoteDate, 
            0.0 AS QuoteAmount, 
            CAST(NULL AS NVARCHAR(100)) AS ClientName
        FROM [OMEGA_STDD].dbo.GT9000
        WHERE 1 = 0; 
    END

    
    -- =========================================================================
    -- 5. TOP 20 MÃ PHẢI GIAO TRONG 3 THÁNG TỚI (SORT THEO GIÁ TRỊ)
    -- *ĐỊNH DẠNG NGÀY & SẮP XẾP*
    -- =========================================================================
    IF EXISTS (
        SELECT 1 FROM [OMEGA_STDD].[dbo].[OT2002] AS T1 
        INNER JOIN [OMEGA_STDD].[dbo].[OT2001] AS T2 ON T1.SOrderID = T2.SOrderID
        WHERE T2.orderStatus = 1 AND T1.Date01 BETWEEN @NextThreeMonthsStart AND @NextThreeMonthsEnd
            AND T1.DeliverQuantity < T1.OrderQuantity AND (@SalesmanID IS NULL OR RTRIM(T2.SalesManID) = RTRIM(@SalesmanID))
    )
    BEGIN
        -- CÓ DỮ LIỆU THẬT
        SELECT TOP 20
            T2.VoucherNo, T2.ObjectID, T3.ShortObjectName AS ClientName, 
            T1.InventoryID, 
            T4.InventoryName, 
            CONVERT(VARCHAR(10), T1.Date01, 120) AS DeliverDate, -- ĐỊNH DẠNG NGÀY
            (T1.OrderQuantity - T1.DeliverQuantity) AS RemainingQuantity, 
            T1.SalePrice * (T1.OrderQuantity - T1.DeliverQuantity) AS RemainingValue 
        FROM [OMEGA_STDD].[dbo].[OT2002] AS T1 
        INNER JOIN [OMEGA_STDD].[dbo].[OT2001] AS T2 ON T1.SOrderID = T2.SOrderID
        LEFT JOIN [OMEGA_STDD].[dbo].[IT1202] AS T3 ON T2.ObjectID = T3.ObjectID
        LEFT JOIN [OMEGA_STDD].[dbo].[IT1302] AS T4 ON T1.InventoryID = T4.InventoryID
        WHERE T2.orderStatus = 1 
            AND T1.Date01 BETWEEN @NextThreeMonthsStart AND @NextThreeMonthsEnd
            AND T1.DeliverQuantity < T1.OrderQuantity 
            AND (@SalesmanID IS NULL OR RTRIM(T2.SalesManID) = RTRIM(@SalesmanID))
        ORDER BY RemainingValue DESC; -- SẮP XẾP THEO GIÁ TRỊ CÒN LẠI GIẢM DẦN
    END
    ELSE
    BEGIN
        -- KHÔNG CÓ DỮ LIỆU -> TRẢ VỀ CẤU TRÚC CỘT 0 HÀNG
        SELECT TOP 0
            CAST(NULL AS NVARCHAR(10)) AS VoucherNo, 
            CAST(NULL AS NVARCHAR(10)) AS ObjectID, 
            CAST(NULL AS NVARCHAR(100)) AS ClientName, 
            CAST(NULL AS NVARCHAR(10)) AS InventoryID, 
            CAST(NULL AS NVARCHAR(100)) AS InventoryName,
            CAST(NULL AS DATE) AS DeliverDate, 
            0 AS RemainingQuantity, 
            0.0 AS RemainingValue
        FROM [OMEGA_STDD].dbo.GT9000
        WHERE 1 = 0;
    END

END
GO

