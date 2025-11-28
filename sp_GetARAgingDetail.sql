USE [CRM_STDD]
GO

/****** Object:  StoredProcedure [dbo].[sp_GetARAgingDetail]    Script Date: 20/11/2025 14:40:29 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

-- ĐỀ XUẤT: Tạo SP mới để lấy chi tiết Công nợ Quá hạn (VoucherNo)
-- File: SQL Server Management Studio (SSMS)
ALTER PROCEDURE [dbo].[sp_GetARAgingDetail]
    @SalesmanID NVARCHAR(10) = NULL,
    @ObjectID NVARCHAR(20) = NULL,
    @CurrentYear INT -- THÊM THAM SỐ NĂM
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 1. Lấy danh sách khách hàng mà Salesman phụ trách (từ DTCL) VÀ LỌC THEO NĂM
    DECLARE @ResponsibleObjectIDs NVARCHAR(MAX);
    
    IF @SalesmanID IS NOT NULL
    BEGIN
        SELECT @ResponsibleObjectIDs = COALESCE(@ResponsibleObjectIDs + ', ', '') + QUOTENAME(RTRIM([MA KH]), '''')
        FROM [dbo].[DTCL] 
        -- ÁP DỤNG LỌC NĂM CHO BẢNG PHỤ TRÁCH DS
        WHERE RTRIM([PHU TRACH DS]) = @SalesmanID AND Nam = @CurrentYear;
    END
    -- Nếu @SalesmanID là NULL (Admin xem tất cả), @ResponsibleObjectIDs vẫn là NULL.

    -- Bảng tạm Nợ (Debit) từ Sổ cái (GT9000)
    SELECT 
        T1.VoucherID, T1.ObjectID, T1.VoucherDate, T1.SalesManID,
        -- SỬA: LẤY INVOICE NO
        T1.InvoiceNo, 
        SUM(T1.OriginalAmount) AS TotalInvoiceAmount,
        DATEDIFF(day, T1.VoucherDate, GETDATE()) AS AgeDays
    INTO #Debits
    FROM [OMEGA_STDD].[dbo].[GT9000] AS T1
    WHERE 
        T1.DebitAccountID = '13111' 
        AND T1.CreditAccountID LIKE '5%' 
    GROUP BY 
        T1.VoucherID, T1.ObjectID, T1.VoucherDate, T1.SalesManID, T1.InvoiceNo, T1.OriginalAmount;

    -- Bảng tạm Thanh toán (Credit) từ Bảng Giải trừ (GT0303)
    SELECT 
        T2.DebitVoucherID, T2.ObjectID,
        SUM(T2.ConvertedAmount) AS TotalPaidAmount
    INTO #Credits
    FROM [OMEGA_STDD].[dbo].[GT0303] AS T2
    WHERE 
        T2.DebitVoucherID IN (SELECT VoucherID FROM #Debits)
        AND T2.ObjectID IN (SELECT ObjectID FROM #Debits)
    GROUP BY 
        T2.DebitVoucherID, T2.ObjectID;

    -- 2. Tính số dư và thông tin Hạn nợ cho TỪNG VOUCHER
    SELECT 
        D.VoucherID, D.ObjectID, D.VoucherDate, D.SalesManID, 
        D.InvoiceNo, D.TotalInvoiceAmount, -- CỘT TỔNG GIÁ TRỊ HÓA ĐƠN
        
        (D.TotalInvoiceAmount - ISNULL(C.TotalPaidAmount, 0)) AS RemainingBalance,
        
        T_KH.ReDueDays, T_KH.ShortObjectName, T_NV.SHORTNAME AS SalesManName,
        
        DATEADD(DAY, T_KH.ReDueDays, D.VoucherDate) AS DueDate,
        DATEDIFF(DAY, DATEADD(DAY, T_KH.ReDueDays, D.VoucherDate), GETDATE()) AS OverdueDays
    INTO #InvoiceDetails
    FROM #Debits AS D
    LEFT JOIN #Credits AS C ON D.VoucherID = C.DebitVoucherID AND D.ObjectID = C.ObjectID
    LEFT JOIN [OMEGA_STDD].[dbo].[IT1202] AS T_KH ON D.ObjectID = T_KH.ObjectID
    LEFT JOIN [dbo].[GD - NGUOI DUNG] AS T_NV ON D.SalesManID = T_NV.USERCODE
    WHERE 
        (D.TotalInvoiceAmount - ISNULL(C.TotalPaidAmount, 0)) > 1 -- Chỉ lấy số dư > 1
        -- LỌC CHỈ NỢ QUÁ HẠN HOẶC TRONG HẠN (KHÔNG CẦN CHỈ LỌC QUÁ HẠN LÚC NÀY)
        -- Tạm thời không lọc quá hạn ở đây để có thể tính Nợ Trong Hạn.
        
        -- Áp dụng LỌC SALESMAN (PHU TRACH DS) (Sử dụng danh sách từ bước 1)
        AND (@SalesmanID IS NULL 
             OR D.ObjectID IN (SELECT [MA KH] FROM [dbo].[DTCL] WHERE RTRIM([PHU TRACH DS]) = @SalesmanID AND Nam = @CurrentYear))
        
        -- Áp dụng LỌC KHÁCH HÀNG
        AND (@ObjectID IS NULL OR D.ObjectID = @ObjectID);

    -- 3. Tính Nợ Trong Hạn / Quá Hạn
    SELECT
        T_Inv.ObjectID, T_Inv.ShortObjectName, T_Inv.SalesManName, T_Inv.RemainingBalance,
        T_Inv.InvoiceNo, T_Inv.VoucherDate, T_Inv.DueDate, T_Inv.OverdueDays, T_Inv.TotalInvoiceAmount,
        
        -- YÊU CẦU 1: NỢ TRONG HẠN (OverdueDays <= 0)
        CASE WHEN T_Inv.OverdueDays <= 0 THEN T_Inv.RemainingBalance ELSE 0 END AS Debt_In_Term,
        
        -- YÊU CẦU 1: NỢ QUÁ HẠN (OverdueDays > 0)
        CASE WHEN T_Inv.OverdueDays > 0 THEN T_Inv.RemainingBalance ELSE 0 END AS Debt_Total_Overdue
        
    FROM #InvoiceDetails AS T_Inv;
    
    -- Dọn dẹp
    DROP TABLE #Debits;
    DROP TABLE #Credits;
    DROP TABLE #InvoiceDetails;
END
GO

