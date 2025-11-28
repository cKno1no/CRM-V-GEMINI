USE [CRM_STDD]
GO

DECLARE @RC int
DECLARE @FromDate date
DECLARE @ToDate date
DECLARE @SalesmanID nvarchar(50)

-- TODO: Set parameter values here.

EXECUTE @RC = [dbo].[sp_GetSalesGrossProfit_Analysis] 
   @FromDate
  ,@ToDate
  ,@SalesmanID
GO

