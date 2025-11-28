USE [CRM_STDD]
GO

DECLARE @RC int
DECLARE @CurrentYear int
DECLARE @UserCode nvarchar(50)
DECLARE @IsAdmin bit

-- TODO: Set parameter values here.

EXECUTE @RC = [dbo].[sp_GetSalesPerformanceSummary] 
   @CurrentYear
  ,@UserCode
  ,@IsAdmin
GO

