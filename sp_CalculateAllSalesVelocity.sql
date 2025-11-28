USE [CRM_STDD]
GO

DECLARE @RC int

-- TODO: Set parameter values here.

EXECUTE @RC = [dbo].[sp_CalculateAllSalesVelocity] 
GO

