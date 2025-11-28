USE [CRM_STDD]
GO

DECLARE @RC int
DECLARE @ObjectID nvarchar(100)

-- TODO: Set parameter values here.

EXECUTE @RC = [dbo].[sp_GetCustomerReplenishmentSuggest] 
   @ObjectID
GO

