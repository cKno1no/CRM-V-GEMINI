USE [CRM_STDD]
GO

DECLARE @RC int
DECLARE @ItemSearchTerm nvarchar(max)
DECLARE @ObjectID nvarchar(50)

-- TODO: Set parameter values here.

EXECUTE @RC = [dbo].[sp_GetSalesLookup_Block1] 
   @ItemSearchTerm
  ,@ObjectID
GO

