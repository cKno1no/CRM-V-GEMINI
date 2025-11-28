USE [CRM_STDD]
GO

DECLARE @RC int
DECLARE @InventoryIDList nvarchar(50)

-- TODO: Set parameter values here.

EXECUTE @RC = [dbo].[sp_GetInventoryAging] 
   @InventoryIDList
GO

