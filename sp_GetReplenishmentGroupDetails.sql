USE [CRM_STDD]
GO

DECLARE @RC int
DECLARE @Varchar05 nvarchar(100)

-- TODO: Set parameter values here.

EXECUTE @RC = [dbo].[sp_GetReplenishmentGroupDetails] 
   @Varchar05
GO

