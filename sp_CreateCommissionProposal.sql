USE [CRM_STDD]
GO

DECLARE @RC int
DECLARE @UserCode nvarchar(50)
DECLARE @CustomerID nvarchar(20)
DECLARE @DateFrom date
DECLARE @DateTo date
DECLARE @CommissionRate float

-- TODO: Set parameter values here.

EXECUTE @RC = [dbo].[sp_CreateCommissionProposal] 
   @UserCode
  ,@CustomerID
  ,@DateFrom
  ,@DateTo
  ,@CommissionRate
GO

