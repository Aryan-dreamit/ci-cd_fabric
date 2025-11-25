-- Creating schema for Security
-- CREATE SCHEMA Security;
-- GO
 
-- Creating a function for the SalesRep evaluation
CREATE FUNCTION dbo.tvf_securitypredicate1(@SalesRep AS nvarchar(50))
    RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS tvf_securitypredicate_result1
WHERE @SalesRep = USER_NAME() 
GO
 
-- Using the function to create a Security Policy
CREATE SECURITY POLICY SalesFilter
ADD FILTER PREDICATE dbo.tvf_securitypredicate1(SalesRep)
ON dbo.Orders
WITH (STATE = ON);
GO

-- DROP SECURITY POLICY IF EXISTS SalesFilter;
-- GO
-- DROP FUNCTION IF EXISTS dbo.tvf_securitypredicate1;
-- GO
-- select distinct SalesRep from dbo.Ordersf 

-- select * from [dbo].[Orders1]


    -- UPDATE dbo.Ordersf 
    -- SET SalesRep=Shivam@dreamitcs.com
    -- WHERE SalesRep=shivam@dreamitcs.com[rls_warehouse_task]