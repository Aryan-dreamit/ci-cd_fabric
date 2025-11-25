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