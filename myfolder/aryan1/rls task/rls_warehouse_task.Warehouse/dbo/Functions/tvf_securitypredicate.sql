-- Creating schema for Security
-- CREATE SCHEMA Security;
-- GO
 
-- Creating a function for the SalesRep evaluation
CREATE FUNCTION dbo.tvf_securitypredicate(@SalesRep AS nvarchar(50))
    RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS tvf_securitypredicate_result
WHERE @SalesRep = USER_NAME() OR USER_NAME() = 'shivam@dreamitcs.com' OR USER_NAME()='samiksha@dreamitcs.com';