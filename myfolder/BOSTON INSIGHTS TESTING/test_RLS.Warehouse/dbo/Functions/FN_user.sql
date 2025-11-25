CREATE FUNCTION dbo.FN_user(@email VARCHAR(255))
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
(
    SELECT 1 AS fn_userResult
    WHERE @email = USER_NAME()             -- normal users see their own rows
       OR USER_NAME() = 'Aryan@dreamitcs.com'  -- this email can see all rows
);