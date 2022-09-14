CREATE OR ALTER FUNCTION study_database_name (@study_name NVARCHAR(1000)) RETURNS NVARCHAR(1000) AS
BEGIN
	DECLARE @result NVARCHAR(1000)
	SELECT @result = 'wh_study_' + TRANSLATE(LOWER(@study_name), ' -', '__')
    RETURN @result
END
;
