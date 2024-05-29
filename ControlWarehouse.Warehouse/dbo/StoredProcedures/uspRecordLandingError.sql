CREATE PROCEDURE [dbo].[uspRecordLandingError]
    @dataSourceKey INT
	,@message VARCHAR(4000)
AS
BEGIN
    UPDATE 
	[dbo].[DataSources] 
	
	SET 
	[LandingCompletedDateTime] = GETDATE()
	,[LandingErrorMessage] = @message
	
	WHERE 
	[DataSourceKey] = @dataSourceKey
END