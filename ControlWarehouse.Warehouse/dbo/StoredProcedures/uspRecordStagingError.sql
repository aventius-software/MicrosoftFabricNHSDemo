CREATE PROCEDURE [dbo].[uspRecordStagingError]
    @dataSourceKey INT
	,@message VARCHAR(4000)
AS
BEGIN
    UPDATE 
	[dbo].[DataSources] 
	
	SET 
	[StagingCompletedDateTime] = GETDATE()
	,[StagingErrorMessage] = @message
	
	WHERE 
	[DataSourceKey] = @dataSourceKey
END