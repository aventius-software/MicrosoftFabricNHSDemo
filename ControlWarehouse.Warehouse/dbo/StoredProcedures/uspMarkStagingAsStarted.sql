CREATE PROCEDURE [dbo].[uspMarkStagingAsStarted]
    @dataSourceKey INT
AS
BEGIN
    UPDATE 
	[dbo].[DataSources] 
	
	SET 
	[StagingStartedDateTime] = GETDATE() 
	,[StagingCompletedDateTime] = NULL -- Reset incase of previous run
	,[StagingErrorMessage] = NULL -- Reset incase of previous run error

	WHERE 
	[DataSourceKey] = @dataSourceKey
END