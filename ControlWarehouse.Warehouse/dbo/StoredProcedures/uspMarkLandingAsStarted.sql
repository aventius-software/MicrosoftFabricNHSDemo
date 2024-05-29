CREATE PROCEDURE [dbo].[uspMarkLandingAsStarted]
    @dataSourceKey INT
AS
BEGIN
    UPDATE 
	[dbo].[DataSources] 
	
	SET 
	[LandingStartedDateTime] = GETDATE()
	,[LandingCompletedDateTime] = NULL -- Reset incase of previous run
	,[LandingErrorMessage] = NULL -- Reset incase of previous run error

	WHERE 
	[DataSourceKey] = @dataSourceKey
END