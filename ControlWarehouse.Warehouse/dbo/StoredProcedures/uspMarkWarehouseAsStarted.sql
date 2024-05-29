CREATE PROCEDURE [dbo].[uspMarkWarehouseAsStarted]
    @dataSourceKey INT
AS
BEGIN
    UPDATE 
	[dbo].[DataSources] 
	
	SET 
	[WarehouseStartedDateTime] = GETDATE() 
	,[WarehouseCompletedDateTime] = NULL -- Reset incase of previous run
	,[WarehouseErrorMessage] = NULL -- Reset incase of previous run error

	WHERE 
	[DataSourceKey] = @dataSourceKey
END