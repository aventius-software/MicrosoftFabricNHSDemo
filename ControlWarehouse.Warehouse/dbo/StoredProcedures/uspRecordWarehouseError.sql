CREATE PROCEDURE [dbo].[uspRecordWarehouseError]
    @dataSourceKey INT
	,@message VARCHAR(4000)
AS
BEGIN
    UPDATE 
	[dbo].[DataSources] 
	
	SET 
	[WarehouseCompletedDateTime] = GETDATE()
	,[WarehouseErrorMessage] = @message
	
	WHERE 
	[DataSourceKey] = @dataSourceKey
END