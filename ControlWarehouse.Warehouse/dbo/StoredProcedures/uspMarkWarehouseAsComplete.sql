CREATE PROCEDURE [dbo].[uspMarkWarehouseAsComplete]
    @dataSourceKey INT
AS
BEGIN
    UPDATE [dbo].[DataSources] SET [WarehouseCompletedDateTime] = GETDATE() WHERE [DataSourceKey] = @dataSourceKey
END