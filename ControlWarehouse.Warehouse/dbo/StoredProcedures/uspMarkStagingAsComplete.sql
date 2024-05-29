CREATE PROCEDURE [dbo].[uspMarkStagingAsComplete]
    @dataSourceKey INT
AS
BEGIN
    UPDATE [dbo].[DataSources] SET [StagingCompletedDateTime] = GETDATE() WHERE [DataSourceKey] = @dataSourceKey
END