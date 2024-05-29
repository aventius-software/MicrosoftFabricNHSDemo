CREATE PROCEDURE [dbo].[uspMarkLandingAsComplete]
    @dataSourceKey INT
AS
BEGIN
    UPDATE [dbo].[DataSources] SET [LandingCompletedDateTime] = GETDATE() WHERE [DataSourceKey] = @dataSourceKey
END