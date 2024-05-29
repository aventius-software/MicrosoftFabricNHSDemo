CREATE PROCEDURE [DatabaseAdministration].[uspCreateSchemasForDataset]
    @dataset VARCHAR(128)
AS
BEGIN
    DECLARE @tsql NVARCHAR(MAX)

    IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = @dataset)
    BEGIN
        SET @tsql = N'CREATE SCHEMA [' + @dataset + ']';	
        EXEC sp_executesql @stmt = @tsql;
    END
	
	IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = @dataset + 'Staging')
	BEGIN
        SET @tsql = N'CREATE SCHEMA [' + @dataset + 'Staging]';	
        EXEC sp_executesql @stmt = @tsql;
    END

    IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = @dataset + 'Dimension')
	BEGIN
        SET @tsql = N'CREATE SCHEMA [' + @dataset + 'Dimension]';	
        EXEC sp_executesql @stmt = @tsql;
    END

    IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = @dataset + 'Fact')
	BEGIN
        SET @tsql = N'CREATE SCHEMA [' + @dataset + 'Fact]';	
        EXEC sp_executesql @stmt = @tsql;
    END
END