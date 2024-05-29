CREATE PROCEDURE [dbo].uspAddNewUrlDataSource
	@url VARCHAR(500)
	,@dataset VARCHAR(128)
	,@subset VARCHAR(128)
	,@format VARCHAR(50)
	,@hasHeaderRow BIT
	,@joinColumns VARCHAR(1000)
AS
BEGIN
	-- Find next primary key value to use
	DECLARE @maxID INT = (
		SELECT 
		MAX([DataSourceKey]) 
	
		FROM 
		[dbo].[DataSources]
	)

	IF @maxID IS NULL SET @maxID = 0

	-- Create a 'temp' table
	DROP TABLE IF EXISTS [dbo].[DataSourceStaging];
	CREATE TABLE [dbo].[DataSourceStaging] AS
	SELECT
	[Dataset] = @dataset
	,[Subset] = @subset
	,[Url] = @url
	,[DataFormat] = UPPER(@format)
	,[HasHeaderRow] = @hasHeaderRow
	,[JoinColumns] = @joinColumns	
	
	-- Add new data source if not already present
	INSERT INTO 
	[dbo].[DataSources] (
		[DataSourceKey]
		,[IsEnabled]
		,[IngestionMechanism]
		,[Dataset]
        ,[Subset]
        ,[Url]
        ,[DataFormat]
        ,[HasHeaderRow]
        ,[JoinColumns]
        ,[ProcessIntoLanding]
        ,[ProcessIntoStaging]
        ,[ProcessIntoWarehouse]
	)
     
	SELECT
	[DataSourceKey] = @maxID + ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	,[IsEnabled] = 1
	,[IngestionMechanism] = 'Notebook'
	,[Dataset]
	,[Subset]
	,[Url]
	,[DataFormat]
	,[HasHeaderRow]
	,[JoinColumns]
	,[ProcessIntoLanding] = 1
	,[ProcessIntoStaging] = 1
	,[ProcessIntoWarehouse] = 1

	FROM
    [dbo].[DataSourceStaging] [src]

	WHERE
    NOT EXISTS(
		SELECT
        1

		FROM
        [dbo].[DataSources] [trg]

		WHERE
        [src].[Url] = [trg].[Url]
	)

	-- All done
	DROP TABLE IF EXISTS [dbo].[DataSourceStaging];
END