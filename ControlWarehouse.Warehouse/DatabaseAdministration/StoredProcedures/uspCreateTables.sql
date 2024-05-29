CREATE PROCEDURE [DatabaseAdministration].[uspCreateTables] AS 
BEGIN
	-- Create table
	DROP TABLE IF EXISTS [dbo].[DataSources];
	CREATE TABLE [dbo].[DataSources] (
		[DataSourceKey]                 INT NOT NULL            -- Identity key
		,[IsEnabled]                    BIT NOT NULL            -- Flag to enable or disable
		,[IngestionMechanism]           VARCHAR(500) NOT NULL   -- Name of ingestion mechanism e.g. Notebook, Query    
		,[Dataset]						VARCHAR(500) NOT NULL   -- Name of this dataset
		,[Subset]						VARCHAR(500) NULL		-- Name of this dataset subset
	
		,[Notebook]                     VARCHAR(500) NULL       -- Name of notebook to run if using one instead of COPY activity
		,[Url]                          VARCHAR(500) NULL       -- Url of data source if its a http source
		,[DataFormat]                   VARCHAR(500) NULL       -- Type of data source e.g. CSV, JSON, etc...
		,[HasHeaderRow]					BIT NOT NULL
		,[JoinColumns]                  VARCHAR(1000) NULL      -- Comma separated list of join columns for merges

		,[ProcessIntoLanding]           BIT NOT NULL            -- Flag to enable or disable
		,[ProcessIntoStaging]           BIT NOT NULL            -- Flag to enable or disable
		,[ProcessIntoWarehouse]         BIT NOT NULL            -- Flag to enable or disable
    
		,[LandingStartedDateTime]       DATETIME2(0) NULL       -- DateTime for when the landing started (i.e. start landing)
		,[LandingCompletedDateTime]     DATETIME2(0) NULL       -- DateTime for when the landing completed
		,[LandingErrorMessage]			VARCHAR(4000) NULL

		,[StagingStartedDateTime]       DATETIME2(0) NULL       -- DateTime when staging (bronze/silver) was started
		,[StagingCompletedDateTime]     DATETIME2(0) NULL       -- DateTime when staging (bronze/silver) was completed
		,[StagingErrorMessage]			VARCHAR(4000) NULL

		,[WarehouseStartedDateTime]     DATETIME2(0) NULL       -- DateTime all warehouse (gold) processing was started
		,[WarehouseCompletedDateTime]   DATETIME2(0) NULL       -- DateTime all warehouse (gold) processing was completed
		,[WarehouseErrorMessage]		VARCHAR(4000) NULL

		,[WarehouseProcedureSchema]		VARCHAR(128) NULL		-- Optional name of warehouse proc schema to run
		,[WarehouseProcedureName]		VARCHAR(128) NULL		-- Optional name of warehouse proc to run
		,[WarehouseProcedureParameter]	VARCHAR(4000) NULL		-- Optional name of warehouse proc parameter  
		
		,[Notes]						VARCHAR(4000) NULL		-- Optional notes or references
	);

	-- Add primary key to table
	ALTER TABLE [dbo].[DataSources]
	ADD CONSTRAINT [PK_DataSources] PRIMARY KEY NONCLUSTERED ([DataSourceKey]) NOT ENFORCED;
END