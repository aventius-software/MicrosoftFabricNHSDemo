CREATE VIEW [dbo].[vwDataSources] AS

SELECT
[DataSourceKey] -- A unique identifier to refer to this data source

-- A simple flag to indicate if any step of the import is in progress
,[IsInProgress] = CAST(CASE
	WHEN (
		[LandingStartedDateTime] IS NOT NULL -- If landing has started...
		AND [LandingCompletedDateTime] IS NULL -- ...and has not completed yet
	) OR (
		[StagingStartedDateTime] IS NOT NULL -- If staging has started...
		AND [StagingCompletedDateTime] IS NULL -- ...and has not completed yet
	) OR (
		[WarehouseStartedDateTime] IS NOT NULL -- If warehouse has started...
		AND [WarehouseCompletedDateTime] IS NULL -- ...and has not completed yet
	)
	THEN 1 -- Then this data source load is in process

	ELSE 0 -- Otherwise it isn't
END AS BIT)

-- Name of the current step in progress, if nothing is in progress then this will be NULL
,[CurrentStep] = CASE
	-- If landing has started but not completed then we're still running landing step...
	WHEN [LandingStartedDateTime] IS NOT NULL
	AND [LandingCompletedDateTime] IS NULL
	THEN 'Landing'

	-- If staging has started but not completed then we're still running staging step...
	WHEN [StagingStartedDateTime] IS NOT NULL
	AND [StagingCompletedDateTime] IS NULL
	THEN 'Staging'

	-- If warehouse has started but not completed then we're still running warehouse step...
	WHEN [WarehouseStartedDateTime] IS NOT NULL
	AND [WarehouseCompletedDateTime] IS NULL
	THEN 'Warehouse'

	ELSE NULL
END

-- Add's up each steps duration into an hours/minutes format
,[TotalDuration] = CASE
	WHEN [WarehouseStartedDateTime] IS NOT NULL
	THEN CONVERT(
		VARCHAR(5), 
		DATEADD(MINUTE, DATEDIFF(MINUTE, [LandingStartedDateTime], ISNULL([WarehouseCompletedDateTime], GETDATE())), 0), 
		114
	)

	WHEN [StagingStartedDateTime] IS NOT NULL
	THEN CONVERT(
		VARCHAR(5), 
		DATEADD(MINUTE, DATEDIFF(MINUTE, [LandingStartedDateTime], ISNULL([StagingCompletedDateTime], GETDATE())), 0), 
		114
	)

	WHEN [LandingStartedDateTime] IS NOT NULL
	THEN CONVERT(
		VARCHAR(5), 
		DATEADD(MINUTE, DATEDIFF(MINUTE, [LandingStartedDateTime], GETDATE()), 0), 
		114
	)

	ELSE NULL
END

-- If any step has an error message then obviously this import has failed ;-)
,[HasFailed] = CAST(CASE
	WHEN [LandingErrorMessage] IS NOT NULL
	OR [StagingErrorMessage] IS NOT NULL
	OR [WarehouseErrorMessage] IS NOT NULL
	THEN 1

	ELSE 0
END AS BIT)

,[IsEnabled]
,[IngestionMechanism]

,[Dataset]
,[Subset]

-- The Fabric lakehouse names mean lowercase and underscores, no spaces, so we do a little text adjustment
,[LakehouseDatasetName] = REPLACE(LOWER([Dataset]), ' ', '_')
,[LakehouseSubsetName] = REPLACE(LOWER([Subset]), ' ', '_')
,[LakehouseTableName] = REPLACE(LOWER([Dataset] + CASE WHEN [Subset] IS NULL THEN '' ELSE ' ' + [Subset] END), ' ', '_')

-- We don't want spaces in our warehouse schema names!
,[WarehouseSchemaName] = REPLACE([Dataset], ' ', '')

,[Notebook] -- Name of the custom notebook to execute (not yet implemented)
,[Url] -- URL of the data source
,[DataFormat] = UPPER([DataFormat]) -- Format, e.g. CSV, JSON... currently only loads CSV data
,[HasHeaderRow] -- A bit field to indicate if the file has a header row or not
,[JoinColumns] -- A comma separated list of unique fields on which to join when doing a merge or update

-- Some flags to specify which steps to process or not
,[ProcessIntoLanding]
,[ProcessIntoStaging]
,[ProcessIntoWarehouse]

-- Landing step stuff
,[LandingStartedDateTime]
,[LandingCompletedDateTime]
,[LandingErrorMessage]

,[LandingDuration] = CASE
	WHEN [LandingStartedDateTime] IS NOT NULL
	AND [LandingCompletedDateTime] IS NULL
	THEN CONVERT(
		VARCHAR(5), 
		DATEADD(MINUTE, DATEDIFF(MINUTE, [LandingStartedDateTime], GETDATE()), 0), 
		114
	)

	ELSE CONVERT(
		VARCHAR(5), 
		DATEADD(MINUTE, DATEDIFF(MINUTE, [LandingStartedDateTime], [LandingCompletedDateTime]), 0), 
		114
	) 
END

,[LandingIsInProgress] = CAST(CASE
	WHEN [LandingStartedDateTime] IS NOT NULL
	AND [LandingCompletedDateTime] IS NULL
	THEN 1

	ELSE 0
END AS BIT)

-- Staging step stuff
,[StagingStartedDateTime]
,[StagingCompletedDateTime]
,[StagingErrorMessage]

,[StagingDuration] = CASE
	WHEN [StagingStartedDateTime] IS NOT NULL
	AND [StagingCompletedDateTime] IS NULL
	THEN CONVERT(
		VARCHAR(5), 
		DATEADD(MINUTE, DATEDIFF(MINUTE, [StagingStartedDateTime], GETDATE()), 0), 
		114
	)

	ELSE CONVERT(
		VARCHAR(5), 
		DATEADD(MINUTE, DATEDIFF(MINUTE, [StagingStartedDateTime], [StagingCompletedDateTime]), 0), 
		114
	)
END

,[StagingIsInProgress] = CAST(CASE
	WHEN [StagingStartedDateTime] IS NOT NULL
	AND [StagingCompletedDateTime] IS NULL
	THEN 1

	ELSE 0
END AS BIT)

-- Warehouse step stuff
,[WarehouseStartedDateTime]
,[WarehouseCompletedDateTime]
,[WarehouseErrorMessage]

,[WarehouseDuration] = CASE
	WHEN [WarehouseStartedDateTime] IS NOT NULL
	AND [WarehouseCompletedDateTime] IS NULL
	THEN CONVERT(
		VARCHAR(5), 
		DATEADD(MINUTE, DATEDIFF(MINUTE, [WarehouseStartedDateTime], GETDATE()), 0), 
		114
	)

	ELSE CONVERT(
		VARCHAR(5), 
		DATEADD(MINUTE, DATEDIFF(MINUTE, [WarehouseStartedDateTime], [WarehouseCompletedDateTime]), 0), 
		114
	)
END

,[WarehouseIsInProgress] = CAST(CASE
	WHEN [WarehouseStartedDateTime] IS NOT NULL
	AND [WarehouseCompletedDateTime] IS NULL
	THEN 1

	ELSE 0
END AS BIT)

-- Name of the warehouse proc to run after staging completes
,[WarehouseProcedure] = CASE
	-- Custom procedure supplied?
	WHEN [WarehouseProcedureSchema] IS NOT NULL
	AND [WarehouseProcedureName] IS NOT NULL
	THEN [WarehouseProcedureSchema] + '.' + [WarehouseProcedureName]

	-- No, use default naming convention for procedure name to run
	ELSE REPLACE([Dataset], ' ', '') + '.uspProcess' + REPLACE(ISNULL([Subset], ''), ' ', '')
END

,[WarehouseProcedureParameter] -- Optional parameter value to pass to the warehouse proc

FROM
[dbo].[DataSources]