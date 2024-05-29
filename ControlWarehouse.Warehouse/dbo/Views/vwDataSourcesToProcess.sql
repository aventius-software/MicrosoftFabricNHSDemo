CREATE VIEW [dbo].[vwDataSourcesToProcess] AS

SELECT 
[DataSourceKey]

,[IsInProgress]
,[CurrentStep] 
,[TotalDuration]
,[HasFailed]

,[IngestionMechanism]
,[Dataset]
,[Subset]

,[LakehouseDatasetName]
,[LakehouseSubsetName]
,[LakehouseTableName]
,[WarehouseSchemaName]

,[Notebook]
,[Url]
,[DataFormat]
,[HasHeaderRow]
,[JoinColumns]

,[ProcessIntoLanding] = CASE
	-- Don't process into landing if we've already successfully landed the data
	WHEN [LandingCompletedDateTime] IS NOT NULL
	AND [LandingErrorMessage] IS NULL
	THEN 0

	-- Otherwise...
	ELSE [ProcessIntoLanding]
END

,[ProcessIntoStaging] = CASE
	-- Don't process into staging if we've already successfully merged the data into staging
	WHEN [StagingCompletedDateTime] IS NOT NULL
	AND [StagingErrorMessage] IS NULL
	THEN 0

	-- Otherwise...
	ELSE [ProcessIntoStaging]
END

,[ProcessIntoWarehouse] = CASE
	-- Don't process into warehouse if we've already successfully loaded the data into the warehouse
	WHEN [WarehouseCompletedDateTime] IS NOT NULL
	AND [WarehouseErrorMessage] IS NULL
	THEN 0

	-- Otherwise...
	ELSE [ProcessIntoWarehouse]
END

,[LandingStartedDateTime]
,[LandingCompletedDateTime]
,[LandingErrorMessage]
,[LandingDuration]
,[LandingIsInProgress]

,[StagingStartedDateTime]
,[StagingCompletedDateTime]
,[StagingErrorMessage]
,[StagingDuration]
,[StagingIsInProgress]

,[WarehouseStartedDateTime]
,[WarehouseCompletedDateTime]
,[WarehouseErrorMessage]
,[WarehouseDuration]
,[WarehouseIsInProgress]
,[WarehouseProcedure]
,[WarehouseProcedureParameter]

FROM
[dbo].[vwDataSources]

WHERE
[IsEnabled] = 1 -- Only list enabled data sources

-- Also, include data sources that haven't been completely processed, or steps not successfully completed yet
AND (
	-- Include rows where we've either not done this step, or have but it failed the last time
    [LandingCompletedDateTime] IS NULL -- Not done yet
	OR (
		[LandingCompletedDateTime] IS NOT NULL -- Completed
		AND [LandingErrorMessage] IS NOT NULL -- But with errors
	)

	-- Include rows where we've either not done this step, or have but it failed the last time
    OR [StagingCompletedDateTime] IS NULL -- Not done yet
	OR (
		[StagingCompletedDateTime] IS NOT NULL -- Completed
		AND [StagingErrorMessage] IS NOT NULL -- But with errors
	)

	-- Include rows where we've either not done this step, or have but it failed the last time
    OR [WarehouseCompletedDateTime] IS NULL -- Not done yet
	OR (
		[WarehouseCompletedDateTime] IS NOT NULL -- Completed
		AND [WarehouseErrorMessage] IS NOT NULL -- But with errors
	)
)