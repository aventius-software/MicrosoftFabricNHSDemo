CREATE VIEW [dbo].[vwDataSourcesWithErrors] AS

SELECT 
[DataSourceKey]

,[IsInProgress]
,[TotalDuration]

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

,[ProcessIntoLanding]
,[ProcessIntoStaging]
,[ProcessIntoWarehouse]

,[LandingStartedDateTime]
,[LandingCompletedDateTime]
,[LandingErrorMessage]
,[LandingDuration]

,[StagingStartedDateTime]
,[StagingCompletedDateTime]
,[StagingErrorMessage]
,[StagingDuration]

,[WarehouseStartedDateTime]
,[WarehouseCompletedDateTime]
,[WarehouseErrorMessage]
,[WarehouseDuration]

,[WarehouseProcedure]
,[WarehouseProcedureParameter]

FROM
[dbo].[vwDataSources]

WHERE
[HasFailed] = 1