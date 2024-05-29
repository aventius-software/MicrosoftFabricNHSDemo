CREATE PROCEDURE [DatabaseAdministration].[uspSeedOrganisationDataServiceTestData] AS
BEGIN
	WITH [cteDataSources] AS (
		SELECT
		[DataSourceKey]
		,[IsEnabled]
		,[IngestionMechanism]
		,[Dataset]
		,[Subset]
		,[Notebook]
		,[Url]
		,[DataFormat]
		,[HasHeaderRow]
		,[JoinColumns]
		,[ProcessIntoLanding]
		,[ProcessIntoStaging]
		,[ProcessIntoWarehouse]
		,[Notes]

		FROM (
			VALUES			
			(
				100
				,1
				,'Notebook'
				,'Organisation Data Service'
				,'NHS Provider Trust'
				,'Download file and import landing'
				,'https://files.digital.nhs.uk/assets/ods/current/etr.zip'
				,'CSV'
				,0 -- No header row, so can't use header row as column names
				,'_c0' -- No headers, but the org code is just the first column (usually)
				,1 -- Download to landing
				,1 -- Do staging merge
				,1 -- No warehouse part yet
				,'See https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/other-nhs-organisations'
			)		
		) AS [v] (
			[DataSourceKey]
			,[IsEnabled]
			,[IngestionMechanism]    
			,[Dataset]
			,[Subset]
			,[Notebook]
			,[Url]
			,[DataFormat]
			,[HasHeaderRow]
			,[JoinColumns]
			,[ProcessIntoLanding]
			,[ProcessIntoStaging]
			,[ProcessIntoWarehouse]
			,[Notes]
		)
	)

	-- Insert any rows that aren't in there already
	INSERT INTO
	[dbo].[DataSources] (
		[DataSourceKey]
		,[IsEnabled]
		,[IngestionMechanism]
		,[Dataset]
		,[Subset]
		,[Notebook]
		,[Url]
		,[DataFormat]
		,[HasHeaderRow]
		,[JoinColumns]
		,[ProcessIntoLanding]
		,[ProcessIntoStaging]
		,[ProcessIntoWarehouse]
		,[Notes]
	) 

	SELECT
	[DataSourceKey]
	,[IsEnabled]
	,[IngestionMechanism]
	,[Dataset]
	,[Subset]
	,[Notebook]
	,[Url]
	,[DataFormat]
	,[HasHeaderRow]
	,[JoinColumns]
	,[ProcessIntoLanding]
	,[ProcessIntoStaging]
	,[ProcessIntoWarehouse]
	,[Notes]

	FROM
	[cteDataSources] [src]

	WHERE
	NOT EXISTS(
		SELECT
		1

		FROM
		[dbo].[DataSources] [trg]

		WHERE
		[src].[DataSourceKey] = [trg].[DataSourceKey]
	)
END