CREATE PROCEDURE [DatabaseAdministration].[uspSeedReferralToTreatmentTestData] AS
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
			-- Feb 2024
			(
				1 -- Key
				,1
				,'Notebook'        
				,'Referral To Treatment'
				,NULL -- No subset
				,'Download file and import landing'
				,'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2024/04/Full-CSV-data-file-Feb24-ZIP-3642K-08666.zip'
				,'CSV'
				,1 -- Use header row as column names
				,'Period,ProviderOrgCode,CommissionerOrgCode,RTTPartType,TreatmentFunctionCode'
				,1
				,1
				,1
				,'From https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/'
			)
			-- Jan 2024
			,(
				2 -- Key
				,0
				,'Notebook'        
				,'Referral To Treatment'
				,NULL -- No subset
				,'Download file and import landing'
				,'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2024/03/Full-CSV-data-file-Jan24-ZIP-3702K-55749-1.zip'
				,'CSV'
				,1 -- Use header row as column names
				,'Period,ProviderOrgCode,CommissionerOrgCode,RTTPartType,TreatmentFunctionCode'
				,1
				,1
				,1
				,'From https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/'
			)
			-- Dec 2023
			,(
				3 -- Key
				,0
				,'Notebook'        
				,'Referral To Treatment'
				,NULL -- No subset
				,'Download file and import landing'
				,'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2024/02/Full-CSV-data-file-Dec23-ZIP-3569K-58522.zip'
				,'CSV'
				,1 -- Use header row as column names
				,'Period,ProviderOrgCode,CommissionerOrgCode,RTTPartType,TreatmentFunctionCode'
				,1
				,1
				,1
				,'From https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/'
			)
			-- Nov 2023
			,(
				4 -- Key
				,1
				,'Notebook'        
				,'Referral To Treatment'
				,NULL -- No subset
				,'Download file and import landing'
				,'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2024/01/Full-CSV-data-file-Nov23-ZIP-3711K-61260.zip'
				,'CSV'
				,1 -- Use header row as column names
				,'Period,ProviderOrgCode,CommissionerOrgCode,RTTPartType,TreatmentFunctionCode'
				,1
				,1
				,1
				,'From https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/'
			)
			-- Oct 2023
			,(
				5 -- Key
				,0
				,'Notebook'        
				,'Referral To Treatment'
				,NULL -- No subset
				,'Download file and import landing'
				,'https://www.england.nhs.uk/statistics/wp-content/uploads/sites/2/2023/12/Full-CSV-data-file-Oct23-ZIP-3928K-39245-1.zip'
				,'CSV'
				,1 -- Use header row as column names
				,'Period,ProviderOrgCode,CommissionerOrgCode,RTTPartType,TreatmentFunctionCode'
				,1
				,1
				,1
				,'From https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/'
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