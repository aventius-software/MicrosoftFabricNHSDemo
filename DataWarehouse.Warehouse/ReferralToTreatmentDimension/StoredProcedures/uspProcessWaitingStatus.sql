CREATE PROCEDURE [ReferralToTreatmentDimension].[uspProcessWaitingStatus] AS
BEGIN
	-- Figure out the current max ID
	DECLARE @maxID INT = (
		SELECT 
		MAX([WaitingStatusKey]) 
	
		FROM 
		[ReferralToTreatmentDimension].[WaitingStatus]
	)

	IF @maxID IS NULL SET @maxID = 0

	-- Figure out current high watermark
	DECLARE @currentHighWatermark DATETIME2(0) = (
		SELECT
        MAX([HighWatermark])

		FROM
        [ReferralToTreatmentDimension].[WaitingStatus]
	)
	
	IF @currentHighWatermark IS NULL SET @currentHighWatermark = '19700101'

	-- Create staging table
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[WaitingStatus];
	CREATE TABLE [ReferralToTreatmentStaging].[WaitingStatus] 
	AS
	SELECT
	[HighWatermark] = MAX(ISNULL([DateTimeUpdated], [DateTimeLoaded]))
	,[WaitingStatusType] = [RTTPartType]
	,[WaitingStatusDescription] = [RTTPartDescription]

	-- New column 'category' for use in a hierarchy
	,[WaitingStatusCategory] = CASE
		WHEN LEFT([RTTPartType], 6) = 'Part_1' THEN 'Completed Pathways'
		WHEN LEFT([RTTPartType], 6) = 'Part_2' THEN 'Incomplete Pathways'
		WHEN [RTTPartType] = 'Part_3' THEN 'New RTT Periods'
		ELSE 'Unknown category'
	END
		
	FROM
	[Staginglakehouse].[dbo].[referral_to_treatment]
	
	WHERE
	CAST(ISNULL([DateTimeUpdated], [DateTimeLoaded]) AS DATETIME2(0)) > @currentHighWatermark

	GROUP BY
	[RTTPartType]
	,[RTTPartDescription]
	
	-- Insert new records
	INSERT INTO 
	[ReferralToTreatmentDimension].[WaitingStatus] (
		[WaitingStatusKey]
		,[DateTimeLoaded]
		,[DateTimeUpdated]
		,[DateTimeDeleted]
		,[IsCurrent]
		,[ValidFromDate]
		,[ValidToDate]
		,[HighWatermark]
		,[WaitingStatusType]
		,[WaitingStatusDescription]
		,[WaitingStatusCategory]
	) 

	SELECT
	[WaitingStatusKey] = @maxID + ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	,[DateTimeLoaded] = GETDATE()
	,[DateTimeUpdated] = NULL
	,[DateTimeDeleted] = NULL
	,[IsCurrent] = 1
	,[ValidFromDate] = CAST('19000101' AS DATE)
	,[ValidToDate] = CAST('99991231' AS DATE)
	,[HighWatermark] = [src].[HighWatermark]
	,[WaitingStatusType] = [src].[WaitingStatusType]
	,[WaitingStatusDescription] = [src].[WaitingStatusDescription]
	,[WaitingStatusCategory] = [src].[WaitingStatusCategory]

	FROM
	[ReferralToTreatmentStaging].[WaitingStatus] [src]

	WHERE 
	NOT EXISTS(
		SELECT
		1

		FROM
		[ReferralToTreatmentDimension].[WaitingStatus] [trg]

		WHERE
		[src].[WaitingStatusType] = [trg].[WaitingStatusType]
	)

	-- Update existing rows	
	UPDATE
	[trg]

	SET
	[DateTimeUpdated] = GETDATE()
	,[HighWatermark] = [src].[HighWatermark]	
	,[WaitingStatusDescription] = [src].[WaitingStatusDescription]
	,[WaitingStatusCategory] = [src].[WaitingStatusCategory]

	FROM
	[ReferralToTreatmentDimension].[WaitingStatus] [trg]

	INNER JOIN
	[ReferralToTreatmentStaging].[WaitingStatus] [src]
	ON
	[trg].[WaitingStatusType] = [src].[WaitingStatusType]
	AND (
		[trg].[WaitingStatusDescription] <> [src].[WaitingStatusDescription]
		OR [trg].[WaitingStatusCategory] = [src].[WaitingStatusCategory]
	)

	-- Done
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[WaitingStatus];
END