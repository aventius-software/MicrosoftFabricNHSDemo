CREATE PROCEDURE [ReferralToTreatmentDimension].[uspProcessTreatmentFunction] AS
BEGIN
	-- Figure out the current max ID
	DECLARE @maxID INT = (
		SELECT 
		MAX([TreatmentFunctionKey]) 
	
		FROM 
		[ReferralToTreatmentDimension].[TreatmentFunction]
	)

	IF @maxID IS NULL SET @maxID = 0

	-- Figure out current high watermark
	DECLARE @currentHighWatermark DATETIME2(0) = (
		SELECT
        MAX([HighWatermark])

		FROM
        [ReferralToTreatmentDimension].[TreatmentFunction]
	)
	
	IF @currentHighWatermark IS NULL SET @currentHighWatermark = '19700101'

	-- Create staging table
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[TreatmentFunction];
	CREATE TABLE [ReferralToTreatmentStaging].[TreatmentFunction] 
	AS
	SELECT
	[HighWatermark] = MAX(ISNULL([DateTimeUpdated], [DateTimeLoaded]))
	,[TreatmentFunctionCode]
	,[TreatmentFunctionName]
		
	FROM
	[Staginglakehouse].[dbo].[referral_to_treatment]
	
	WHERE
	CAST(ISNULL([DateTimeUpdated], [DateTimeLoaded]) AS DATETIME2(0)) > @currentHighWatermark

	GROUP BY
	[TreatmentFunctionCode]
	,[TreatmentFunctionName]

	-- Insert new records
	INSERT INTO 
	[ReferralToTreatmentDimension].[TreatmentFunction] (
		[TreatmentFunctionKey]
		,[DateTimeLoaded]
		,[DateTimeUpdated]
		,[DateTimeDeleted]
		,[IsCurrent]
		,[ValidFromDate]
		,[ValidToDate]
		,[HighWatermark]
		,[TreatmentFunctionCode]
		,[TreatmentFunctionName]		
	) 

	SELECT
	[TreatmentFunctionKey] = @maxID + ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	,[DateTimeLoaded] = GETDATE()
	,[DateTimeUpdated] = NULL
	,[DateTimeDeleted] = NULL
	,[IsCurrent] = 1
	,[ValidFromDate] = CAST('19000101' AS DATE)
	,[ValidToDate] = CAST('99991231' AS DATE)
	,[HighWatermark] = [src].[HighWatermark]
	,[TreatmentFunctionCode] = [src].[TreatmentFunctionCode]
	,[TreatmentFunctionName] = [src].[TreatmentFunctionName]
	
	FROM
	[ReferralToTreatmentStaging].[TreatmentFunction] [src]

	WHERE 
	NOT EXISTS(
		SELECT
		1

		FROM
		[ReferralToTreatmentDimension].[TreatmentFunction] [trg]

		WHERE
		[src].[TreatmentFunctionCode] = [trg].[TreatmentFunctionCode]
	)

	-- Update existing rows	
	UPDATE
	[trg]

	SET
	[DateTimeUpdated] = GETDATE()
	,[HighWatermark] = [src].[HighWatermark]
	,[TreatmentFunctionName] = [src].[TreatmentFunctionName]	

	FROM
	[ReferralToTreatmentDimension].[TreatmentFunction] [trg]	

	INNER JOIN
	[ReferralToTreatmentStaging].[TreatmentFunction] [src]
	ON
	[trg].[TreatmentFunctionCode] = [src].[TreatmentFunctionCode]
	AND (
		[trg].[TreatmentFunctionName] <> [src].[TreatmentFunctionName]		
	)

	-- Done
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[TreatmentFunction];
END