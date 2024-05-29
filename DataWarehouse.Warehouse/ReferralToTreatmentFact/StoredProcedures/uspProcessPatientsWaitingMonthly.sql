CREATE PROCEDURE [ReferralToTreatmentFact].[uspProcessPatientsWaitingMonthly] AS
BEGIN
	-- Figure out the current max ID
	DECLARE @maxID INT = (
		SELECT 
		MAX([PatientsWaitingMonthlyKey]) 
	
		FROM 
		[ReferralToTreatmentFact].[PatientsWaitingMonthly]
	)

	IF @maxID IS NULL SET @maxID = 0

	-- Figure out current high watermark
	DECLARE @currentHighWatermark DATETIME2(0) = (
		SELECT
        MAX([HighWatermark])

		FROM
        [ReferralToTreatmentFact].[PatientsWaitingMonthly]
	)
	
	IF @currentHighWatermark IS NULL SET @currentHighWatermark = '19700101'

	-- Create staging table
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[PatientsWaitingMonthlyUnpivoted];
	CREATE TABLE [ReferralToTreatmentStaging].[PatientsWaitingMonthlyUnpivoted] AS
	SELECT
	[HighWatermark] = ISNULL([DateTimeUpdated], [DateTimeLoaded])

	--,[staging].[Period]

	-- Translate period into period ending date for the calendar later	
	,[PeriodEndingDate] = DATEADD(DAY, -1, DATEADD(MONTH, 1, 
		CAST('01-'+ RIGHT([staging].[Period], LEN([staging].[Period]) - 4) AS DATE)
	))
		
	,[staging].[ProviderParentOrgCode]
	--,[staging].[ProviderParentName]
	,[staging].[ProviderOrgCode]
	--,[staging].[ProviderOrgName]
	,[staging].[CommissionerParentOrgCode]
	--,[staging].[CommissionerParentName]
	,[staging].[CommissionerOrgCode]
	--,[staging].[CommissionerOrgName]
	,[staging].[RTTPartType]
	--,[staging].[RTTPartDescription]
	,[staging].[TreatmentFunctionCode]
	--,[staging].[TreatmentFunctionName]

	-- Unpivot the week banding into rows
	,[unpvt].[WeekBandingCode]

	-- Main measure
	,[NumberOfPatientsWaiting] = CAST([unpvt].[NumberOfPatientsWaiting] AS INT)	
	
	FROM
	[StagingLakehouse].[dbo].[referral_to_treatment] [staging]

	-- We take the week banding columns and unpivot them into
	-- rows so we can join the 'code' column onto a dimension
	CROSS APPLY (
		VALUES
		 ('00 To 01'				,[staging].[Gt00To01WeeksSUM1])
		,('01 To 02'				,[staging].[Gt01To02WeeksSUM1])
		,('02 To 03'				,[staging].[Gt02To03WeeksSUM1])
		,('03 To 04'				,[staging].[Gt03To04WeeksSUM1])
		,('04 To 05'				,[staging].[Gt04To05WeeksSUM1])
		,('05 To 06'				,[staging].[Gt05To06WeeksSUM1])
		,('06 To 07'				,[staging].[Gt06To07WeeksSUM1])
		,('07 To 08'				,[staging].[Gt07To08WeeksSUM1])
		,('08 To 09'				,[staging].[Gt08To09WeeksSUM1])
		,('09 To 10'				,[staging].[Gt09To10WeeksSUM1])
		,('10 To 11'				,[staging].[Gt10To11WeeksSUM1])
		,('11 To 12'				,[staging].[Gt11To12WeeksSUM1])
		,('12 To 13'				,[staging].[Gt12To13WeeksSUM1])
		,('13 To 14'				,[staging].[Gt13To14WeeksSUM1])
		,('14 To 15'				,[staging].[Gt14To15WeeksSUM1])
		,('15 To 16'				,[staging].[Gt15To16WeeksSUM1])
		,('16 To 17'				,[staging].[Gt16To17WeeksSUM1])
		,('17 To 18'				,[staging].[Gt17To18WeeksSUM1])
		,('18 To 19'				,[staging].[Gt18To19WeeksSUM1])
		,('19 To 20'				,[staging].[Gt19To20WeeksSUM1])
		,('20 To 21'				,[staging].[Gt20To21WeeksSUM1])
		,('21 To 22'				,[staging].[Gt21To22WeeksSUM1])
		,('22 To 23'				,[staging].[Gt22To23WeeksSUM1])
		,('23 To 24'				,[staging].[Gt23To24WeeksSUM1])
		,('24 To 25'				,[staging].[Gt24To25WeeksSUM1])
		,('25 To 26'				,[staging].[Gt25To26WeeksSUM1])
		,('26 To 27'				,[staging].[Gt26To27WeeksSUM1])
		,('27 To 28'				,[staging].[Gt27To28WeeksSUM1])
		,('28 To 29'				,[staging].[Gt28To29WeeksSUM1])
		,('29 To 30'				,[staging].[Gt29To30WeeksSUM1])
		,('30 To 31'				,[staging].[Gt30To31WeeksSUM1])
		,('31 To 32'				,[staging].[Gt31To32WeeksSUM1])
		,('32 To 33'				,[staging].[Gt32To33WeeksSUM1])
		,('33 To 34'				,[staging].[Gt33To34WeeksSUM1])
		,('34 To 35'				,[staging].[Gt34To35WeeksSUM1])
		,('35 To 36'				,[staging].[Gt35To36WeeksSUM1])
		,('36 To 37'				,[staging].[Gt36To37WeeksSUM1])
		,('37 To 38'				,[staging].[Gt37To38WeeksSUM1])
		,('38 To 39'				,[staging].[Gt38To39WeeksSUM1])
		,('39 To 40'				,[staging].[Gt39To40WeeksSUM1])
		,('40 To 41'				,[staging].[Gt40To41WeeksSUM1])
		,('41 To 42'				,[staging].[Gt41To42WeeksSUM1])
		,('42 To 43'				,[staging].[Gt42To43WeeksSUM1])
		,('43 To 44'				,[staging].[Gt43To44WeeksSUM1])
		,('44 To 45'				,[staging].[Gt44To45WeeksSUM1])
		,('45 To 46'				,[staging].[Gt45To46WeeksSUM1])
		,('46 To 47'				,[staging].[Gt46To47WeeksSUM1])
		,('47 To 48'				,[staging].[Gt47To48WeeksSUM1])
		,('48 To 49'				,[staging].[Gt48To49WeeksSUM1])
		,('49 To 50'				,[staging].[Gt49To50WeeksSUM1])
		,('50 To 51'				,[staging].[Gt50To51WeeksSUM1])
		,('51 To 52'				,[staging].[Gt51To52WeeksSUM1])
		,('52 To 53'				,[staging].[Gt52To53WeeksSUM1])
		,('53 To 54'				,[staging].[Gt53To54WeeksSUM1])
		,('54 To 55'				,[staging].[Gt54To55WeeksSUM1])
		,('55 To 56'				,[staging].[Gt55To56WeeksSUM1])
		,('56 To 57'				,[staging].[Gt56To57WeeksSUM1])
		,('57 To 58'				,[staging].[Gt57To58WeeksSUM1])
		,('58 To 59'				,[staging].[Gt58To59WeeksSUM1])
		,('59 To 60'				,[staging].[Gt59To60WeeksSUM1])
		,('60 To 61'				,[staging].[Gt60To61WeeksSUM1])
		,('61 To 62'				,[staging].[Gt61To62WeeksSUM1])
		,('62 To 63'				,[staging].[Gt62To63WeeksSUM1])
		,('63 To 64'				,[staging].[Gt63To64WeeksSUM1])
		,('64 To 65'				,[staging].[Gt64To65WeeksSUM1])
		,('65 To 66'				,[staging].[Gt65To66WeeksSUM1])
		,('66 To 67'				,[staging].[Gt66To67WeeksSUM1])
		,('67 To 68'				,[staging].[Gt67To68WeeksSUM1])
		,('68 To 69'				,[staging].[Gt68To69WeeksSUM1])
		,('69 To 70'				,[staging].[Gt69To70WeeksSUM1])
		,('70 To 71'				,[staging].[Gt70To71WeeksSUM1])
		,('71 To 72'				,[staging].[Gt71To72WeeksSUM1])
		,('72 To 73'				,[staging].[Gt72To73WeeksSUM1])
		,('73 To 74'				,[staging].[Gt73To74WeeksSUM1])
		,('74 To 75'				,[staging].[Gt74To75WeeksSUM1])
		,('75 To 76'				,[staging].[Gt75To76WeeksSUM1])
		,('76 To 77'				,[staging].[Gt76To77WeeksSUM1])
		,('77 To 78'				,[staging].[Gt77To78WeeksSUM1])
		,('78 To 79'				,[staging].[Gt78To79WeeksSUM1])
		,('79 To 80'				,[staging].[Gt79To80WeeksSUM1])
		,('80 To 81'				,[staging].[Gt80To81WeeksSUM1])
		,('81 To 82'				,[staging].[Gt81To82WeeksSUM1])
		,('82 To 83'				,[staging].[Gt82To83WeeksSUM1])
		,('83 To 84'				,[staging].[Gt83To84WeeksSUM1])
		,('84 To 85'				,[staging].[Gt84To85WeeksSUM1])
		,('85 To 86'				,[staging].[Gt85To86WeeksSUM1])
		,('86 To 87'				,[staging].[Gt86To87WeeksSUM1])
		,('87 To 88'				,[staging].[Gt87To88WeeksSUM1])
		,('88 To 89'				,[staging].[Gt88To89WeeksSUM1])
		,('89 To 90'				,[staging].[Gt89To90WeeksSUM1])
		,('90 To 91'				,[staging].[Gt90To91WeeksSUM1])
		,('91 To 92'				,[staging].[Gt91To92WeeksSUM1])
		,('92 To 93'				,[staging].[Gt92To93WeeksSUM1])
		,('93 To 94'				,[staging].[Gt93To94WeeksSUM1])
		,('94 To 95'				,[staging].[Gt94To95WeeksSUM1])
		,('95 To 96'				,[staging].[Gt95To96WeeksSUM1])
		,('96 To 97'				,[staging].[Gt96To97WeeksSUM1])
		,('97 To 98'				,[staging].[Gt97To98WeeksSUM1])
		,('98 To 99'				,[staging].[Gt98To99WeeksSUM1])
		,('99 To 100'				,[staging].[Gt99To100WeeksSUM1])
		,('100 To 101'				,[staging].[Gt100To101WeeksSUM1])
		,('101 To 102'				,[staging].[Gt101To102WeeksSUM1])
		,('102 To 103'				,[staging].[Gt102To103WeeksSUM1])
		,('103 To 104'				,[staging].[Gt103To104WeeksSUM1])
		,('104+'					,[staging].[Gt104WeeksSUM1])
		,('Unknown number of weeks'	,[staging].[Patientswithunknownclockstartdate])
		,('New RTT Period'			,IIF([RTTPartType] = 'Part_3', [staging].[TotalAll], 0))
	) [unpvt] (
		[WeekBandingCode]
		,[NumberOfPatientsWaiting]
	)
	
	WHERE
	CAST(ISNULL([staging].[DateTimeUpdated], [staging].[DateTimeLoaded]) AS DATETIME2(0)) > @currentHighWatermark
	AND [TreatmentFunctionCode] <> 'C_999' -- Ignore total rows!

	-- Need to add unknown member rows!
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[PatientsWaitingMonthly];
	CREATE TABLE [ReferralToTreatmentStaging].[PatientsWaitingMonthly] AS
	SELECT
	[unpvt].[HighWatermark]
	
	--,[unpvt].[PeriodEndingDate]
	,[ChronologicalCalendarKey] = [dim_calendar].[ChronologicalCalendarKey]
	
	--,[unpvt].[WeekBandingCode]
	,[ChronologicalWeekTimeBandingKey] = [dim_week_banding].[ChronologicalWeekTimeBandingKey]

	--,[unpvt].[ProviderOrgCode]
	,[ProviderOrganisationKey] = [dim_provider].[ProviderOrganisationKey]

	--,[unpvt].[CommissionerOrgCode]
	,[CommissionerOrganisationKey] = [dim_commissioner].[CommissionerOrganisationKey]

	--,[unpvt].[RTTPartType]
	,[WaitingStatusKey] = [dim_status].[WaitingStatusKey]

	--,[unpvt].[TreatmentFunctionCode]
	,[TreatmentFunctionKey] = [dim_treatment_function].[TreatmentFunctionKey]
		
	,[unpvt].[NumberOfPatientsWaiting]

	FROM
	[ReferralToTreatmentStaging].[PatientsWaitingMonthlyUnpivoted] [unpvt]

	INNER JOIN
	[Chronological].[Calendar] [dim_calendar]
	ON
	[unpvt].[PeriodEndingDate] = [dim_calendar].[CalendarDate]

	INNER JOIN
	[Chronological].[WeekTimeBanding] [dim_week_banding]
	ON
	LOWER([unpvt].[WeekBandingCode]) = LOWER([dim_week_banding].[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks])

	INNER JOIN
	[ReferralToTreatmentDimension].[ProviderOrganisation] [dim_provider]
	ON
	[unpvt].[ProviderOrgCode] = [dim_provider].[OrganisationCode]

	INNER JOIN
	[ReferralToTreatmentDimension].[CommissionerOrganisation] [dim_commissioner]
	ON
	[unpvt].[CommissionerOrgCode] = [dim_commissioner].[OrganisationCode]

	INNER JOIN
	[ReferralToTreatmentDimension].[WaitingStatus] [dim_status]		
	ON
	[unpvt].[RTTPartType] = [dim_status].[WaitingStatusType]

	INNER JOIN
	[ReferralToTreatmentDimension].[TreatmentFunction] [dim_treatment_function]
	ON
	[unpvt].[TreatmentFunctionCode] = [dim_treatment_function].[TreatmentFunctionCode]

	-- Load new rows
	INSERT INTO
    [ReferralToTreatmentFact].[PatientsWaitingMonthly] (
		[PatientsWaitingMonthlyKey]
		,[DateTimeLoaded]
		,[DateTimeUpdated]
		,[DateTimeDeleted]
		,[HighWatermark]
		,[ChronologicalCalendarKey]
		,[ProviderOrganisationKey]
		,[CommissionerOrganisationKey]
		,[WaitingStatusKey]
		,[TreatmentFunctionKey]
		,[ChronologicalWeekTimeBandingKey]
		,[NumberOfPatientsWaiting]
	)

	SELECT
    [PatientsWaitingMonthlyKey] = @maxID + ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	,[DateTimeLoaded] = GETDATE()
	,[DateTimeUpdated] = NULL
	,[DateTimeDeleted] = NULL
	,[HighWatermark]
	,[ChronologicalCalendarKey]
	,[ProviderOrganisationKey]
	,[CommissionerOrganisationKey]
	,[WaitingStatusKey]
	,[TreatmentFunctionKey]
	,[ChronologicalWeekTimeBandingKey]
	,[NumberOfPatientsWaiting]

	FROM
    [ReferralToTreatmentStaging].[PatientsWaitingMonthly] [src]

	WHERE
    NOT EXISTS(
		SELECT
        1

		FROM
        [ReferralToTreatmentFact].[PatientsWaitingMonthly] [trg]

		WHERE
        [trg].[ChronologicalCalendarKey]				= [src].[ChronologicalCalendarKey]
		AND [trg].[ProviderOrganisationKey]				= [src].[ProviderOrganisationKey]
		AND [trg].[CommissionerOrganisationKey]			= [src].[CommissionerOrganisationKey]
		AND [trg].[WaitingStatusKey]					= [src].[WaitingStatusKey]
		AND [trg].[TreatmentFunctionKey]				= [src].[TreatmentFunctionKey]
		AND [trg].[ChronologicalWeekTimeBandingKey]		= [src].[ChronologicalWeekTimeBandingKey]
	)

	-- Update existing rows	where there are differences
	UPDATE
	[trg]

	SET
	[DateTimeUpdated]					= GETDATE()
	,[HighWatermark]					= [src].[HighWatermark]
	,[ChronologicalCalendarKey]			= [src].[ChronologicalCalendarKey]
	,[ProviderOrganisationKey]			= [src].[ProviderOrganisationKey]
	,[CommissionerOrganisationKey]		= [src].[CommissionerOrganisationKey]
	,[WaitingStatusKey]					= [src].[WaitingStatusKey]
	,[TreatmentFunctionKey]				= [src].[TreatmentFunctionKey]
	,[ChronologicalWeekTimeBandingKey]	= [src].[ChronologicalWeekTimeBandingKey]
	,[NumberOfPatientsWaiting]			= [src].[NumberOfPatientsWaiting]

	FROM
	[ReferralToTreatmentFact].[PatientsWaitingMonthly] [trg]	
	
	INNER JOIN
	[ReferralToTreatmentStaging].[PatientsWaitingMonthly] [src]
	ON
	[trg].[ChronologicalCalendarKey]				= [src].[ChronologicalCalendarKey]
	AND [trg].[ProviderOrganisationKey]				= [src].[ProviderOrganisationKey]
	AND [trg].[CommissionerOrganisationKey]			= [src].[CommissionerOrganisationKey]
	AND [trg].[WaitingStatusKey]					= [src].[WaitingStatusKey]
	AND [trg].[TreatmentFunctionKey]				= [src].[TreatmentFunctionKey]
	AND [trg].[ChronologicalWeekTimeBandingKey]		= [src].[ChronologicalWeekTimeBandingKey]
	AND (
		NOT EXISTS(SELECT [trg].[NumberOfPatientsWaiting] INTERSECT SELECT [src].[NumberOfPatientsWaiting])
	)
	
	-- Delete rows from the period being loaded/updated if no longer there in source!
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[PatientsWaitingMonthlyUnpivoted];
	DROP TABLE IF EXISTS [ReferralToTreatmentStaging].[PatientsWaitingMonthly];
END