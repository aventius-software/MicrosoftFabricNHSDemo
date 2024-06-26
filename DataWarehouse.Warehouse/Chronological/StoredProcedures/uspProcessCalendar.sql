CREATE PROCEDURE [Chronological].[uspProcessCalendar] AS
BEGIN
	-- Insert any new rows
	INSERT INTO
    [Chronological].[Calendar] (
		[ChronologicalCalendarKey]
		,[CalendarDate]
		,[CalendarDateTime]
		,[CalendarYear]
		,[CalendarQuarter]
		,[CalendarMonth]
		,[CalendarQuarterNameAndYearSortOrder]
		,[CalendarQuarterNameAndYear]
		,[CalendarMonthNameAndYearSortOrder]
		,[CalendarMonthNameAndYear]
		,[DayOfTheYear]
		,[DayOfTheQuarter]
		,[DayOfTheMonth]
		,[DayOfTheWeek]
		,[FiscalYearSortOrder]
		,[FiscalYear]
		,[FiscalQuarter]
		,[FiscalMonth]
		,[FiscalDay]
		,[FiscalQuarterName]
		,[FiscalQuarterNameAndYearSortOrder]
		,[FiscalQuarterNameAndYear]
		,[FiscalMonthNameAndYearSortOrder]
		,[FiscalMonthNameAndYear]
		,[FiscalYearBeginningDate]
		,[FiscalYearEndingDate]
		,[YearBeginningDate]
		,[QuarterBeginningDate]
		,[MonthBeginningDate]
		,[WeekBeginningDate]
		,[YearEndingDate]
		,[QuarterEndingDate]
		,[MonthEndingDate]
		,[WeekEndingDate]
		,[MonthNameSortOrder]
		,[MonthNameShort]
		,[MonthNameLong]
		,[DayNameSortOrder]
		,[DayNameShort]
		,[DayNameLong]
		,[NumberOfDaysInMonth]
		,[IsWeekend]
		,[IsWorkingDay]
		,[IsUKBankHoliday]
		,[IsNewYearsDay]
		,[IsGoodFriday]
		,[IsEasterSunday]
		,[IsEasterMonday]
		,[IsEarlyMayBankHoliday]
		,[IsSpringBankHoliday]
		,[IsSummerBankHoliday]
		,[IsChristmasDay]
		,[IsBoxingDay]
		,[IsLeapYear]
		,[IsPastDate]
		,[IsYesterday]
		,[IsToday]
		,[IsTomorrow]
		,[IsFutureDate]
		,[IsCurrentCalendarYear]
		,[IsCurrentCalendarQuarter]
		,[IsCurrentMonth]
		,[IsCurrentWeek]
		,[IsCurrentFiscalYear]
		,[IsCurrentFiscalQuarter]
		,[IsLastCalendarYear]
		,[IsLastCalendarQuarter]
		,[IsLastMonth]
		,[IsLastWeek]
		,[IsLastFiscalYear]
		,[IsLastFiscalQuarter]
		,[IsFirstDayOfTheWeek]
		,[IsFirstDayOfTheMonth]
		,[IsFirstDayOfTheQuarter]
		,[IsFirstDayOfTheCalendarYear]
		,[IsFirstDayOfTheFiscalYear]
		,[IsFirstDayOfTheDecade]
		,[IsFirstDayOfTheCentury]
		,[IsLastDayOfTheWeek]
		,[IsLastDayOfTheMonth]
		,[IsLastDayOfTheQuarter]
		,[IsLastDayOfTheCalendarYear]
		,[IsLastDayOfTheFiscalYear]
		,[IsLastDayOfTheDecade]
		,[IsLastDayOfTheCentury]
	)

	SELECT
	[ChronologicalCalendarKey]
	,[CalendarDate]
	,[CalendarDateTime]
	,[CalendarYear]
	,[CalendarQuarter]
	,[CalendarMonth]
	,[CalendarQuarterNameAndYearSortOrder]
	,[CalendarQuarterNameAndYear]
	,[CalendarMonthNameAndYearSortOrder]
	,[CalendarMonthNameAndYear]
	,[DayOfTheYear]
	,[DayOfTheQuarter]
	,[DayOfTheMonth]
	,[DayOfTheWeek]
	,[FiscalYearSortOrder]
	,[FiscalYear]
	,[FiscalQuarter]
	,[FiscalMonth]
	,[FiscalDay]
	,[FiscalQuarterName]
	,[FiscalQuarterNameAndYearSortOrder]
	,[FiscalQuarterNameAndYear]
	,[FiscalMonthNameAndYearSortOrder]
	,[FiscalMonthNameAndYear]
	,[FiscalYearBeginningDate]
	,[FiscalYearEndingDate]
	,[YearBeginningDate]
	,[QuarterBeginningDate]
	,[MonthBeginningDate]
	,[WeekBeginningDate]
	,[YearEndingDate]
	,[QuarterEndingDate]
	,[MonthEndingDate]
	,[WeekEndingDate]
	,[MonthNameSortOrder]
	,[MonthNameShort]
	,[MonthNameLong]
	,[DayNameSortOrder]
	,[DayNameShort]
	,[DayNameLong]
	,[NumberOfDaysInMonth]
	,[IsWeekend]
	,[IsWorkingDay]
	,[IsUKBankHoliday]
	,[IsNewYearsDay]
	,[IsGoodFriday]
	,[IsEasterSunday]
	,[IsEasterMonday]
	,[IsEarlyMayBankHoliday]
	,[IsSpringBankHoliday]
	,[IsSummerBankHoliday]
	,[IsChristmasDay]
	,[IsBoxingDay]
	,[IsLeapYear]
	,[IsPastDate]
	,[IsYesterday]
	,[IsToday]
	,[IsTomorrow]
	,[IsFutureDate]
	,[IsCurrentCalendarYear]
	,[IsCurrentCalendarQuarter]
	,[IsCurrentMonth]
	,[IsCurrentWeek]
	,[IsCurrentFiscalYear]
	,[IsCurrentFiscalQuarter]
	,[IsLastCalendarYear]
	,[IsLastCalendarQuarter]
	,[IsLastMonth]
	,[IsLastWeek]
	,[IsLastFiscalYear]
	,[IsLastFiscalQuarter]
	,[IsFirstDayOfTheWeek]
	,[IsFirstDayOfTheMonth]
	,[IsFirstDayOfTheQuarter]
	,[IsFirstDayOfTheCalendarYear]
	,[IsFirstDayOfTheFiscalYear]
	,[IsFirstDayOfTheDecade]
	,[IsFirstDayOfTheCentury]
	,[IsLastDayOfTheWeek]
	,[IsLastDayOfTheMonth]
	,[IsLastDayOfTheQuarter]
	,[IsLastDayOfTheCalendarYear]
	,[IsLastDayOfTheFiscalYear]
	,[IsLastDayOfTheDecade]
	,[IsLastDayOfTheCentury]
	
	FROM 
	[Chronological].[vwCalendarValues] [src]

	WHERE
    NOT EXISTS(
		SELECT
        1

		FROM
        [Chronological].[Calendar] [trg]

		WHERE
        [trg].[ChronologicalCalendarKey] = [src].[ChronologicalCalendarKey]
	)

	-- Update existing rows where there are differences
	UPDATE
	[trg]

	SET
	[CalendarDate]							= [src].[CalendarDate]
	,[CalendarDateTime]						= [src].[CalendarDateTime]
	,[CalendarYear]							= [src].[CalendarYear]
	,[CalendarQuarter]						= [src].[CalendarQuarter]
	,[CalendarMonth]						= [src].[CalendarMonth]
	,[CalendarQuarterNameAndYearSortOrder]	= [src].[CalendarQuarterNameAndYearSortOrder]
	,[CalendarQuarterNameAndYear]			= [src].[CalendarQuarterNameAndYear]
	,[CalendarMonthNameAndYearSortOrder]	= [src].[CalendarMonthNameAndYearSortOrder]
	,[CalendarMonthNameAndYear]				= [src].[CalendarMonthNameAndYear]
	,[DayOfTheYear]							= [src].[DayOfTheYear]
	,[DayOfTheQuarter]						= [src].[DayOfTheQuarter]
	,[DayOfTheMonth]						= [src].[DayOfTheMonth]
	,[DayOfTheWeek]							= [src].[DayOfTheWeek]
	,[FiscalYearSortOrder]					= [src].[FiscalYearSortOrder]
	,[FiscalYear]							= [src].[FiscalYear]
	,[FiscalQuarter]						= [src].[FiscalQuarter]
	,[FiscalMonth]							= [src].[FiscalMonth]
	,[FiscalDay]							= [src].[FiscalDay]
	,[FiscalQuarterName]					= [src].[FiscalQuarterName]
	,[FiscalQuarterNameAndYearSortOrder]	= [src].[FiscalQuarterNameAndYearSortOrder]
	,[FiscalQuarterNameAndYear]				= [src].[FiscalQuarterNameAndYear]
	,[FiscalMonthNameAndYearSortOrder]		= [src].[FiscalMonthNameAndYearSortOrder]
	,[FiscalMonthNameAndYear]				= [src].[FiscalMonthNameAndYear]
	,[FiscalYearBeginningDate]				= [src].[FiscalYearBeginningDate]
	,[FiscalYearEndingDate]					= [src].[FiscalYearEndingDate]
	,[YearBeginningDate]					= [src].[YearBeginningDate]
	,[QuarterBeginningDate]					= [src].[QuarterBeginningDate]
	,[MonthBeginningDate]					= [src].[MonthBeginningDate]
	,[WeekBeginningDate]					= [src].[WeekBeginningDate]
	,[YearEndingDate]						= [src].[YearEndingDate]
	,[QuarterEndingDate]					= [src].[QuarterEndingDate]
	,[MonthEndingDate]						= [src].[MonthEndingDate]
	,[WeekEndingDate]						= [src].[WeekEndingDate]
	,[MonthNameSortOrder]					= [src].[MonthNameSortOrder]
	,[MonthNameShort]						= [src].[MonthNameShort]
	,[MonthNameLong]						= [src].[MonthNameLong]
	,[DayNameSortOrder]						= [src].[DayNameSortOrder]
	,[DayNameShort]							= [src].[DayNameShort]
	,[DayNameLong]							= [src].[DayNameLong]
	,[NumberOfDaysInMonth]					= [src].[NumberOfDaysInMonth]
	,[IsWeekend]							= [src].[IsWeekend]
	,[IsWorkingDay]							= [src].[IsWorkingDay]
	,[IsUKBankHoliday]						= [src].[IsUKBankHoliday]
	,[IsNewYearsDay]						= [src].[IsNewYearsDay]
	,[IsGoodFriday]							= [src].[IsGoodFriday]
	,[IsEasterSunday]						= [src].[IsEasterSunday]
	,[IsEasterMonday]						= [src].[IsEasterMonday]
	,[IsEarlyMayBankHoliday]				= [src].[IsEarlyMayBankHoliday]
	,[IsSpringBankHoliday]					= [src].[IsSpringBankHoliday]
	,[IsSummerBankHoliday]					= [src].[IsSummerBankHoliday]
	,[IsChristmasDay]						= [src].[IsChristmasDay]
	,[IsBoxingDay]							= [src].[IsBoxingDay]
	,[IsLeapYear]							= [src].[IsLeapYear]
	,[IsPastDate]							= [src].[IsPastDate]
	,[IsYesterday]							= [src].[IsYesterday]
	,[IsToday]								= [src].[IsToday]
	,[IsTomorrow]							= [src].[IsTomorrow]
	,[IsFutureDate]							= [src].[IsFutureDate]
	,[IsCurrentCalendarYear]				= [src].[IsCurrentCalendarYear]
	,[IsCurrentCalendarQuarter]				= [src].[IsCurrentCalendarQuarter]
	,[IsCurrentMonth]						= [src].[IsCurrentMonth]
	,[IsCurrentWeek]						= [src].[IsCurrentWeek]
	,[IsCurrentFiscalYear]					= [src].[IsCurrentFiscalYear]
	,[IsCurrentFiscalQuarter]				= [src].[IsCurrentFiscalQuarter]
	,[IsLastCalendarYear]					= [src].[IsLastCalendarYear]
	,[IsLastCalendarQuarter]				= [src].[IsLastCalendarQuarter]
	,[IsLastMonth]							= [src].[IsLastMonth]
	,[IsLastWeek]							= [src].[IsLastWeek]
	,[IsLastFiscalYear]						= [src].[IsLastFiscalYear]
	,[IsLastFiscalQuarter]					= [src].[IsLastFiscalQuarter]
	,[IsFirstDayOfTheWeek]					= [src].[IsFirstDayOfTheWeek]
	,[IsFirstDayOfTheMonth]					= [src].[IsFirstDayOfTheMonth]
	,[IsFirstDayOfTheQuarter]				= [src].[IsFirstDayOfTheQuarter]
	,[IsFirstDayOfTheCalendarYear]			= [src].[IsFirstDayOfTheCalendarYear]
	,[IsFirstDayOfTheFiscalYear]			= [src].[IsFirstDayOfTheFiscalYear]
	,[IsFirstDayOfTheDecade]				= [src].[IsFirstDayOfTheDecade]
	,[IsFirstDayOfTheCentury]				= [src].[IsFirstDayOfTheCentury]
	,[IsLastDayOfTheWeek]					= [src].[IsLastDayOfTheWeek]
	,[IsLastDayOfTheMonth]					= [src].[IsLastDayOfTheMonth]
	,[IsLastDayOfTheQuarter]				= [src].[IsLastDayOfTheQuarter]
	,[IsLastDayOfTheCalendarYear]			= [src].[IsLastDayOfTheCalendarYear]
	,[IsLastDayOfTheFiscalYear]				= [src].[IsLastDayOfTheFiscalYear]
	,[IsLastDayOfTheDecade]					= [src].[IsLastDayOfTheDecade]
	,[IsLastDayOfTheCentury]				= [src].[IsLastDayOfTheCentury]

	FROM
    [Chronological].[Calendar] [trg]

	INNER JOIN
    [Chronological].[vwCalendarValues] [src]
	ON
	[trg].[ChronologicalCalendarKey] = [src].[ChronologicalCalendarKey]
	AND (		
		[trg].[CalendarDate]							<> [src].[CalendarDate]
		OR [trg].[CalendarDateTime]						<> [src].[CalendarDateTime]
		OR [trg].[CalendarYear]							<> [src].[CalendarYear]
		OR [trg].[CalendarQuarter]						<> [src].[CalendarQuarter]
		OR [trg].[CalendarMonth]						<> [src].[CalendarMonth]
		OR [trg].[CalendarQuarterNameAndYearSortOrder]	<> [src].[CalendarQuarterNameAndYearSortOrder]
		OR [trg].[CalendarQuarterNameAndYear]			<> [src].[CalendarQuarterNameAndYear]
		OR [trg].[CalendarMonthNameAndYearSortOrder]	<> [src].[CalendarMonthNameAndYearSortOrder]
		OR [trg].[CalendarMonthNameAndYear]				<> [src].[CalendarMonthNameAndYear]
		OR [trg].[DayOfTheYear]							<> [src].[DayOfTheYear]
		OR [trg].[DayOfTheQuarter]						<> [src].[DayOfTheQuarter]
		OR [trg].[DayOfTheMonth]						<> [src].[DayOfTheMonth]
		OR [trg].[DayOfTheWeek]							<> [src].[DayOfTheWeek]
		OR [trg].[FiscalYearSortOrder]					<> [src].[FiscalYearSortOrder]
		OR [trg].[FiscalYear]							<> [src].[FiscalYear]
		OR [trg].[FiscalQuarter]						<> [src].[FiscalQuarter]
		OR [trg].[FiscalMonth]							<> [src].[FiscalMonth]
		OR [trg].[FiscalDay]							<> [src].[FiscalDay]
		OR [trg].[FiscalQuarterName]					<> [src].[FiscalQuarterName]
		OR [trg].[FiscalQuarterNameAndYearSortOrder]	<> [src].[FiscalQuarterNameAndYearSortOrder]
		OR [trg].[FiscalQuarterNameAndYear]				<> [src].[FiscalQuarterNameAndYear]
		OR [trg].[FiscalMonthNameAndYearSortOrder]		<> [src].[FiscalMonthNameAndYearSortOrder]
		OR [trg].[FiscalMonthNameAndYear]				<> [src].[FiscalMonthNameAndYear]
		OR [trg].[FiscalYearBeginningDate]				<> [src].[FiscalYearBeginningDate]
		OR [trg].[FiscalYearEndingDate]					<> [src].[FiscalYearEndingDate]
		OR [trg].[YearBeginningDate]					<> [src].[YearBeginningDate]
		OR [trg].[QuarterBeginningDate]					<> [src].[QuarterBeginningDate]
		OR [trg].[MonthBeginningDate]					<> [src].[MonthBeginningDate]
		OR [trg].[WeekBeginningDate]					<> [src].[WeekBeginningDate]
		OR [trg].[YearEndingDate]						<> [src].[YearEndingDate]
		OR [trg].[QuarterEndingDate]					<> [src].[QuarterEndingDate]
		OR [trg].[MonthEndingDate]						<> [src].[MonthEndingDate]
		OR [trg].[WeekEndingDate]						<> [src].[WeekEndingDate]
		OR [trg].[MonthNameSortOrder]					<> [src].[MonthNameSortOrder]
		OR [trg].[MonthNameShort]						<> [src].[MonthNameShort]
		OR [trg].[MonthNameLong]						<> [src].[MonthNameLong]
		OR [trg].[DayNameSortOrder]						<> [src].[DayNameSortOrder]
		OR [trg].[DayNameShort]							<> [src].[DayNameShort]
		OR [trg].[DayNameLong]							<> [src].[DayNameLong]
		OR [trg].[NumberOfDaysInMonth]					<> [src].[NumberOfDaysInMonth]
		OR [trg].[IsWeekend]							<> [src].[IsWeekend]
		OR [trg].[IsWorkingDay]							<> [src].[IsWorkingDay]
		OR [trg].[IsUKBankHoliday]						<> [src].[IsUKBankHoliday]
		OR [trg].[IsNewYearsDay]						<> [src].[IsNewYearsDay]
		OR [trg].[IsGoodFriday]							<> [src].[IsGoodFriday]
		OR [trg].[IsEasterSunday]						<> [src].[IsEasterSunday]
		OR [trg].[IsEasterMonday]						<> [src].[IsEasterMonday]
		OR [trg].[IsEarlyMayBankHoliday]				<> [src].[IsEarlyMayBankHoliday]
		OR [trg].[IsSpringBankHoliday]					<> [src].[IsSpringBankHoliday]
		OR [trg].[IsSummerBankHoliday]					<> [src].[IsSummerBankHoliday]
		OR [trg].[IsChristmasDay]						<> [src].[IsChristmasDay]
		OR [trg].[IsBoxingDay]							<> [src].[IsBoxingDay]
		OR [trg].[IsLeapYear]							<> [src].[IsLeapYear]
		OR [trg].[IsPastDate]							<> [src].[IsPastDate]
		OR [trg].[IsYesterday]							<> [src].[IsYesterday]
		OR [trg].[IsToday]								<> [src].[IsToday]
		OR [trg].[IsTomorrow]							<> [src].[IsTomorrow]
		OR [trg].[IsFutureDate]							<> [src].[IsFutureDate]
		OR [trg].[IsCurrentCalendarYear]				<> [src].[IsCurrentCalendarYear]
		OR [trg].[IsCurrentCalendarQuarter]				<> [src].[IsCurrentCalendarQuarter]
		OR [trg].[IsCurrentMonth]						<> [src].[IsCurrentMonth]
		OR [trg].[IsCurrentWeek]						<> [src].[IsCurrentWeek]
		OR [trg].[IsCurrentFiscalYear]					<> [src].[IsCurrentFiscalYear]
		OR [trg].[IsCurrentFiscalQuarter]				<> [src].[IsCurrentFiscalQuarter]
		OR [trg].[IsLastCalendarYear]					<> [src].[IsLastCalendarYear]
		OR [trg].[IsLastCalendarQuarter]				<> [src].[IsLastCalendarQuarter]
		OR [trg].[IsLastMonth]							<> [src].[IsLastMonth]
		OR [trg].[IsLastWeek]							<> [src].[IsLastWeek]
		OR [trg].[IsLastFiscalYear]						<> [src].[IsLastFiscalYear]
		OR [trg].[IsLastFiscalQuarter]					<> [src].[IsLastFiscalQuarter]
		OR [trg].[IsFirstDayOfTheWeek]					<> [src].[IsFirstDayOfTheWeek]
		OR [trg].[IsFirstDayOfTheMonth]					<> [src].[IsFirstDayOfTheMonth]
		OR [trg].[IsFirstDayOfTheQuarter]				<> [src].[IsFirstDayOfTheQuarter]
		OR [trg].[IsFirstDayOfTheCalendarYear]			<> [src].[IsFirstDayOfTheCalendarYear]
		OR [trg].[IsFirstDayOfTheFiscalYear]			<> [src].[IsFirstDayOfTheFiscalYear]
		OR [trg].[IsFirstDayOfTheDecade]				<> [src].[IsFirstDayOfTheDecade]
		OR [trg].[IsFirstDayOfTheCentury]				<> [src].[IsFirstDayOfTheCentury]
		OR [trg].[IsLastDayOfTheWeek]					<> [src].[IsLastDayOfTheWeek]
		OR [trg].[IsLastDayOfTheMonth]					<> [src].[IsLastDayOfTheMonth]
		OR [trg].[IsLastDayOfTheQuarter]				<> [src].[IsLastDayOfTheQuarter]
		OR [trg].[IsLastDayOfTheCalendarYear]			<> [src].[IsLastDayOfTheCalendarYear]
		OR [trg].[IsLastDayOfTheFiscalYear]				<> [src].[IsLastDayOfTheFiscalYear]
		OR [trg].[IsLastDayOfTheDecade]					<> [src].[IsLastDayOfTheDecade]
		OR [trg].[IsLastDayOfTheCentury]				<> [src].[IsLastDayOfTheCentury]
	)
END