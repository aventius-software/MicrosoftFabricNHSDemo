CREATE VIEW [Chronological].[vwCalendarValues] AS

WITH [cteDigits] AS (
	SELECT n FROM (
		VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
	) v(n)
), [cteNumberSequence] AS (
	SELECT TOP (DATEDIFF(DAY, '18991231', GETDATE()))
	[RowNumber] = ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
	
	FROM 
	[cteDigits] [units]
	
	CROSS JOIN [cteDigits] [tens]
	CROSS JOIN [cteDigits] [hundreds]
	CROSS JOIN [cteDigits] [thousands]
	CROSS JOIN [cteDigits] [hundredthousands]
	
	ORDER BY 
	[RowNumber]
), [cteDates] AS (
	SELECT
	[DateValue] = CAST(DATEADD(DAY, [RowNumber], '18991231') AS DATE)
	
	FROM 
	[cteNumberSequence]
), [cteFiscalDates] AS (
	SELECT
	[DateValue]

	,[FiscalYear] = CAST(CASE
		WHEN MONTH([DateValue]) < 4 THEN YEAR([DateValue]) - 1
		ELSE YEAR([DateValue])
	END AS VARCHAR(4)) + '-' + RIGHT(CAST(1 + CASE
		WHEN MONTH([DateValue]) < 4 THEN YEAR([DateValue]) - 1
		ELSE YEAR([DateValue])
	END AS VARCHAR(4)), 2)
	
	,[FiscalQuarter] = CASE DATEPART(QUARTER, [DateValue])
		WHEN 1 THEN 4
		WHEN 2 THEN	1
		WHEN 3 THEN	2
		ELSE 3
	END

	,[FiscalMonth] = (DATEPART(MONTH, [DateValue]) + 8) % 12 + 1

	,[FiscalDay] = 1 + DATEDIFF(DAY, CASE
		WHEN MONTH([DateValue]) < 4 THEN CAST(YEAR([DateValue]) - 1 AS VARCHAR(4)) + '0401' 
		ELSE CAST(YEAR([DateValue]) AS VARCHAR(4)) + '0401'
	END, [DateValue])

	FROM
    [cteDates]
)

SELECT
[ChronologicalCalendarKey] = CAST(FORMAT([d].[DateValue], 'yyyyMMdd') AS INT)
    
,[CalendarDate] = [d].[DateValue]
,[CalendarDateTime] = CAST([d].[DateValue] AS DATETIME2(0))
	
,[CalendarYear] = YEAR([d].[DateValue])
,[CalendarQuarter] = DATEPART(QUARTER, [d].[DateValue])
,[CalendarMonth] = MONTH([d].[DateValue])

,[CalendarQuarterNameAndYearSortOrder] = CAST(FORMAT([d].[DateValue], 'yyyy') + CAST(DATEPART(QUARTER, [d].[DateValue]) AS VARCHAR(1)) AS INT)
,[CalendarQuarterNameAndYear] = 'Q' + CAST(DATEPART(QUARTER, [d].[DateValue]) AS VARCHAR(1)) + ' ' + FORMAT([d].[DateValue], 'yyyy')
,[CalendarMonthNameAndYearSortOrder] = CAST(FORMAT([d].[DateValue], 'yyyyMM') AS INT)
,[CalendarMonthNameAndYear] = FORMAT([d].[DateValue], 'MMM yyyy')

,[DayOfTheYear] = DATEPART(DAYOFYEAR, [d].[DateValue])
,[DayOfTheQuarter] = 0
,[DayOfTheMonth] = DAY([d].[DateValue])
,[DayOfTheWeek] = 0
    
,[FiscalYearSortOrder] = CAST(REPLACE([d].[FiscalYear], '-', '') AS INT)
,[FiscalYear] = [d].[FiscalYear]
,[FiscalQuarter] = [d].[FiscalQuarter]
,[FiscalMonth] = [d].[FiscalMonth]
,[FiscalDay] = [d].[FiscalDay]

,[FiscalQuarterName] = 'Q' + CAST([d].[FiscalQuarter] AS VARCHAR(1))
,[FiscalQuarterNameAndYearSortOrder] = CAST(REPLACE([d].[FiscalYear], '-', '') + CAST([d].[FiscalQuarter] AS VARCHAR(1)) AS INT)
,[FiscalQuarterNameAndYear] = 'Q' + CAST([d].[FiscalQuarter] AS VARCHAR(1)) + ' ' + [d].[FiscalYear]
,[FiscalMonthNameAndYearSortOrder] = CAST(REPLACE([d].[FiscalYear], '-', '') + FORMAT([d].[FiscalMonth], '00') AS INT)
,[FiscalMonthNameAndYear] = FORMAT([d].[DateValue], 'MMM') + ' ' + [d].[FiscalYear]

,[FiscalYearBeginningDate] = '99991231'
,[FiscalYearEndingDate] = '99991231'

,[YearBeginningDate] = '99991231'
,[QuarterBeginningDate] = '99991231'
,[MonthBeginningDate] = '99991231'
,[WeekBeginningDate] = '99991231'

,[YearEndingDate] = '99991231'
,[QuarterEndingDate] = '99991231'
,[MonthEndingDate] = '99991231'
,[WeekEndingDate] = '99991231'

,[MonthNameSortOrder] = MONTH([d].[DateValue])
,[MonthNameShort] = LEFT(DATENAME(MONTH, [d].[DateValue]), 3)
,[MonthNameLong] = DATENAME(MONTH, [d].[DateValue])

,[DayNameSortOrder] = DATEPART(WEEKDAY, [d].[DateValue]) -- todo
,[DayNameShort] = LEFT(DATENAME(WEEKDAY, [d].[DateValue]), 3)
,[DayNameLong] = DATENAME(WEEKDAY, [d].[DateValue])

,[NumberOfDaysInMonth] = 0

,[IsWeekend] = 0
,[IsWorkingDay] = 0
    	
,[IsUKBankHoliday] = 0
,[IsNewYearsDay] = 0
,[IsGoodFriday] = 0
,[IsEasterSunday] = 0
,[IsEasterMonday] = 0
,[IsEarlyMayBankHoliday] = 0
,[IsSpringBankHoliday] = 0
,[IsSummerBankHoliday] = 0
,[IsChristmasDay] = 0
,[IsBoxingDay] = 0
	
,[IsLeapYear] = 0

,[IsPastDate] = 0
,[IsYesterday] = 0
,[IsToday] = 0
,[IsTomorrow] = 0
,[IsFutureDate] = 0
    
,[IsCurrentCalendarYear] = 0
,[IsCurrentCalendarQuarter] = 0
,[IsCurrentMonth] = 0
,[IsCurrentWeek] = 0
    
,[IsCurrentFiscalYear] = 0
,[IsCurrentFiscalQuarter] = 0
    
,[IsLastCalendarYear] = 0
,[IsLastCalendarQuarter] = 0
,[IsLastMonth] = 0
,[IsLastWeek] = 0
    
,[IsLastFiscalYear] = 0
,[IsLastFiscalQuarter] = 0
    	
,[IsFirstDayOfTheWeek] = 0
,[IsFirstDayOfTheMonth] = 0
,[IsFirstDayOfTheQuarter] = 0
,[IsFirstDayOfTheCalendarYear] = 0
,[IsFirstDayOfTheFiscalYear] = 0
,[IsFirstDayOfTheDecade] = 0
,[IsFirstDayOfTheCentury] = 0
    
,[IsLastDayOfTheWeek] = 0
,[IsLastDayOfTheMonth] = 0
,[IsLastDayOfTheQuarter] = 0
,[IsLastDayOfTheCalendarYear] = 0
,[IsLastDayOfTheFiscalYear] = 0
,[IsLastDayOfTheDecade] = 0
,[IsLastDayOfTheCentury] = 0

FROM 
[cteFiscalDates] [d]