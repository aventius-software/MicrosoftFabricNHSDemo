CREATE PROCEDURE [DatabaseAdministration].[uspCreateChronologicalTables] AS
BEGIN
	/************************************************************************
	
	Creates all the chronological tables
	
	References
	https://learn.microsoft.com/en-us/fabric/data-warehouse/tables
	https://learn.microsoft.com/en-us/fabric/data-warehouse/table-constraints

	************************************************************************/

	-- First create schemas
    IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = 'Chronological')
    BEGIN    
        EXEC sp_executesql @stmt = N'CREATE SCHEMA [Chronological]';
    END
	
	-- Now create tables
	IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Chronological' AND TABLE_NAME = 'Calendar')
	BEGIN
		CREATE TABLE [Chronological].[Calendar] (
			[ChronologicalCalendarKey]						INT				NOT NULL
    
			--,[DateDescription]								VARCHAR(50)		NOT NULL	-- CONSTRAINT [DF_Calendar_DateDescription] DEFAULT ('Unknown') NOT NULL
	
			,[CalendarDate]									DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_CalendarDate] DEFAULT ('9999-12-01') NOT NULL
			,[CalendarDateTime]								DATETIME2(0)	NOT NULL	-- CONSTRAINT [DF_Calendar_CalendarDateTime] DEFAULT ('9999-12-01') NOT NULL
	
			,[CalendarYear]									SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_CalendarYear] DEFAULT ((0)) NOT NULL
			,[CalendarQuarter]								SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_CalendarQuarter] DEFAULT ((0)) NOT NULL
			,[CalendarMonth]								SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_CalendarMonth] DEFAULT ((0)) NOT NULL

			,[CalendarQuarterNameAndYearSortOrder]			INT				NOT NULL	-- CONSTRAINT [DF_Calendar_CalendarQuarterNameAndYearSortOrder] DEFAULT ((0)) NOT NULL
			,[CalendarQuarterNameAndYear]					VARCHAR(7)		NOT NULL	-- CONSTRAINT [DF_Calendar_CalendarQuarterNameAndYear] DEFAULT ('Unknown') NOT NULL
			,[CalendarMonthNameAndYearSortOrder]			INT				NOT NULL	-- CONSTRAINT [DF_Calendar_CalendarMonthNameAndYearSortOrder] DEFAULT ((0)) NOT NULL
			,[CalendarMonthNameAndYear]						VARCHAR(8)		NOT NULL	-- CONSTRAINT [DF_Calendar_CalendarMonthNameAndYear] DEFAULT ('Unknown') NOT NULL

			,[DayOfTheYear]									SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_DayOfTheYear] DEFAULT ((0)) NOT NULL
			,[DayOfTheQuarter]								SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_DayOfTheQuarter] DEFAULT ((0)) NOT NULL
			,[DayOfTheMonth]								SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_DayOfTheMonth] DEFAULT ((0)) NOT NULL
			,[DayOfTheWeek]									SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_DayOfTheWeek] DEFAULT ((0)) NOT NULL
    
			,[FiscalYearSortOrder]							INT				NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalYearSortOrder] DEFAULT ((0)) NOT NULL
			,[FiscalYear]									VARCHAR(7)		NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalYear] DEFAULT ('Unknown') NOT NULL
			,[FiscalQuarter]								SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalQuarter] DEFAULT ('Unknown') NOT NULL    
			,[FiscalMonth]									SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalMonth] DEFAULT ((0)) NOT NULL
			,[FiscalDay]									SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalDay] DEFAULT ((0)) NOT NULL
	
			,[FiscalQuarterName]							VARCHAR(2)		NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalQuarterName] DEFAULT ('Unknown') NOT NULL
			,[FiscalQuarterNameAndYearSortOrder]			INT				NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalQuarterNameAndYearSortOrder] DEFAULT ((0)) NOT NULL
			,[FiscalQuarterNameAndYear]						VARCHAR(10)		NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalQuarterNameAndYear] DEFAULT ('Unknown') NOT NULL
			,[FiscalMonthNameAndYearSortOrder]				INT				NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalMonthNameAndYearSortOrder] DEFAULT ((0)) NOT NULL
			,[FiscalMonthNameAndYear]						VARCHAR(11)		NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalMonthNameAndYear] DEFAULT ('Unknown') NOT NULL

			,[FiscalYearBeginningDate]						DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalYearBeginningDate] DEFAULT ('9999-12-01') NOT NULL
			,[FiscalYearEndingDate]							DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_FiscalYearEndingDate] DEFAULT ('9999-12-01') NOT NULL 

			,[YearBeginningDate]							DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_YearBeginningDate] DEFAULT ('9999-12-01') NOT NULL
			,[QuarterBeginningDate]							DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_QuarterBeginningDate] DEFAULT ('9999-12-01') NOT NULL
			,[MonthBeginningDate]							DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_MonthBeginningDate] DEFAULT ('9999-12-01') NOT NULL
			,[WeekBeginningDate]							DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_WeekBeginningDate] DEFAULT ('9999-12-01') NOT NULL

			,[YearEndingDate]								DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_YearEndingDate] DEFAULT ('9999-12-01') NOT NULL
			,[QuarterEndingDate]							DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_QuarterEndingDate] DEFAULT ('9999-12-01') NOT NULL
			,[MonthEndingDate]								DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_MonthEndingDate] DEFAULT ('9999-12-01') NOT NULL
			,[WeekEndingDate]								DATE			NOT NULL	-- CONSTRAINT [DF_Calendar_WeekEndingDate] DEFAULT ('9999-12-01') NOT NULL

			,[MonthNameSortOrder]							SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_MonthNameSortOrder] DEFAULT ((0)) NOT NULL
			,[MonthNameShort]								VARCHAR(3)		NOT NULL	-- CONSTRAINT [DF_Calendar_MonthNameShort] DEFAULT ('Unknown') NOT NULL
			,[MonthNameLong]								VARCHAR(15)		NOT NULL	-- CONSTRAINT [DF_Calendar_MonthNameLong] DEFAULT ('Unknown') NOT NULL
	
			,[DayNameSortOrder]								SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_DayNameSortOrder] DEFAULT ((0)) NOT NULL
			,[DayNameShort]									VARCHAR(3)		NOT NULL	-- CONSTRAINT [DF_Calendar_DayNameShort] DEFAULT ('Unknown') NOT NULL
			,[DayNameLong]									VARCHAR(15)		NOT NULL	-- CONSTRAINT [DF_Calendar_DayNameLong] DEFAULT ('Unknown') NOT NULL

			,[NumberOfDaysInMonth]							SMALLINT		NOT NULL	-- CONSTRAINT [DF_Calendar_NumberOfDaysInMonth] DEFAULT ((0)) NOT NULL

			,[IsWeekend]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsWeekend] DEFAULT ((0)) NOT NULL
			,[IsWorkingDay]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsWorkingDay] DEFAULT ((0)) NOT NULL
    	
			,[IsUKBankHoliday]								BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsUKBankHoliday] DEFAULT ((0)) NOT NULL
			,[IsNewYearsDay]								BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsNewYearsDay] DEFAULT ((0)) NOT NULL
			,[IsGoodFriday]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsGoodFriday] DEFAULT ((0)) NOT NULL
			,[IsEasterSunday]								BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsEasterSunday] DEFAULT ((0)) NOT NULL
			,[IsEasterMonday]								BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsEasterMonday] DEFAULT ((0)) NOT NULL
			,[IsEarlyMayBankHoliday]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsEarlyMayBankHoliday] DEFAULT ((0)) NOT NULL
			,[IsSpringBankHoliday]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsSpringBankHoliday] DEFAULT ((0)) NOT NULL
			,[IsSummerBankHoliday]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsSummerBankHoliday] DEFAULT ((0)) NOT NULL
			,[IsChristmasDay]								BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsChristmasDay] DEFAULT ((0)) NOT NULL
			,[IsBoxingDay]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsBoxingDay] DEFAULT ((0)) NOT NULL
	
			,[IsLeapYear]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLeapYear] DEFAULT ((0)) NOT NULL

			,[IsPastDate]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsPastDate] DEFAULT ((0)) NOT NULL
			,[IsYesterday]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsYesterday] DEFAULT ((0)) NOT NULL
			,[IsToday]										BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsToday] DEFAULT ((0)) NOT NULL    
			,[IsTomorrow]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsTomorrow] DEFAULT ((0)) NOT NULL
			,[IsFutureDate]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsFutureDate] DEFAULT ((0)) NOT NULL
    
			,[IsCurrentCalendarYear]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsCurrentCalendarYear] DEFAULT ((0)) NOT NULL
			,[IsCurrentCalendarQuarter]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsCurrentCalendarQuarter] DEFAULT ((0)) NOT NULL
			,[IsCurrentMonth]								BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsCurrentMonth] DEFAULT ((0)) NOT NULL
			,[IsCurrentWeek]								BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsCurrentWeek] DEFAULT ((0)) NOT NULL
    
			,[IsCurrentFiscalYear]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsCurrentFiscalYear] DEFAULT ((0)) NOT NULL
			,[IsCurrentFiscalQuarter]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsCurrentFiscalQuarter] DEFAULT ((0)) NOT NULL
    
			,[IsLastCalendarYear]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastCalendarYear] DEFAULT ((0)) NOT NULL
			,[IsLastCalendarQuarter]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastCalendarQuarter] DEFAULT ((0)) NOT NULL	
			,[IsLastMonth]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastMonth] DEFAULT ((0)) NOT NULL
			,[IsLastWeek]									BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastWeek] DEFAULT ((0)) NOT NULL    
    
			,[IsLastFiscalYear]								BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastFiscalYear] DEFAULT ((0)) NOT NULL
			,[IsLastFiscalQuarter]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastFiscalQuarter] DEFAULT ((0)) NOT NULL    
    	
			,[IsFirstDayOfTheWeek]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsFirstDayOfTheWeek] DEFAULT ((0)) NOT NULL
			,[IsFirstDayOfTheMonth]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsFirstDayOfTheMonth] DEFAULT ((0)) NOT NULL
			,[IsFirstDayOfTheQuarter]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsFirstDayOfTheQuarter] DEFAULT ((0)) NOT NULL
			,[IsFirstDayOfTheCalendarYear]					BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsFirstDayOfTheCalendarYear] DEFAULT ((0)) NOT NULL
			,[IsFirstDayOfTheFiscalYear]					BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsFirstDayOfTheFiscalYear] DEFAULT ((0)) NOT NULL
			,[IsFirstDayOfTheDecade]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsFirstDayOfTheDecade] DEFAULT ((0)) NOT NULL
			,[IsFirstDayOfTheCentury]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsFirstDayOfTheCentury] DEFAULT ((0)) NOT NULL
    
			,[IsLastDayOfTheWeek]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastDayOfTheWeek] DEFAULT ((0)) NOT NULL
			,[IsLastDayOfTheMonth]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastDayOfTheMonth] DEFAULT ((0)) NOT NULL
			,[IsLastDayOfTheQuarter]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastDayOfTheQuarter] DEFAULT ((0)) NOT NULL
			,[IsLastDayOfTheCalendarYear]					BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastDayOfTheCalendarYear] DEFAULT ((0)) NOT NULL
			,[IsLastDayOfTheFiscalYear]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastDayOfTheFiscalYear] DEFAULT ((0)) NOT NULL
			,[IsLastDayOfTheDecade]							BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastDayOfTheDecade] DEFAULT ((0)) NOT NULL
			,[IsLastDayOfTheCentury]						BIT				NOT NULL	-- CONSTRAINT [DF_Calendar_IsLastDayOfTheCentury] DEFAULT ((0)) NOT NULL           			
		);

		-- Add the primary key
		ALTER TABLE [Chronological].[Calendar] 
		ADD CONSTRAINT [PK_Calendar] PRIMARY KEY NONCLUSTERED ([ChronologicalCalendarKey]) NOT ENFORCED;
	END
	
	IF NOT EXISTS(SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Chronological' AND TABLE_NAME = 'WeekTimeBanding')
	BEGIN
		--DROP TABLE [Chronological].[WeekTimeBanding]
		CREATE TABLE [Chronological].[WeekTimeBanding] (
			[ChronologicalWeekTimeBandingKey]							INT			NOT NULL
			,[NumberOfWeeks]											SMALLINT	NOT NULL

			-- Greater than
			,[GreaterThanOneWeekBandShortDescription]					VARCHAR(50) NOT NULL
			,[GreaterThanOneWeekBandLongDescription]					VARCHAR(50) NOT NULL
			,[GreaterThanTwoWeeksBandShortDescription]					VARCHAR(50) NOT NULL
			,[GreaterThanTwoWeeksBandLongDescription]					VARCHAR(50) NOT NULL
			,[GreaterThanThreeWeeksBandShortDescription]				VARCHAR(50) NOT NULL
			,[GreaterThanThreeWeeksBandLongDescription]					VARCHAR(50) NOT NULL

			,[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks]	VARCHAR(50) NOT NULL

			-- Over
			,[OneWeekOrOverDescription]									VARCHAR(50) NOT NULL
			,[TwoWeeksOrOverDescription]								VARCHAR(50) NOT NULL
			,[ThreeWeeksOrOverDescription]								VARCHAR(50) NOT NULL
	
			-- Under
			,[OneWeekOrUnderDescription]								VARCHAR(50) NOT NULL
			,[TwoWeeksOrUnderDescription]								VARCHAR(50) NOT NULL
			,[ThreeWeeksOrUnderDescription]								VARCHAR(50) NOT NULL

			-- 18 weeks
			,[IsUnder18Weeks]											BIT			NOT NULL
			,[IsUnderOrEqualTo18Weeks]									BIT			NOT NULL
			,[OverOrUnder18WeeksDescription]							VARCHAR(50) NOT NULL
			,[OverOrUnderOrEqualTo18WeeksDescription]					VARCHAR(50) NOT NULL
		);

		-- Add the primary key
		ALTER TABLE [Chronological].[WeekTimeBanding] 		
		ADD CONSTRAINT [PK_WeekTimeBanding] PRIMARY KEY NONCLUSTERED ([ChronologicalWeekTimeBandingKey]) NOT ENFORCED;
	END
END