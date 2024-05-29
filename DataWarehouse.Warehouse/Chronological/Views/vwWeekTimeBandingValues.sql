CREATE VIEW [Chronological].[vwWeekTimeBandingValues] AS

WITH [cteDigits] AS (
	SELECT 
	[Digit] 
	
	FROM (
		VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)
	) [v](
		[Digit]
	)
), [cteNumberSequence] AS (
	SELECT TOP (200)
	[NumberOfWeeks] = ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1
	
	FROM 
	[cteDigits] [units]
	
	CROSS JOIN [cteDigits] [tens]
	CROSS JOIN [cteDigits] [hundreds]

	ORDER BY 
	[NumberOfWeeks]
)

SELECT
[ChronologicalWeekTimeBandingKey] = [NumberOfWeeks] + 1
,[NumberOfWeeks]

-- Greater than bands. Logic is = greater than X weeks, up to and including X+1 weeks
,[GreaterThanOneWeekBandShortDescription] = FORMAT(1 * ([dv].[NumberOfWeeks] / 1), '00') 
	+ ' to ' 
	+ FORMAT(1 * ([dv].[NumberOfWeeks] / 1) + 1, '00')
	
,[GreaterThanOneWeekBandLongDescription] = 'Greater than '
	+ FORMAT(1 * ([dv].[NumberOfWeeks] / 1), '00') 
	+ ' to ' 
	+ FORMAT(1 * ([dv].[NumberOfWeeks] / 1) + 1, '00')
	+ ' week(s)'

,[GreaterThanTwoWeeksBandShortDescription] = FORMAT(2 * ([dv].[NumberOfWeeks] / 2), '00') 
	+ ' to ' 
	+ FORMAT(2 * ([dv].[NumberOfWeeks] / 2) + 2, '00')
	
,[GreaterThanTwoWeeksBandLongDescription] = 'Greater than '
	+ FORMAT(2 * ([dv].[NumberOfWeeks] / 2), '00') 
	+ ' to ' 
	+ FORMAT(2 * ([dv].[NumberOfWeeks] / 2) + 2, '00')
	+ ' week(s)'

,[GreaterThanThreeWeeksBandShortDescription] = FORMAT(3 * ([dv].[NumberOfWeeks] / 3), '00') 
	+ ' to ' 
	+ FORMAT(3 * ([dv].[NumberOfWeeks] / 3) + 3, '00')

,[GreaterThanThreeWeeksBandLongDescription] = 'Greater than '
	+ FORMAT(3 * ([dv].[NumberOfWeeks] / 3), '00') 
	+ ' to ' 
	+ FORMAT(3 * ([dv].[NumberOfWeeks] / 3) + 3, '00')
	+ ' week(s)'

-- 104+ weeks (useful for some NHS RTT reporting)
,[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks] = CASE
	WHEN [dv].[NumberOfWeeks] < 104 THEN FORMAT([dv].[NumberOfWeeks], '00') + ' to ' + FORMAT([dv].[NumberOfWeeks] + 1, '00')
	WHEN [dv].[NumberOfWeeks] = 104 THEN '104+'
	ELSE 'Not recorded'
END

-- Over bands. Logic is just whether its X weeks or over
,[OneWeekOrOverDescription] = FORMAT(1 * ([dv].[NumberOfWeeks] / 1), '0') + ' week(s) or over'
,[TwoWeeksOrOverDescription] = FORMAT(2 * ([dv].[NumberOfWeeks] / 2), '0') + ' week(s) or over'
,[ThreeWeeksOrOverDescription] = FORMAT(3 * ([dv].[NumberOfWeeks] / 3), '0') + ' week(s) or over'

-- Under bands. Logic is just whether its X weeks or under
,[OneWeekOrUnderDescription] = FORMAT(1 * ((1 + [dv].[NumberOfWeeks]) / 1), '0') + ' week(s) or under'
,[TwoWeeksOrUnderDescription] = FORMAT(2 * ((2 + [dv].[NumberOfWeeks]) / 2), '0') + ' week(s) or under'
,[ThreeWeeksOrUnderDescription] = FORMAT(3 * ((3 + [dv].[NumberOfWeeks]) / 3), '0') + ' week(s) or under'

-- 18 weeks
,[IsUnder18Weeks] = CASE
	WHEN [dv].[NumberOfWeeks] < 18 THEN 1
	ELSE 0
END

,[IsUnderOrEqualTo18Weeks] = CASE
	WHEN [dv].[NumberOfWeeks] <= 18 THEN 1
	ELSE 0
END

,[OverOrUnder18WeeksDescription] = CASE
	WHEN [dv].[NumberOfWeeks] < 18 THEN 'Under 18 weeks'
	ELSE 'Over 18 weeks'
END

,[OverOrUnderOrEqualTo18WeeksDescription] = CASE
	WHEN [dv].[NumberOfWeeks] <= 18 THEN 'Under or equal to 18 weeks'
	ELSE 'Over 18 weeks'
END

FROM
[cteNumberSequence] [dv]

UNION ALL 

SELECT
[ChronologicalWeekTimeBandingKey] = -1
,[NumberOfWeeks] = -1

,[GreaterThanOneWeekBandShortDescription] = 'Unknown'
,[GreaterThanOneWeekBandLongDescription] = 'Unknown number of weeks'
,[GreaterThanTwoWeeksBandShortDescription] = 'Unknown'
,[GreaterThanTwoWeeksBandLongDescription] = 'Unknown number of weeks'
,[GreaterThanThreeWeeksBandShortDescription] = 'Unknown'
,[GreaterThanThreeWeeksBandLongDescription] = 'Unknown number of weeks'

,[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks] = 'Unknown number of weeks'

,[OneWeekOrOverDescription] = 'Unknown number of weeks'
,[TwoWeeksOrOverDescription] = 'Unknown number of weeks'
,[ThreeWeeksOrOverDescription] = 'Unknown number of weeks'
,[OneWeekOrUnderDescription] = 'Unknown number of weeks'
,[TwoWeeksOrUnderDescription] = 'Unknown number of weeks'
,[ThreeWeeksOrUnderDescription] = 'Unknown number of weeks'

,[IsUnder18Weeks] = 0
,[IsUnderOrEqualTo18Weeks] = 0
,[OverOrUnder18WeeksDescription] = 'Unknown number of weeks'
,[OverOrUnderOrEqualTo18WeeksDescription] = 'Unknown number of weeks'

UNION ALL

SELECT
[ChronologicalWeekTimeBandingKey] = -2
,[NumberOfWeeks] = -2

,[GreaterThanOneWeekBandCode] = 'Unspecified'
,[GreaterThanOneWeekBandLongDescription] = 'Unspecified number of weeks'
,[GreaterThanTwoWeeksBandShortDescription] = 'Unspecified'
,[GreaterThanTwoWeeksBandLongDescription] = 'Unspecified number of weeks'
,[GreaterThanThreeWeeksBandShortDescription] = 'Unspecified'
,[GreaterThanThreeWeeksBandLongDescription] = 'Unspecified number of weeks'

,[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks] = 'Unspecified number of weeks'

,[OneWeekOrOverDescription] = 'Unspecified number of weeks'
,[TwoWeeksOrOverDescription] = 'Unspecified number of weeks'
,[ThreeWeeksOrOverDescription] = 'Unspecified number of weeks'
,[OneWeekOrUnderDescription] = 'Unspecified number of weeks'
,[TwoWeeksOrUnderDescription] = 'Unspecified number of weeks'
,[ThreeWeeksOrUnderDescription] = 'Unspecified number of weeks'

,[IsUnder18Weeks] = 0
,[IsUnderOrEqualTo18Weeks] = 0
,[OverOrUnder18WeeksDescription] = 'Unspecified number of weeks'
,[OverOrUnderOrEqualTo18WeeksDescription] = 'Unspecified number of weeks'

UNION ALL

-- NHS RTT specific
SELECT
[ChronologicalWeekTimeBandingKey] = -99
,[NumberOfWeeks] = -99

,[GreaterThanOneWeekBandCode] = 'New RTT Period'
,[GreaterThanOneWeekBandLongDescription] = 'New RTT Period'
,[GreaterThanTwoWeeksBandShortDescription] = 'New RTT Period'
,[GreaterThanTwoWeeksBandLongDescription] = 'New RTT Period'
,[GreaterThanThreeWeeksBandShortDescription] = 'New RTT Period'
,[GreaterThanThreeWeeksBandLongDescription] = 'New RTT Period'

,[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks] = 'New RTT Period'

,[OneWeekOrOverDescription] = 'New RTT Period'
,[TwoWeeksOrOverDescription] = 'New RTT Period'
,[ThreeWeeksOrOverDescription] = 'New RTT Period'
,[OneWeekOrUnderDescription] = 'New RTT Period'
,[TwoWeeksOrUnderDescription] = 'New RTT Period'
,[ThreeWeeksOrUnderDescription] = 'New RTT Period'

,[IsUnder18Weeks] = 1
,[IsUnderOrEqualTo18Weeks] = 1
,[OverOrUnder18WeeksDescription] = 'New RTT Period'
,[OverOrUnderOrEqualTo18WeeksDescription] = 'New RTT Period'