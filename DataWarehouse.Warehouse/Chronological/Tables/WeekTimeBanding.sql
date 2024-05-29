CREATE TABLE [Chronological].[WeekTimeBanding] (

	[ChronologicalWeekTimeBandingKey] int NOT NULL, 
	[NumberOfWeeks] smallint NOT NULL, 
	[GreaterThanOneWeekBandShortDescription] varchar(50) NOT NULL, 
	[GreaterThanOneWeekBandLongDescription] varchar(50) NOT NULL, 
	[GreaterThanTwoWeeksBandShortDescription] varchar(50) NOT NULL, 
	[GreaterThanTwoWeeksBandLongDescription] varchar(50) NOT NULL, 
	[GreaterThanThreeWeeksBandShortDescription] varchar(50) NOT NULL, 
	[GreaterThanThreeWeeksBandLongDescription] varchar(50) NOT NULL, 
	[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks] varchar(50) NOT NULL, 
	[OneWeekOrOverDescription] varchar(50) NOT NULL, 
	[TwoWeeksOrOverDescription] varchar(50) NOT NULL, 
	[ThreeWeeksOrOverDescription] varchar(50) NOT NULL, 
	[OneWeekOrUnderDescription] varchar(50) NOT NULL, 
	[TwoWeeksOrUnderDescription] varchar(50) NOT NULL, 
	[ThreeWeeksOrUnderDescription] varchar(50) NOT NULL, 
	[IsUnder18Weeks] bit NOT NULL, 
	[IsUnderOrEqualTo18Weeks] bit NOT NULL, 
	[OverOrUnder18WeeksDescription] varchar(50) NOT NULL, 
	[OverOrUnderOrEqualTo18WeeksDescription] varchar(50) NOT NULL
);


GO
ALTER TABLE [Chronological].[WeekTimeBanding] ADD CONSTRAINT PK_WeekTimeBanding primary key NONCLUSTERED ([ChronologicalWeekTimeBandingKey]);