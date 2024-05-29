CREATE TABLE [ReferralToTreatmentDimension].[TreatmentFunction] (

	[TreatmentFunctionKey] int NOT NULL, 
	[DateTimeLoaded] datetime2(0) NOT NULL, 
	[DateTimeUpdated] datetime2(0) NULL, 
	[DateTimeDeleted] datetime2(0) NULL, 
	[IsCurrent] bit NOT NULL, 
	[ValidFromDate] date NOT NULL, 
	[ValidToDate] date NOT NULL, 
	[HighWatermark] datetime2(0) NOT NULL, 
	[TreatmentFunctionCode] varchar(50) NOT NULL, 
	[TreatmentFunctionName] varchar(500) NOT NULL
);


GO
ALTER TABLE [ReferralToTreatmentDimension].[TreatmentFunction] ADD CONSTRAINT PK_TreatmentFunction primary key NONCLUSTERED ([TreatmentFunctionKey]);