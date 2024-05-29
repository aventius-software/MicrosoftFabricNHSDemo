CREATE TABLE [ReferralToTreatmentDimension].[WaitingStatus] (

	[WaitingStatusKey] int NOT NULL, 
	[DateTimeLoaded] datetime2(0) NOT NULL, 
	[DateTimeUpdated] datetime2(0) NULL, 
	[DateTimeDeleted] datetime2(0) NULL, 
	[IsCurrent] bit NOT NULL, 
	[ValidFromDate] date NOT NULL, 
	[ValidToDate] date NOT NULL, 
	[HighWatermark] datetime2(0) NOT NULL, 
	[WaitingStatusType] varchar(50) NOT NULL, 
	[WaitingStatusDescription] varchar(500) NOT NULL, 
	[WaitingStatusCategory] varchar(50) NULL
);


GO
ALTER TABLE [ReferralToTreatmentDimension].[WaitingStatus] ADD CONSTRAINT PK_WaitingStatus primary key NONCLUSTERED ([WaitingStatusKey]);