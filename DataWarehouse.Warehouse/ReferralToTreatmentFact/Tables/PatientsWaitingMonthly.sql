CREATE TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly] (

	[PatientsWaitingMonthlyKey] int NOT NULL, 
	[DateTimeLoaded] datetime2(0) NOT NULL, 
	[DateTimeUpdated] datetime2(0) NULL, 
	[DateTimeDeleted] datetime2(0) NULL, 
	[HighWatermark] datetime2(0) NOT NULL, 
	[ChronologicalCalendarKey] int NOT NULL, 
	[ProviderOrganisationKey] int NOT NULL, 
	[CommissionerOrganisationKey] int NOT NULL, 
	[WaitingStatusKey] int NOT NULL, 
	[TreatmentFunctionKey] int NOT NULL, 
	[ChronologicalWeekTimeBandingKey] int NOT NULL, 
	[NumberOfPatientsWaiting] int NULL
);


GO
ALTER TABLE [ReferralToTreatmentFact].[PatientsWaitingMonthly] ADD CONSTRAINT PK_PatientsWaitingMonthly primary key NONCLUSTERED ([PatientsWaitingMonthlyKey]);