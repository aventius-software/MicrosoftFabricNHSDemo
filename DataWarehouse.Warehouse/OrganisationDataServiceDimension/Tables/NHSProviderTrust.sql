CREATE TABLE [OrganisationDataServiceDimension].[NHSProviderTrust] (

	[NHSProviderTrustKey] int NOT NULL, 
	[DateTimeLoaded] datetime2(0) NOT NULL, 
	[DateTimeUpdated] datetime2(0) NULL, 
	[DateTimeDeleted] datetime2(0) NULL, 
	[IsCurrent] bit NOT NULL, 
	[ValidFromDate] date NOT NULL, 
	[ValidToDate] date NOT NULL, 
	[HighWatermark] datetime2(0) NOT NULL, 
	[OrganisationCode] varchar(5) NOT NULL, 
	[OrganisationName] varchar(100) NOT NULL, 
	[NationalGroupingCode] varchar(5) NOT NULL, 
	[HighLevelHealthGeographyCode] varchar(5) NOT NULL, 
	[AddressLine1] varchar(35) NULL, 
	[AddressLine2] varchar(35) NULL, 
	[AddressLine3] varchar(35) NULL, 
	[AddressLine4] varchar(35) NULL, 
	[AddressLine5] varchar(35) NULL, 
	[TownOrCity] varchar(35) NULL, 
	[PostcodeArea] varchar(4) NULL, 
	[Postcode] varchar(8) NULL, 
	[OpenDate] date NOT NULL, 
	[CloseDate] date NULL
);


GO
ALTER TABLE [OrganisationDataServiceDimension].[NHSProviderTrust] ADD CONSTRAINT PK_[NHSProviderTrust primary key NONCLUSTERED ([NHSProviderTrustKey]);
GO
ALTER TABLE [OrganisationDataServiceDimension].[NHSProviderTrust] ADD CONSTRAINT UQ_bb8ad674_a7a9_4a64_a229_bac14f0123e7 unique NONCLUSTERED ([OrganisationCode]);