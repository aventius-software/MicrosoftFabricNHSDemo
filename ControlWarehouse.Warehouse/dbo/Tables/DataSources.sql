CREATE TABLE [dbo].[DataSources] (

	[DataSourceKey] int NOT NULL, 
	[IsEnabled] bit NOT NULL, 
	[IngestionMechanism] varchar(500) NOT NULL, 
	[Dataset] varchar(500) NOT NULL, 
	[Subset] varchar(500) NULL, 
	[Notebook] varchar(500) NULL, 
	[Url] varchar(500) NULL, 
	[DataFormat] varchar(500) NULL, 
	[HasHeaderRow] bit NOT NULL, 
	[JoinColumns] varchar(1000) NULL, 
	[ProcessIntoLanding] bit NOT NULL, 
	[ProcessIntoStaging] bit NOT NULL, 
	[ProcessIntoWarehouse] bit NOT NULL, 
	[LandingStartedDateTime] datetime2(0) NULL, 
	[LandingCompletedDateTime] datetime2(0) NULL, 
	[LandingErrorMessage] varchar(4000) NULL, 
	[StagingStartedDateTime] datetime2(0) NULL, 
	[StagingCompletedDateTime] datetime2(0) NULL, 
	[StagingErrorMessage] varchar(4000) NULL, 
	[WarehouseStartedDateTime] datetime2(0) NULL, 
	[WarehouseCompletedDateTime] datetime2(0) NULL, 
	[WarehouseErrorMessage] varchar(4000) NULL, 
	[WarehouseProcedureSchema] varchar(128) NULL, 
	[WarehouseProcedureName] varchar(128) NULL, 
	[WarehouseProcedureParameter] varchar(4000) NULL, 
	[Notes] varchar(4000) NULL
);


GO
ALTER TABLE [dbo].[DataSources] ADD CONSTRAINT PK_DataSources primary key NONCLUSTERED ([DataSourceKey]);