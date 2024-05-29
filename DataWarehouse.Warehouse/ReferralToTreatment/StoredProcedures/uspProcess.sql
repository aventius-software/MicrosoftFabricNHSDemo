CREATE PROCEDURE [ReferralToTreatment].[uspProcess]
	@customOptions VARCHAR(4000)
AS
BEGIN
	PRINT @customOptions

	IF @customOptions IS NULL
	BEGIN
		-- First process dimensions
		EXEC [ReferralToTreatmentDimension].[uspProcessCommissionerOrganisation]
		EXEC [ReferralToTreatmentDimension].[uspProcessProviderOrganisation]
		EXEC [ReferralToTreatmentDimension].[uspProcessTreatmentFunction]
		EXEC [ReferralToTreatmentDimension].[uspProcessWaitingStatus]

		-- Then facts
		EXEC [ReferralToTreatmentFact].[uspProcessPatientsWaitingMonthly]
	END
END