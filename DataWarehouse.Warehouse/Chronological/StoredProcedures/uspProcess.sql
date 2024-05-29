CREATE PROCEDURE [Chronological].[uspProcess] 
	@customOptions VARCHAR(4000)
AS
BEGIN
	EXEC [Chronological].[uspProcessCalendar]
	EXEC [Chronological].[uspProcessWeekTimeBanding]
END