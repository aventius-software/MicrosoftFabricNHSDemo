CREATE PROCEDURE [Chronological].[uspProcessWeekTimeBanding] AS
BEGIN
	INSERT INTO
	[Chronological].[WeekTimeBanding] (
		[ChronologicalWeekTimeBandingKey]
		,[NumberOfWeeks]						
										
		-- Greater than							
		,[GreaterThanOneWeekBandShortDescription]			
		,[GreaterThanOneWeekBandLongDescription]	
		,[GreaterThanTwoWeeksBandShortDescription]			
		,[GreaterThanTwoWeeksBandLongDescription]	
		,[GreaterThanThreeWeeksBandShortDescription]			
		,[GreaterThanThreeWeeksBandLongDescription]	
		
		,[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks]

		-- Over									
		,[OneWeekOrOverDescription]				
		,[TwoWeeksOrOverDescription]			
		,[ThreeWeeksOrOverDescription]			
										
		-- Under								
		,[OneWeekOrUnderDescription]			
		,[TwoWeeksOrUnderDescription]			
		,[ThreeWeeksOrUnderDescription]			
										
		-- 18 weeks								
		,[IsUnder18Weeks]						
		,[IsUnderOrEqualTo18Weeks]				
		,[OverOrUnder18WeeksDescription]		
		,[OverOrUnderOrEqualTo18WeeksDescription]
	)

	SELECT
	[ChronologicalWeekTimeBandingKey]
	,[NumberOfWeeks]						
										
	-- Greater than							
	,[GreaterThanOneWeekBandShortDescription]			
	,[GreaterThanOneWeekBandLongDescription]	
	,[GreaterThanTwoWeeksBandShortDescription]			
	,[GreaterThanTwoWeeksBandLongDescription]	
	,[GreaterThanThreeWeeksBandShortDescription]			
	,[GreaterThanThreeWeeksBandLongDescription]	
		
	,[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks]
										
	-- Over									
	,[OneWeekOrOverDescription]				
	,[TwoWeeksOrOverDescription]			
	,[ThreeWeeksOrOverDescription]			
										
	-- Under								
	,[OneWeekOrUnderDescription]			
	,[TwoWeeksOrUnderDescription]			
	,[ThreeWeeksOrUnderDescription]			
										
	-- 18 weeks								
	,[IsUnder18Weeks]						
	,[IsUnderOrEqualTo18Weeks]				
	,[OverOrUnder18WeeksDescription]		
	,[OverOrUnderOrEqualTo18WeeksDescription]

	FROM
	[Chronological].[vwWeekTimeBandingValues] [src]

	WHERE
	NOT EXISTS(
		SELECT
		1

		FROM
		[Chronological].[WeekTimeBanding] [trg]

		WHERE
		[src].[ChronologicalWeekTimeBandingKey] = [trg].[ChronologicalWeekTimeBandingKey]
	)

	UPDATE
	[trg]

	SET
	[NumberOfWeeks]												= [src].[NumberOfWeeks]						
										  
	-- Greater than							  
	,[GreaterThanOneWeekBandShortDescription]					= [src].[GreaterThanOneWeekBandShortDescription]			
	,[GreaterThanOneWeekBandLongDescription]					= [src].[GreaterThanOneWeekBandLongDescription]	
	,[GreaterThanTwoWeeksBandShortDescription]					= [src].[GreaterThanTwoWeeksBandShortDescription]			
	,[GreaterThanTwoWeeksBandLongDescription]					= [src].[GreaterThanTwoWeeksBandLongDescription]	
	,[GreaterThanThreeWeeksBandShortDescription]				= [src].[GreaterThanThreeWeeksBandShortDescription]			
	,[GreaterThanThreeWeeksBandLongDescription]					= [src].[GreaterThanThreeWeeksBandLongDescription]	
																
	,[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks]	= [src].[GreaterThanOneWeekBandShortDescriptionLimitedTo104Weeks]
										  
	-- Over									  
	,[OneWeekOrOverDescription]									= [src].[OneWeekOrOverDescription]				
	,[TwoWeeksOrOverDescription]								= [src].[TwoWeeksOrOverDescription]			
	,[ThreeWeeksOrOverDescription]								= [src].[ThreeWeeksOrOverDescription]			
										  
	-- Under													
	,[OneWeekOrUnderDescription]								= [src].[OneWeekOrUnderDescription]			
	,[TwoWeeksOrUnderDescription]								= [src].[TwoWeeksOrUnderDescription]			
	,[ThreeWeeksOrUnderDescription]								= [src].[ThreeWeeksOrUnderDescription]			
										  
	-- 18 weeks													
	,[IsUnder18Weeks]											= [src].[IsUnder18Weeks]						
	,[IsUnderOrEqualTo18Weeks]									= [src].[IsUnderOrEqualTo18Weeks]				
	,[OverOrUnder18WeeksDescription]							= [src].[OverOrUnder18WeeksDescription]		
	,[OverOrUnderOrEqualTo18WeeksDescription]					= [src].[OverOrUnderOrEqualTo18WeeksDescription]

	FROM
	[Chronological].[WeekTimeBanding] [trg]

	INNER JOIN
	[Chronological].[vwWeekTimeBandingValues] [src]
	ON
	[trg].[ChronologicalWeekTimeBandingKey] = [src].[ChronologicalWeekTimeBandingKey]
END