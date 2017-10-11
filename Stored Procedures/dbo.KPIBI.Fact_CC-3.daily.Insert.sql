SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO







-- ===========================================================================
-- Author:		Jasminka Dukic-Jovicevic
-- Create date: 13/06/2017
-- Description:	Insert the KPI.Fact_CC-3.A table from Work.KPI.Fact_CC-3.A tables
--
-- Update History 
--		22/06/2017	JDJ: added MostInNeed column
--		11/10/2017	GK: testing to check the version control of redgate tool

-- Calling Arguments:
--		JobID			Tells the SP who called it, which will then allow it to look up the run params 

--	1. Associate this SP with JobID = 3100 :
--				Exec dbo.[KPIBI.Fact_CC-3.daily.Insert] '3100'
--
--
-- ===========================================================================


CREATE PROCEDURE [dbo].[KPIBI.Fact_CC-3.daily.Insert] (
	@JobID		INT				=0
	)			-- The Calling Program or Service name
AS
	BEGIN
		Set NoCount On;

-- set up most ov the variables and pre-plug them with default values

	Declare @Mode			Varchar(32)		= 'Sync',			-- Mode is always 'Sync'
			@LoggingFlag	Varchar (1)		= 'Y'				-- Set to 'Y' to write to table ApplicationErrorLog, else 'N'
	
	Declare	@USN			nVarchar (64)	= 'Executed by Unknown Job ''' + ltrim(str(@JobID)) + '''' + '(' + @Mode + ' Mode)' ,
			@ProcName		varchar(64),
			@Starting		datetime,
			@Ending			datetime,
			@MergedRows		int = 0,
			@InsertedRows	int = 0,
			@ProvidedRows	int = 0,
			@ErrorReturned	int = 0,
			@ReturnStatus	int = 0,
			@Inserted		Int = 0,
			@Updated		Int = 0,
			@Deleted		Int = 0,
			@InsertStatus	int = 0,
			@Debug			varchar(1) = 'N',
			@InsertCount	Int = 0,
			@UpdateCount	Int = 0,
			@DeleteCount	Int = 0,
			@DWHLoadedDate datetime = getDate()


		If @Debug = 'Y' 	Select @JobID = 3100	;								-- debugging setting


	Select	@LoggingFlag	= isnull(LoggingFlag,'Y'),
			@USN			= 'Executed by Job ''' + ltrim(str(@JobID)) + '''' + '(' + @Mode + ' Mode)'
	From	[DWH_Archive].dbo.[ApplicationControl]
	Where	[JobID] = @JobID
			

	select	@ProcName=OBJECT_NAME(@@PROCID),
			@Starting = Getdate();
		
	If @Debug = 'Y'
		Begin
			print @Mode
			Print @LoggingFlag
			PRINT @USN
		END	;		

-- START OF THE MAIN ETL QUERY

BEGIN TRY

	TRUNCATE TABLE DWH_BI.dbo.[KPIBI.Fact_CC-3.daily]

	INSERT INTO /*WDWH_OT*/DWH_BI.[dbo].[KPIBI.Fact_CC-3.daily]  
	SELECT w.[EVT_ID] as EventID
		  ,[EVT_DESC] as EventDesc
		  ,[EVT_CATEGORY] as EventCategory
		  ,[BookedRoom] 
		  ,[BookingDate]
		  ,[MostInNeed]
		  ,'1050' as [FinCostCenterID]
		  ,[ClientID]
		  ,@Starting as  [DWHLoadedDate]
		  ,@Starting as [DWHUpdatedDate]
	  FROM [DWH_Archive].[dbo].[Work.KPI.Fact_CC-3.A] w 

---------- -- END OF THE MAIN ETL QUERY


	Set @InsertedRows	= @@ROwCount
	Set @ErrorReturned	= @@Error
	Set	@Ending			= getdate()

	INSERT INTO [DWH_Archive].[dbo].ApplicationRunLog
	SELECT	DB_NAME() AS HostDB, 
		@ProcName AS StoredProc, 
		@Mode AS RunMode,
		NULL AS SelectKey1,
		NULL AS SelectKey2,
		@Starting AS StartDateTime, 
		@Ending AS EndDateTime, 
		@InsertedRows AS RowsProcessed, 
		@InsertedRows AS Inserted,
		0 AS Updated,
		@ErrorReturned AS ErrorCode

END TRY

BEGIN CATCH
	
	DECLARE @ErrorNum INT = ERROR_NUMBER () ,
			@ErrorMsg NVARCHAR (4000) = ERROR_MESSAGE(),
			@ErrorProc NVARCHAR(126) = ERROR_PROCEDURE(),
			@ErrorSeverity INT = ERROR_SEVERITY() ,
			@ErrorState INT = ERROR_STATE(),
			@ErrorLine INT = ERROR_LINE()
	
	DECLARE @DataError NVARCHAR (4000) = 'Error '+ CONVERT (NVARCHAR (10), @ErrorNum)
			+ ' in package (' + @ErrorProc + ')'  
			+ ', Error Details: ' + @ErrorMsg 
			+ '(Severity ' + CONVERT (NVARCHAR (5), @ErrorSeverity) 
			+ '/ State '  + CONVERT (NVARCHAR (5), @ErrorState) 
			+ '/ Line '  + CONVERT (NVARCHAR (5), @ErrorLine) + ')'

	RAISERROR (@DataError, @ErrorSeverity, @ErrorState);
				
	INSERT INTO dbo.ApplicationErrorLog 
	VALUES (
			GETDATE(),
			@USN,
			@ErrorProc,
			@ErrorLine,
			@ErrorNum,
			@ErrorMsg ,
			@ErrorSeverity,
			@ErrorState );

END CATCH;


END; 





GO
