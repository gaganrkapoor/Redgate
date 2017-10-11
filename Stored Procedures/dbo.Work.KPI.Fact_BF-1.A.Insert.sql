SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO











-- ===========================================================================
-- Author:		Gagan Kapoor
-- Create date: 07/02/2017
-- Description:	Insert the Work.KPI.Fact_BF-1.A table from a Synchronisation
--
-- Update History
--		10/02/2017	-- Removed the node name from code to refer to departments using KPIID
--		14/02/2017	-- renamed tables and SP to include - rather then _
--      14/02/2017	-- Logic changed to read funding or Exit Date whichever is lowest
--		29/03/2017	-- GK: removed the temporary table to fill directly into the work table
--		07/06/2017	-- JDJ: added Category table and set it toe "MAIN"
--		15/06/2017	--	RW: added new column [MostInNeed] before column [CarCostCenterID]
--		26-06-2017	-- GK: filer the fictitious results nas the category to include only MAIN clients
--		28/7/2017	-- JDJ: added documentation tokens
--		11/10/2017	-- GK: testing the source control of redgate with git
--		11/10/2017	-- GK: testing 2 the source control of redgate with git
--		11/10/2017	-- GK: testing 3 the source control of redgate with git


-- Calling Arguments:
--		JobID			Tells the SP who called it, which will then allow it to look up the run params 

--	1. Associate this SP with JobID = 3100 :
--				Exec dbo.[Work.KPI.Fact_BF-1.A.Insert] '3100'
--
--
-- ===========================================================================


CREATE PROCEDURE [dbo].[Work.KPI.Fact_BF-1.A.Insert] (
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
			@DWHLoadedDate datetime = getDAte()

		If @Debug = 'Y' 	Select @JobID = 3100	;								-- debugging setting


	Select	@LoggingFlag	= isnull(LoggingFlag,'Y'),
			@USN			= 'Executed by Job ''' + ltrim(str(@JobID)) + '''' + '(' + @Mode + ' Mode)'
	From	[DWH_Archive].dbo.[ApplicationControl]
	Where	[JobID] = @JobID
			

		select @ProcName=OBJECT_NAME(@@PROCID),
				@Starting = Getdate();
		
	If @Debug = 'Y'
		Begin
			print @Mode
			Print @LoggingFlag
			PRINT @USN
		END	;		
		
-- START OF THE MAIN ETL QUERY	

BEGIN TRY

TRUNCATE TABLE [dbo].[Work.KPI.Fact_BF-1.A]

INSERT INTO /*WDWH_OT*/[dbo].[Work.KPI.Fact_BF-1.A]
SELECT	t.DateTimeStart AS RecordStartDate, t.DateTimeEnd AS RecordEndDate, 
		t.RecordStartDateSource, t.RecordEndDateSource,
		'N' AS MostInNeed, 
		t.CarCostCenterID, 
		t.DepartmentName AS CarCostCenter, t.CarFBCostCenterID,  t.CarFBCostCenter, KPI.DeptID as FinCostCenterID, 
		ClientID, ClientNumber, CaseNumber, BFExitDate, PersonID, FirstName, LastName, ServicesRequiredID, ServiceID,category,
		@DWHLoadedDate,@DWHLoadedDate
FROM
(
		SELECT	sr.DateTimeStart,
		sr.DateTimeEnd, 
		N'[CareLink.ServicesRequired].DateTimeStart' AS RecordStartDateSource,
		N'[CareLink.ServicesRequired].DateTimeEnd'	 AS RecordEndDateSource,
		CASE WHEN d.DepartmentName IS NULL THEN NULL	
			 WHEN LEFT(LTRIM(RTRIM(d.DepartmentName)),4) NOT LIKE '%[^0-9]%'  THEN LEFT(LTRIM(RTRIM(d.DepartmentName)),4) -- check if the first 4 digits of department are numbers.
		ELSE NULL
		END AS CarCostCenterID,			
		d.DepartmentName,
		CASE WHEN os.OrgSiteName IS NULL THEN NULL	
			 WHEN LEFT(LTRIM(RTRIM(os.OrgSiteName)),4) NOT LIKE '%[^0-9]%'  THEN LEFT(LTRIM(RTRIM(os.OrgSiteName)),4) -- check if the first 4 digits of funding body are numbers.
		ELSE NULL
		END AS CarFBCostCenterID,	
		os.OrgSiteName AS CarFBCostCenter,		
		c.ClientID,	
		c.ClientNumber,
		pcn.CaseNumber,
		c.UserDate11 AS BFExitDate,
		p.PersonID,p.FirstName,p.LastName,
		sr.ServicesRequiredID, sr.ServiceID,
		pc.Category
FROM /*WDWH_IT*/[CareLink.ServicesRequired] sr 
INNER JOIN /*WDWH_IT*/[CareLink.Person] p ON p.PersonID = sr.PersonID and sr.Active =1 AND p.Active = 1
INNER JOIN /*WDWH_IT*/[Carelink.Client] c ON c.personID = p.PersonID AND c.Active = 1
INNER JOIN /*WDWH_IT*/[CareLink.PersonDepartments] pd ON pd.PersonID = p.PersonID and pd.Active = 1
INNER JOIN /*WDWH_IT*/[Carelink.Departments] d ON d.DepartmentID = pd.DepartmentID AND d.Active = 1
LEFT JOIN /*WDWH_IT*/[Carelink.PersonCaseNumbers] pcn ON pcn.PersonDepartmentId = pd.PersonDepartmentID
LEFT JOIN /*WDWH_IT*/[CareLink.OrganisationSites] os ON os.AddressId = sr.FundingBodyID AND os.Active =1 
left join /*WDWH_IT*/[Carelink.PersonCategories] pc on p.CategoryID = pc.CategoryID and  pc.Active = 1 
WHERE p.Fictitious = 0
and pc.Category ='MAIN'
) t
LEFT join  [DWH_Archive].[dbo].[Work.BIPortalKPIListByDeptID] KPI on t.[CarFBCostCenterID] = KPI.DeptID and KPIID = 'BF-1'


SET @InsertedRows	= @@ROwCount
SET @ErrorReturned	= @@Error
SET	@Ending			= getdate()

---------- -- END OF THE MAIN ETL QUERY

		INSERT INTO [DWH_Archive].[dbo].ApplicationRunLog
		SELECT	DB_NAME() AS HostDB, 
				@ProcName AS StoredProc, 
				@Mode AS RunMode,
				NULL AS SelectKey1,
				NULL AS SelectKey2,
				@Starting AS StartDateTime, 
				@Ending AS EndDateTime, 
				@ProvidedRows AS RowsProcessed, 
				@ProvidedRows AS Inserted,
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

END 









GO
