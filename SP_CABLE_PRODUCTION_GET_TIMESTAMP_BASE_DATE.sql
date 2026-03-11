CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_TIMESTAMP_BASE_DATE]
(
      @P_VERSION             VARCHAR(20)
    , @P_MODE                VARCHAR(10)  -- 'TEMP' 또는 'VERSION'
    , @P_PJT_LIST            dbo.UDT_DATA_SALE_OPP_NO READONLY  
    , @P_FIXED_PJT_LIST      dbo.UDT_DATA_SALE_OPP_NO READONLY
    , @O_TIMESTAMP_BASE_DATE DATE OUTPUT
)
AS 
BEGIN
    SET NOCOUNT ON;

    /*
       TB_PRD_PLAN_MASTER PRD_CNFM_STRT_DATE 양쪽에서 날짜를 가져와 UNION ALL 후 MIN() 처리

    */

    BEGIN TRY
		SELECT @O_TIMESTAMP_BASE_DATE = MIN(TIMESTAMP_BASE_DATE)
		  FROM (
		        ---------------------------------------------------------------------
		        -- 1) TB_PRD_PLAN_MASTER.PJT_STRT_DATE (P_MODE가 TEMP, VERSION 모두 SELECT)
		        ---------------------------------------------------------------------
		        SELECT CAST(D.PJT_STRT_DATE AS DATE) AS TIMESTAMP_BASE_DATE
		        FROM SOP_DB.dbo.TB_PRD_PLAN_MASTER D
		        WHERE EXISTS (
			    				SELECT 1
			    				  FROM @P_PJT_LIST P
			    				 WHERE D.SALE_OPP_NO = P.SALE_OPP_NO
			    
			                 )
		
		        UNION ALL
		
		        ---------------------------------------------------------------------
		        -- 1) TB_SIMUL_VERSION_DATA.PRD_CNFM_STRT_DATE (P_MODE가 VERSION일때만 SELECT)
		        ---------------------------------------------------------------------
		        SELECT CAST(D.PRD_CNFM_STRT_DATE AS DATE) AS TIMESTAMP_BASE_DATE
		        FROM SOP_DB.dbo.TB_SIMUL_VERSION_DATA D
		        WHERE @P_MODE = 'VERSION'
		          AND D.SIMUL_VERSION = @P_VERSION
		          AND EXISTS (
			    				SELECT 1
			    				  FROM @P_FIXED_PJT_LIST P
			    				 WHERE D.SALE_OPP_NO = P.SALE_OPP_NO
					         )
		  
		       ) T
		  
    END TRY
    BEGIN CATCH
        -- 에러 처리
        THROW 52001, 'SP_CABLE_PRODUCTION_GET_TIMESTAMP_BASE_DATE Query error', 1;
    END CATCH
    
END;