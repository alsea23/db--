CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_CREATE_TEMP_VERSION]
      @P_MODE                VARCHAR(10)   -- 'TEMP' 또는 'VERSION'
    , @P_SIMUL_VERSION       VARCHAR(50)   -- FIX 데이터 조회할 SIMUL VERSION
    , @P_TEMP_VERSION        VARCHAR(50)   -- AUD에서 생성된 TEMP VERSION, TEMP 테이블에 저장될 TEMP VERSION
    
    , @P_JSON_PJT_LIST       NVARCHAR(MAX)  -- 자동배정할 PROJECT (JSON) 
    , @P_JSON_FIXED_PJT_LIST NVARCHAR(MAX)  -- FIXED된 PROJECT (JSON)
    
    
    , @P_USER_CODE           VARCHAR(50)    -- 호출한 사용자코드
AS
BEGIN
    SET NOCOUNT ON;

    
    /* =========================================================================================================
	   DEBUG LOG TEMP TABLE Create
	========================================================================================================= */    
    DECLARE @DEBUG_MODE BIT = 0; -- debug 모드 활성화 1, 비활성화 0
    
	IF @DEBUG_MODE = 1
	BEGIN
	    IF OBJECT_ID('tempdb..#DEBUG_LOG') IS NOT NULL
	        DROP TABLE #DEBUG_LOG;
	
	    CREATE TABLE #DEBUG_LOG
	    (
	          LOG_SEQ     INT IDENTITY(1,1) PRIMARY KEY
	        , LOG_DTM     DATETIME2(3)      NOT NULL DEFAULT SYSDATETIME()
	        , PROC_NAME   SYSNAME           NOT NULL
	        , USER_CODE   NVARCHAR(50)      NULL
	        , STEP        NVARCHAR(100)     NULL
	        , MESSAGE     NVARCHAR(MAX)     NULL
	        , KEY1        NVARCHAR(100)     NULL
	        , KEY2        NVARCHAR(100)     NULL
	        , KEY3        NVARCHAR(100)     NULL
	    );
	
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1)
	    VALUES (OBJECT_NAME(@@PROCID), 
	                        @P_USER_CODE, 
	                        '****************START*****************', 
	                        CONCAT('debug=', @DEBUG_MODE),
	                        CONCAT('P_TEMP_VERSION : ', @P_TEMP_VERSION)
	    );
	END

    /* =========================================================================================================
	   자동배정 프로젝트 JSON 테이블화 
	========================================================================================================= */ 
    DECLARE @P_PJT_LIST dbo.UDT_DATA_SALE_OPP_NO;
    INSERT INTO @P_PJT_LIST (SALE_OPP_NO)
    EXEC dbo.SP_CABLE_PRODUCTION_GET_JSON_TO_SALE_OPP_NO @P_JSON_PJT_LIST;
    
    IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3)
	    VALUES
	    (
	        OBJECT_NAME(@@PROCID), @P_USER_CODE,
	        'P_PJT_LIST',  -- STEP
	        'Complete',  -- MESSAGE
	        CAST(@@ROWCOUNT AS NVARCHAR(10)), -- KEY1   
	        '',  -- KEY2
	        ''  -- KEY3
	    );
	END
	

    /* =========================================================================================================
	   FIXED 프로젝트 JSON 테이블화 
	========================================================================================================= */ 
    DECLARE @P_FIXED_PJT_LIST dbo.UDT_DATA_SALE_OPP_NO;
    
    INSERT INTO @P_FIXED_PJT_LIST (SALE_OPP_NO)
    EXEC dbo.SP_CABLE_PRODUCTION_GET_JSON_TO_SALE_OPP_NO @P_JSON_FIXED_PJT_LIST;
    PRINT '@P_FIXED_PJT_LIST 생성 완료'
    
	
    IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3)
	    VALUES
	    (
	        OBJECT_NAME(@@PROCID), @P_USER_CODE,
	        'P_FIXED_PJT_LIST',  -- STEP
	        'Complete',  -- MESSAGE
	        CAST(@@ROWCOUNT AS NVARCHAR(10)), -- KEY1    
	        '',  -- KEY2
	        ''  -- KEY3
	    );
	END
	    
    /* =========================================================================================================
	   공정별 설비호기별 마스터 데이터, Timestamp Base 0
	========================================================================================================= */ 
    DECLARE @TB_PROCESS_WORK_CNTR_TIMESTAMP dbo.UDT_MST_PROCESS_WORK_CNTR_TIMESTAMP;
    INSERT INTO @TB_PROCESS_WORK_CNTR_TIMESTAMP (SEQ, PROCESS_CODE, WORK_CNTR_CD, TIMESTAMP_POINT)
    EXEC dbo.SP_CABLE_PRODUCTION_GET_PROCESS_WORK_CNTR_TIMESTAMP;
    
    IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3)
	    VALUES
	    (
	        OBJECT_NAME(@@PROCID), @P_USER_CODE,
	        'TB_PROCESS_WORK_CNTR_TIMESTAMP',  -- STEP
	        'Complete',  -- MESSAGE
	        '',  -- KEY1
	        '',  -- KEY2
	        ''  -- KEY3
	    );
	END
    
    /* =========================================================================================================
	   파라미터로 넘어온 전체(자동배정, Fixed 포함) 프로젝트 중 가장 빠른 생산요청일을 TIMESTAMP_BASE_DATE로 set
	========================================================================================================= */ 
    DECLARE @TIMESTAMP_BASE_DATE DATE;
    EXEC SP_CABLE_PRODUCTION_GET_TIMESTAMP_BASE_DATE @P_SIMUL_VERSION, @P_MODE, @P_PJT_LIST, @P_FIXED_PJT_LIST, @TIMESTAMP_BASE_DATE OUTPUT;
    
    IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3)
	    VALUES
	    (
	        OBJECT_NAME(@@PROCID), @P_USER_CODE,
	        'TIMESTAMP_BASE_DATE',  -- STEP
	        'Complete',  -- MESSAGE
	        CONVERT(VARCHAR(19), @TIMESTAMP_BASE_DATE, 120),  -- KEY1
	        '',  -- KEY2
	        ''  -- KEY3
	    );
	END
    
    
    /* =========================================================================================================
	   자동배정할 Project Data - Priority (PJT-LOT-FJ LeadTime) 
	========================================================================================================= */ 
    DECLARE @TB_PRIORITY_PJT_LOT_LEADTIME dbo.UDT_DATA_CABLE_PJT_LOT_FJ;

    INSERT INTO @TB_PRIORITY_PJT_LOT_LEADTIME
    EXEC dbo.SP_CABLE_PRODUCTION_GET_PRIORITY_PJT_LOT_LEADTIME @P_PJT_LIST;
    
    IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3)
	    VALUES
	    (
	        OBJECT_NAME(@@PROCID), @P_USER_CODE,
	        '자동배정 데이터 @TB_PRIORITY_PJT_LOT_LEADTIME',  -- STEP
	        'Complete',  -- MESSAGE
	        '',  -- KEY1
	        '',  -- KEY2
	        ''  -- KEY3
	    );
	END
	
    /* =========================================================================================================
	   Fixed Project Data - (Fix PJT LOT Timestamp)
	========================================================================================================= */   
    DECLARE @TB_FIXED_PJT_LOT_TIMESTAMP dbo.UDT_DATA_CABLE_PJT_LOT_FJ;
    INSERT INTO @TB_FIXED_PJT_LOT_TIMESTAMP
    EXEC dbo.SP_CABLE_PRODUCTION_GET_FIXED_PJT_LOT_TIMESTAMP @P_SIMUL_VERSION, @P_FIXED_PJT_LIST, @TIMESTAMP_BASE_DATE;
    
    IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3)
	    VALUES
	    (
	        OBJECT_NAME(@@PROCID), @P_USER_CODE,
	        'FIX 데이터 @TB_FIXED_PJT_LOT_TIMESTAMP',  -- STEP
	        'Complete',  -- MESSAGE
	        CONCAT('TIMESTAMP_BASE_DATE : ', @TIMESTAMP_BASE_DATE),  -- KEY1
	        CONCAT('P_SIMUL_VERSION : ', @P_SIMUL_VERSION),  -- KEY2
	        ''  -- KEY3
	    );
	END
    
    
    
    /* =========================================================================================================
	   1. 자동배정 Project Data -> Auto Schedule 한 후 Temp Version에 insert
	========================================================================================================= */   
    
    
    
    INSERT INTO SOP_DB.dbo.TB_TEMP_VERSION_DATA
    (
          TEMP_VERSION
        , SEQ
        , SALE_OPP_NO
        , PJT_SHIP
        , SHIP_SEQ
        , SHIP_SEQ_LOT
        , SUL_NO
		, REV_SEQ
        , ASSEMBLY_SEQ
        , ASSEMBLY
        , FJ_ASSEMBLY_SEQ
        , FJ_ASSEMBLY
        , PROCESS_SEQ
        , PROCESS_CODE
        , WORK_CNTR_SEQ
        , WORK_CNTR_CD
        , EQUIP_SPEED
        , LEAD_TIME
        , PRD_CNFM_STRT_DATE
        , PRD_CNFM_END_DATE
        , REG_EMP
        , REG_DATE
    )
    EXEC dbo.SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER
         @TIMESTAMP_BASE_DATE,
         @TB_PROCESS_WORK_CNTR_TIMESTAMP, 
         @TB_PRIORITY_PJT_LOT_LEADTIME, 
         @TB_FIXED_PJT_LOT_TIMESTAMP,
         @P_TEMP_VERSION, 
         @P_USER_CODE;
    
    /* =========================================================================================================
	   Fixed Project Data를 Temp Version에 insert
	========================================================================================================= */    
    DECLARE @TB_FIX_DATA_CNT INT;
    
	SELECT @TB_FIX_DATA_CNT = COUNT(1)
	FROM  SOP_DB.dbo.TB_TEMP_VERSION_DATA
	WHERE TEMP_VERSION = @P_TEMP_VERSION;
	
	
    IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3)
	    VALUES
	    (
	        OBJECT_NAME(@@PROCID), @P_USER_CODE,
	        'AUTO SCHEDULER 후 TEMP Version insert',  -- STEP
	        'Complete',  -- MESSAGE
	        CONCAT('TIMESTAMP_BASE_DATE : ', @TIMESTAMP_BASE_DATE),  -- KEY1
	        CONCAT('TEMP_VERSION : ', @P_TEMP_VERSION),  -- KEY2
	        CAST(@TB_FIX_DATA_CNT AS NVARCHAR(10)) -- KEY3
	    );
	END
    
	
	
    INSERT INTO SOP_DB.dbo.TB_TEMP_VERSION_DATA
    (
          TEMP_VERSION
        , SEQ
        , SALE_OPP_NO
        , PJT_SHIP
        , SHIP_SEQ
        , SHIP_SEQ_LOT
        , SUL_NO
		, REV_SEQ
        , ASSEMBLY_SEQ
        , ASSEMBLY
        , FJ_ASSEMBLY_SEQ
        , FJ_ASSEMBLY
        , PROCESS_SEQ
        , PROCESS_CODE
        , WORK_CNTR_SEQ
        , WORK_CNTR_CD
        , EQUIP_SPEED
        , LEAD_TIME
        , PRD_CNFM_STRT_DATE
        , PRD_CNFM_END_DATE
        , REG_EMP
        , REG_DATE
    )
    SELECT 
          @P_TEMP_VERSION
        , @TB_FIX_DATA_CNT + SEQ AS SEQ
        , SALE_OPP_NO
        , PJT_SHIP
        , SHIP_SEQ
        , SHIP_SEQ_LOT 
        , SUL_NO
		, REV_SEQ
        , ASSEMBLY_SEQ
        , ASSEMBLY
        , FJ_ASSEMBLY_SEQ
        , FJ_ASSEMBLY
        , PROCESS_SEQ
        , PROCESS_CODE
        , WORK_CNTR_SEQ
        , WORK_CNTR_CD
        , EQUIP_SPEED
        , LEAD_TIME
        , PRD_CNFM_STRT_DATE
        , PRD_CNFM_END_DATE
        , @P_USER_CODE
        , GETDATE()
	FROM  @TB_FIXED_PJT_LOT_TIMESTAMP;
    
    IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3)
	    VALUES
	    (
	        OBJECT_NAME(@@PROCID), @P_USER_CODE,
	        'FIXED DATA insert TB_TEMP_VERSION_DATA',  -- STEP
	        'Complete',  -- MESSAGE
	        CONCAT('P_MODE : ', @P_MODE),  -- KEY1
	        CONCAT('TEMP_VERSION : ', @P_TEMP_VERSION),  -- KEY2
	        ''  -- KEY3
	    );
	END
	
	
	
    /* =========================================================================================================
	   Scheduler Chart에 시각화할 데이터 JSON 형태로 select
	========================================================================================================= */    
    EXEC SP_CABLE_PRODUCTION_SELECT_CHART_DATA @P_MODE, @P_TEMP_VERSION;
    
    IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO #DEBUG_LOG (PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3)
	    VALUES
	    (
	        OBJECT_NAME(@@PROCID), @P_USER_CODE,
	        'JSON SELECT_CHART_DATA',  -- STEP
	        'Complete',  -- MESSAGE
	        CONCAT('P_MODE : ', @P_MODE),  -- KEY1
	        CONCAT('TEMP_VERSION : ', @P_TEMP_VERSION),  -- KEY2
	        ''  -- KEY3
	    );
	END
	
    
    /* =========================================================================================================
	   DEBUG LOG TEMP Data를 TB_SIMUL_DEBUG_LOG에 기록
	========================================================================================================= */    
	IF @DEBUG_MODE = 1
	BEGIN
	    INSERT INTO dbo.TB_SIMUL_DEBUG_LOG
	    (
	        LOG_DTM, PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3
	    )
	    SELECT
	        LOG_DTM, PROC_NAME, USER_CODE, STEP, MESSAGE, KEY1, KEY2, KEY3
	    FROM #DEBUG_LOG;
	END

END;