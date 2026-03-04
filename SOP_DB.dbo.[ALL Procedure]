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

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER]
(
      @TIMESTAMP_BASE_DATE            DATE
    , @TB_PROCESS_WORK_CNTR_TIMESTAMP dbo.UDT_MST_PROCESS_WORK_CNTR_TIMESTAMP READONLY
    , @TB_PRIORITY_PJT_LOT_LEADTIME   dbo.UDT_DATA_CABLE_PJT_LOT_FJ READONLY
    , @TB_FIXED_PJT_LOT_TIMESTAMP     dbo.UDT_DATA_CABLE_PJT_LOT_FJ READONLY
    , @P_TEMP_VERSION                 VARCHAR(50)
    , @P_USER_CODE                    VARCHAR(50)
    , @P_SENS_BEFORE                  INT = 0
    , @P_SENS_AFTER                   INT = 0
    , @P_ALLOW_OVERLAP                INT = 0
    , @P_PREP_PROC_DAYS               INT = 1
    , @P_PREP_FJ_DAYS                 INT = 1
)
AS
BEGIN
    SET NOCOUNT ON;

    /* ✅ (중요) 공유 temp는 WRAPPER에서 CREATE 해야 RUN에서 안 사라짐 */

    -- 0) #WORK_CNTR_TIMESTAMP
    IF OBJECT_ID('tempdb..#WORK_CNTR_TIMESTAMP') IS NOT NULL DROP TABLE #WORK_CNTR_TIMESTAMP;
    CREATE TABLE #WORK_CNTR_TIMESTAMP
    (
        SEQ             INT         NULL,
        PROCESS_CODE    VARCHAR(20) NULL,
        WORK_CNTR_CD    VARCHAR(20) NULL,
        TIMESTAMP_POINT INT         NULL
    );

    -- 1) #FIXED_PJT_LOT_DATA
    IF OBJECT_ID('tempdb..#FIXED_PJT_LOT_DATA') IS NOT NULL DROP TABLE #FIXED_PJT_LOT_DATA;
    CREATE TABLE #FIXED_PJT_LOT_DATA
    (
        SALE_OPP_NO   VARCHAR(20)  NULL,
        PJT_SHIP      INT          NULL,
        SHIP_SEQ      INT          NULL,
        SHIP_SEQ_LOT  INT          NULL,
        PROCESS_SEQ   INT          NULL,
        PROCESS_CODE  VARCHAR(20)  NULL,
        WORK_CNTR_SEQ INT          NULL,
        WORK_CNTR_CD  VARCHAR(20)  NULL,
        FIX_START_TS  INT          NULL,
        FIX_END_TS    INT          NULL
    );

    -- 2) #ASSIGN_BLOCK_DATA
    IF OBJECT_ID('tempdb..#ASSIGN_BLOCK_DATA') IS NOT NULL DROP TABLE #ASSIGN_BLOCK_DATA;
    CREATE TABLE #ASSIGN_BLOCK_DATA
    (
        ASSIGN_SEQ      INT         NOT NULL,
        SALE_OPP_NO     VARCHAR(20) NULL,
        PJT_SHIP        INT         NULL,
        SHIP_SEQ        INT         NULL,
        SHIP_SEQ_LOT    INT         NULL,
        LOT_NO          VARCHAR(50) NULL,
        ATWRT02         VARCHAR(50) NULL,
        ASSEMBLY_SEQ    INT         NULL,
        ASSEMBLY        INT         NULL,
        PROCESS_SEQ     INT         NULL,
        PROCESS_CODE    VARCHAR(20) NULL
    );

    -- 3) #LOT_ORDER 
    IF OBJECT_ID('tempdb..#LOT_ORDER') IS NOT NULL DROP TABLE #LOT_ORDER;
    CREATE TABLE #LOT_ORDER
    (
        LOT_ORDER_ID  INT IDENTITY(1,1) NOT NULL,
        ASSIGN_SEQ    INT               NOT NULL,
        SALE_OPP_NO   VARCHAR(20)       NULL,
        PJT_SHIP      INT               NULL,
        SHIP_SEQ      INT               NULL,
        SHIP_SEQ_LOT  INT               NULL,
        LOT_NO        VARCHAR(50)       NULL,
        ATWRT02       VARCHAR(50)       NULL,
        ASSEMBLY_SEQ  INT               NULL,
        ASSEMBLY      INT               NULL,
        PROCESS_SEQ   INT               NULL,
        PROCESS_CODE  VARCHAR(20)       NULL
    );

    -- 4) #SRC_FJ
    IF OBJECT_ID('tempdb..#SRC_FJ') IS NOT NULL DROP TABLE #SRC_FJ;
    CREATE TABLE #SRC_FJ
    (
        SEQ             INT         NOT NULL,
        SALE_OPP_NO     VARCHAR(20) NULL,
        PJT_SHIP        INT         NULL,
        SHIP_SEQ        INT         NULL,
        SHIP_SEQ_LOT    INT         NULL,
        LOT_NO          VARCHAR(50) NULL,
        SUL_NO          VARCHAR(50) NULL,
        ASSEMBLY_SEQ    INT         NULL,
        ASSEMBLY        INT         NULL,
        FJ_ASSEMBLY_SEQ INT         NULL,
        FJ_ASSEMBLY     INT         NULL,
        PROCESS_SEQ     INT         NULL,
        PROCESS_CODE    VARCHAR(20) NULL,
        WORK_CNTR_SEQ   INT         NULL,
        WORK_CNTR_CD    VARCHAR(20) NULL,
        EQUIP_SPEED     FLOAT       NULL,
        LEAD_TIME_DAYS  INT         NULL
    );

    -- 5) #FJ_READY_TS
    IF OBJECT_ID('tempdb..#FJ_READY_TS') IS NOT NULL DROP TABLE #FJ_READY_TS;
    CREATE TABLE #FJ_READY_TS
    (
        SALE_OPP_NO     VARCHAR(20) NOT NULL,
        PJT_SHIP        INT         NOT NULL,
        SHIP_SEQ        INT         NOT NULL,
        SHIP_SEQ_LOT    INT         NOT NULL,
        LOT_NO          VARCHAR(50) NOT NULL,
        ASSEMBLY_SEQ    INT         NOT NULL,
        FJ_ASSEMBLY_SEQ INT         NOT NULL,
        READY_TS        INT         NOT NULL,
        CONSTRAINT PK_FJ_READY PRIMARY KEY
        (
            SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
            LOT_NO, ASSEMBLY_SEQ, FJ_ASSEMBLY_SEQ
        )
    );

    IF OBJECT_ID('tempdb..#WC_GRP') IS NOT NULL DROP TABLE #WC_GRP;
	CREATE TABLE #WC_GRP
	(
	    SIMUL_PROCESS_CD VARCHAR(20) NOT NULL,
	    GRP_WORK_CNTR_CD VARCHAR(20) NOT NULL,
	    WORK_CNTR_CD     VARCHAR(20) NOT NULL,
	    CONSTRAINT PK_WC_GRP PRIMARY KEY (SIMUL_PROCESS_CD, GRP_WORK_CNTR_CD, WORK_CNTR_CD)
	);

    -- 6) #RESULT_STEP
    IF OBJECT_ID('tempdb..#RESULT_STEP') IS NOT NULL DROP TABLE #RESULT_STEP;
    CREATE TABLE #RESULT_STEP
    (
        INSERT_ORDER          INT IDENTITY(1,1) NOT NULL,
        SALE_OPP_NO           VARCHAR(20)       NULL,
        PJT_SHIP              INT               NULL,
        SHIP_SEQ              INT               NULL,
        SHIP_SEQ_LOT          INT               NULL,
        LOT_NO                VARCHAR(50)       NULL,
        SUL_NO                VARCHAR(50)       NULL,
        ASSEMBLY_SEQ          INT               NULL,
        ASSEMBLY              INT               NULL,
        FJ_ASSEMBLY_SEQ       INT               NULL,
        FJ_ASSEMBLY           INT               NULL,
        PROCESS_SEQ           INT               NULL,
        PROCESS_CODE          VARCHAR(20)       NULL,
        WORK_CNTR_SEQ         INT               NULL,
        WORK_CNTR_CD          VARCHAR(20)       NULL,
        EQUIP_SPEED           FLOAT             NULL,
        LEAD_TIME             INT               NULL,
        START_TS              INT               NULL,
        END_TS                INT               NULL,
        HAS_CONFLICT          INT               NULL,
        OVERLAP_DAYS          INT               NULL,
        CONFLICT_FIX_START_TS INT               NULL,
        CONFLICT_FIX_END_TS   INT               NULL,
        FIX_BLOCK_START_TS    INT               NULL,
        FIX_BLOCK_END_TS      INT               NULL
    );

    /* ✅ 이제 INIT은 "INSERT 적재"만 수행 */
    EXEC dbo.SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER_INIT
          @TIMESTAMP_BASE_DATE
        , @TB_PROCESS_WORK_CNTR_TIMESTAMP
        , @TB_PRIORITY_PJT_LOT_LEADTIME
        , @TB_FIXED_PJT_LOT_TIMESTAMP;

    EXEC dbo.SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER_RUN
          @P_SENS_BEFORE
        , @P_SENS_AFTER
        , @P_ALLOW_OVERLAP
        , @P_PREP_PROC_DAYS
        , @P_PREP_FJ_DAYS;

    EXEC dbo.SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER_SELECT
          @TIMESTAMP_BASE_DATE
        , @P_TEMP_VERSION
        , @P_USER_CODE;
    

END;

CREATE   PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER_INIT]
(
      @TIMESTAMP_BASE_DATE            DATE
    , @TB_PROCESS_WORK_CNTR_TIMESTAMP dbo.UDT_MST_PROCESS_WORK_CNTR_TIMESTAMP READONLY
    , @TB_PRIORITY_PJT_LOT_LEADTIME   dbo.UDT_DATA_CABLE_PJT_LOT_FJ READONLY
    , @TB_FIXED_PJT_LOT_TIMESTAMP     dbo.UDT_DATA_CABLE_PJT_LOT_FJ READONLY
)
AS
BEGIN
    SET NOCOUNT ON;

    /* =====================================================================================
       0) TEMP: WORK_CNTR_TIMESTAMP
    ===================================================================================== */

    INSERT INTO #WORK_CNTR_TIMESTAMP(SEQ, PROCESS_CODE, WORK_CNTR_CD, TIMESTAMP_POINT)
    SELECT SEQ, PROCESS_CODE, WORK_CNTR_CD, TIMESTAMP_POINT 
    FROM @TB_PROCESS_WORK_CNTR_TIMESTAMP;

    CREATE UNIQUE CLUSTERED INDEX CX_WCT ON #WORK_CNTR_TIMESTAMP(PROCESS_CODE, SEQ);
    CREATE INDEX IX_WCT_CD ON #WORK_CNTR_TIMESTAMP(PROCESS_CODE, WORK_CNTR_CD);

    /* =====================================================================================
       1) TEMP: FIXED_PJT_LOT_DATA
    ===================================================================================== */
 
    INSERT INTO #FIXED_PJT_LOT_DATA
    (
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        PROCESS_SEQ, PROCESS_CODE,
        WORK_CNTR_SEQ, WORK_CNTR_CD,
        FIX_START_TS, FIX_END_TS
    )
    SELECT
          F.SALE_OPP_NO
        , F.PJT_SHIP
        , F.SHIP_SEQ
        , F.SHIP_SEQ_LOT
        , F.PROCESS_SEQ
        , F.PROCESS_CODE
        , F.WORK_CNTR_SEQ
        , F.WORK_CNTR_CD
        , MIN(
              COALESCE(
                  F.FIXED_TIMESTAMP_POINT,
                  CASE WHEN F.PRD_CNFM_STRT_DATE IS NULL THEN 0
                       ELSE DATEDIFF(DAY, @TIMESTAMP_BASE_DATE, F.PRD_CNFM_STRT_DATE)
                  END
              )
          ) AS FIX_START_TS
        , MIN(
              COALESCE(
                  F.FIXED_TIMESTAMP_POINT,
                  CASE WHEN F.PRD_CNFM_STRT_DATE IS NULL THEN 0
                       ELSE DATEDIFF(DAY, @TIMESTAMP_BASE_DATE, F.PRD_CNFM_STRT_DATE)
                  END
              )
          )
          + SUM(CAST(ISNULL(F.LEAD_TIME,0) AS INT)) AS FIX_END_TS
    FROM @TB_FIXED_PJT_LOT_TIMESTAMP F
    GROUP BY
          F.SALE_OPP_NO, F.PJT_SHIP, F.SHIP_SEQ, F.SHIP_SEQ_LOT,
          F.PROCESS_SEQ, F.PROCESS_CODE, F.WORK_CNTR_SEQ, F.WORK_CNTR_CD;

    CREATE INDEX IX_FIX ON #FIXED_PJT_LOT_DATA(PROCESS_CODE, WORK_CNTR_CD, FIX_START_TS, FIX_END_TS);

    /* =====================================================================================
       2) TEMP: ASSIGN_BLOCK_DATA
    ===================================================================================== */

    INSERT INTO #ASSIGN_BLOCK_DATA
    (
        ASSIGN_SEQ, SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, ATWRT02, ASSEMBLY_SEQ, ASSEMBLY,
        PROCESS_SEQ, PROCESS_CODE
    )
    SELECT
          MIN(P.SEQ) AS ASSIGN_SEQ
        , P.SALE_OPP_NO
        , P.PJT_SHIP
        , P.SHIP_SEQ
        , P.SHIP_SEQ_LOT
        , P.LOT_NO
        , CS.ATWRT02
        , P.ASSEMBLY_SEQ
        , P.ASSEMBLY
        , P.PROCESS_SEQ
        , P.PROCESS_CODE
    FROM @TB_PRIORITY_PJT_LOT_LEADTIME P                         
         INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_LIST PL
                 ON P.SALE_OPP_NO  = PL.SALE_OPP_NO
                AND P.PJT_SHIP     = PL.PJT_SHIP
                AND P.SHIP_SEQ     = PL.SHIP_SEQ
                AND P.SHIP_SEQ_LOT = PL.SHIP_SEQ_LOT
         LEFT OUTER JOIN SOP_DB.dbo.TB_PRD_PLAN_CABLE_SPEC CS
                      ON PL.SUL_NO  = CS.SUL_NO
                     AND PL.REV_SEQ = CS.REV_SEQ
    GROUP BY
          P.SALE_OPP_NO, P.PJT_SHIP, P.SHIP_SEQ, P.SHIP_SEQ_LOT,
          P.LOT_NO, CS.ATWRT02, P.ASSEMBLY_SEQ, P.ASSEMBLY,
          P.PROCESS_SEQ, P.PROCESS_CODE
    ORDER BY ASSIGN_SEQ;

    CREATE CLUSTERED INDEX CX_ASSIGN_BLOCK ON #ASSIGN_BLOCK_DATA(ASSIGN_SEQ);

    /* =====================================================================================
       3) TEMP: LOT_ORDER
    ===================================================================================== */

    INSERT INTO #LOT_ORDER
    (
        ASSIGN_SEQ,
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, ATWRT02, ASSEMBLY_SEQ, ASSEMBLY,
        PROCESS_SEQ, PROCESS_CODE
    )
    SELECT
        A.ASSIGN_SEQ,
        A.SALE_OPP_NO, A.PJT_SHIP, A.SHIP_SEQ, A.SHIP_SEQ_LOT,
        A.LOT_NO, A.ATWRT02, A.ASSEMBLY_SEQ, A.ASSEMBLY,
        A.PROCESS_SEQ, A.PROCESS_CODE
    FROM #ASSIGN_BLOCK_DATA A
    ORDER BY A.ASSIGN_SEQ;

    CREATE UNIQUE CLUSTERED INDEX CX_LOT_ORDER ON #LOT_ORDER(LOT_ORDER_ID);

    /* =====================================================================================
       4) TEMP: SRC_FJ
    ===================================================================================== */


    INSERT INTO #SRC_FJ
    (
        SEQ, SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, SUL_NO, ASSEMBLY_SEQ, ASSEMBLY,
        FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
        PROCESS_SEQ, PROCESS_CODE,
        WORK_CNTR_SEQ, WORK_CNTR_CD,
        EQUIP_SPEED, LEAD_TIME_DAYS
    )
    SELECT
          P.SEQ
        , P.SALE_OPP_NO, P.PJT_SHIP, P.SHIP_SEQ, P.SHIP_SEQ_LOT
        , P.LOT_NO, P.SUL_NO, P.ASSEMBLY_SEQ, P.ASSEMBLY
        , P.FJ_ASSEMBLY_SEQ, P.FJ_ASSEMBLY
        , P.PROCESS_SEQ, P.PROCESS_CODE
        , P.WORK_CNTR_SEQ, P.WORK_CNTR_CD
        , P.EQUIP_SPEED
        , CAST(ISNULL(P.LEAD_TIME,0) AS INT) AS LEAD_TIME_DAYS
    FROM @TB_PRIORITY_PJT_LOT_LEADTIME P
    ORDER BY P.SEQ;

    CREATE INDEX IX_SRCFJ ON #SRC_FJ
    (
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, ASSEMBLY_SEQ, FJ_ASSEMBLY_SEQ,
        PROCESS_CODE, WORK_CNTR_CD
    );


    /* =====================================================================================
       6) TEMP: FJ_READY_TS
    ===================================================================================== */


    INSERT INTO #FJ_READY_TS
    (
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, ASSEMBLY_SEQ, FJ_ASSEMBLY_SEQ, READY_TS
    )
    SELECT DISTINCT
          S.SALE_OPP_NO
        , S.PJT_SHIP
        , S.SHIP_SEQ
        , S.SHIP_SEQ_LOT
        , S.LOT_NO
        , S.ASSEMBLY_SEQ
        , S.FJ_ASSEMBLY_SEQ
        , 0
    FROM #SRC_FJ S
    ORDER BY SALE_OPP_NO, PJT_SHIP, SHIP_SEQ DESC, SHIP_SEQ_LOT, ASSEMBLY_SEQ, FJ_ASSEMBLY_SEQ;
    

    /* =====================================================================================
       7) TEMP: WC_GRP
    ===================================================================================== */
	INSERT INTO #WC_GRP(SIMUL_PROCESS_CD, GRP_WORK_CNTR_CD, WORK_CNTR_CD)
	SELECT SIMUL_PROCESS_CD, GRP_WORK_CNTR_CD, WORK_CNTR_CD
	FROM TB_SIMUL_WORK_CENTER_GRP;
	
	CREATE INDEX IX_WC_GRP_1 ON #WC_GRP(SIMUL_PROCESS_CD, GRP_WORK_CNTR_CD);
	CREATE INDEX IX_WC_GRP_2 ON #WC_GRP(SIMUL_PROCESS_CD, WORK_CNTR_CD);

END;

CREATE   PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER_RUN]
(
      @P_SENS_BEFORE      INT = 0      -- FIX 시작 전 buffer(일)
    , @P_SENS_AFTER       INT = 0      -- FIX 종료 후 buffer(일)
    , @P_ALLOW_OVERLAP    INT = 0      -- FIX와 겹침 허용 일수
    , @P_PREP_PROC_DAYS   INT = 1      -- 공정 종료 후 다음 공정 준비일
    , @P_PREP_FJ_DAYS     INT = 0      -- 설비 작업 종료 후 같은 설비 준비일
)
AS
BEGIN
    SET NOCOUNT ON;

    /* =====================================================================================
       [전체 처리 단위]
       - CUR_LOT: (SALE_OPP_NO + PJT_SHIP + SHIP_SEQ + SHIP_SEQ_LOT + LOT_NO + ASSEMBLY_SEQ) 단위로 루프
       - 각 단위마다:
           1) 공정 리스트(#PROC_LIST)를 만들고
           2) FJ 리스트(#FJ_LIST)를 만들고
           3) 공정 → FJ 순서로 TS 계산 후 결과 INSERT
    ===================================================================================== */

    DECLARE
          @SALE_OPP_NO    VARCHAR(20)
        , @PJT_SHIP       INT
        , @SHIP_SEQ       INT
        , @SHIP_SEQ_LOT   INT
        , @LOT_NO         VARCHAR(50)
        , @ATWRT02        VARCHAR(50) -- ✅ 케이블 코어
        , @ASSEMBLY_SEQ   INT
        , @ASSEMBLY       FLOAT

        , @FJ_ASM_SEQ     INT
        , @FJ_ASSEMBLY    FLOAT
        , @FJ_LAST_ASM_SEQ INT   -- ✅ SMA 공정에서 "마지막 FJ만" 처리할 때 사용

        , @SUL_NO         VARCHAR(50)
        , @EQUIP_SPEED    FLOAT
        , @LEAD_TIME      INT
        , @READY_TS       INT     -- 공정 시작 가능 시점(이전 공정 종료 + 준비일)
        , @START_TS       INT     -- 실제 시작 TS
        , @END_TS         INT     -- 실제 종료 TS
        , @WORK_CNTR_SEQ  INT

        , @CHOSEN_GRP_EQUIP VARCHAR(20) -- LDS/UST 그룹 대표 설비
        , @CUR_GRP_TS       INT          -- 그룹 대표 설비의 현재 TS(선택 기준)

        , @PROCESS_SEQ    INT
        , @PROCESS_CODE   VARCHAR(20)
        , @CHOSEN_EQUIP   VARCHAR(20)   -- 실제 배정 설비 (단일 or 그룹 내부 서브 설비)
        , @CUR_TS         INT            -- 해당 설비의 현재 TS

        -- FIX 충돌 판단용 변수들
        , @FX_S           INT
        , @FX_E           INT
        , @BLOCK_S        INT
        , @BLOCK_E        INT
        , @OV_S           INT
        , @OV_E           INT
        , @OV_D           INT
        , @HAS_CONFLICT   INT
        , @OVERLAP_DAYS   INT
        , @CFX_S          INT
        , @CFX_E          INT
        , @CBLOCK_S       INT
        , @CBLOCK_E       INT;
        
        
    /* =====================================================================================
       [CUR_LOT]
       - #LOT_ORDER에서 (SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, LOT_NO, ASSEMBLY_SEQ) 별로 1건만 뽑아
         LOT 단위 루프를 돈다.
       - ORDER BY로 처리 순서를 고정 (SHIP_SEQ DESC 포함)
    ===================================================================================== */
	DECLARE CUR_LOT CURSOR LOCAL FAST_FORWARD FOR
	SELECT DISTINCT
	       SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, LOT_NO, ATWRT02, ASSEMBLY_SEQ
	FROM #LOT_ORDER
	ORDER BY SALE_OPP_NO, PJT_SHIP, SHIP_SEQ DESC, SHIP_SEQ_LOT, ASSEMBLY_SEQ;

    OPEN CUR_LOT;

    FETCH NEXT FROM CUR_LOT
    INTO @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT, @LOT_NO, @ATWRT02, @ASSEMBLY_SEQ;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        /* =================================================================================
           1) 공정 리스트 만들기 (#PROC_LIST)
           - 해당 LOT/ASSEMBLY에서 수행해야 하는 공정들을 PROCESS_SEQ 기준으로 정렬해 RN 부여
        ================================================================================= */
        IF OBJECT_ID('tempdb..#PROC_LIST') IS NOT NULL DROP TABLE #PROC_LIST;
        CREATE TABLE #PROC_LIST
        (
            RN            INT IDENTITY(1,1) NOT NULL,
            PROCESS_SEQ   INT               NULL,
            PROCESS_CODE  VARCHAR(20)       NULL
        );

        INSERT INTO #PROC_LIST(PROCESS_SEQ, PROCESS_CODE)        
        SELECT DISTINCT
               L.PROCESS_SEQ
             , L.PROCESS_CODE
          FROM #LOT_ORDER L
         WHERE L.SALE_OPP_NO  = @SALE_OPP_NO
           AND L.PJT_SHIP     = @PJT_SHIP
           AND L.SHIP_SEQ     = @SHIP_SEQ
           AND L.SHIP_SEQ_LOT = @SHIP_SEQ_LOT
           AND L.LOT_NO       = @LOT_NO
           AND L.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
         ORDER BY L.PROCESS_SEQ

        /* =================================================================================
           2) FJ 리스트 만들기 (#FJ_LIST)
           - 해당 LOT/ASSEMBLY에서 흘려야 하는 FJ_ASSEMBLY_SEQ 목록
           - 이후 공정별로 이 FJ들을 순서대로 처리
        ================================================================================= */
        IF OBJECT_ID('tempdb..#FJ_LIST') IS NOT NULL DROP TABLE #FJ_LIST;
        CREATE TABLE #FJ_LIST
        (
            RN              INT IDENTITY(1,1) NOT NULL,
            FJ_ASSEMBLY_SEQ INT               NOT NULL
        );

        INSERT INTO #FJ_LIST(FJ_ASSEMBLY_SEQ)
        SELECT DISTINCT S.FJ_ASSEMBLY_SEQ
        FROM #SRC_FJ S
        WHERE S.SALE_OPP_NO  = @SALE_OPP_NO
          AND S.PJT_SHIP     = @PJT_SHIP
          AND S.SHIP_SEQ     = @SHIP_SEQ
          AND S.SHIP_SEQ_LOT = @SHIP_SEQ_LOT
          AND S.LOT_NO       = @LOT_NO
          AND S.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
        ORDER BY S.FJ_ASSEMBLY_SEQ;

        SELECT @FJ_LAST_ASM_SEQ = MAX(FJ_ASSEMBLY_SEQ)
		FROM #FJ_LIST;
		        
        
        /* =================================================================================
           3) 파이프라인 실행: 공정(바깥) -> FJ(안쪽)
        ================================================================================= */
        DECLARE @P_RN INT = 1;
        DECLARE @P_CNT INT = (SELECT COUNT(*) FROM #PROC_LIST);

        WHILE @P_RN <= @P_CNT
        BEGIN
            SELECT
                  @PROCESS_SEQ  = PROCESS_SEQ
                , @PROCESS_CODE = PROCESS_CODE
            FROM #PROC_LIST
            WHERE RN = @P_RN;


            /* =============================================================================
               (A) 공정별 설비 선택
               - 기본은 #WORK_CNTR_TIMESTAMP에서 TS가 가장 작은 설비를 선택
               - LDS/UST는 그룹 설비 선택 로직
               - INS는 ATWRT02(코어) 값으로 후보 설비를 제한
            ============================================================================= */
            IF @PROCESS_CODE IN ('LDS','UST')
            BEGIN
                SELECT TOP (1)
                      @CHOSEN_GRP_EQUIP = W.WORK_CNTR_CD
                    , @CUR_GRP_TS       = W.TIMESTAMP_POINT
                FROM #WORK_CNTR_TIMESTAMP W
                WHERE W.PROCESS_CODE = @PROCESS_CODE
                  AND EXISTS
                  (
                      SELECT 1
                      FROM #WC_GRP G
                      WHERE G.SIMUL_PROCESS_CD = @PROCESS_CODE
                        AND G.GRP_WORK_CNTR_CD = W.WORK_CNTR_CD
                        AND G.WORK_CNTR_CD     = W.WORK_CNTR_CD
                  )
                ORDER BY W.TIMESTAMP_POINT ASC, W.SEQ ASC;

                IF @CHOSEN_GRP_EQUIP IS NULL
                BEGIN
                    SET @CHOSEN_GRP_EQUIP = '';
                    SET @CUR_GRP_TS = 0;
                END

                SET @CHOSEN_EQUIP = @CHOSEN_GRP_EQUIP;
                SET @CUR_TS = @CUR_GRP_TS;
            END
            ELSE
            BEGIN
                /* ✅ INS 공정: ATWRT02(코어) 값으로 설비 후보군 제한 */
			   IF @PROCESS_CODE = 'INS'
			   BEGIN
			       SELECT TOP (1)
			             @CHOSEN_EQUIP = W.WORK_CNTR_CD
			           , @CUR_TS       = W.TIMESTAMP_POINT
			       FROM #WORK_CNTR_TIMESTAMP W
			       WHERE W.PROCESS_CODE = @PROCESS_CODE
			         AND (
			              (ISNULL(@ATWRT02, '') = '3'  AND W.WORK_CNTR_CD IN ('INS044', 'INS048', 'INS049'))
			           OR (ISNULL(@ATWRT02, '') = '1'  AND W.WORK_CNTR_CD IN ('INS050', 'INS051', 'INS052'))
			         )
			       ORDER BY W.TIMESTAMP_POINT ASC, W.SEQ ASC;
			
			       IF @CHOSEN_EQUIP IS NULL
			       BEGIN
			           -- 후보군에 해당 설비가 없거나 TS 테이블에 없으면, 디폴트 처리(원래 로직으로 fallback할지 선택)
			           SET @CHOSEN_EQUIP = '';
			           SET @CUR_TS = 0;
			       END
			
			       SET @CHOSEN_GRP_EQUIP = NULL;
			       SET @CUR_GRP_TS = NULL;
			   END
			   ELSE
			   BEGIN
			       -- 기존 일반 공정 선택 로직 그대로
			       SELECT TOP (1)
			             @CHOSEN_EQUIP = W.WORK_CNTR_CD
			           , @CUR_TS       = W.TIMESTAMP_POINT
			       FROM #WORK_CNTR_TIMESTAMP W
			       WHERE W.PROCESS_CODE = @PROCESS_CODE
			       ORDER BY W.TIMESTAMP_POINT ASC, W.SEQ ASC;
			
			       IF @CHOSEN_EQUIP IS NULL
			       BEGIN
			           SET @CHOSEN_EQUIP = '';
			           SET @CUR_TS = 0;
			       END
			
			       SET @CHOSEN_GRP_EQUIP = NULL;
			       SET @CUR_GRP_TS = NULL;
			   END
            END

            /* =============================================================================
               (B) 이 공정에서 FJ들을 순서대로 처리
               - 공정은 고정(@PROCESS_CODE), FJ별로 READY/TS/충돌체크/누적을 진행
            ============================================================================= */
            DECLARE @FJ_RN INT = 1;
            DECLARE @FJ_CNT INT = (SELECT COUNT(*) FROM #FJ_LIST);

            WHILE @FJ_RN <= @FJ_CNT
            BEGIN
                SELECT @FJ_ASM_SEQ = FJ_ASSEMBLY_SEQ
                FROM #FJ_LIST
                WHERE RN = @FJ_RN;
            
                /* ------------------------------------------------------------
                   [Skip Rule 1] FJY/FJT 설비는 FJ_ASSEMBLY_SEQ >= 2부터만 처리
                   - (LDS/UST 그룹일 때만 의미있지만, 공정 레벨에서 한번 더 안전하게)
                ------------------------------------------------------------ */
				IF (@PROCESS_CODE IN ('LDS','UST'))
				   AND (LEFT(ISNULL(@CHOSEN_EQUIP, ''), 3) IN ('FJY', 'FJT')) 
				   AND (@FJ_ASM_SEQ < 2 )				
				BEGIN
				    SET @FJ_RN += 1;
				    CONTINUE;  -- 이 FJ는 통째로 스킵 (TS/결과/누적 모두 안 함)
				END
				
				

                /* ------------------------------------------------------------
                   [Skip Rule 2] SMA 공정은 마지막 FJ만 처리
                ------------------------------------------------------------ */
				IF (@PROCESS_CODE = 'SMA')
				   AND (ISNULL(@FJ_ASM_SEQ, 0) <> ISNULL(@FJ_LAST_ASM_SEQ, 0))
				BEGIN
				    SET @FJ_RN += 1;
				    CONTINUE;
				END

				
                /* =============================================================================
                   [핵심] LDS/UST 그룹 설비 전개
                   - 그룹 대표(@CHOSEN_GRP_EQUIP) 안의 서브 설비들을 WORK_CNTR_SEQ 순으로 "직렬" 처리
                   - 그룹 내부 직렬 READY는 @STAGE_READY_TS 로 유지
                   - 서브 설비별 TS, FIX 체크, 결과 insert, TS 누적을 각각 수행
                   - 그룹 공정이 끝나면 #FJ_READY_TS를 마지막 END 기준으로 갱신 후 CONTINUE
                ============================================================================= */
				IF @PROCESS_CODE IN ('LDS','UST')
				BEGIN
				    /* ------------------------------------------------------------
				       0) 현재 FJ에 대해, 선택된 그룹(@CHOSEN_GRP_EQUIP)에 속한 서브 설비 목록 구성
				          - 기본 정렬: WORK_CNTR_SEQ
				          - (옵션) UST 단계 순서를 prefix로 고정하고 싶으면 CASE ORDER BY 사용 가능
				    ------------------------------------------------------------ */
				    IF OBJECT_ID('tempdb..#SUB_EQUIP_LIST') IS NOT NULL DROP TABLE #SUB_EQUIP_LIST;
				    CREATE TABLE #SUB_EQUIP_LIST
				    (
				        RN            INT IDENTITY(1,1) NOT NULL,
				        WORK_CNTR_SEQ INT NULL,
				        WORK_CNTR_CD  NVARCHAR(20) NOT NULL
				    );
				
				    INSERT INTO #SUB_EQUIP_LIST(WORK_CNTR_SEQ, WORK_CNTR_CD)
				    SELECT
				          S.WORK_CNTR_SEQ
				        , S.WORK_CNTR_CD
				    FROM #SRC_FJ S
				    WHERE S.SALE_OPP_NO = @SALE_OPP_NO
				      AND S.PJT_SHIP     = @PJT_SHIP
				      AND S.SHIP_SEQ     = @SHIP_SEQ
				      AND S.SHIP_SEQ_LOT = @SHIP_SEQ_LOT
				      AND S.LOT_NO       = @LOT_NO
				      AND S.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
				      AND S.FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ
				      AND S.PROCESS_CODE = @PROCESS_CODE
				      AND EXISTS
				      (
				          SELECT 1
				          FROM #WC_GRP G
				          WHERE G.SIMUL_PROCESS_CD = @PROCESS_CODE
				            AND G.GRP_WORK_CNTR_CD = @CHOSEN_GRP_EQUIP
				            AND G.WORK_CNTR_CD     = S.WORK_CNTR_CD
				      )
				    ORDER BY
				      S.WORK_CNTR_SEQ;
				
				    DECLARE @SUB_RN INT = 1;
				    DECLARE @SUB_CNT INT = (SELECT COUNT(*) FROM #SUB_EQUIP_LIST);
				
				    /* ------------------------------------------------------------
				       1) 그룹 공정 내부 "직렬 체인" READY
				          - 최초는 FJ READY(#FJ_READY_TS)로 시작
				          - 이후는 직전 단계 END로 갱신
				    ------------------------------------------------------------ */
				    DECLARE @STAGE_READY_TS INT = NULL;
				    DECLARE @LAST_END_TS    INT = NULL;
				
				    /* FJ READY 로드 (그룹 내부 최초 READY의 기준) */
				    SELECT @READY_TS = READY_TS
				    FROM #FJ_READY_TS
				    WHERE SALE_OPP_NO = @SALE_OPP_NO
				      AND PJT_SHIP     = @PJT_SHIP
				      AND SHIP_SEQ     = @SHIP_SEQ
				      AND SHIP_SEQ_LOT = @SHIP_SEQ_LOT
				      AND LOT_NO       = @LOT_NO
				      AND ASSEMBLY_SEQ = @ASSEMBLY_SEQ
				      AND FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ;
				
				    IF @READY_TS IS NULL SET @READY_TS = 0;
				    SET @STAGE_READY_TS = @READY_TS;
				
				    /* ------------------------------------------------------------
				       2) 서브 설비를 호기 순서대로 직렬 처리
				    ------------------------------------------------------------ */
				    WHILE @SUB_RN <= @SUB_CNT
				    BEGIN
				        SELECT
				              @CHOSEN_EQUIP  = WORK_CNTR_CD
				            , @WORK_CNTR_SEQ = WORK_CNTR_SEQ
				        FROM #SUB_EQUIP_LIST
				        WHERE RN = @SUB_RN;
				
				        /* ------------------------------------------------------------
				           2-1) FJY/FJT 설비 스킵 규칙
				                - FJ_ASSEMBLY_SEQ < 2 인 경우 해당 서브 설비만 스킵
				        ------------------------------------------------------------ */
				        IF (LEFT(ISNULL(@CHOSEN_EQUIP, ''), 3) IN ('FJY', 'FJT'))
				           AND ISNULL(@FJ_ASM_SEQ, 0) < 2
				        BEGIN
				            SET @SUB_RN += 1;
				            CONTINUE;
				        END
				
				        /* ------------------------------------------------------------
				           2-2) 리드타임/속도/기타 정보 로드 (해당 서브 설비 기준)
				        ------------------------------------------------------------ */
				        SET @LEAD_TIME = NULL;
				
				        SELECT TOP (1)
				              @LEAD_TIME     = S.LEAD_TIME_DAYS
				            , @EQUIP_SPEED   = S.EQUIP_SPEED
				            , @SUL_NO        = S.SUL_NO
				            , @ASSEMBLY      = S.ASSEMBLY
				            , @FJ_ASSEMBLY   = S.FJ_ASSEMBLY
				            , @WORK_CNTR_SEQ = S.WORK_CNTR_SEQ
				        FROM #SRC_FJ S
				        WHERE S.SALE_OPP_NO = @SALE_OPP_NO
				          AND S.PJT_SHIP     = @PJT_SHIP
				          AND S.SHIP_SEQ     = @SHIP_SEQ
				          AND S.SHIP_SEQ_LOT = @SHIP_SEQ_LOT
				          AND S.LOT_NO       = @LOT_NO
				          AND S.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
				          AND S.FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ
				          AND S.PROCESS_CODE = @PROCESS_CODE
				          AND S.WORK_CNTR_CD = @CHOSEN_EQUIP;
				
				        IF @LEAD_TIME IS NULL SET @LEAD_TIME = 0;
				
				        /* ------------------------------------------------------------
				           2-3) 설비 TS 로드 (해당 서브 설비 기준)
				        ------------------------------------------------------------ */
				        SELECT @CUR_TS = TIMESTAMP_POINT
				        FROM #WORK_CNTR_TIMESTAMP
				        WHERE PROCESS_CODE = @PROCESS_CODE
				          AND WORK_CNTR_CD  = @CHOSEN_EQUIP;
				
				        IF @CUR_TS IS NULL SET @CUR_TS = 0;
				
				        /* ------------------------------------------------------------
				           2-4) 직렬 START/END 계산
				                - START = max(설비 TS, 직렬 체인 READY)
				        ------------------------------------------------------------ */
				        SET @START_TS = CASE WHEN @CUR_TS > @STAGE_READY_TS THEN @CUR_TS ELSE @STAGE_READY_TS END;
				        SET @END_TS   = @START_TS + ISNULL(@LEAD_TIME,0);
				
				        /* ------------------------------------------------------------
				           2-5) FIX 체크 (해당 서브 설비 기준)
				                - 겹침이 허용치 초과면 START를 BLOCK_E로 밀고 재계산
				                - 밀릴 때도 직렬 체인 READY(@STAGE_READY_TS)는 반드시 만족 (직렬 유지)
				        ------------------------------------------------------------ */
				        SET @HAS_CONFLICT = 0;
				        SET @OVERLAP_DAYS = 0;
				        SET @CFX_S = NULL; SET @CFX_E = NULL;
				        SET @CBLOCK_S = NULL; SET @CBLOCK_E = NULL;
				
				        WHILE 1=1
				        BEGIN
				            SELECT TOP (1)
				                  @FX_S = F.FIX_START_TS
				                , @FX_E = F.FIX_END_TS
				            FROM #FIXED_PJT_LOT_DATA F
				            WHERE F.PROCESS_CODE = @PROCESS_CODE
				              AND F.WORK_CNTR_CD  = @CHOSEN_EQUIP
				              AND (F.FIX_END_TS + @P_SENS_AFTER) > @START_TS
				            ORDER BY F.FIX_START_TS;
				
				            IF @@ROWCOUNT = 0 BREAK;
				
				            SET @BLOCK_S = @FX_S - @P_SENS_BEFORE;
				            SET @BLOCK_E = @FX_E + @P_SENS_AFTER;
				
				            SET @OV_S = CASE WHEN @START_TS > @BLOCK_S THEN @START_TS ELSE @BLOCK_S END;
				            SET @OV_E = CASE WHEN @END_TS   < @BLOCK_E THEN @END_TS   ELSE @BLOCK_E END;
				            SET @OV_D = CASE WHEN @OV_E > @OV_S THEN (@OV_E - @OV_S) ELSE 0 END;
				
				            SET @HAS_CONFLICT = CASE WHEN @OV_D > 0 THEN 1 ELSE 0 END;
				            SET @OVERLAP_DAYS = @OV_D;
				
				            SET @CFX_S = @FX_S; SET @CFX_E = @FX_E;
				            SET @CBLOCK_S = @BLOCK_S; SET @CBLOCK_E = @BLOCK_E;
				
                            -- 겹침이 허용치 이하면 그대로 진행
				            IF (@OV_D <= @P_ALLOW_OVERLAP) BREAK;
				
				            /* ✅ FIX로 밀리면 BLOCK_E로 이동하되, 직렬 체인 READY도 만족 */
				            SET @START_TS = @BLOCK_E;
				            SET @START_TS = CASE WHEN @START_TS > @STAGE_READY_TS THEN @START_TS ELSE @STAGE_READY_TS END;
				            SET @END_TS   = @START_TS + ISNULL(@LEAD_TIME,0);
				        END
				
				        /* ------------------------------------------------------------
				           2-6) 결과 저장
				        ------------------------------------------------------------ */
				        INSERT INTO #RESULT_STEP
				        (
				            SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
				            LOT_NO, SUL_NO,
				            ASSEMBLY_SEQ, ASSEMBLY,
				            FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
				            PROCESS_SEQ, PROCESS_CODE,
				            WORK_CNTR_SEQ, WORK_CNTR_CD,
				            EQUIP_SPEED, LEAD_TIME,
				            START_TS, END_TS,
				            HAS_CONFLICT, OVERLAP_DAYS,
				            CONFLICT_FIX_START_TS, CONFLICT_FIX_END_TS,
				            FIX_BLOCK_START_TS, FIX_BLOCK_END_TS
				        )
				        VALUES
				        (
				            @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
				            @LOT_NO, @SUL_NO,
				            @ASSEMBLY_SEQ, @ASSEMBLY,
				            @FJ_ASM_SEQ, @FJ_ASSEMBLY,
				            @PROCESS_SEQ, @PROCESS_CODE,
				            @WORK_CNTR_SEQ, @CHOSEN_EQUIP,
				            @EQUIP_SPEED, ISNULL(@LEAD_TIME,0),
				            @START_TS, @END_TS,
				            @HAS_CONFLICT, @OVERLAP_DAYS,
				            @CFX_S, @CFX_E,
				            @CBLOCK_S, @CBLOCK_E
				        );
				
				        /* ------------------------------------------------------------
				           2-7) 설비 TS 누적 (해당 설비만)
				                - 다음 작업 가능 시점 = END + FJ 준비일수
				        ------------------------------------------------------------ */
				        UPDATE #WORK_CNTR_TIMESTAMP
				        SET TIMESTAMP_POINT = @END_TS + @P_PREP_FJ_DAYS
				        WHERE PROCESS_CODE = @PROCESS_CODE
				          AND WORK_CNTR_CD  = @CHOSEN_EQUIP;
				
				        /* ------------------------------------------------------------
				           2-8) 그룹 내부 직렬 체인 READY 갱신
				                - 다음 서브 설비는 현재 END 이후에만 시작 가능
				        ------------------------------------------------------------ */
				        SET @STAGE_READY_TS = @END_TS;
				
				        /* (공정간 READY 업데이트용) 마지막 END 보관 */
				        SET @LAST_END_TS = CASE WHEN @LAST_END_TS IS NULL OR @END_TS > @LAST_END_TS THEN @END_TS ELSE @LAST_END_TS END;
				
				        SET @SUB_RN += 1;
				    END -- WHILE SUB_EQUIP
				
				    /* ------------------------------------------------------------
				       3) 공정간 READY 갱신 (해당 FJ의 다음 공정 시작 가능 시점)
				          - 그룹 공정의 마지막 종료 기준 + 공정 준비일수
				    ------------------------------------------------------------ */
				    IF @LAST_END_TS IS NOT NULL
				    BEGIN
				        UPDATE #FJ_READY_TS
				        SET READY_TS = @LAST_END_TS + @P_PREP_PROC_DAYS
				        WHERE SALE_OPP_NO = @SALE_OPP_NO
				          AND PJT_SHIP     = @PJT_SHIP
				          AND SHIP_SEQ     = @SHIP_SEQ
				          AND SHIP_SEQ_LOT = @SHIP_SEQ_LOT
				          AND LOT_NO       = @LOT_NO
				          AND ASSEMBLY_SEQ = @ASSEMBLY_SEQ
				          AND FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ;
				    END
				
				    SET @FJ_RN += 1;
				    CONTINUE;  -- ✅ 그룹 공정은 여기서 끝. 아래 단일 설비 로직 스킵
				END


                /* =============================================================================
                   [기본] 단일 설비 공정 처리 로직
                   - (LDS/UST가 아니거나, 그룹 처리로 넘어가지 않는 경우)
                   - START = max(설비 TS, FJ READY)
                   - FIX 충돌 검사 후 결과 저장
                   - 설비 TS, FJ READY 갱신
                ============================================================================= */

                /* 이 FJ+공정의 리드타임/속도/기타 */
                SET @WORK_CNTR_SEQ = NULL;
                SET @LEAD_TIME = NULL;

                SELECT TOP (1)
                      @LEAD_TIME     = S.LEAD_TIME_DAYS
                    , @EQUIP_SPEED   = S.EQUIP_SPEED
                    , @SUL_NO        = S.SUL_NO
                    , @ASSEMBLY      = S.ASSEMBLY
                    , @FJ_ASSEMBLY   = S.FJ_ASSEMBLY
                    , @WORK_CNTR_SEQ = S.WORK_CNTR_SEQ
                FROM #SRC_FJ S
                WHERE S.SALE_OPP_NO = @SALE_OPP_NO
                  AND S.PJT_SHIP     = @PJT_SHIP
                  AND S.SHIP_SEQ     = @SHIP_SEQ
                  AND S.SHIP_SEQ_LOT = @SHIP_SEQ_LOT
                  AND S.LOT_NO       = @LOT_NO
                  AND S.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                  AND S.FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ
                  AND S.PROCESS_CODE = @PROCESS_CODE
                  AND S.WORK_CNTR_CD = @CHOSEN_EQUIP;

                /* 설비 TS */
                SELECT @CUR_TS = TIMESTAMP_POINT
                FROM #WORK_CNTR_TIMESTAMP
                WHERE PROCESS_CODE = @PROCESS_CODE
                  AND WORK_CNTR_CD = @CHOSEN_EQUIP;
                IF @CUR_TS IS NULL SET @CUR_TS = 0;

                /* FJ READY */
                SELECT @READY_TS = READY_TS
                FROM #FJ_READY_TS
                WHERE SALE_OPP_NO = @SALE_OPP_NO
                  AND PJT_SHIP     = @PJT_SHIP
                  AND SHIP_SEQ     = @SHIP_SEQ
                  AND SHIP_SEQ_LOT = @SHIP_SEQ_LOT
                  AND LOT_NO       = @LOT_NO
                  AND ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                  AND FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ;
                IF @READY_TS IS NULL SET @READY_TS = 0;

                SET @START_TS = CASE WHEN @CUR_TS > @READY_TS THEN @CUR_TS ELSE @READY_TS END;
                SET @END_TS   = @START_TS + ISNULL(@LEAD_TIME,0);

                /* FIX 체크 */
                SET @HAS_CONFLICT = 0;
                SET @OVERLAP_DAYS = 0;
                SET @CFX_S = NULL; SET @CFX_E = NULL;
                SET @CBLOCK_S = NULL; SET @CBLOCK_E = NULL;

                WHILE 1=1
                BEGIN
                    SELECT TOP (1)
                          @FX_S = F.FIX_START_TS
                        , @FX_E = F.FIX_END_TS
                    FROM #FIXED_PJT_LOT_DATA F
                    WHERE F.PROCESS_CODE = @PROCESS_CODE
                      AND F.WORK_CNTR_CD  = @CHOSEN_EQUIP
                      AND (F.FIX_END_TS + @P_SENS_AFTER) > @START_TS
                    ORDER BY F.FIX_START_TS;

                    IF @@ROWCOUNT = 0 BREAK;

                    SET @BLOCK_S = @FX_S - @P_SENS_BEFORE;
                    SET @BLOCK_E = @FX_E + @P_SENS_AFTER;

                    SET @OV_S = CASE WHEN @START_TS > @BLOCK_S THEN @START_TS ELSE @BLOCK_S END;
                    SET @OV_E = CASE WHEN @END_TS   < @BLOCK_E THEN @END_TS   ELSE @BLOCK_E END;
                    SET @OV_D = CASE WHEN @OV_E > @OV_S THEN (@OV_E - @OV_S) ELSE 0 END;

                    SET @HAS_CONFLICT = CASE WHEN @OV_D > 0 THEN 1 ELSE 0 END;
                    SET @OVERLAP_DAYS = @OV_D;

                    SET @CFX_S = @FX_S; SET @CFX_E = @FX_E;
                    SET @CBLOCK_S = @BLOCK_S; SET @CBLOCK_E = @BLOCK_E;

                    IF (@OV_D <= @P_ALLOW_OVERLAP) BREAK;

                    SET @START_TS = @BLOCK_E;
                    SET @START_TS = CASE WHEN @START_TS > @READY_TS THEN @START_TS ELSE @READY_TS END;
                    SET @END_TS   = @START_TS + ISNULL(@LEAD_TIME,0);
                END

                INSERT INTO #RESULT_STEP
                (
                    SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
                    LOT_NO, SUL_NO,
                    ASSEMBLY_SEQ, ASSEMBLY,
                    FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
                    PROCESS_SEQ, PROCESS_CODE,
                    WORK_CNTR_SEQ, WORK_CNTR_CD,
                    EQUIP_SPEED, LEAD_TIME,
                    START_TS, END_TS,
                    HAS_CONFLICT, OVERLAP_DAYS,
                    CONFLICT_FIX_START_TS, CONFLICT_FIX_END_TS,
                    FIX_BLOCK_START_TS, FIX_BLOCK_END_TS
                )
                VALUES
                (
                    @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
                    @LOT_NO, @SUL_NO,
                    @ASSEMBLY_SEQ, @ASSEMBLY,
                    @FJ_ASM_SEQ, @FJ_ASSEMBLY,
                    @PROCESS_SEQ, @PROCESS_CODE,
                    @WORK_CNTR_SEQ, @CHOSEN_EQUIP,
                    @EQUIP_SPEED, ISNULL(@LEAD_TIME,0),
                    @START_TS, @END_TS,
                    @HAS_CONFLICT, @OVERLAP_DAYS,
                    @CFX_S, @CFX_E,
                    @CBLOCK_S, @CBLOCK_E
                );

                UPDATE #WORK_CNTR_TIMESTAMP
                SET TIMESTAMP_POINT = @END_TS + @P_PREP_FJ_DAYS
                WHERE PROCESS_CODE = @PROCESS_CODE
                  AND WORK_CNTR_CD  = @CHOSEN_EQUIP;

                UPDATE #FJ_READY_TS
                SET READY_TS = @END_TS + @P_PREP_PROC_DAYS
                WHERE SALE_OPP_NO = @SALE_OPP_NO
                  AND PJT_SHIP     = @PJT_SHIP
                  AND SHIP_SEQ     = @SHIP_SEQ
                  AND SHIP_SEQ_LOT = @SHIP_SEQ_LOT
                  AND LOT_NO       = @LOT_NO
                  AND ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                  AND FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ;

                SET @FJ_RN += 1;
            END -- WHILE FJ

            SET @P_RN += 1;
        END -- WHILE PROC

        FETCH NEXT FROM CUR_LOT
        INTO @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT, @LOT_NO, @ATWRT02, @ASSEMBLY_SEQ;
    END -- WHILE LOT

    CLOSE CUR_LOT;
    DEALLOCATE CUR_LOT;
END;

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER_SELECT]
(
      @TIMESTAMP_BASE_DATE DATE
    , @P_TEMP_VERSION      VARCHAR(50)
    , @P_USER_CODE         VARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
          @P_TEMP_VERSION AS TEMP_VERSION
        , ROW_NUMBER() OVER(
              ORDER BY
                  R.SALE_OPP_NO, R.PJT_SHIP, R.SHIP_SEQ DESC, R.SHIP_SEQ_LOT,
                  R.ASSEMBLY_SEQ, R.FJ_ASSEMBLY_SEQ, R.PROCESS_SEQ, R.INSERT_ORDER
          ) AS SEQ
        , R.SALE_OPP_NO
        , R.PJT_SHIP
        , R.SHIP_SEQ
        , R.SHIP_SEQ_LOT
        , R.SUL_NO
        , R.ASSEMBLY_SEQ
        , R.ASSEMBLY
        , R.FJ_ASSEMBLY_SEQ
        , R.FJ_ASSEMBLY
        , R.PROCESS_SEQ
        , R.PROCESS_CODE
        , ROW_NUMBER() OVER
          (
              PARTITION BY R.SALE_OPP_NO, R.PJT_SHIP, R.SHIP_SEQ, R.SHIP_SEQ_LOT, R.ASSEMBLY, R.FJ_ASSEMBLY_SEQ
              ORDER BY R.PROCESS_SEQ, R.INSERT_ORDER
          ) AS WORK_CNTR_SEQ
        , R.WORK_CNTR_CD
        , R.EQUIP_SPEED
        , R.LEAD_TIME
        , DATEADD(DAY, R.START_TS, @TIMESTAMP_BASE_DATE) AS PRD_CNFM_STRT_DATE
        , DATEADD(DAY, R.END_TS,   @TIMESTAMP_BASE_DATE) AS PRD_CNFM_END_DATE
        , @P_USER_CODE AS REG_EMP
        , GETDATE() AS REG_DATE
    FROM #RESULT_STEP R
    ORDER BY SEQ;
END;

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_FIXED_PJT_LOT_TIMESTAMP]
(
      @P_VERSION            VARCHAR(50)  -- FIX 데이터를 읽을 VersionCode
    , @P_FIXED_PJT_LIST     dbo.UDT_DATA_SALE_OPP_NO READONLY
    , @TIMESTAMP_BASE_DATE  DATE         -- Base timestamp 0 기준일
) 
AS
BEGIN
    SET NOCOUNT ON;

    /* =====================================================================================
       - SEQ            : 전체 데이터 전역 순번
       - PRODUCTION_SEQ : SALE_OPP_NO 내 순번
       - FIXED_TIMESTAMP_POINT = DATEDIFF(DAY, @TIMESTAMP_BASE_DATE, PRD_CNFM_STRT_DATE)
    ===================================================================================== */

    SELECT 'F' AS DATA_KIND
          -- ✅ 전체 데이터 전역 순번
         , ROW_NUMBER() OVER
          (
              ORDER BY
                  T1.SALE_OPP_NO,
                  T1.PJT_SHIP,
                  T1.SHIP_SEQ DESC,
                  T1.SHIP_SEQ_LOT,
                  T1.ASSEMBLY_SEQ,
                  T1.PROCESS_SEQ,
                  T1.WORK_CNTR_SEQ,
                  T1.PRD_CNFM_STRT_DATE
          ) AS SEQ

         , ROW_NUMBER() OVER
           (
               PARTITION BY T1.SALE_OPP_NO
               ORDER BY
                   T1.PJT_SHIP,
                   T1.SHIP_SEQ DESC,
                   T1.SHIP_SEQ_LOT,
                   T1.ASSEMBLY_SEQ,
                   T1.PROCESS_SEQ,
                   T1.WORK_CNTR_SEQ,
                   T1.PRD_CNFM_STRT_DATE
           ) AS PRODUCTION_SEQ

        , T1.SALE_OPP_NO
        , CONCAT(CAST(T1.PJT_SHIP AS VARCHAR(3)), '-', CAST(T1.SHIP_SEQ AS VARCHAR(3)), '-', CAST(T1.SHIP_SEQ_LOT AS VARCHAR(3))) AS LOT_NO
        , T1.PJT_SHIP
        , T1.SHIP_SEQ
        , T1.SHIP_SEQ_LOT
        , T1.SUL_NO
        , T1.ASSEMBLY_SEQ
        , T1.ASSEMBLY
        , T1.FJ_ASSEMBLY_SEQ
        , T1.FJ_ASSEMBLY
        , T1.PROCESS_SEQ
        , T1.PROCESS_CODE
        , T1.WORK_CNTR_SEQ
        , T1.WORK_CNTR_CD
        , T1.EQUIP_SPEED
        , T1.LEAD_TIME
        , T1.PRD_CNFM_STRT_DATE
        , T1.PRD_CNFM_END_DATE
        , DATEDIFF(DAY, @TIMESTAMP_BASE_DATE, T1.PRD_CNFM_STRT_DATE) AS FIXED_TIMESTAMP_POINT -- ✅ BaseDate 기준 FIX Timestamp (Day offset)
    FROM SOP_DB.dbo.TB_SIMUL_VERSION_DATA T1           -- <<<< 예시 테이블(환경에 맞게 치환)
         INNER JOIN @P_FIXED_PJT_LIST F
                ON T1.SALE_OPP_NO = F.SALE_OPP_NO
    WHERE T1.SIMUL_VERSION = @P_VERSION      -- <<<< 예시 버전 컬럼(환경에 맞게 치환)
      AND T1.LEAD_TIME > 0
    ORDER BY SEQ, PRODUCTION_SEQ;

END;

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_JSON_TO_SALE_OPP_NO]
	@JSON_PARAMS NVARCHAR(MAX) -- JSON 데이터는 보통 NVARCHAR(MAX)로 받습니다.
AS
BEGIN 
    SET NOCOUNT ON; -- 불필요한 행 수 메시지 출력 방지
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    BEGIN TRY
        --  JSON 데이터를 파싱하여 리스트로 조회
        SELECT 
            PARAM_SALE_OPP_NO AS SALE_OPP_NO
        FROM OPENJSON(@JSON_PARAMS)
        WITH (
            PARAM_SALE_OPP_NO VARCHAR(20) '$.PARAM_SALE_OPP_NO'
        );
        
    END TRY
    BEGIN CATCH
        -- 에러 처리
        THROW 52001, 'SP_CABLE_PRODUCTION_GET_JSON_TO_SALE_OPP_NO Json Parse error', 1;
    END CATCH
END;

CREATE   PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_PRIORITY_PJT_LOT_LEADTIME]
(
    @P_PJT_LIST dbo.UDT_DATA_SALE_OPP_NO READONLY
)
AS
BEGIN
    SET NOCOUNT ON;


    DECLARE @RESULT dbo.UDT_DATA_CABLE_PJT_LOT_FJ;

    DECLARE @SALE_OPP_NO VARCHAR(20);
    DECLARE @SEQ  INT = 0;

    /* =========================================================================
       0) 커서 밖에서 #temp 1회 생성 (프로젝트마다 TRUNCATE)
    ========================================================================= */

    -- 0-1) 생산 기본 데이터(프로젝트 1건 분)
    IF OBJECT_ID('tempdb..#TEMP_PRODUCTION_LIST') IS NOT NULL DROP TABLE #TEMP_PRODUCTION_LIST;
    CREATE TABLE #TEMP_PRODUCTION_LIST
    (
        SALE_OPP_NO          VARCHAR(20)   NULL,
        PJT_SHIP             INT           NULL,
        SHIP_SEQ             INT           NULL,
        SHIP_SEQ_LOT         INT           NULL,
        SUL_NO               VARCHAR(50)   NULL,
        ASSEMBLY_SEQ         INT           NULL,
        ASSEMBLY             INT           NULL,
        PROCESS_SEQ          INT           NULL,
        PROCESS_CODE         VARCHAR(20)   NULL,
        WORK_CNTR_SEQ        INT           NULL,
        WORK_CNTR_CD         VARCHAR(20)   NULL,
        EQUIP_SPEED          FLOAT         NULL,
        LEAD_TIME            INT           NULL,
        SIMUL_PREPARE_DAYS   NUMERIC(2,1)  NULL,
        SIMUL_WORK_DAYS      NUMERIC(2,1)  NULL,
        PRD_STRT_DATE        DATE          NULL,
        PJT_PRD_STRT_DATE    DATE          NULL,
        LOT_PRD_STRT_DATE    DATE          NULL,
        SIMUL_VALUE1         FLOAT         NULL,
        SIMUL_VALUE2         FLOAT         NULL,
        SIMUL_VALUE3         FLOAT         NULL,
        SIMUL_VALUE4         FLOAT         NULL,
        ATTRIBUTE2           VARCHAR(100)  NULL,
        SIMUL_SEQ            INT           NULL,
        ATWRT01              NVARCHAR(50)  NULL,
        ATWRT02              NVARCHAR(50)  NULL,
        ATWRT03              NVARCHAR(50)  NULL,
        ATWRT04              NVARCHAR(50)  NULL,
        ATWRT05              NVARCHAR(50)  NULL
    );

    -- (선택) 인덱스: TRUNCATE 반복이므로 유지가치 있음 (데이터량 많을 때만)
    CREATE INDEX IX_TPL_ASSY ON #TEMP_PRODUCTION_LIST(ASSEMBLY_SEQ, ASSEMBLY);
    CREATE INDEX IX_TPL_PROC ON #TEMP_PRODUCTION_LIST(PROCESS_CODE, WORK_CNTR_CD);

    -- 0-2) FJ 분할 개수 테이블
    IF OBJECT_ID('tempdb..#FJ_CNT_CALC') IS NOT NULL DROP TABLE #FJ_CNT_CALC;
    CREATE TABLE #FJ_CNT_CALC
    (
        ASSEMBLY_SEQ INT  NULL,
        ASSEMBLY     INT  NULL,
        FJ_CNT       INT  NULL
    );
    CREATE INDEX IX_FJ_ASSY ON #FJ_CNT_CALC(ASSEMBLY_SEQ, ASSEMBLY);

    -- 0-3) NUMBERS (1~30) : 1회 생성
    IF OBJECT_ID('tempdb..#NUMBERS') IS NOT NULL DROP TABLE #NUMBERS;
    CREATE TABLE #NUMBERS (CREATE_FJ_NUMBER INT NOT NULL PRIMARY KEY);

    INSERT INTO #NUMBERS(CREATE_FJ_NUMBER)
    SELECT TOP (30) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
    FROM sys.all_objects;

    /* =========================================================================
       1) 프로젝트 커서 루프
    ========================================================================= */
    DECLARE CUR_PJT CURSOR LOCAL FAST_FORWARD FOR
    SELECT P.SALE_OPP_NO 
      FROM @P_PJT_LIST P
           INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_MASTER M
                   ON P.SALE_OPP_NO = M.SALE_OPP_NO
     ORDER BY M.PJT_STRT_DATE ASC; 

    OPEN CUR_PJT;
    FETCH NEXT FROM CUR_PJT INTO @SALE_OPP_NO;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        ---------------------------------------------------------------------
        -- (A) 프로젝트마다 #temp 초기화
        ---------------------------------------------------------------------
        TRUNCATE TABLE #TEMP_PRODUCTION_LIST;
        TRUNCATE TABLE #FJ_CNT_CALC;

        ---------------------------------------------------------------------
        -- (B) #TEMP_PRODUCTION_LIST 적재 (원본 로직 유지)
        ---------------------------------------------------------------------
        INSERT INTO #TEMP_PRODUCTION_LIST
        (
            SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
            SUL_NO, ASSEMBLY_SEQ, ASSEMBLY, 
            PROCESS_SEQ, PROCESS_CODE, WORK_CNTR_SEQ, WORK_CNTR_CD,
            EQUIP_SPEED, LEAD_TIME,
            SIMUL_PREPARE_DAYS, SIMUL_WORK_DAYS, PRD_STRT_DATE, PJT_PRD_STRT_DATE, LOT_PRD_STRT_DATE,
            SIMUL_VALUE1, SIMUL_VALUE2, SIMUL_VALUE3, SIMUL_VALUE4, ATTRIBUTE2,
            SIMUL_SEQ,
            ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05
        )
        SELECT LIST.SALE_OPP_NO
             , LIST.PJT_SHIP
             , LIST.SHIP_SEQ
             , LIST.SHIP_SEQ_LOT
             , LIST.SUL_NO
             , LIST.ASSEMBLY_SEQ
             , LIST.ASSEMBLY
             , LIST.PROCESS_SEQ
             , LIST.PROCESS_CODE
             , LIST.WORK_CNTR_SEQ
             , LIST.WORK_CNTR_CD
             , DATA.EQUIP_SPEED
             , 0 AS LEAD_TIME
             , LIST.SIMUL_PREPARE_DAYS
             , LIST.SIMUL_WORK_DAYS
             , LIST.PRD_STRT_DATE
             , LIST.PJT_PRD_STRT_DATE
             , LIST.LOT_PRD_STRT_DATE
             , DATA.SIMUL_VALUE1
             , DATA.SIMUL_VALUE2
             , DATA.SIMUL_VALUE3
             , DATA.SIMUL_VALUE4
             , DATA.ATTRIBUTE2
             , LIST.SIMUL_SEQ
             , LIST.ATWRT01
             , LIST.ATWRT02
             , LIST.ATWRT03
             , LIST.ATWRT04
             , LIST.ATWRT05
        FROM (
            SELECT DEF.SALE_OPP_NO
                 , DEF.PJT_SHIP
                 , DEF.SHIP_SEQ
                 , DEF.SHIP_SEQ_LOT
                 , DEF.SUL_NO
                 , DEF.ASSEMBLY_SEQ
                 , DEF.ASSEMBLY
                 , DEF.PROCESS_SEQ
                 , DEF.PROCESS_CODE
                 , DEF.WORK_CNTR_SEQ
                 , DEF.WORK_CNTR_CD
                 , DEF.WORK_DAYS                                                       AS SIMUL_WORK_DAYS
                 , DEF.PREPARE_DAYS                                                    AS SIMUL_PREPARE_DAYS
                 , COALESCE(LOT.PRD_STRT_DATE , PJT.PRD_STRT_DATE , DEF.PRD_STRT_DATE) AS PRD_STRT_DATE
                 , PJT.PRD_STRT_DATE                                                   AS PJT_PRD_STRT_DATE
                 , LOT.PRD_STRT_DATE                                                   AS LOT_PRD_STRT_DATE
                 , DEF.SIMUL_SEQ
                 , DEF.ATWRT01
                 , DEF.ATWRT02
                 , DEF.ATWRT03
                 , DEF.ATWRT04
                 , DEF.ATWRT05
            FROM (
                SELECT SALE_OPP_NO
                     , PJT_SHIP
                     , SHIP_SEQ
                     , SHIP_SEQ_LOT
                     , SUL_NO
                     , DENSE_RANK() OVER (ORDER BY PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT) AS ASSEMBLY_SEQ
                     , ASSEMBLY
                     , DENSE_RANK() OVER (ORDER BY PROCESS_SEQ) AS PROCESS_SEQ
                     , PROCESS_CODE
                     , ROW_NUMBER() OVER (PARTITION BY PJT_SHIP, SHIP_SEQ , SHIP_SEQ_LOT, ASSEMBLY ORDER BY SIMUL_SEQ) AS WORK_CNTR_SEQ
                     , WORK_CNTR_CD
                     , ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05
                     , PREPARE_DAYS
                     , NEXT_PRC_PREPARE_DAYS
                     , WORK_DAYS
                     , PRD_STRT_DATE
                     , SIMUL_SEQ
                FROM (
                    SELECT T1.SALE_OPP_NO
                         , T1.PJT_SHIP
                         , T1.SHIP_SEQ
                         , T1.SHIP_SEQ_LOT
                         , T1.SUL_NO
                         , T1.PJT_LENGTH * 1000 AS ASSEMBLY
                         , T2.PROCESS_CODE
                         , T2.WORK_CNTR_CD
                         , T4.CABLE_CORE_FLAG
                         , T6.ATWRT01, T6.ATWRT02, T6.ATWRT03, T6.ATWRT04, T6.ATWRT05
                         , T5.PREPARE_DAYS
                         , 1 AS NEXT_PRC_PREPARE_DAYS
                         , CASE WHEN T2.WORK_CNTR_CD LIKE 'FJT%' OR T2.WORK_CNTR_CD LIKE 'FJY%' THEN NULL
                                ELSE T5.WORK_DAYS
                           END WORK_DAYS
                         , T1.PRD_STRT_DATE
                         , T5.SEQ AS PROCESS_SEQ
                         , T4.SIMUL_SEQ
                    FROM SOP_DB.dbo.TB_PRD_PLAN_LIST T1
                        INNER JOIN SOP_DB.dbo.TB_SIMUL_SUL_ASSY_PROC_LIST T2
                           ON T1.SUL_NO              = T2.SUL_NO
                          AND (T1.PJT_LENGTH * 1000) = T2.ASSEMBLY
                        --AND T2.WORK_CNTR_CD        = (CASE WHEN T2.PROCESS_CODE = 'UST' THEN T2.ATTRIBUTE2 ELSE T2.WORK_CNTR_CD END)
                        INNER JOIN (
                                        SELECT SIMUL_PROCESS_CD, WORK_CNTR_CD, SIMUL_SEQ, DEFAULT_YN, CABLE_CORE_FLAG
                                        FROM SOP_DB.dbo.TB_SIMUL_WORK_CENTER_MST
                                      --  WHERE DEFAULT_YN = 'Y'
                                    ) T4
                                ON T2.WORK_CNTR_CD = T4.WORK_CNTR_CD
                        INNER JOIN SOP_DB.dbo.TB_SIMUL_FAC_PC T5
                                ON T2.PROCESS_CODE = T5.PROCESS_CODE
                               AND T5.Q_YEAR       = YEAR(GETDATE())
                        INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_CABLE_SPEC T6
                                ON T1.SUL_NO  = T6.SUL_NO
                               AND T1.REV_SEQ = T6.REV_SEQ
                   WHERE T1.SALE_OPP_NO = @SALE_OPP_NO
                     AND (T4.CABLE_CORE_FLAG IS NULL
                      OR T4.CABLE_CORE_FLAG <> CASE WHEN T6.ATWRT02 = '3' THEN 'DC' ELSE 'AC' END) 
                ) A
            ) DEF
            LEFT OUTER JOIN SOP_DB.DBO.TB_SIMUL_DATA_PJT_MODIFY PJT
                         ON DEF.SALE_OPP_NO           = PJT.SALE_OPP_NO
                        AND DEF.PROCESS_CODE          = PJT.PROCESS_CODE
                        AND LEFT(DEF.WORK_CNTR_CD, 3) = LEFT(PJT.WORK_CNTR_CD, 3)
            LEFT OUTER JOIN SOP_DB.DBO.TB_SIMUL_DATA_LOT_MODIFY LOT
                         ON DEF.SALE_OPP_NO           = LOT.SALE_OPP_NO
                        AND DEF.PJT_SHIP              = LOT.PJT_SHIP
                        AND DEF.SHIP_SEQ              = LOT.SHIP_SEQ
                        AND DEF.SHIP_SEQ_LOT          = LOT.SHIP_SEQ_LOT
                        AND DEF.ASSEMBLY              = LOT.ASSEMBLY
                        AND DEF.PROCESS_CODE          = LOT.PROCESS_CODE
                        AND LEFT(DEF.WORK_CNTR_CD, 3) = LEFT(LOT.WORK_CNTR_CD, 3)
        ) LIST
        INNER JOIN SOP_DB.dbo.TB_SIMUL_SUL_ASSY_PROC_LIST DATA
                ON LIST.SUL_NO       = DATA.SUL_NO
               AND LIST.ASSEMBLY     = DATA.ASSEMBLY
               AND LIST.PROCESS_CODE = DATA.PROCESS_CODE
               AND LIST.WORK_CNTR_CD = DATA.WORK_CNTR_CD;

        ---------------------------------------------------------------------
        -- (C) FJ 분할개수 계산 (프로젝트마다 재계산)
        ---------------------------------------------------------------------
        INSERT INTO #FJ_CNT_CALC(ASSEMBLY_SEQ, ASSEMBLY, FJ_CNT)
        SELECT ASSEMBLY_SEQ
             , ASSEMBLY
             , CAST(CEILING(ASSEMBLY /
                    NULLIF(MAX(CASE WHEN ATWRT02 = '3' AND PROCESS_CODE = 'UST' AND WORK_CNTR_CD = ATTRIBUTE2 THEN SIMUL_VALUE1 
                                    WHEN ATWRT02 = '1' AND PROCESS_CODE = 'INS'                               THEN SIMUL_VALUE1
                                    ELSE 0
                               END), 0)
               ) AS INT) AS FJ_CNT
        FROM #TEMP_PRODUCTION_LIST
        GROUP BY ASSEMBLY_SEQ, ASSEMBLY;

        ---------------------------------------------------------------------
        -- (D) 이번 프로젝트 결과를 @RESULT로 누적(= RETURN할 데이터에 INSERT)
        ---------------------------------------------------------------------
        INSERT INTO @RESULT
        (
            SEQ, PRODUCTION_SEQ,
            SALE_OPP_NO, LOT_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
            SUL_NO, ASSEMBLY_SEQ, ASSEMBLY, FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
            PROCESS_SEQ, PROCESS_CODE, WORK_CNTR_SEQ, WORK_CNTR_CD,
            EQUIP_SPEED, LEAD_TIME
        )
        SELECT 
               CAST(@SEQ + T.PRODUCTION_SEQ AS INT) AS SEQ
             , T.PRODUCTION_SEQ
             , T.SALE_OPP_NO
             , CONCAT(CAST(T.PJT_SHIP AS VARCHAR(3)), '-', CAST(T.SHIP_SEQ AS VARCHAR(3)), '-', CAST(T.SHIP_SEQ_LOT AS VARCHAR(3))) AS LOT_NO
             , T.PJT_SHIP
             , T.SHIP_SEQ
             , T.SHIP_SEQ_LOT
             , T.SUL_NO
             , T.ASSEMBLY_SEQ
             , T.ASSEMBLY
             , T.FJ_ASSEMBLY_SEQ
             , T.FJ_ASSEMBLY
             , T.PROCESS_SEQ
             , T.PROCESS_CODE
             , T.WORK_CNTR_SEQ
             , T.WORK_CNTR_CD
             , T.EQUIP_SPEED
             , ISNULL(CALC.LEAD_TIME, 0) AS LEAD_TIME
        FROM (
            SELECT 
--                   RANK () OVER(ORDER BY PRD_CNFM_STRT_DATE ASC) PRODUCTION_SEQ
                   ROW_NUMBER() OVER(
                       ORDER BY T1.SALE_OPP_NO, T1.PJT_SHIP, T1.SHIP_SEQ DESC, T1.SHIP_SEQ_LOT, T1.ASSEMBLY_SEQ,
                                N.CREATE_FJ_NUMBER, T1.PROCESS_SEQ, T1.WORK_CNTR_SEQ
                   ) AS PRODUCTION_SEQ
                 , T1.SALE_OPP_NO
                 , T1.PJT_SHIP
                 , T1.SHIP_SEQ
                 , T1.SHIP_SEQ_LOT
                 , T1.SUL_NO
                 , T1.ASSEMBLY_SEQ
                 , T1.ASSEMBLY
                 , N.CREATE_FJ_NUMBER AS FJ_ASSEMBLY_SEQ
                 , CAST(T1.ASSEMBLY / NULLIF(FJ.FJ_CNT, 0) AS INT) AS FJ_ASSEMBLY
                 , T1.PROCESS_SEQ
                 , T1.PROCESS_CODE
                 , T1.WORK_CNTR_SEQ
                 , T1.WORK_CNTR_CD
                 , T1.EQUIP_SPEED
                 , T1.SIMUL_PREPARE_DAYS
                 , T1.SIMUL_WORK_DAYS
                 , CASE WHEN T1.LOT_PRD_STRT_DATE IS NOT NULL AND N.CREATE_FJ_NUMBER = 1 THEN T1.LOT_PRD_STRT_DATE
                        WHEN T1.PJT_PRD_STRT_DATE IS NOT NULL AND T1.ASSEMBLY_SEQ = 1 AND N.CREATE_FJ_NUMBER = 1 THEN T1.PJT_PRD_STRT_DATE
                   END AS PRD_STRT_DATE
                 , T1.SIMUL_VALUE1
                 , T1.SIMUL_VALUE2
                 , T1.SIMUL_VALUE3
                 , T1.SIMUL_VALUE4
                 , T1.SIMUL_SEQ
                 , T1.ATWRT01
                 , T1.ATWRT02
                 , T1.ATWRT03
                 , T1.ATWRT04
                 , T1.ATWRT05
            FROM #TEMP_PRODUCTION_LIST T1
            INNER JOIN #FJ_CNT_CALC FJ
                ON T1.ASSEMBLY_SEQ = FJ.ASSEMBLY_SEQ
               AND T1.ASSEMBLY     = FJ.ASSEMBLY
            CROSS JOIN #NUMBERS N
            WHERE N.CREATE_FJ_NUMBER <= FJ.FJ_CNT
        ) T
        CROSS APPLY
        (
            SELECT CASE
                WHEN T.PROCESS_CODE IN ('CST', 'WSD')
                     THEN ROUND(((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(T.ATWRT02 AS INT) ,0 )

                WHEN T.PROCESS_CODE = 'INS' AND T.ATWRT02 = '3'
                     THEN ROUND(((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(T.ATWRT02 AS INT) ,0 )
                WHEN T.PROCESS_CODE = 'INS' AND T.ATWRT02 = '1'
                     THEN ROUND((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE1, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0) ,0 )

                WHEN T.PROCESS_CODE = 'LDS' AND T.WORK_CNTR_CD LIKE 'LDS%'
                     THEN ROUND(((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(T.ATWRT02 AS INT) ,0 )
                
                WHEN T.PROCESS_CODE = 'LDS' AND T.WORK_CNTR_CD LIKE 'RWDL%' THEN 0
                
                WHEN T.PROCESS_CODE = 'LDS' AND T.WORK_CNTR_CD LIKE 'FJYL%' THEN COALESCE(T.SIMUL_WORK_DAYS, T.SIMUL_VALUE1)

                WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'REW%' THEN CEILING(1 + (CAST(T.FJ_ASSEMBLY AS FLOAT) * 3) / 10000)
                
                WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'FJT%' THEN COALESCE(T.SIMUL_WORK_DAYS, T.SIMUL_VALUE1)
                
                WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'UST%' AND T.ATWRT02 = '3'
                     THEN ROUND((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0) ,0 )
                     
                WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'UST%' AND T.ATWRT02 = '1' THEN 0

                WHEN T.PROCESS_CODE = 'SMA'
                     THEN ROUND(((T.ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0) ,0 )
            END AS LEAD_TIME
        ) CALC;

        SET @SEQ = @SEQ + @@ROWCOUNT;
        
        FETCH NEXT FROM CUR_PJT INTO @SALE_OPP_NO;
    END

    CLOSE CUR_PJT;
    DEALLOCATE CUR_PJT;

    SELECT 'P' AS DATA_KIND
         , SEQ
         , PRODUCTION_SEQ
         , SALE_OPP_NO
         , LOT_NO
         , PJT_SHIP
         , SHIP_SEQ
         , SHIP_SEQ_LOT
         , SUL_NO
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
         , NULL
         , NULL
         , NULL
    FROM @RESULT
    WHERE LEAD_TIME > 0
    ORDER BY SEQ,PRODUCTION_SEQ;
END;

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_PROCESS_WORK_CNTR_TIMESTAMP]
AS 
BEGIN
    SET NOCOUNT ON; -- 불필요한 행 수 메시지 출력 방지
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    BEGIN TRY
		SELECT A.SIMUL_SEQ
		     , A.SIMUL_PROCESS_CD
		     , A.WORK_CNTR_CD
		     , 0 AS TIMESTAMP_POINT
		  FROM SOP_DB.dbo.TB_SIMUL_WORK_CENTER_MST A
		       INNER JOIN SOP_DB.dbo.TB_SIMUL_FAC_PC B
		               ON A.SIMUL_PROCESS_CD = B.PROCESS_CODE
		              AND B.Q_YEAR = YEAR(GETDATE())
		 ORDER BY A.SIMUL_SEQ
        
    END TRY
    BEGIN CATCH
        -- 에러 처리
        THROW 52001, 'SP_CABLE_PRODUCTION_PROCESS_WORK_CNTR_TIMESTAMP Query error', 1;
    END CATCH
END;

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

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_SELECT_CHART_DATA]
      @P_MODE            VARCHAR(10)  -- 'SIMUL' or 'TEMP'
    , @P_VERSION         VARCHAR(50)   -- Version Key
    , @P_FILTER_PJT_LIST NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE @SQL            NVARCHAR(MAX);
    DECLARE @TABLE_NAME     NVARCHAR(50);
    DECLARE @COLUMN_NAME    NVARCHAR(50);
    DECLARE @PARAM_DEF      NVARCHAR(200);
    
	DECLARE @P_PJT_LIST dbo.UDT_DATA_SALE_OPP_NO;
	
	-- 1) JSON 정규화 (NULL/공백 → NULL)
	SET @P_FILTER_PJT_LIST = NULLIF(LTRIM(RTRIM(@P_FILTER_PJT_LIST)), '');
	
	-- 2) JSON이 "있다면" 유효성 검사
	IF @P_FILTER_PJT_LIST IS NOT NULL AND ISJSON(@P_FILTER_PJT_LIST) = 0
	BEGIN
	    -- 정책 1) 에러로 막기 (추천: 조용히 무시보다 안전)
	    -- THROW 50001, 'Invalid JSON : @P_FILTER_PJT_LIST', 1;
	
	    -- 정책 2) 필터 해제 처리(전체 허용)
	    SET @P_FILTER_PJT_LIST = NULL;
	END
	
	-- 3) JSON이 유효하면 TVP 채우기
	IF @P_FILTER_PJT_LIST IS NOT NULL
	BEGIN
	    INSERT INTO @P_PJT_LIST (SALE_OPP_NO)
	    EXEC dbo.SP_CABLE_PRODUCTION_GET_JSON_TO_SALE_OPP_NO @P_FILTER_PJT_LIST;
	END
	
	
    -- 1. 동적 테이블 및 컬럼 결정
    IF @P_MODE = 'SIMUL'
    BEGIN
        SET @TABLE_NAME  = 'SOP_DB.dbo.TB_SIMUL_VERSION_DATA';
        SET @COLUMN_NAME = 'SIMUL_VERSION';
    END
    ELSE
    BEGIN
        SET @TABLE_NAME  = 'SOP_DB.dbo.TB_TEMP_VERSION_DATA';
        SET @COLUMN_NAME = 'TEMP_VERSION';
    END

    -- 2. 임시 테이블 생성 (데이터 캐싱용)
    -- 필요한 컬럼만 정의하여 인덱싱 효과 및 메모리 최적화
    CREATE TABLE #RawData (
        SEQ                 INT,
        SALE_OPP_NO         VARCHAR(50),
        PJT_SHIP            INT,
        SHIP_SEQ            INT,
        SHIP_SEQ_LOT        INT,
        SUL_NO              VARCHAR(50),
        ASSEMBLY_SEQ        INT,
        ASSEMBLY            INT,
        FJ_ASSEMBLY_SEQ     INT,
        FJ_ASSEMBLY         INT,
        PROCESS_CODE        VARCHAR(20),
        WORK_CNTR_SEQ       INT,
        WORK_CNTR_CD        VARCHAR(50),
        PRD_CNFM_STRT_DATE  DATE,
        PRD_CNFM_END_DATE   DATE,
        LEAD_TIME           INT,
        COLOR_SEQ           VARCHAR(3)
    );

    -- 3. 동적 쿼리로 데이터 추출하여 임시 테이블 적재
    SET @SQL = N'
        INSERT INTO #RawData
        SELECT A.SEQ
             , A.SALE_OPP_NO
             , A.PJT_SHIP
             , A.SHIP_SEQ
             , A.SHIP_SEQ_LOT
             , A.SUL_NO
             , A.ASSEMBLY_SEQ
             , A.ASSEMBLY
             , A.FJ_ASSEMBLY_SEQ
             , A.FJ_ASSEMBLY
             , A.PROCESS_CODE
             , A.WORK_CNTR_SEQ
             , A.WORK_CNTR_CD
             , A.PRD_CNFM_STRT_DATE
             , A.PRD_CNFM_END_DATE
             , A.LEAD_TIME
             , CAST(((DENSE_RANK() OVER(ORDER BY A.SALE_OPP_NO, A.PJT_SHIP, A.SHIP_SEQ, A.SHIP_SEQ_LOT) - 1) % 30) + 1 AS NVARCHAR(3)) AS COLOR_SEQ    
          FROM ' + @TABLE_NAME + N' A
         WHERE ' + @COLUMN_NAME + N' = @VERSION
	       AND (
                NOT EXISTS (SELECT 1 FROM @P_PJT_LIST)
	            OR A.SALE_OPP_NO IN (SELECT SALE_OPP_NO FROM @P_PJT_LIST)
               )
           AND A.LEAD_TIME > 0
         ORDER BY A.SEQ;
    ';
    
	SET @PARAM_DEF = N'
	    @VERSION    NVARCHAR(50),
	    @P_PJT_LIST dbo.UDT_DATA_SALE_OPP_NO READONLY
	';
    
    BEGIN TRY
        EXEC sp_executesql @SQL
                         , @PARAM_DEF
                         , @VERSION = @P_VERSION
                         , @P_PJT_LIST = @P_PJT_LIST;
    END TRY
    BEGIN CATCH
        THROW; -- 에러 발생 시 상위로 전파
    END CATCH

    -- 인덱스 생성 (Join 및 정렬 성능 향상)
    CREATE CLUSTERED INDEX IX_RawData_SEQ ON #RawData(SEQ);
    CREATE NONCLUSTERED INDEX IX_RawData_Sort ON #RawData(SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT);

    -------------------------------------------------------
    -- 4. 최종 JSON 생성
    -------------------------------------------------------
    /* 
       FOR JSON 결과를 서브쿼리로 감싸고 alias를 지정합니다.
       이렇게 하면 컬럼명이 [JSON_DATA]로 고정되며, 
       결과도 여러 줄이 아닌 '한 줄'로 합쳐져서 나옵니다.
    */
    SELECT 
           @P_MODE AS MODE
         , @P_VERSION AS VERSION
         , (
	        SELECT 
	            -- (1) Resources
	            (
	                SELECT 
	                       A.WORK_CNTR_CD        AS [id]
	                     , A.SIMUL_WORK_CNTR_NM  AS [name]
	                     , B.PROCESS_NAME        AS [process]
	                     , '#FEF5E7'             AS [bgColor]
	                     , '#F5B041'             AS [textColor]
	                  FROM SOP_DB.dbo.TB_SIMUL_WORK_CENTER_MST A
	                       INNER JOIN SOP_DB.dbo.TB_SIMUL_FAC_PC B
	                               ON A.SIMUL_PROCESS_CD = B.PROCESS_CODE
	                              AND B.Q_YEAR = YEAR(GETDATE())
	                 ORDER BY A.SIMUL_SEQ
	                 FOR JSON PATH
	            ) AS [resources]
	
	            -- (2) Events
	          , (
	                SELECT 
	                       A.SEQ                                                                                                                               AS [id]
	                     , A.WORK_CNTR_CD                                                                                                                      AS [resourceId]
	                     , CONVERT(VARCHAR(10), A.PRD_CNFM_STRT_DATE, 120)                                                                                     AS [startDate]
	                     , A.LEAD_TIME                                                                                                                         AS [duration]
	                     , 'D'                                                                                                                                 AS [durationUnit]
	                     , C.COLOR_CODE                                                                                                                        AS [eventColor]
	                     , CONCAT(PM.SALE_OPP_NM, ' (', A.SALE_OPP_NO , ')')                                                                                   AS [saleOppInfo]
	                     , CAST(A.PJT_SHIP AS VARCHAR) + '-' + CAST(A.SHIP_SEQ AS VARCHAR) + '-' + CAST(A.SHIP_SEQ_LOT AS VARCHAR)                             AS [lotInfo]
	                     , A.SUL_NO + ' (' + CASE WHEN CS.ATWRT02 = '3' THEN 'AC' ELSE 'DC' END + ')'                                                          AS [sulNoInfo]
	                     , CAST(A.ASSEMBLY AS VARCHAR) + ' ( ' + CAST(A.FJ_ASSEMBLY AS VARCHAR) + ' - ' + CAST(A.FJ_ASSEMBLY_SEQ AS VARCHAR) + ' )'            AS [assemblyInfo]
	                     , CONCAT(A.SALE_OPP_NO, '_', CAST(A.PJT_SHIP AS VARCHAR) + '-' + CAST(A.SHIP_SEQ AS VARCHAR) + '-' + CAST(A.SHIP_SEQ_LOT AS VARCHAR)) AS [groupLot]
	                  FROM #RawData A
	                       INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_MASTER PM
	                               ON A.SALE_OPP_NO = PM.SALE_OPP_NO	                               
	                       INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_LIST PL
	                               ON A.SALE_OPP_NO = PL.SALE_OPP_NO
	                              AND A.PJT_SHIP = PL.PJT_SHIP
	                              AND A.SHIP_SEQ = PL.SHIP_SEQ
	                              AND A.SHIP_SEQ_LOT = PL.SHIP_SEQ_LOT
	                       LEFT OUTER JOIN SOP_DB.dbo.TB_PRD_PLAN_CABLE_SPEC CS
	                                    ON PL.SUL_NO = CS.SUL_NO
	                                   AND PL.REV_SEQ = CS.REV_SEQ
	                       INNER JOIN SOP_DB.dbo.TB_SIMUL_FAC_PC F
	                               ON A.PROCESS_CODE = F.PROCESS_CODE
	                              AND F.Q_YEAR = YEAR(GETDATE())
	                       LEFT OUTER JOIN SOP_DB.dbo.TB_SIMUL_COLOR_MST C
	                                    ON A.COLOR_SEQ = C.SEQ
	                  ORDER BY A.SEQ
	                  FOR JSON PATH
	            ) AS [events]
	
	            -- (3) Dependencies
	          , (
	                SELECT 
	                       SEQ      AS [id]
	                     , FROM_SEQ AS [from]
	                     , TO_SEQ   AS [to]
	                     , 1        AS [lag]
	                     , 'day'    AS [lagUnit]
	                  FROM (
	                      SELECT 
	                             SEQ
	                           , SEQ AS FROM_SEQ
	                           , LEAD(SEQ) OVER(
	                                 PARTITION BY SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, ASSEMBLY_SEQ, FJ_ASSEMBLY_SEQ
	                                 ORDER BY WORK_CNTR_SEQ
	                             ) AS TO_SEQ
	                        FROM #RawData
	                  ) A
	                  WHERE TO_SEQ IS NOT NULL
	                  ORDER BY SEQ
	                  FOR JSON PATH
	            ) AS [dependencies]
	
	            -- (4) DisplayDate
	          , (
	                SELECT 
	                       CONVERT(VARCHAR(19), DATEADD(MONTH, -2, MIN(PRD_CNFM_STRT_DATE)), 120) AS [startDate]
	                     , CONVERT(VARCHAR(19), DATEADD(MONTH,  2, MAX(PRD_CNFM_END_DATE)), 120)  AS [endDate]
	                  FROM #RawData
	                  FOR JSON PATH
	            ) AS [displayDate]
	
	        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
	    ) AS [JSON_DATA];

    -- 임시 테이블 정리
    DROP TABLE #RawData;
END;

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_UPDATE_CHART_DATA]
      @P_MODE         VARCHAR(10)   -- 'SIMUL' Only
    , @P_VERSION      VARCHAR(50)   -- SIMUL VERSION
    , @P_UPDATE_JSON  NVARCHAR(MAX)  -- Scheduler Data to JSON
    , @P_USER_CODE    VARCHAR(50)    -- 호출한 사용자코드
AS
BEGIN
    SET NOCOUNT ON;
    

    -- 2. JSON 파싱 및 임시 테이블 적재 (데이터 타입 매핑)
    -- OPENJSON을 사용하면 JSON 배열을 테이블처럼 쓸 수 있습니다.
    SELECT * INTO #JsonUpdates
    FROM OPENJSON(@P_UPDATE_JSON)
    WITH (
        id           INT          '$.id',
        resourceId   VARCHAR(50) '$.resourceId',
        startDate    VARCHAR(50)  '$.startDate', -- 일단 문자열로 받고 나중에 변환
        endDate      VARCHAR(50)  '$.endDate'
    );

    -- SIMUL VERSION에 Scheduler Data Update
    UPDATE T
       SET WORK_CNTR_CD       = J.resourceId
         , PRD_CNFM_STRT_DATE = CAST(J.startDate AS DATETIMEOFFSET)
         , PRD_CNFM_END_DATE  = CAST(J.endDate AS DATETIMEOFFSET)
         , UPD_EMP            = @P_USER_CODE
         , UPD_DATE           = GETDATE()
      FROM SOP_DB.dbo.TB_SIMUL_VERSION_DATA T
           INNER JOIN #JsonUpdates J
              ON T.SEQ = J.id
     WHERE T.SIMUL_VERSION = @P_VERSION;

    -- 5. 임시 테이블 정리
    DROP TABLE #JsonUpdates;
END;