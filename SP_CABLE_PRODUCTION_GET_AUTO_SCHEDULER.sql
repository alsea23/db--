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
        SUL_NO          VARCHAR(50) NULL,
        REV_SEQ         VARCHAR(4)  NULL,
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
        SUL_NO        VARCHAR(50)       NULL,
        REV_SEQ       VARCHAR(4)        NULL,
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
        SEQ              INT           NOT NULL,
        SALE_OPP_NO      VARCHAR(20)   NULL,
        PJT_SHIP         INT           NULL,
        SHIP_SEQ         INT           NULL,
        SHIP_SEQ_LOT     INT           NULL,
        LOT_NO           VARCHAR(50)   NULL,
        SUL_NO           VARCHAR(50)   NULL,
        REV_SEQ          VARCHAR(4)    NULL,
        ASSEMBLY_SEQ     INT           NULL,
        ASSEMBLY         INT           NULL,
        FJ_ASSEMBLY_SEQ  INT           NULL,
        FJ_ASSEMBLY      INT           NULL,
        PROCESS_SEQ      INT           NULL,
        PROCESS_CODE     VARCHAR(20)   NULL,
        WORK_CNTR_SEQ    INT           NULL,
        WORK_CNTR_CD     VARCHAR(20)   NULL,
        EQUIP_SPEED      FLOAT         NULL,
        LEAD_TIME_DAYS   INT           NULL,
        PREPARE_DAYS     NUMERIC(2,1)  NULL,
        WORK_DAYS        NUMERIC(2,1)  NULL,
        PRD_STRT_DATE    DATE          NULL
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
        REV_SEQ               VARCHAR(4)        NULL,
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
    

END
