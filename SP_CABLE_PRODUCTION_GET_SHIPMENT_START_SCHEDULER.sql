CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_SHIPMENT_START_SCHEDULER]
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

    -------------------------------------------------------------------------
    -- 1. 임시 테이블 정의 (전역 참조용)
    -------------------------------------------------------------------------

    -- [Main Timeline] 전역 마스터 타임라인 (Conflict 체크 기준)
    IF OBJECT_ID('tempdb..#WORK_CNTR_TIMESTAMP') IS NOT NULL DROP TABLE #WORK_CNTR_TIMESTAMP;
    CREATE TABLE #WORK_CNTR_TIMESTAMP (
        PROCESS_CODE    VARCHAR(20),
        WORK_CNTR_CD    VARCHAR(20),
        TS_IDX          INT,
        IS_ASSIGNED     BIT DEFAULT 0,
        SALE_OPP_NO     VARCHAR(20),
        LOT_NO          VARCHAR(50),
        PRIMARY KEY (PROCESS_CODE, WORK_CNTR_CD, TS_IDX)
    );

    -- [Source Data] 원천 데이터
    IF OBJECT_ID('tempdb..#SRC_FJ') IS NOT NULL DROP TABLE #SRC_FJ;
    CREATE TABLE #SRC_FJ (
        SEQ             INT           NOT NULL,
        SALE_OPP_NO     VARCHAR(20)   NULL,
        PJT_SHIP        INT           NULL,
        SHIP_SEQ        INT           NULL,
        SHIP_SEQ_LOT    INT           NULL,
        LOT_NO          VARCHAR(50)   NULL,
        SUL_NO          VARCHAR(50)   NULL,
        REV_SEQ         VARCHAR(4)    NULL,
        ASSEMBLY_SEQ    INT           NULL,
        ASSEMBLY        INT           NULL,
        FJ_ASSEMBLY_SEQ INT           NULL,
        FJ_ASSEMBLY     INT           NULL,
        PROCESS_SEQ     INT           NULL,
        PROCESS_CODE    VARCHAR(20)   NULL,
        WORK_CNTR_SEQ   INT           NULL,
        WORK_CNTR_CD    VARCHAR(20)   NULL,
        EQUIP_SPEED     FLOAT         NULL,
        LEAD_TIME_DAYS  INT           NULL,
        PREPARE_DAYS    NUMERIC(2,1)  NULL,
        WORK_DAYS       NUMERIC(2,1)  NULL,
        PRD_STRT_DATE   DATE          NULL
    );

    -- [Shipment Meta] 프로젝트-항차별 시작 기준점
    IF OBJECT_ID('tempdb..#SHIP_META') IS NOT NULL DROP TABLE #SHIP_META;
    CREATE TABLE #SHIP_META (
        SALE_OPP_NO     VARCHAR(20) NOT NULL,
        PJT_SHIP        INT         NOT NULL,
        SHIP_TS_POINT   INT,
        PRIMARY KEY (SALE_OPP_NO, PJT_SHIP)
    );

    -- [Legacy Logic Tables] 기존 오토 스케줄러 호환 임시 테이블
    IF OBJECT_ID('tempdb..#WC_GRP') IS NOT NULL DROP TABLE #WC_GRP;
	CREATE TABLE #WC_GRP (
	    SIMUL_PROCESS_CD VARCHAR(20) NOT NULL,
	    GRP_WORK_CNTR_CD VARCHAR(20) NOT NULL,
	    WORK_CNTR_CD     VARCHAR(20) NOT NULL,
	    CONSTRAINT PK_WC_GRP PRIMARY KEY (SIMUL_PROCESS_CD, GRP_WORK_CNTR_CD, WORK_CNTR_CD)
	);

    IF OBJECT_ID('tempdb..#ASSIGN_BLOCK_DATA') IS NOT NULL DROP TABLE #ASSIGN_BLOCK_DATA;
    CREATE TABLE #ASSIGN_BLOCK_DATA (
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

    IF OBJECT_ID('tempdb..#LOT_ORDER') IS NOT NULL DROP TABLE #LOT_ORDER;
    CREATE TABLE #LOT_ORDER (
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

    -- [Virtual Result] 가상 스케줄링 결과
    IF OBJECT_ID('tempdb..#VIRTUAL_RESULT') IS NOT NULL DROP TABLE #VIRTUAL_RESULT;
    CREATE TABLE #VIRTUAL_RESULT (
        SALE_OPP_NO     VARCHAR(20),
        PJT_SHIP        INT,
        LOT_NO          VARCHAR(50),
        PROCESS_CODE    VARCHAR(20),
        WORK_CNTR_CD    VARCHAR(20),
        V_STRT_TS       INT,
        V_END_TS        INT,
        SEQ             INT
    );

    -- [Final Result] 최종 결과
    IF OBJECT_ID('tempdb..#FINAL_ASSIGN_DATA') IS NOT NULL DROP TABLE #FINAL_ASSIGN_DATA;
    CREATE TABLE #FINAL_ASSIGN_DATA (
        SALE_OPP_NO     VARCHAR(20),
        PJT_SHIP        INT,
        LOT_NO          VARCHAR(50),
        PROCESS_CODE    VARCHAR(20),
        WORK_CNTR_CD    VARCHAR(20),
        STRT_TS         INT,
        END_TS          INT,
        IS_MANUAL       BIT DEFAULT 0,
        RESULT_STEP     INT
    );

    -------------------------------------------------------------------------
    -- 2. 초기화 단계 실행
    -------------------------------------------------------------------------
    EXEC dbo.SP_CABLE_PRODUCTION_GET_SHIPMENT_START_SCHEDULER_INIT 
        @TIMESTAMP_BASE_DATE, 
        @TB_PROCESS_WORK_CNTR_TIMESTAMP, 
        @TB_PRIORITY_PJT_LOT_LEADTIME, 
        @TB_FIXED_PJT_LOT_TIMESTAMP;

    -------------------------------------------------------------------------
    -- 3. Virtual Run 및 Merge 실행 (점검 후 구현)
    -------------------------------------------------------------------------

    -------------------------------------------------------------------------
    -- 4. 결과 반환
    -------------------------------------------------------------------------
    EXEC dbo.SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER_SELECT 
        @TIMESTAMP_BASE_DATE, @P_TEMP_VERSION, @P_USER_CODE;

END