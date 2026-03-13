CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_INSERT_CHART_DATA]
      @P_MODE          VARCHAR(10)    -- 'TEMP' 또는 'SIMUL'
    , @P_VERSION       VARCHAR(50)    -- 소스 버전 키
    , @P_SIMUL_VERSION VARCHAR(50)    -- 신규 저장될 SIMUL VERSION
    , @P_INSERT_JSON   NVARCHAR(MAX)  -- Scheduler Data (JSON)
    , @P_USER_CODE     VARCHAR(50)    -- 호출자
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL          NVARCHAR(MAX);
    DECLARE @TABLE_NAME   NVARCHAR(100);
    DECLARE @COLUMN_NAME  NVARCHAR(100);
    DECLARE @PARAM_DEF    NVARCHAR(500);

    -- 1. 모드에 따른 동적 테이블 및 컬럼 결정
    IF @P_MODE = 'SIMUL'
    BEGIN
        SET @TABLE_NAME  = 'SOP_DB.dbo.TB_SIMUL_VERSION_DATA';
        SET @COLUMN_NAME = 'SIMUL_VERSION';
    END
    ELSE IF @P_MODE = 'TEMP'
    BEGIN
        SET @TABLE_NAME  = 'SOP_DB.dbo.TB_TEMP_VERSION_DATA';
        SET @COLUMN_NAME = 'TEMP_VERSION';
    END

    -- 2. JSON 데이터 임시 테이블 적재 (이 부분은 고정 로직)
    SELECT * INTO #JsonUpdates
    FROM OPENJSON(@P_INSERT_JSON)
    WITH (
        id           INT          '$.id',
        resourceId   VARCHAR(50)  '$.resourceId',
        startDate    VARCHAR(50)  '$.startDate',
        endDate      VARCHAR(50)  '$.endDate'
    );

    -- 3. 동적 SQL 구성 (Set 기반 적재)
    -- 명세서의 핵심 개체 모델(2.6) 및 제약 규칙을 준수하여 작성
    SET @SQL = N'
        INSERT INTO SOP_DB.dbo.TB_SIMUL_VERSION_DATA (
              SIMUL_VERSION
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
              @SIMUL_VER
            , T.SEQ
            , T.SALE_OPP_NO
            , T.PJT_SHIP
            , T.SHIP_SEQ
            , T.SHIP_SEQ_LOT
            , T.SUL_NO
            , T.REV_SEQ
            , T.ASSEMBLY_SEQ
            , T.ASSEMBLY
            , T.FJ_ASSEMBLY_SEQ
            , T.FJ_ASSEMBLY
            , T.PROCESS_SEQ
            , T.PROCESS_CODE
            , T.WORK_CNTR_SEQ
            , J.resourceId                         -- JSON에서 전달된 수정 설비
            , T.EQUIP_SPEED
            , T.LEAD_TIME
            , CAST(J.startDate AS DATETIMEOFFSET)  -- JSON에서 전달된 수정 시작일
            , CAST(J.endDate AS DATETIMEOFFSET)    -- JSON에서 전달된 수정 종료일
            , @USER
            , GETDATE()
        FROM ' + @TABLE_NAME + N' T
              INNER JOIN #JsonUpdates J ON T.SEQ = J.id
        WHERE T.' + @COLUMN_NAME + N' = @VERSION;
    ';

    -- 4. 파라미터 정의 및 실행
    SET @PARAM_DEF = N'
        @SIMUL_VER  VARCHAR(50),
        @VERSION    VARCHAR(50),
        @USER       VARCHAR(50)
    ';

    BEGIN TRY
        EXEC sp_executesql @SQL
                         , @PARAM_DEF
                         , @SIMUL_VER = @P_SIMUL_VERSION
                         , @VERSION   = @P_VERSION
                         , @USER      = @P_USER_CODE;
    END TRY
    BEGIN CATCH
        -- 에러 발생 시 처리 (명세서 7번 무결성 조건 위배 등)
        THROW;
    END CATCH

    -- 5. 임시 테이블 정리
    DROP TABLE #JsonUpdates;
END