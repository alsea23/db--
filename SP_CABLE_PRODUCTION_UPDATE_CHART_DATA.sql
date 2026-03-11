CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_UPDATE_CHART_DATA]
      @P_MODE         VARCHAR(10)    -- 'SIMUL' Only
    , @P_VERSION      VARCHAR(50)    -- SIMUL VERSION
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
        resourceId   VARCHAR(50)  '$.resourceId',
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
END