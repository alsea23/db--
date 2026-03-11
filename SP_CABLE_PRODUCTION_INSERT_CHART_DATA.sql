
CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_INSERT_CHART_DATA]
      @P_TEMP_VERSION  VARCHAR(50)    -- 저장할 TEMP VERSION
    , @P_SIMUL_VERSION VARCHAR(50)    -- 신규 저장되는 SIMUL VERSION
    , @P_INSERT_JSON   NVARCHAR(MAX)  -- Scheduler Data to JSON
    , @P_USER_CODE     VARCHAR(50)    -- 호출한 사용자코드
AS
BEGIN
    SET NOCOUNT ON;
    

    -- 2. JSON 파싱 및 임시 테이블 적재 (데이터 타입 매핑)
    -- OPENJSON을 사용하면 JSON 배열을 테이블처럼 쓸 수 있습니다.
    SELECT * INTO #JsonUpdates
    FROM OPENJSON(@P_INSERT_JSON)
    WITH (
        id           INT          '$.id',
        resourceId   VARCHAR(50)  '$.resourceId',
        startDate    VARCHAR(50)  '$.startDate', -- 일단 문자열로 받고 나중에 변환
        endDate      VARCHAR(50)  '$.endDate'
    );

    -- SIMUL VERSION에 Scheduler Data INSERT
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
	SELECT @P_SIMUL_VERSION
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
	     , J.resourceId                         -- Scheduer Chart 데이터
	     , EQUIP_SPEED
	     , LEAD_TIME
	     , CAST(J.startDate AS DATETIMEOFFSET)  -- Scheduer Chart 데이터
	     , CAST(J.endDate AS DATETIMEOFFSET)    -- Scheduer Chart 데이터
	     , @P_USER_CODE
	     , GETDATE()
      FROM SOP_DB.dbo.TB_TEMP_VERSION_DATA T
           INNER JOIN #JsonUpdates J
              ON T.SEQ = J.id
     WHERE T.TEMP_VERSION = @P_TEMP_VERSION;

    -- 5. 임시 테이블 정리
    DROP TABLE #JsonUpdates;
END