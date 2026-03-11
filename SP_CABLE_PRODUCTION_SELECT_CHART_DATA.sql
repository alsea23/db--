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
             , CAST(((DENSE_RANK() OVER(ORDER BY A.SALE_OPP_NO) - 1) % 30) + 1 AS NVARCHAR(3)) AS COLOR_SEQ    
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
END

