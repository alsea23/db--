CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_SIMULATION_CALC_SMA]
(
    @INPUT_DATA DBO.CABLE_PRODUCTION_BASE_DATA READONLY
  --  @OUTPUT_DATA DBO.CABLE_PROC_WORK_DATA READONLY
)
AS
BEGIN

    DECLARE @RESULT DBO.CABLE_PRODUCTION_BASE_DATA;

    DECLARE @조장_SEQ INT;
    

    DECLARE ASSEMBLY_SEQ_CURSOR CURSOR FOR SELECT DISTINCT 조장_SEQ FROM @INPUT_DATA;

    OPEN ASSEMBLY_SEQ_CURSOR;

    FETCH NEXT FROM ASSEMBLY_SEQ_CURSOR INTO @조장_SEQ;

    WHILE @@FETCH_STATUS = 0
    BEGIN
    	
	    
	    DECLARE @PREV_PROC_END DATE = (    
	       SELECT MAX(완료일) 
	         FROM @INPUT_DATA
	        WHERE 공정 = 'UST' -- 연합 공정
	          AND 조장_SEQ = @조장_SEQ
	          AND 완료일 IS NOT NULL
	    )

        DECLARE @PREV_LOT_PROC_END DATE = (
            SELECT MAX(완료일)
            FROM @RESULT
            WHERE 공정 = 'SMA' -- 외장 공정
              AND 조장_SEQ = @조장_SEQ - 1
	          AND 완료일 IS NOT NULL
           --   AND PRODUCTION_SEQ = @PRODUCTION_SEQ - 1 -- 또는 구체적 LOT 구분 컬럼 필요
        );
	    
	    DECLARE @LAST_END_DATE DATE = (    
	       SELECT MAX(완료일) 
	         FROM @RESULT
	        WHERE 공정 = 'SMA' -- 외장 공정
	          AND 조장_SEQ = @조장_SEQ
	          AND 완료일 IS NOT NULL
	    )
	    
        -- 기준일 계산 (NULL 제외 최대 날짜)
        DECLARE @BASE_DATE DATE = (
            SELECT MAX(D)
            FROM (VALUES (@PREV_PROC_END), (@PREV_LOT_PROC_END), (@LAST_END_DATE) ) AS DATES(D)
            WHERE D IS NOT NULL
        );
	    
	    INSERT INTO @RESULT
	    (
	        PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
	      , 공정_SEQ, 공정
	      , 소요기간_일 
	      , 시작일, 완료일
	    )
	    SELECT
	           T1.PRODUCTION_SEQ
	         , T1.조장_SEQ
	         , T1.조장_KM
	         , T1.FJ_SEQ
	         , T1.FJ_조장_KM
	         , T1.공정_SEQ
	         , T1.공정
	           -- 리드타임 : ((외장 조장 / 외장 일 생산량) + 준비일수) X (7일/주작업일수) X 조장수
	         , ROUND(((T1.조장_KM / NULLIF(T1.시뮬값4, 0) ) + T1.준비기간_일) * CAST(7 AS FLOAT) / NULLIF(T1.주작업일수, 0), 0) AS 소요기간_일
	           --, T2.MAX_연합_완료일,
	         , DATEADD(DAY, T1.준비기간_일, COALESCE(T1.생산시작일, @BASE_DATE) ) AS 시작일
	         , DATEADD(DAY, ISNULL(ROUND(((T1.조장_KM / NULLIF(T1.시뮬값4, 0) ) + T1.준비기간_일) * CAST(7 AS FLOAT) / NULLIF(T1.주작업일수, 0), 0), 0 ), DATEADD(DAY, T1.준비기간_일, COALESCE(T1.생산시작일, @BASE_DATE) ) ) AS 완료일
	      FROM @INPUT_DATA T1
	           INNER JOIN (
	                       SELECT 항차
	                            , 선적순서
	                            , LOT번호
	                            , 설계번호
	                            , 조장_SEQ
	                            , MAX(FJ_SEQ) AS MAX_FJ_SEQ
	                         FROM @INPUT_DATA
	                        WHERE 공정 = 'SMA' -- 연합 공정
	                          AND 조장_SEQ = @조장_SEQ
	                        GROUP BY 항차
	                               , 선적순서
	                               , LOT번호
	                               , 설계번호
	                               , 조장_SEQ 
	                   ) T2
	                ON T1.항차 = T2.항차
	               AND T1.선적순서 = T2.선적순서
	               AND T1.LOT번호 = T2.LOT번호
	               AND T1.설계번호 = T2.설계번호
	               AND T1.조장_SEQ = T2.조장_SEQ
	               AND T1.FJ_SEQ = T2.MAX_FJ_SEQ
	      WHERE T1.공정 = 'SMA' -- 외장 공정
	      ;
        FETCH NEXT FROM ASSEMBLY_SEQ_CURSOR INTO @조장_SEQ;
    END

    CLOSE ASSEMBLY_SEQ_CURSOR;
    DEALLOCATE ASSEMBLY_SEQ_CURSOR;
       
    DECLARE result_cursor CURSOR FOR
SELECT PRODUCTION_SEQ
         , 조장_KM
         , FJ_SEQ
         , FJ_조장_KM
         , 공정_SEQ
         , 소요기간_일
         , 시작일
         , 완료일
      FROM @RESULT;

DECLARE @PRODUCTION_SEQ INT, @조장_KM FLOAT, @FJ_SEQ INT, @FJ_조장_KM FLOAT, 
        @공정_SEQ INT, @소요기간_일 FLOAT, 
        @시작일 DATE, @완료일 DATE;

OPEN result_cursor;
FETCH NEXT FROM result_cursor INTO 
    @PRODUCTION_SEQ,  @조장_KM , @FJ_SEQ , @FJ_조장_KM , 
        @공정_SEQ,  @소요기간_일 , 
        @시작일,  @완료일 ;

WHILE @@FETCH_STATUS = 0
BEGIN
    -- 여기서 변수 사용 (PRINT, 처리 로직 등)
    --PRINT CAST(@PRODUCTION_SEQ AS VARCHAR(20)) + ' | ' + CAST(@공정_SEQ AS VARCHAR(10));

PRINT '외장 PRODUCTION_SEQ ' + CAST(@PRODUCTION_SEQ AS NVARCHAR(10))
      + '조장_KM ' + CAST(@조장_KM AS NVARCHAR(10))
      + ' FJ_SEQ ' + CAST(@FJ_SEQ AS NVARCHAR(10))
      + ' @소요기간_일:' +  ISNULL(CAST(@소요기간_일 AS NVARCHAR(10)), '')
      + ' 시작일: ' + ISNULL(CONVERT(NVARCHAR(8), @시작일, 112), '') + ' '
      + ' 완료일 ' + ISNULL(CONVERT(NVARCHAR(8), @완료일, 112), '');


    FETCH NEXT FROM result_cursor INTO 
    @PRODUCTION_SEQ,  @조장_KM , @FJ_SEQ , @FJ_조장_KM , 
        @공정_SEQ,  @소요기간_일 , 
        @시작일,  @완료일 ;
END;

CLOSE result_cursor;
DEALLOCATE result_cursor;
    
    
    
    
    SELECT PRODUCTION_SEQ
         , 조장_SEQ
         , 조장_KM
         , FJ_SEQ
         , FJ_조장_KM
         , 공정_SEQ
         , 선속_MPM
         , 소요기간_일
         , 준비기간_일
         , 다음공정준비_일
         , 주작업일수
         , 시뮬값1
         , 시뮬값2
         , 시뮬값3
         , 시뮬값4
         , 시작일
         , 완료일
      FROM @RESULT;
END;