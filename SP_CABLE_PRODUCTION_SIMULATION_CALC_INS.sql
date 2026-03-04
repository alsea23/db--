CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_SIMULATION_CALC_INS]
(
    @INPUT_DATA DBO.CABLE_PRODUCTION_BASE_DATA READONLY
  --  @OUTPUT_DATA DBO.CABLE_PROC_WORK_DATA READONLY
)
AS 
BEGIN

    DECLARE @RESULT DBO.CABLE_PRODUCTION_BASE_DATA;

    DECLARE @PROC_START_DATE DATE;
    DECLARE @PROC_END_DATE DATE;

    DECLARE @ROWNUM INT, @PRODUCTION_SEQ INT, @조장_SEQ INT, @조장_KM FLOAT, @FJ_SEQ INT, @FJ_조장_KM FLOAT, 
            @공정_SEQ INT, @선속_MPM FLOAT,
            @소요기간_일 FLOAT, @준비기간_일 FLOAT, @다음공정준비_일 FLOAT, @주작업일수 FLOAT, @생산시작일 DATE,
            @시뮬값1 FLOAT, @시뮬값2 FLOAT, @시뮬값3 FLOAT, @시뮬값4 FLOAT,
            @ATWRT02 VARCHAR(40);

    -- 커서 선언
    DECLARE INPUT_CURSOR CURSOR FOR
    SELECT
           ROWNUM
         , PRODUCTION_SEQ
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
         , 생산시작일
         , 시뮬값1
         , 시뮬값2
         , 시뮬값3
         , 시뮬값4
         , ATWRT02
      FROM @INPUT_DATA
     ORDER BY ROWNUM;

    OPEN INPUT_CURSOR;
    FETCH NEXT FROM INPUT_CURSOR INTO
        @ROWNUM, @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, @공정_SEQ, @선속_MPM,
        @소요기간_일, @준비기간_일, @다음공정준비_일, @주작업일수, @생산시작일,
        @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4,
        @ATWRT02;

    DECLARE @LEAD_TIME FLOAT = 0;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
	    -- 리드타임 : ((절연 조장 / 절연 일 생산량) X (7일/주작업일수) + 준비일수) X Core수 X 조장수
	    -- 코어수는 향후 파라미터화 가능할때 변수 변경 필요

		PRINT '절연 리드타임 계산  ROWNUM : ' +  CAST(@ROWNUM AS NVARCHAR(10)) +
		      ' 조장 : ' + CAST(@FJ_조장_KM AS NVARCHAR(10)) +
		      ' 일생산량 : ' + CAST(@시뮬값4 AS NVARCHAR(10)) + ', ' + CAST(@시뮬값1 AS NVARCHAR(10)) +
		      ' 준비기간_일 : ' + CAST(@준비기간_일 AS NVARCHAR(10)) +
		      ' 주작업일수 : ' + CAST(@주작업일수 AS NVARCHAR(10));
	    IF @ATWRT02 = '3'
	    	SET @LEAD_TIME = ROUND(( ISNULL( ( ISNULL(@FJ_조장_KM, 0) / NULLIF(@시뮬값4, 0) )  * ( CAST(7 AS FLOAT) / NULLIF(@주작업일수, 0) ) , 0) + ISNULL(@준비기간_일, 0) ) * 3 , 0); 
		
	    ELSE IF @ATWRT02 = '1'
	        SET @LEAD_TIME = ROUND(( ISNULL( ( ISNULL(@FJ_조장_KM, 0) / NULLIF(@시뮬값1, 0) )  * ( CAST(7 AS FLOAT) / NULLIF(@주작업일수, 0) ) , 0) + ISNULL(@준비기간_일, 0) ) , 0);
	    
		PRINT ' 리드타임 : ' + CAST(@LEAD_TIME AS NVARCHAR(10));

        DECLARE @PREV_PROC_END DATE = (
            SELECT MAX(완료일)
            FROM @INPUT_DATA
            WHERE 공정_SEQ = @공정_SEQ - 1
              AND 조장_SEQ = @조장_SEQ
              AND FJ_SEQ = @FJ_SEQ
	          AND 완료일 IS NOT NULL
              --AND 조장_KM = @조장_KM
        );

        DECLARE @PREV_LOT_PROC_END DATE = (
            SELECT MAX(완료일)
            FROM @RESULT
            WHERE 조장_SEQ = @조장_SEQ - 1
              AND 공정_SEQ = @공정_SEQ
	          AND 완료일 IS NOT NULL
           --   AND PRODUCTION_SEQ = @PRODUCTION_SEQ - 1 -- 또는 구체적 LOT 구분 컬럼 필요
        );

        DECLARE @LAST_END_DATE DATE = (
            SELECT MAX(완료일)
            FROM @RESULT
            WHERE 조장_SEQ = @조장_SEQ
              AND 공정_SEQ = @공정_SEQ
	          AND 완료일 IS NOT NULL
        );

      
        -- 기준일 계산 (NULL 제외 최대 날짜)
        DECLARE @BASE_DATE DATE = (
            SELECT MAX(D)
            FROM (VALUES (@PREV_PROC_END), (@PREV_LOT_PROC_END), (@LAST_END_DATE)  ) AS DATES(D)
            WHERE D IS NOT NULL
        );

        -- 기준일 기준 준비기간 산정, 시작일과 종료일 계산
        SET @PROC_START_DATE = DATEADD(DAY, @준비기간_일, COALESCE(@생산시작일, @BASE_DATE));
        SET @PROC_END_DATE = DATEADD(DAY, @LEAD_TIME, @PROC_START_DATE);

PRINT '절연 ROWNUM :' + CAST(@ROWNUM AS NVARCHAR(10))
      + ' PRODUCTION_SEQ : ' + CAST(@PRODUCTION_SEQ AS NVARCHAR(10))
      + ' 조장_SEQ : ' + CAST(@조장_SEQ AS NVARCHAR(10))
      + ' FJ_SEQ : ' + CAST(@FJ_SEQ AS NVARCHAR(10))
      + ' 공정_SEQ : ' + CAST(@공정_SEQ AS NVARCHAR(10))
      + ' @LEAD_TIME:' + CAST(@LEAD_TIME AS NVARCHAR(10))
      + ' : ' + ISNULL(CONVERT(NVARCHAR(8), @BASE_DATE, 112), '') + ' '
      + ' - ' + ISNULL(CONVERT(NVARCHAR(8), @PROC_START_DATE, 112), '') + ' , '
      + ISNULL(CONVERT(NVARCHAR(8), @PROC_END_DATE, 112), '') + ' , '
      + ISNULL(CONVERT(NVARCHAR(8), @PREV_PROC_END, 112), '') + ' , '
      + ISNULL(CONVERT(NVARCHAR(8), @PREV_LOT_PROC_END, 112), '') + ' , '
      + ISNULL(CONVERT(NVARCHAR(8), @LAST_END_DATE, 112), '') + ' , ';

        -- 결과 테이블에 삽입
        INSERT INTO @RESULT
   ( 
            PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
          , 공정_SEQ, 선속_MPM
          , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
          , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
          , 시작일, 완료일
        )
        VALUES (
            @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, @공정_SEQ, @선속_MPM,
            @LEAD_TIME, @준비기간_일, @다음공정준비_일, @주작업일수,
            @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4,
            @PROC_START_DATE, @PROC_END_DATE
        );

        FETCH NEXT FROM INPUT_CURSOR INTO            
            @ROWNUM, @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, @공정_SEQ, @선속_MPM,
            @소요기간_일, @준비기간_일, @다음공정준비_일, @주작업일수, @생산시작일,
            @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4,
            @ATWRT02;
    END;

    CLOSE INPUT_CURSOR;
    DEALLOCATE INPUT_CURSOR;

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