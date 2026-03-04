CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_SIMULATION_CALC_LDS]
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
            @공정_SEQ INT, @공정 NVARCHAR(10),  @연피_공정_내부_SEQ INT, @선속_MPM FLOAT,
            @소요기간_일 FLOAT, @준비기간_일 FLOAT, @다음공정준비_일 FLOAT, @주작업일수 FLOAT, @생산시작일 DATE,
            @시뮬값1 FLOAT, @시뮬값2 FLOAT, @시뮬값3 FLOAT, @시뮬값4 FLOAT,
            @ATWRT02 VARCHAR(40);

    DECLARE @건조기간_일 INT = 7;
    
    -- 커서 선언
    DECLARE INPUT_CURSOR CURSOR FOR
        SELECT 
            ROWNUM,
            PRODUCTION_SEQ,
            조장_SEQ,
            조장_KM,
            FJ_SEQ,
            FJ_조장_KM,
            공정_SEQ,
            공정,
            CASE WHEN 공정 = 'LDS' AND 시뮬호기코드 LIKE 'LDS%'  THEN 2
				 WHEN 공정 = 'LDS' AND 시뮬호기코드 LIKE 'RWDL%' THEN 3
				 WHEN 공정 = 'LDS' AND 시뮬호기코드 LIKE 'FJYL%' THEN 4
				 ELSE 1
		    END AS 연피_공정_내부_SEQ,
            선속_MPM,
            소요기간_일,
            준비기간_일,
            다음공정준비_일,
            주작업일수,
            생산시작일,
            시뮬값1,
            시뮬값2,
            시뮬값3,
            시뮬값4,
            ATWRT02
        FROM @INPUT_DATA
        ORDER BY 항차,선적순서,LOT번호,조장_SEQ,FJ_SEQ,공정_SEQ, 연피_공정_내부_SEQ;

    OPEN INPUT_CURSOR;
    FETCH NEXT FROM INPUT_CURSOR INTO
        @ROWNUM, @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, 
        @공정_SEQ, @공정, @연피_공정_내부_SEQ, @선속_MPM,
        @소요기간_일, @준비기간_일, @다음공정준비_일, @주작업일수, @생산시작일,
        @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4,
        @ATWRT02;

    WHILE @@FETCH_STATUS = 0
    BEGIN
	    
		IF @공정 = 'LDS'		
		
			BEGIN
				
				
		    DECLARE @LEAD_TIME FLOAT = 0;
		    
	        IF @연피_공정_내부_SEQ = 2 --LDS 연피 공정
	        BEGIN
	             IF @ATWRT02 = '3' -- AC ((연피 조장 / 연피 일 생산량) X (7일/주작업일수) + 준비일수) X Core수 X 조장수
		              SET @LEAD_TIME = ROUND(((@FJ_조장_KM / NULLIF(@시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(@주작업일수, 0) )  + @준비기간_일) * 3, 0);
	             ELSE IF @ATWRT02 = '1'  --  DC ((연피 조장 / 연피 일 생산량) X (7일/주작업일수) + 준비일수) X 조장수
		   		      SET @LEAD_TIME = ROUND((@FJ_조장_KM / NULLIF(@시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(@주작업일수, 0) )  + @준비기간_일, 0);
	        END
	        
		    ELSE IF @연피_공정_내부_SEQ = 3 -- LDS 연피 DC RW공정 
		    BEGIN
		   		 SET @LEAD_TIME = 0;
		         SET @준비기간_일 = 0;
		    END
		    
	        ELSE IF @연피_공정_내부_SEQ = 4 AND @FJ_SEQ <> 1 --LDS 연피 DC FJ공정
	             -- 조장 연결부위 8일 Default
		   		 SET @LEAD_TIME = COALESCE(@주작업일수, @시뮬값1);
		    /*
			PRINT '연피쉬스 리드타임 계산  조장 : ' + CAST(@FJ_조장_KM AS NVARCHAR(10)) +
			      '  일생산량 : ' + CAST(@시뮬값4 AS NVARCHAR(10)) +
			      '  준비기간_일 : ' + CAST(@준비기간_일 AS NVARCHAR(10)) +
			      '  주작업일수 : ' + CAST(@주작업일수 AS NVARCHAR(10)) +
			      '  리드타임 : ' + CAST(@LEAD_TIME AS NVARCHAR(10));*/
			
	        -- 이전 공정 완료일
	        
			PRINT '연피쉬스 ROWNUM :' + CAST(@ROWNUM AS NVARCHAR(10))
			
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
	
	        SET @PREV_PROC_END = DATEADD(DAY, @건조기간_일 , @PREV_PROC_END);
	        
	        -- 기준일 계산 (NULL 제외 최대 날짜)
	        DECLARE @BASE_DATE DATE = (
	            SELECT MAX(D)
	            FROM (VALUES (@PREV_PROC_END), (@PREV_LOT_PROC_END), (@LAST_END_DATE) ) AS DATES(D)
	            WHERE D IS NOT NULL
	        );
	
	        -- 기준일 기준 준비기간 산정, 시작일과 종료일 계산
	        SET @PROC_START_DATE = DATEADD(DAY, @준비기간_일 , COALESCE(@생산시작일, @BASE_DATE));
	        SET @PROC_END_DATE = DATEADD(DAY, @LEAD_TIME, @PROC_START_DATE);
	
			PRINT '연피쉬스 ROWNUM :' + CAST(@ROWNUM AS NVARCHAR(10))
			      + ' PRODUCTION_SEQ : ' + CAST(@PRODUCTION_SEQ AS NVARCHAR(10))
			      + ' 조장_SEQ : ' + CAST(@조장_SEQ AS NVARCHAR(10))
			      + ' FJ_SEQ : ' + CAST(@FJ_SEQ AS NVARCHAR(10))
			      + ' 공정_SEQ : ' + CAST(@공정_SEQ AS NVARCHAR(10))
			      + ' @LEAD_TIME:' + CAST(@LEAD_TIME AS NVARCHAR(10))
			      + ' : ' + ISNULL(CONVERT(NVARCHAR(8), @BASE_DATE, 112), '') + ' '
			      + ' - ' + ISNULL(CONVERT(NVARCHAR(8), @PROC_START_DATE, 112), '') + ' ~ '
			      + ISNULL(CONVERT(NVARCHAR(8), @PROC_END_DATE, 112), '');
	
	 -- 결과 테이블에 삽입
	        INSERT INTO @RESULT
	        ( 
	            PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
	          , 공정_SEQ, 공정, 선속_MPM
	          , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
	          , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
	          , 시작일, 완료일
	        )
	        VALUES (
	            @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, 
                @공정_SEQ, @공정, @선속_MPM,
	            @LEAD_TIME, @준비기간_일, @다음공정준비_일, @주작업일수,
	            @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4,
	            @PROC_START_DATE, @PROC_END_DATE
	        );
        END
        
        FETCH NEXT FROM INPUT_CURSOR INTO       
            @ROWNUM, @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, 
            @공정_SEQ, @공정, @연피_공정_내부_SEQ, @선속_MPM,
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