CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_CREATE_TEMP_DATA]
(
    @JSON_PARAMS NVARCHAR(MAX),
    @RESULT_FORMAT NVARCHAR(5),
    @USER_CODE NVARCHAR(20),
    @SIMUL_VERSION NVARCHAR(50)
)
AS
BEGIN
    SET NOCOUNT ON;

    PRINT 'SP_CABLE_PRODUCTION_CREATE_TEMP_DATA ' + 
           '@JSON_PARAMS : ' + @JSON_PARAMS 
           + ', RESULT_FORMAT : ' + @RESULT_FORMAT
           + ', USER_CODE : ' + @USER_CODE;
   
   
    DECLARE @PARAM_TABLE TABLE (PARAM_SALE_OPP_NO NVARCHAR(20));

    INSERT INTO @PARAM_TABLE (PARAM_SALE_OPP_NO)
    SELECT PARAM_SALE_OPP_NO
    FROM OPENJSON(@JSON_PARAMS)
    WITH (
        PARAM_SALE_OPP_NO NVARCHAR(20) '$.PARAM_SALE_OPP_NO'
    );

    DECLARE @SALE_OPP_NO NVARCHAR(20);

    DECLARE PARAM_CURSOR CURSOR FOR SELECT PARAM_SALE_OPP_NO FROM @PARAM_TABLE;

    OPEN PARAM_CURSOR;

    FETCH NEXT FROM PARAM_CURSOR INTO @SALE_OPP_NO;

    
    DECLARE @RESULT_DATA dbo.CABLE_PRODUCTION_BASE_DATA;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT '서브 프로시저 호출 시작: ' + @SALE_OPP_NO;
    
	    DECLARE @START_DATE DATE;
	     SELECT @START_DATE = PJT_STRT_DATE
	       FROM SOP_DB.dbo.TB_PRD_PLAN_MASTER
	      WHERE SALE_OPP_NO = @SALE_OPP_NO;
		
    	DECLARE @PRODUCTION_LIST dbo.CABLE_PRODUCTION_BASE_DATA;
    	
    	DELETE FROM @PRODUCTION_LIST;
    	
    	INSERT INTO @PRODUCTION_LIST
        ( 
        	ROWNUM, PRODUCTION_SEQ, 프로젝트, 항차, 선적순서, LOT번호, 설계번호
          , 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
          , 공정_SEQ, 공정, 시뮬호기_SEQ, 시뮬호기코드
          , 선속_MPM , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수, 생산시작일
          , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4, 시뮬_순서
          , ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05
        )
    	EXEC dbo.SP_CABLE_PRODUCTION_SIMULATION_BASE_DATA @SALE_OPP_NO;

		
	    -- 공정별 LOOP 시작
	    --DECLARE @PROCESS_IDX INT = 1;
	    DECLARE @PROCESS_CODE NVARCHAR(20);
	    DECLARE @PROCESS_SEQ INT;
	
	    -- LOOP 기준인 공정코드 DISTNCT로 구하기
	    DECLARE PROCESS_CURSOR CURSOR LOCAL FAST_FORWARD FOR
	    SELECT PROCESS_CODE, SEQ 
	      FROM SOP_DB.dbo.TB_SIMUL_FAC_PC
	     WHERE Q_YEAR = YEAR(GETDATE())
	     ORDER BY SEQ;
	
	    OPEN PROCESS_CURSOR;
	    FETCH NEXT FROM PROCESS_CURSOR INTO @PROCESS_CODE, @PROCESS_SEQ;
	
	    PRINT ' @PRODUCTION_LIST 들어있는 DISTINCT 공정 수 만큼 LOOP 실행 LOOP START';
	    -- @PRODUCTION_LIST 들어있는 DISTINCT 공정 수 만큼 LOOP 실행
	    WHILE @@FETCH_STATUS = 0
	    BEGIN
	
	        PRINT CAST(@PROCESS_SEQ AS NVARCHAR(10)) + ' ' + @PROCESS_CODE + ' LOOP START';
	        DECLARE @INPUT_DATA dbo.CABLE_PRODUCTION_BASE_DATA;
	        DECLARE @OUTPUT_DATA dbo.CABLE_PRODUCTION_BASE_DATA;
	
	        
    		DELETE FROM @INPUT_DATA;    	
	        DELETE FROM @OUTPUT_DATA;             
	        
	        INSERT INTO @INPUT_DATA 
	        ( 
	        	ROWNUM
	          , PRODUCTION_SEQ, 프로젝트, 항차, 선적순서, LOT번호, 설계번호
	          , 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
      		  , 공정_SEQ, 공정, 시뮬호기_SEQ, 시뮬호기코드, 선속_MPM
	          , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수, 생산시작일
	          , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
	          , 시작일, 완료일
              , ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05
	        )
	        SELECT ROW_NUMBER() OVER(ORDER BY PRODUCTION_SEQ) 
	             , PRODUCTION_SEQ, 프로젝트, 항차, 선적순서, LOT번호, 설계번호
	        	 , 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
      		     , 공정_SEQ, 공정, 시뮬호기_SEQ, 시뮬호기코드, 선속_MPM
	             , 소요기간_일, 준비기간_일, 다음공정준비_일, 주작업일수, 생산시작일
	             , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
	             , 시작일, 완료일
                 , ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05
	          FROM @PRODUCTION_LIST
	         WHERE 공정_SEQ BETWEEN @PROCESS_SEQ - 1 AND @PROCESS_SEQ;
	        
    	/****************************************************************************/
		DECLARE @ROWCOUNT INT;
		SELECT @ROWCOUNT = COUNT(*) FROM @INPUT_DATA;
		PRINT 'INPUT_DATA 현재 행수: ' + CAST(@ROWCOUNT AS NVARCHAR(10));
    	/****************************************************************************/
	
	        IF @PROCESS_CODE = 'CST' -- 연선
	        BEGIN
	            PRINT CAST(@PROCESS_SEQ AS NVARCHAR(10)) + ' ' + @PROCESS_CODE + ' CST -- 연선';
	            INSERT INTO @OUTPUT_DATA
	            (
	                PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
	              , 공정_SEQ, 선속_MPM
	              , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
	              , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
	              , 시작일, 완료일
	            )
	            EXEC SP_CABLE_PRODUCTION_SIMULATION_CALC_CST @START_DATE,  @INPUT_DATA;
	        END
	
	        ELSE IF @PROCESS_CODE = 'INS' -- 절연
	        BEGIN
	            PRINT CAST(@PROCESS_SEQ AS NVARCHAR(10)) + ' ' + @PROCESS_CODE + ' INS -- 절연';
	            INSERT INTO @OUTPUT_DATA
	            (
	                PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
	              , 공정_SEQ, 선속_MPM
	              , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
	              , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
	              , 시작일, 완료일
	            )
	            EXEC SP_CABLE_PRODUCTION_SIMULATION_CALC_INS @INPUT_DATA;
	        END
	
	        ELSE IF @PROCESS_CODE = 'WSD' -- 횡권
	        BEGIN
	            PRINT CAST(@PROCESS_SEQ AS NVARCHAR(10)) + ' ' + @PROCESS_CODE + ' WSD -- 횡권';
	            INSERT INTO @OUTPUT_DATA
	            (
	                PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
	              , 공정_SEQ, 선속_MPM
	              , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
	              , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
	              , 시작일, 완료일
	            )
	            EXEC SP_CABLE_PRODUCTION_SIMULATION_CALC_WSD  @INPUT_DATA;
	        END
	
	        
	        ELSE IF @PROCESS_CODE = 'LDS' -- 연피쉬스
	        BEGIN
	            PRINT CAST(@PROCESS_SEQ AS NVARCHAR(10)) + ' ' + @PROCESS_CODE + ' LDS -- 연피쉬스';
	            INSERT INTO @OUTPUT_DATA
	            (
	                PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
	              , 공정_SEQ, 선속_MPM
	              , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
	              , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
	              , 시작일, 완료일
	            )
	            EXEC SP_CABLE_PRODUCTION_SIMULATION_CALC_LDS  @INPUT_DATA;
	        END
	
	        ELSE IF @PROCESS_CODE = 'UST' -- 수직연합
	        BEGIN
	            PRINT CAST(@PROCESS_SEQ AS NVARCHAR(10)) + ' ' + @PROCESS_CODE + ' UST -- 수직연합';
	            INSERT INTO @OUTPUT_DATA
	            (
	                PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
	              , 공정_SEQ, 시뮬호기코드, 선속_MPM
	              , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
	              , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
	              , 시작일, 완료일
	            )
	            EXEC SP_CABLE_PRODUCTION_SIMULATION_CALC_UST  @INPUT_DATA;
	        END
	
	        ELSE IF @PROCESS_CODE = 'SMA' -- 외장
	        BEGIN
				PRINT CAST(@PROCESS_SEQ AS NVARCHAR(10)) + ' ' + @PROCESS_CODE + ' SMA -- 외장';
	            INSERT INTO @OUTPUT_DATA
	            (
	                PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
	              , 공정_SEQ, 선속_MPM
	              , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
	              , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
	              , 시작일, 완료일
	            )
	            EXEC SP_CABLE_PRODUCTION_SIMULATION_CALC_SMA  @INPUT_DATA;
	        END
	
	        UPDATE W
	           SET W.시작일     = O.시작일,
	               W.완료일     = O.완료일,
	               W.소요기간_일 = O.소요기간_일
	          FROM @PRODUCTION_LIST W
	               INNER JOIN @OUTPUT_DATA O ON W.PRODUCTION_SEQ = O.PRODUCTION_SEQ
	         WHERE W.공정 = @PROCESS_CODE;
	 
			--DELETE FROM @OUTPUT_DATA;
	        
	        PRINT CAST(@PROCESS_SEQ AS NVARCHAR(10)) + ' ' + @PROCESS_CODE + ' LOOP END';
	        FETCH NEXT FROM PROCESS_CURSOR INTO @PROCESS_CODE, @PROCESS_SEQ;
	
	        --SET @PROCESS_IDX += 1;
	
	    END
	    CLOSE PROCESS_CURSOR;
	    DEALLOCATE PROCESS_CURSOR;
    	
    	INSERT INTO @RESULT_DATA
        (
            PRODUCTION_SEQ, 프로젝트, 항차, 선적순서, LOT번호, 설계번호
          , 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
          , 공정_SEQ, 공정, 시뮬호기_SEQ, 시뮬호기코드
          , 선속_MPM, 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
          , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
          , 시작일, 완료일, 시뮬_순서
        )
        SELECT PRODUCTION_SEQ, 프로젝트, 항차, 선적순서, LOT번호, 설계번호
      	     , 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
             , 공정_SEQ, 공정, 시뮬호기_SEQ, 시뮬호기코드
             , 선속_MPM, 소요기간_일, 준비기간_일, 다음공정준비_일, 주작업일수
             , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
             , 시작일, 완료일, 시뮬_순서
          FROM @PRODUCTION_LIST
    	
    	

        PRINT '서브 프로시저 호출 완료: ' + @SALE_OPP_NO;

        FETCH NEXT FROM PARAM_CURSOR INTO @SALE_OPP_NO;
    END

    CLOSE PARAM_CURSOR;
    DEALLOCATE PARAM_CURSOR;

    
    DECLARE @TEMP_VERSION NVARCHAR(50) = @USER_CODE + '_' + FORMAT(GETDATE(), 'yyyyMMddHHmmss');
	DECLARE @VERSION_DATA_COUNT INT = 0;

   -- RESULT_DATA COUNT 저장
    SELECT @VERSION_DATA_COUNT = COUNT(1) 
      FROM SOP_DB.dbo.TB_SIMUL_VERSION_DATA
     WHERE @SIMUL_VERSION IS NOT NULL
	   AND SIMUL_VERSION = @SIMUL_VERSION
       AND SALE_OPP_NO IS NOT NULL;
   
    INSERT INTO SOP_DB.dbo.TB_TEMP_VERSION_DATA    
    SELECT @TEMP_VERSION AS TEMP_VERSION
         , @VERSION_DATA_COUNT + ROW_NUMBER() OVER(ORDER BY 프로젝트, 항차, 선적순서 DESC, LOT번호, 조장_SEQ, FJ_SEQ, 공정_SEQ, 시뮬호기_SEQ)
         , 프로젝트
         , PRODUCTION_SEQ
         , 항차
         , 선적순서
         , LOT번호
         , 설계번호
         , 조장_SEQ
         , 조장_KM
         , FJ_SEQ
         , FJ_조장_KM
         , 공정_SEQ
         , 공정
         , 시뮬호기_SEQ
         , 시뮬호기코드
         , 선속_MPM
         , 소요기간_일
         , 시뮬값1
         , 시뮬값2
         , 시뮬값3
         , 시뮬값4
         , 시작일
         , 완료일
         , @USER_CODE
         , GETDATE()
      FROM @RESULT_DATA
     UNION ALL      
    SELECT @TEMP_VERSION AS TEMP_VERSION
         , SEQ
         , SALE_OPP_NO
         , SEQ
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
         , SIMUL_VALUE1
         , SIMUL_VALUE2
         , SIMUL_VALUE3
         , SIMUL_VALUE4
         , PRD_CNFM_STRT_DATE
         , PRD_CNFM_END_DATE
         , REG_EMP
         , REG_DATE
      FROM SOP_DB.dbo.TB_SIMUL_VERSION_DATA
     WHERE @SIMUL_VERSION IS NOT NULL
	   AND SIMUL_VERSION = @SIMUL_VERSION
       AND SALE_OPP_NO IS NOT NULL
      
    
    
    IF @RESULT_FORMAT = 'GRID' -- 출력형태 표
    BEGIN
        PRINT '출력형태 : '+  @RESULT_FORMAT;
        SELECT T1.TEMP_VERSION                                                                                                           AS TEMP_VERSION
             , T1.SALE_OPP_NO                                                                                                            AS 프로젝트코드
             , T2.SALE_OPP_NM                                                                                                            AS 프로젝트
             , T2.PJT_OWNER                                                                                                              AS 담당자
             , T4.NAME_KR                                                                                                                AS 담당자명
             , T1.PJT_SHIP                                                                                                               AS 항차
             , T1.SHIP_SEQ                                                                                                               AS 선적순서
             , T1.SHIP_SEQ_LOT                                                                                                           AS LOT번호
             , T1.SUL_NO                                                                                                                 AS 설계번호
             , T1.ASSEMBLY_SEQ                                                                                                           AS 조장_SEQ
             , T1.ASSEMBLY                                                                                                               AS 조장_KM
             , T1.FJ_ASSEMBLY_SEQ                                                                                                        AS FJ_ASSEMBLY_SEQ
             , T1.FJ_ASSEMBLY                                                                                                            AS FJ_조장_KM
             , CONVERT(VARCHAR, CAST(T3.PRD_STRT_DATE AS DATE), 120)                                                                     AS 영업요청시작일
             , CONVERT(VARCHAR, CAST(T3.PRD_END_DATE AS DATE), 120)                                                                      AS 영업요청종료일
             , MIN(CONVERT(VARCHAR, T1.PRD_CNFM_STRT_DATE, 120))                                                                         AS 생산시작일
             , MAX(CONVERT(VARCHAR, T1.PRD_CNFM_END_DATE, 120))                                                                          AS 생산종료일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'CST' THEN CONVERT(VARCHAR, T1.PRD_CNFM_STRT_DATE, 120) END)                              AS 연선시작일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'CST' THEN CONVERT(VARCHAR, T1.PRD_CNFM_END_DATE, 120) END)                               AS 연선종료일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'INS' THEN CONVERT(VARCHAR, T1.PRD_CNFM_STRT_DATE, 120) END)                              AS 절연시작일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'INS' THEN CONVERT(VARCHAR, T1.PRD_CNFM_END_DATE, 120) END)                               AS 절연종료일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'LDS' THEN CONVERT(VARCHAR, T1.PRD_CNFM_STRT_DATE, 120) END)                              AS 연피쉬스시작일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'LDS' THEN CONVERT(VARCHAR, T1.PRD_CNFM_END_DATE, 120) END)                               AS 연피쉬스종료일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'WSD' THEN CONVERT(VARCHAR, T1.PRD_CNFM_STRT_DATE, 120) END)                              AS 횡권시작일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'WSD' THEN CONVERT(VARCHAR, T1.PRD_CNFM_END_DATE, 120) END)                               AS 횡권종료일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'UST' AND WORK_CNTR_CD LIKE 'REW%' THEN CONVERT(VARCHAR, T1.PRD_CNFM_STRT_DATE, 120) END) AS 수직연합RW시작일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'UST' AND WORK_CNTR_CD LIKE 'REW%' THEN CONVERT(VARCHAR, T1.PRD_CNFM_END_DATE, 120) END)  AS 수직연합RW종료일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'UST' AND WORK_CNTR_CD LIKE 'FJT%' THEN CONVERT(VARCHAR, T1.PRD_CNFM_STRT_DATE, 120) END) AS 수직연합FJ시작일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'UST' AND WORK_CNTR_CD LIKE 'FJT%' THEN CONVERT(VARCHAR, T1.PRD_CNFM_END_DATE, 120) END)  AS 수직연합FJ종료일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'UST' AND WORK_CNTR_CD LIKE 'UST%' THEN CONVERT(VARCHAR, T1.PRD_CNFM_STRT_DATE, 120) END) AS 수직연합시작일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'UST' AND WORK_CNTR_CD LIKE 'UST%' THEN CONVERT(VARCHAR, T1.PRD_CNFM_END_DATE, 120) END)  AS 수직연합종료일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'SMA' THEN CONVERT(VARCHAR, T1.PRD_CNFM_STRT_DATE, 120) END)                              AS 외장시작일
             , MAX(CASE WHEN T1.PROCESS_CODE = 'SMA' THEN CONVERT(VARCHAR, T1.PRD_CNFM_END_DATE, 120) END)                               AS 외장종료일
             , T5.MIN생산시작일
             , T5.MAX생산완료일
          FROM SOP_DB.dbo.TB_TEMP_VERSION_DATA T1
               INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_MASTER T2
			           ON T1.SALE_OPP_NO = T2.SALE_OPP_NO
               INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_LIST T3
			           ON T1.SALE_OPP_NO = T3.SALE_OPP_NO AND T1.PJT_SHIP = T3.PJT_SHIP AND T1.SHIP_SEQ = T3.SHIP_SEQ AND T1.SHIP_SEQ_LOT = T3.SHIP_SEQ_LOT
               INNER JOIN CNS_HR.QUARTZ.DBO.VW_LIVE_USER T4
                       ON T2.PJT_OWNER = T4.EMP_ID
                      AND T4.DISPLAYYN= 'Y'
                      AND T4.PRODUCTIONYN <> 'Y'
                      AND T4.CODE <> 'A002ENG'
                      AND T4.EMAIL IS NOT NULL
               INNER JOIN (SELECT TEMP_VERSION
                                , MIN(CONVERT(VARCHAR, PRD_CNFM_STRT_DATE, 120)) AS MIN생산시작일
                                , MAX(CONVERT(VARCHAR, PRD_CNFM_END_DATE, 120))  AS MAX생산완료일
                            FROM SOP_DB.dbo.TB_TEMP_VERSION_DATA
                           WHERE TEMP_VERSION = @TEMP_VERSION
                           GROUP BY TEMP_VERSION
                          ) T5
                       ON T1.TEMP_VERSION = T5.TEMP_VERSION
         WHERE T1.TEMP_VERSION = @TEMP_VERSION
         GROUP BY T1.TEMP_VERSION
                , T1.SALE_OPP_NO
                , T2.SALE_OPP_NM
                , T2.PJT_OWNER
                , T4.NAME_KR
                , T1.PJT_SHIP
                , T1.SHIP_SEQ
                , T1.SHIP_SEQ_LOT
                , T1.SUL_NO
                , T1.ASSEMBLY_SEQ
                , T1.ASSEMBLY
                , T1.FJ_ASSEMBLY_SEQ
                , T1.FJ_ASSEMBLY
                , T3.PRD_STRT_DATE
                , T3.PRD_END_DATE
                , T5.MIN생산시작일
                , T5.MAX생산완료일
    
    END
	
END;

CREATE PROCEDURE dbo.SP_CABLE_PRODUCTION_GET_FIXED_PJT_LOT_TIMESTAMP
(
      @P_VERSION            NVARCHAR(50)  -- FIX 데이터를 읽을 VersionCode
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

    SELECT
          -- ✅ 전체 데이터 전역 순번
          ROW_NUMBER() OVER
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
        , DATEDIFF(DAY, @TIMESTAMP_BASE_DATE, T1.PRD_CNFM_STRT_DATE) AS FIXED_TIMESTAMP_POINT -- ✅ BaseDate 기준 FIX Timestamp (Day offset)
    FROM SOP_DB.dbo.TB_SIMUL_VERSION_DATA T1           -- <<<< 예시 테이블(환경에 맞게 치환)
         INNER JOIN @P_FIXED_PJT_LIST F
                ON T1.SALE_OPP_NO = F.SALE_OPP_NO
    WHERE T1.SIMUL_VERSION = @P_VERSION      -- <<<< 예시 버전 컬럼(환경에 맞게 치환)
      AND T1.LEAD_TIME > 0
    ORDER BY SEQ, PRODUCTION_SEQ;

END;

CREATE PROCEDURE dbo.SP_CABLE_PRODUCTION_GET_JSON_TO_SALE_OPP_NO
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
            PARAM_SALE_OPP_NO NVARCHAR(20) '$.PARAM_SALE_OPP_NO'
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


    DECLARE @RESULT dbo.UDT_DATA_PRIORITY_PJT_LOT_LEADTIME;

    DECLARE @SALE_OPP_NO NVARCHAR(20);
    DECLARE @SEQ  INT = 0;

    /* =========================================================================
       0) 커서 밖에서 #temp 1회 생성 (프로젝트마다 TRUNCATE)
    ========================================================================= */

    -- 0-1) 생산 기본 데이터(프로젝트 1건 분)
    IF OBJECT_ID('tempdb..#TEMP_PRODUCTION_LIST') IS NOT NULL DROP TABLE #TEMP_PRODUCTION_LIST;
    CREATE TABLE #TEMP_PRODUCTION_LIST
    (
        SALE_OPP_NO          NVARCHAR(20)  NULL,
        PJT_SHIP             INT           NULL,
        SHIP_SEQ             INT           NULL,
        SHIP_SEQ_LOT         INT           NULL,
        SUL_NO               NVARCHAR(50)  NULL,
        ASSEMBLY_SEQ         INT           NULL,
        ASSEMBLY             FLOAT         NULL,
        PROCESS_SEQ          INT           NULL,
        PROCESS_CODE         NVARCHAR(20)  NULL,
        WORK_CNTR_SEQ        INT           NULL,
        WORK_CNTR_CD         NVARCHAR(20)  NULL,
        EQUIP_SPEED          FLOAT         NULL,
        LEAD_TIME            FLOAT         NULL,
        SIMUL_PREPARE_DAYS   FLOAT         NULL,
        SIMUL_WORK_DAYS      FLOAT         NULL,
        PRD_STRT_DATE        DATE          NULL,
        PJT_PRD_STRT_DATE    DATE          NULL,
        LOT_PRD_STRT_DATE    DATE          NULL,
        SIMUL_VALUE1         FLOAT         NULL,
        SIMUL_VALUE2         FLOAT         NULL,
        SIMUL_VALUE3         FLOAT         NULL,
        SIMUL_VALUE4         FLOAT         NULL,
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
        ASSEMBLY_SEQ INT   NULL,
        ASSEMBLY     FLOAT NULL,
        FJ_CNT       INT   NULL
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
    SELECT SALE_OPP_NO FROM @P_PJT_LIST;

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
            SIMUL_VALUE1, SIMUL_VALUE2, SIMUL_VALUE3, SIMUL_VALUE4, SIMUL_SEQ,
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
             , LIST.SIMUL_SEQ
             , LIST.ATWRT01
             , LIST.ATWRT02
             , LIST.ATWRT03
             , LIST.ATWRT04
             , LIST.ATWRT05
        FROM (
            -- ===== 사용자님 원본의 LIST 생성 쿼리 그대로 =====
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
                 , DEF.WORK_DAYS    AS SIMUL_WORK_DAYS
                 , DEF.PREPARE_DAYS AS SIMUL_PREPARE_DAYS
                 , DEF.PRD_STRT_DATE
                 , PJT.PRD_STRT_DATE AS PJT_PRD_STRT_DATE
                 , LOT.PRD_STRT_DATE AS LOT_PRD_STRT_DATE
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
                     , DENSE_RANK() OVER (ORDER BY PJT_SHIP, SHIP_SEQ DESC, SHIP_SEQ_LOT) AS ASSEMBLY_SEQ
                     , ASSEMBLY
                     , DENSE_RANK() OVER (ORDER BY PROCESS_SEQ) AS PROCESS_SEQ
                     , PROCESS_CODE
                     , ROW_NUMBER() OVER (PARTITION BY PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, ASSEMBLY ORDER BY SIMUL_SEQ) AS WORK_CNTR_SEQ
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
                        AND T2.WORK_CNTR_CD        = (CASE WHEN T2.PROCESS_CODE = 'UST' THEN T2.ATTRIBUTE2 ELSE T2.WORK_CNTR_CD END)
                        INNER JOIN (
                                        SELECT SIMUL_PROCESS_CD, WORK_CNTR_CD, SIMUL_SEQ, DEFAULT_YN, CABLE_CORE_FLAG
                                        FROM SOP_DB.dbo.TB_SIMUL_WORK_CENTER_MST
                                        WHERE DEFAULT_YN = 'Y'
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
                    NULLIF(MAX(CASE WHEN ATWRT02 = '3' AND PROCESS_CODE = 'UST' THEN SIMUL_VALUE1 
                                    WHEN ATWRT02 = '1' AND PROCESS_CODE = 'INS' THEN SIMUL_VALUE1
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
             , CONCAT(CAST(T.PJT_SHIP AS NVARCHAR(3)), '-', CAST(T.SHIP_SEQ AS NVARCHAR(3)), '-', CAST(T.SHIP_SEQ_LOT AS NVARCHAR(3))) AS LOT_NO
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
            SELECT ROW_NUMBER() OVER(
                       ORDER BY T1.PJT_SHIP, T1.SHIP_SEQ DESC, T1.SHIP_SEQ_LOT, T1.ASSEMBLY_SEQ,
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
                     THEN ROUND(((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(T.ATWRT02 AS INT), 0)

                WHEN T.PROCESS_CODE = 'INS' AND T.ATWRT02 = '3'
                     THEN ROUND(((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(T.ATWRT02 AS INT), 0)
                WHEN T.PROCESS_CODE = 'INS' AND T.ATWRT02 = '1'
                     THEN ROUND((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE1, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0), 0)

                WHEN T.PROCESS_CODE = 'LDS' AND T.WORK_CNTR_CD LIKE 'LDS%'
                     THEN ROUND(((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(T.ATWRT02 AS INT), 0)
                
                WHEN T.PROCESS_CODE = 'LDS' AND T.WORK_CNTR_CD LIKE 'RWDL%' THEN 0
                
                WHEN T.PROCESS_CODE = 'LDS' AND T.WORK_CNTR_CD LIKE 'FJYL%' THEN COALESCE(T.SIMUL_WORK_DAYS, T.SIMUL_VALUE1)

                WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'REW%' THEN CEILING(1 + (CAST(T.FJ_ASSEMBLY AS FLOAT) * 3) / 10000)
                
                WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'FJT%' THEN COALESCE(T.SIMUL_WORK_DAYS, T.SIMUL_VALUE1)
                
                WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'UST%' AND T.ATWRT02 = '3'
                     THEN ROUND((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0), 0)
                     
                WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'UST%' AND T.ATWRT02 = '1' THEN 0

                WHEN T.PROCESS_CODE = 'SMA'
                     THEN ROUND(((T.ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0), 0)
            END AS LEAD_TIME
        ) CALC;

        SET @SEQ = @SEQ + @@ROWCOUNT;
        
        FETCH NEXT FROM CUR_PJT INTO @SALE_OPP_NO;
    END

    CLOSE CUR_PJT;
    DEALLOCATE CUR_PJT;

    SELECT *
    FROM @RESULT
    ORDER BY SEQ;
END;

CREATE PROCEDURE dbo.SP_CABLE_PRODUCTION_PROCESS_WORK_CNTR_TIMESTAMP
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

/* =========================================================================================
   "CREATE TEMP TABLE" 프로시저는 실제로 만들지 않고, 만들 DDL을 OUTPUT으로 반환
   -> 메인 프로시저가 EXEC(sys.sp_executesql)로 실행 
========================================================================================= */

CREATE   PROCEDURE dbo.SP_CABLE_PRODUCTION_GET_TEMP_TABLE_DDL
(
    @OUTPUT_DDL NVARCHAR(MAX) OUTPUT
)
AS
BEGIN
    SET NOCOUNT ON;

    SET @OUTPUT_DDL = N'
        /* ----------------------------------------------------------------------------------------------------------
            생산일정 계산시 필요한 초기 데이터를 담기 위한 INIT 테이블
            -- 1) #TEMP_MST_PROCESS_WORK_CNTR_TIMESTAMP
            -- 2) #TEMP_DATA_PRIORITY_PJT_LOT_LEADTIME
            -- 3) #TEMP_DATA_FIXED_PJT_LOT_TIMESTAMP
            -- 4) #TEMP_DATA_P_PJT_LIST
            -- 5) #TEMP_DATA_FIXED_PJT_LIST
        ---------------------------------------------------------------------------------------------------------- */

        -- 1) #TEMP_MST_PROCESS_WORK_CNTR_TIMESTAMP
        IF OBJECT_ID(''tempdb..#TEMP_MST_PROCESS_WORK_CNTR_TIMESTAMP'') IS NOT NULL DROP TABLE #TEMP_MST_PROCESS_WORK_CNTR_TIMESTAMP;
        CREATE TABLE #TEMP_MST_PROCESS_WORK_CNTR_TIMESTAMP
        (
            SEQ              INT           NULL,
            PROCESS_CODE     NVARCHAR(20)  NULL,
            WORK_CNTR_CD     NVARCHAR(20)  NULL,
            TIMESTAMP_POINT  DECIMAL(18,6) NULL
        );

        -- 2) #TEMP_DATA_PRIORITY_PJT_LOT_LEADTIME
        IF OBJECT_ID(''tempdb..#TEMP_DATA_PRIORITY_PJT_LOT_LEADTIME'') IS NOT NULL DROP TABLE #TEMP_DATA_PRIORITY_PJT_LOT_LEADTIME;
        CREATE TABLE #TEMP_DATA_PRIORITY_PJT_LOT_LEADTIME
        (
            SEQ             INT          NULL,	
            SALE_OPP_NO     NVARCHAR(20) NULL,
            PJT_SHIP        INT          NULL,
            SHIP_SEQ        INT          NULL,
            SHIP_SEQ_LOT    INT          NULL,
            SUL_NO          NVARCHAR(50) NULL,
            ASSEMBLY_SEQ    INT          NULL,
            ASSEMBLY        FLOAT        NULL,
            FJ_ASSEMBLY_SEQ INT          NULL,
            FJ_ASSEMBLY     FLOAT        NULL,
            PROCESS_SEQ     INT          NULL,
            PROCESS_CODE    NVARCHAR(20) NULL,
            WORK_CNTR_SEQ   INT          NULL,
            WORK_CNTR_CD    NVARCHAR(20) NULL,
            EQUIP_SPEED     FLOAT        NULL,
            LEAD_TIME       FLOAT        NULL
        );

        -- 3) #TEMP_DATA_FIXED_PJT_LOT_TIMESTAMP
        IF OBJECT_ID(''tempdb..#TEMP_DATA_FIXED_PJT_LOT_TIMESTAMP'') IS NOT NULL DROP TABLE #TEMP_DATA_FIXED_PJT_LOT_TIMESTAMP;
        CREATE TABLE #TEMP_DATA_FIXED_PJT_LOT_TIMESTAMP
        (
            SEQ                   INT          NULL,	
            SALE_OPP_NO           NVARCHAR(20) NULL,
            PJT_SHIP              INT          NULL,
            SHIP_SEQ              INT          NULL,
            SHIP_SEQ_LOT          INT          NULL,
            SUL_NO                NVARCHAR(50) NULL,
            ASSEMBLY_SEQ          INT          NULL,
            ASSEMBLY              FLOAT        NULL,
            FJ_ASSEMBLY_SEQ       INT          NULL,
            FJ_ASSEMBLY           FLOAT        NULL,
            PROCESS_SEQ           INT          NULL,
            PROCESS_CODE          NVARCHAR(20) NULL,
            WORK_CNTR_SEQ         INT          NULL,
            WORK_CNTR_CD          NVARCHAR(20) NULL,
            EQUIP_SPEED           FLOAT        NULL,
            LEAD_TIME             FLOAT        NULL,
            PRD_CNFM_STRT_DATE    DATE         NULL,
            FIXED_TIMESTAMP_POINT INT          NULL
        );

        -- 4) #TEMP_DATA_P_PJT_LIST  (자동배정 프로젝트 JSON to Table)
        IF OBJECT_ID(''tempdb..#TEMP_DATA_P_PJT_LIST'') IS NOT NULL DROP TABLE #TEMP_DATA_P_PJT_LIST;
        CREATE TABLE #TEMP_DATA_P_PJT_LIST
        (
            SALE_OPP_NO NVARCHAR(20) NULL
        );

        -- 5) #TEMP_DATA_FIXED_PJT_LIST  (FIX 프로젝트 JSON to Table)
        IF OBJECT_ID(''tempdb..#TEMP_DATA_FIXED_PJT_LIST'') IS NOT NULL DROP TABLE #TEMP_DATA_FIXED_PJT_LIST;
        CREATE TABLE #TEMP_DATA_FIXED_PJT_LIST
        (
            SALE_OPP_NO NVARCHAR(20) NULL
        );



        /* ----------------------------------------------------------------------------------------------------------
           SP_CABLE_PRODUCTION_GET_PRIORITY_PJT_LOT_LEADTIME 프로시저에서 재사용할 임시테이블
            -- 1) #TEMP_SELECT_CABLE_PRODUCTION_PJT_LOT
            -- 2) #TEMP_CREATE_CABLE_PRODUCTION_FJ_NUMBERS
            -- 3) #TEMP_CREATE_CABLE_PRODUCTION_FJ_CNT_CALC
        ---------------------------------------------------------------------------------------------------------- */

        -- 1) #TEMP_SELECT_CABLE_PRODUCTION_PJT_LOT
        IF OBJECT_ID(''tempdb..#TEMP_SELECT_CABLE_PRODUCTION_PJT_LOT'') IS NOT NULL DROP TABLE #TEMP_SELECT_CABLE_PRODUCTION_PJT_LOT;
        CREATE TABLE #TEMP_SELECT_CABLE_PRODUCTION_PJT_LOT
        (
            SALE_OPP_NO          NVARCHAR(20)  NULL,
            PJT_SHIP             INT           NULL,
            SHIP_SEQ             INT           NULL,
            SHIP_SEQ_LOT         INT           NULL,
            SUL_NO               NVARCHAR(50)  NULL,
            ASSEMBLY_SEQ         INT           NULL,
            ASSEMBLY             FLOAT         NULL,        
            SIMUL_PROCESS_SEQ    INT           NULL,
            PROCESS_CODE         NVARCHAR(20)  NULL,
            SIMUL_WORK_CNTR_SEQ  INT           NULL,
            SIMUL_WORK_CNTR_CD   NVARCHAR(20)  NULL,
            EQUIP_SPEED          FLOAT         NULL,
            LEAD_TIME            FLOAT         NULL,
            SIMUL_PREPARE_DAYS   FLOAT         NULL,
            SIMUL_WORK_DAYS      FLOAT         NULL,
            PRD_STRT_DATE        DATE          NULL,
            PJT_PRD_STRT_DATE    DATE          NULL,
            LOT_PRD_STRT_DATE    DATE          NULL,
            SIMUL_VALUE1         FLOAT         NULL,
            SIMUL_VALUE2         FLOAT         NULL,
            SIMUL_VALUE3         FLOAT         NULL,
            SIMUL_VALUE4         FLOAT         NULL,
            SIMUL_SEQ            INT           NULL,
            ATWRT01              NVARCHAR(50)  NULL,
            ATWRT02              NVARCHAR(50)  NULL,
            ATWRT03              NVARCHAR(50)  NULL,
            ATWRT04              NVARCHAR(50)  NULL,
            ATWRT05              NVARCHAR(50)  NULL
        );

        -- 2) #TEMP_CREATE_CABLE_PRODUCTION_FJ_NUMBERS (1~30)
        IF OBJECT_ID(''tempdb..#TEMP_CREATE_CABLE_PRODUCTION_FJ_NUMBERS'') IS NOT NULL DROP TABLE #TEMP_CREATE_CABLE_PRODUCTION_FJ_NUMBERS;
        CREATE TABLE #TEMP_CREATE_CABLE_PRODUCTION_FJ_NUMBERS
        (
            CREATE_FJ_NUMBER INT NOT NULL PRIMARY KEY
        );

        INSERT INTO #TEMP_CREATE_CABLE_PRODUCTION_FJ_NUMBERS(CREATE_FJ_NUMBER)
        SELECT TOP (30) ROW_NUMBER() OVER (ORDER BY (SELECT NULL))
        FROM sys.all_objects;

        -- 3) #TEMP_CREATE_CABLE_PRODUCTION_FJ_CNT_CALC
        IF OBJECT_ID(''tempdb..#TEMP_CREATE_CABLE_PRODUCTION_FJ_CNT_CALC'') IS NOT NULL DROP TABLE #TEMP_CREATE_CABLE_PRODUCTION_FJ_CNT_CALC;
        CREATE TABLE #TEMP_CREATE_CABLE_PRODUCTION_FJ_CNT_CALC
        (
            ASSEMBLY_SEQ INT   NULL,
            ASSEMBLY     FLOAT NULL,
            FJ_CNT       INT   NULL
        );
        ';
END;


/* =========================================================================================
   메인 프로시저에서 사용하는 방식(예시)
========================================================================================= */
--DECLARE @ddl NVARCHAR(MAX);
--EXEC dbo.SP_CABLE_PRODUCTION_GET_TEMP_TABLE_DDL @DDL = @ddl OUTPUT;
--EXEC sys.sp_executesql @ddl;
--
--SELECT name FROM tempdb.sys.tables WHERE name LIKE '#TEMP_DATA_P_PJT_LIST%';
--
--INSERT INTO #TEMP_DATA_P_PJT_LIST(SALE_OPP_NO)
--EXEC dbo.SP_CABLE_PRODUCTION_GET_JSON_TO_SALE_OPP_NO @P_JSON_PJT_LIST;

CREATE PROCEDURE dbo.SP_CABLE_PRODUCTION_GET_TIMESTAMP_BASE_DATE
(
    @P_VERSION             NVARCHAR(20),
    @P_MODE                NVARCHAR(10),  -- 'TEMP' 또는 'VERSION'
    @P_PJT_LIST            dbo.UDT_DATA_SALE_OPP_NO READONLY,  
    @P_FIXED_PJT_LIST      dbo.UDT_DATA_SALE_OPP_NO READONLY,  
    @O_TIMESTAMP_BASE_DATE DATE OUTPUT
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

CREATE PROCEDURE dbo.SP_CABLE_PRODUCTION_PLAN_GENERATOR
    @P_VERSION             NVARCHAR(50),
    @P_MODE                NVARCHAR(10),  -- 'TEMP' 또는 'VERSION'
    @P_JSON_PJT_LIST       NVARCHAR(MAX), -- JSON
    @P_JSON_FIXED_PJT_LIST NVARCHAR(MAX)  -- JSON
AS
BEGIN
    SET NOCOUNT ON;

   
    ---------------------------------------------------------------------
    -- 자동배정 프로젝트 JSON 테이블화
    ---------------------------------------------------------------------
    DECLARE @P_PJT_LIST dbo.UDT_DATA_SALE_OPP_NO;
    INSERT INTO @P_PJT_LIST (SALE_OPP_NO)
    EXEC dbo.SP_CABLE_PRODUCTION_GET_JSON_TO_SALE_OPP_NO @P_JSON_PJT_LIST;
    PRINT '@P_PJT_LIST 생성 완료'

    ---------------------------------------------------------------------
    -- FIXED 프로젝트 JSON 테이블화 
    ---------------------------------------------------------------------
    DECLARE @P_FIXED_PJT_LIST dbo.UDT_DATA_SALE_OPP_NO;
    INSERT INTO @P_FIXED_PJT_LIST (SALE_OPP_NO)
    EXEC dbo.SP_CABLE_PRODUCTION_GET_JSON_TO_SALE_OPP_NO @P_JSON_FIXED_PJT_LIST;
    PRINT '@P_FIXED_PJT_LIST 생성 완료'

    
    -- 2. 로컬 변수 및 UDT 선언
    
    ---------------------------------------------------------------------
    -- 공정별 설비호기별 마스터 데이터, Timestamp Base 0
    ---------------------------------------------------------------------
    DECLARE @TB_PROCESS_WORK_CNTR_TIMESTAMP dbo.UDT_MST_PROCESS_WORK_CNTR_TIMESTAMP;
    INSERT INTO @TB_PROCESS_WORK_CNTR_TIMESTAMP (SEQ, PROCESS_CODE, WORK_CNTR_CD, TIMESTAMP_POINT)
    EXEC dbo.SP_CABLE_PRODUCTION_GET_PROCESS_WORK_CNTR_TIMESTAMP;
    
    PRINT 'TB_PROCESS_WORK_CNTR_TIMESTAMP 생성 완료'
    
    ---------------------------------------------------------------------
    -- 파라미터로 넘어온 모든 프로젝트 중 가장 빠른 요청일을 구하여 TIMESTAMP_BASE_DATE set
    ---------------------------------------------------------------------
    DECLARE @TIMESTAMP_BASE_DATE DATE;
    EXEC SP_CABLE_PRODUCTION_GET_TIMESTAMP_BASE_DATE @P_VERSION, @P_MODE, @P_PJT_LIST, @P_FIXED_PJT_LIST, @TIMESTAMP_BASE_DATE OUTPUT;
    
    PRINT '@TIMESTAMP_BASE_DATE :'
    PRINT @TIMESTAMP_BASE_DATE
    
    
    ---------------------------------------------------------------------
    -- 3) Priority (PJT-LOT-FJ LeadTime) 
    ---------------------------------------------------------------------
    DECLARE @TB_PRIORITY_PJT_LOT_LEADTIME dbo.UDT_DATA_PRIORITY_PJT_LOT_LEADTIME;

    INSERT INTO @TB_PRIORITY_PJT_LOT_LEADTIME
    EXEC dbo.SP_CABLE_PRODUCTION_GET_PRIORITY_PJT_LOT_LEADTIME @P_PJT_LIST;
    
    PRINT '자동배정 데이터 @TB_PRIORITY_PJT_LOT_LEADTIME 완료'

    ---------------------------------------------------------------------
    -- 4) Fixed (Fix PJT LOT Timestamp)
    ---------------------------------------------------------------------
    DECLARE @TB_FIXED_PJT_LOT_TIMESTAMP dbo.UDT_DATA_FIXED_PJT_LOT_TIMESTAMP;
    INSERT INTO @TB_FIXED_PJT_LOT_TIMESTAMP
    EXEC dbo.SP_CABLE_PRODUCTION_GET_FIXED_PJT_LOT_TIMESTAMP @P_VERSION, @P_FIXED_PJT_LIST, @TIMESTAMP_BASE_DATE;
    
    PRINT 'FIX 데이터 @TB_FIXED_PJT_LOT_TIMESTAMP 완료'
    
    
    
    
--    DECLARE @TB_PRIORITY_PJT_LOT_LEADTIME   dbo.UDT_DATA_PRIORITY_PJT_LOT_LEADTIME;
--    DECLARE @TB_FIXED_PJT_LOT_TIMESTAMP     dbo.UDT_DATA_FIXED_PJT_LOT_TIMESTAMP;

    -- 3. 데이터 수집 프로시저 호출 (가정)
    -- 각각의 프로시저에서 관련 데이터를 조회하여 UDT에 채웁니다.
    
--    INSERT INTO @TB_PRIORITY_PJT_LOT_LEADTIME   EXEC dbo.SP_CABLE_PRODUCTION_PRIORITY_PJT_LOT_LEADTIME  @P_PJT_LIST;
--    INSERT INTO @TB_FIXED_PJT_LOT_TIMESTAMP     EXEC dbo.SP_CABLE_PRODUCTION_FIXED_PJT_LOT_TIMESTAMP  @P_VERSION, @P_FIXED_PJT_LIST;
--
--    -- 3. 메인 계산 프로시저 호출
--    EXEC SP_CABLE_PRODUCTION_AUTO_SCHEDULER
--         @P_VERSION, 
--         @P_MODE,
--         @TB_PROCESS_WORK_CNTR_TIMESTAMP, 
--         @TB_PRIORITY_PJT_LOT_LEADTIME, 
--         @TB_FIXED_PJT_LOT_TIMESTAMP;

    --SELECT @TIMESTAMP_BASE_DATE;
         
   -- SELECT * FROM @P_PJT_LIST;    
   -- SELECT * FROM @P_FIXED_PJT_LIST;    
    SELECT * FROM @TB_PROCESS_WORK_CNTR_TIMESTAMP;
    SELECT * FROM @TB_PRIORITY_PJT_LOT_LEADTIME;
    SELECT * FROM @TB_FIXED_PJT_LOT_TIMESTAMP ORDER BY 1,2;
END;

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_SIMULATION_BASE_DATA]
    @SALE_OPP_NO NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    SET ANSI_WARNINGS OFF;



    DECLARE @TEMP_PRODUCTION_LIST dbo.CABLE_PRODUCTION_BASE_DATA;
    INSERT INTO @TEMP_PRODUCTION_LIST
    (
        프로젝트, 항차, 선적순서, LOT번호, 설계번호,
        조장_SEQ, 조장_KM, 공정_SEQ, 공정,
        시뮬호기_SEQ, 시뮬호기코드, 
        선속_MPM, 소요기간_일, 준비기간_일, 다음공정준비_일, 주작업일수, 생산시작일, PJT_생산시작일, LOT_생산시작일,
        시뮬값1, 시뮬값2, 시뮬값3, 시뮬값4, 시뮬_순서,
        ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05
        
    )
	SELECT LIST.SALE_OPP_NO
         , LIST.PJT_SHIP
         , LIST.SHIP_SEQ
         , LIST.SHIP_SEQ_LOT
         , LIST.SUL_NO
         , LIST.ASSEMBLY_SEQ
         , LIST.ASSEMBLY
         , LIST.SIMUL_PROCESS_SEQ
         , LIST.PROCESS_CODE
         , LIST.SIMUL_WORK_CNTR_SEQ
         , LIST.SIMUL_WORK_CNTR_CD
		 , DATA.EQUIP_SPEED
         , 0 AS 소요기간_일
         , LIST.SIMUL_PREPARE_DAYS
         , 1 AS 다음공정준비_일
         , LIST.SIMUL_WORK_DAYS
         , LIST.PRD_STRT_DATE
         , LIST.PJT_PRD_STRT_DATE
         , LIST.LOT_PRD_STRT_DATE
         , DATA.SIMUL_VALUE1
         , DATA.SIMUL_VALUE2
         , DATA.SIMUL_VALUE3
         , DATA.SIMUL_VALUE4
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
                 , DEF.SIMUL_PROCESS_SEQ
                 , DEF.PROCESS_CODE
                 , DEF.SIMUL_WORK_CNTR_SEQ
                 , COALESCE(LOT.WORK_CNTR_CD , PJT.WORK_CNTR_CD , DEF.WORK_CNTR_CD) AS SIMUL_WORK_CNTR_CD
                 , COALESCE(LOT.WORK_DAYS    , PJT.WORK_DAYS    , DEF.WORK_DAYS   ) AS SIMUL_WORK_DAYS
                 , COALESCE(LOT.PREPARE_DAYS , PJT.PREPARE_DAYS , DEF.PREPARE_DAYS) AS SIMUL_PREPARE_DAYS
                 , DEF.PRD_STRT_DATE
                 , PJT.PRD_STRT_DATE AS PJT_PRD_STRT_DATE
                 , LOT.PRD_STRT_DATE AS LOT_PRD_STRT_DATE
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
                         , DENSE_RANK() OVER (ORDER BY PJT_SHIP, SHIP_SEQ DESC, SHIP_SEQ_LOT) AS ASSEMBLY_SEQ
                         , ASSEMBLY
                         , DENSE_RANK() OVER (ORDER BY PROCESS_SEQ) AS SIMUL_PROCESS_SEQ
                         , PROCESS_CODE
                         , ROW_NUMBER() OVER (PARTITION BY  PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, ASSEMBLY ORDER BY SIMUL_SEQ) AS SIMUL_WORK_CNTR_SEQ
                         , WORK_CNTR_CD
                         , ATWRT01
                         , ATWRT02
                         , ATWRT03
                         , ATWRT04
                         , ATWRT05
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
                                 , T6.ATWRT01
                                 , T6.ATWRT02
                                 , T6.ATWRT03
                                 , T6.ATWRT04
                                 , T6.ATWRT05
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
                                          AND T2.WORK_CNTR_CD        = (CASE WHEN T2.PROCESS_CODE = 'UST' THEN T2.ATTRIBUTE2 ELSE T2.WORK_CNTR_CD END)
                                   INNER JOIN (
                                                SELECT SIMUL_PROCESS_CD, WORK_CNTR_CD, SIMUL_SEQ, DEFAULT_YN, CABLE_CORE_FLAG
                                                  FROM SOP_DB.dbo.TB_SIMUL_WORK_CENTER_MST
                                                 WHERE DEFAULT_YN = 'Y'
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
                                    OR T4.CABLE_CORE_FLAG <> CASE WHEN T6.ATWRT02 = '3' THEN 'DC' ELSE 'AC' END )
                                    -- TB_PRD_PLAN_CABLE_SPEC.ATWRT02  AC(1core)일때 'DC' 제외, DC(3core)일때 'AC' 제외
							) A
                    ) DEF
                     LEFT OUTER JOIN SOP_DB.DBO.TB_SIMUL_DATA_PJT_MODIFY PJT
                                  ON DEF.SALE_OPP_NO           = PJT.SALE_OPP_NO
                                 AND DEF.PROCESS_CODE          = PJT.PROCESS_CODE
                                 AND LEFT(DEF.WORK_CNTR_CD, 3) =  LEFT(PJT.WORK_CNTR_CD, 3)
                     LEFT OUTER JOIN SOP_DB.DBO.TB_SIMUL_DATA_LOT_MODIFY LOT
                                  ON DEF.SALE_OPP_NO   = LOT.SALE_OPP_NO
                                 AND DEF.PJT_SHIP      = LOT.PJT_SHIP
                                 AND DEF.SHIP_SEQ      = LOT.SHIP_SEQ
                                 AND DEF.SHIP_SEQ_LOT  = LOT.SHIP_SEQ_LOT
                                 AND DEF.ASSEMBLY      = LOT.ASSEMBLY
                                 AND DEF.PROCESS_CODE  = LOT.PROCESS_CODE
                                 AND LEFT(DEF.WORK_CNTR_CD, 3) =  LEFT(LOT.WORK_CNTR_CD, 3)
            ) LIST
              INNER JOIN SOP_DB.dbo.TB_SIMUL_SUL_ASSY_PROC_LIST DATA
                      ON LIST.SUL_NO             = DATA.SUL_NO
                     AND LIST.ASSEMBLY           = DATA.ASSEMBLY
                     AND LIST.PROCESS_CODE       = DATA.PROCESS_CODE
                     AND LIST.SIMUL_WORK_CNTR_CD = DATA.WORK_CNTR_CD
                 
        


    -- 분할개수 테이블
    DECLARE @FJ_CNT_CALC TABLE (
        조장_SEQ INT,
        조장_KM FLOAT,
        FJ_CNT INT
    );
    INSERT INTO @FJ_CNT_CALC(조장_SEQ, 조장_KM, FJ_CNT)
    SELECT 조장_SEQ
         , 조장_KM
         , CAST(CEILING(조장_KM / 
                        NULLIF(MAX(CASE WHEN ATWRT02 = '3' AND 공정 = 'UST' THEN 시뮬값1
                                        WHEN ATWRT02 = '1' AND 공정 = 'INS' THEN 시뮬값1
                                   END), 0 )
                        ) AS INT) AS FJ_CNT
    FROM @TEMP_PRODUCTION_LIST
    GROUP BY 조장_SEQ, 조장_KM;


    -- 동적 숫자 테이블 (1~30까지; 필요시 늘려도 됨)
    DECLARE @NUMBERS TABLE (CREATE_ROW_NUM INT);
    INSERT INTO @NUMBERS(CREATE_ROW_NUM)
    SELECT TOP (30) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS CREATE_ROW_NUM
    FROM SYS.ALL_OBJECTS;

   -- SELECT ROW_NUMBER() OVER(ORDER BY T1.항차, T1.선적순서 DESC, T1.LOT번호,  T1.조장_SEQ, T3.CREATE_ROW_NUM, T1.공정_SEQ) AS PRODUCTION_SEQ
    SELECT ROW_NUMBER() OVER(ORDER BY T1.항차, T1.선적순서 DESC, T1.LOT번호, T1.조장_SEQ, T3.CREATE_ROW_NUM, T1.공정_SEQ, T1.시뮬호기_SEQ) AS ROWNUM
         , ROW_NUMBER() OVER(ORDER BY T1.항차, T1.선적순서 DESC, T1.LOT번호, T1.조장_SEQ, T3.CREATE_ROW_NUM, T1.공정_SEQ, T1.시뮬호기_SEQ) AS PRODUCTION_SEQ
         , T1.프로젝트
         , T1.항차
         , T1.선적순서
         , T1.LOT번호
         , T1.설계번호
         , T1.조장_SEQ
         , T1.조장_KM
         , T3.CREATE_ROW_NUM AS FJ_SEQ
         , CAST(T1.조장_KM / NULLIF(T2.FJ_CNT, 0) AS INT) AS FJ_조장_KM
         , T1.공정_SEQ
         , T1.공정
         , T1.시뮬호기_SEQ
         , T1.시뮬호기코드
         , T1.선속_MPM
         , T1.소요기간_일
         , T1.준비기간_일
         , T1.다음공정준비_일
         , T1.주작업일수
         , CASE WHEN LOT_생산시작일 IS NOT NULL AND T3.CREATE_ROW_NUM = 1 THEN LOT_생산시작일
                WHEN PJT_생산시작일 IS NOT NULL AND T1.조장_SEQ = 1 AND T3.CREATE_ROW_NUM = 1 THEN PJT_생산시작일
		   END AS 생산시작일
         , T1.시뮬값1
         , T1.시뮬값2
         , T1.시뮬값3
         , T1.시뮬값4
         , T1.시뮬_순서
         , T1.ATWRT01
         , T1.ATWRT02
         , T1.ATWRT03
         , T1.ATWRT04
         , T1.ATWRT05
      FROM @TEMP_PRODUCTION_LIST T1
           INNER JOIN @FJ_CNT_CALC T2
                   ON T1.조장_SEQ = T2.조장_SEQ AND T1.조장_KM = T2.조장_KM
           CROSS JOIN @NUMBERS T3
     WHERE T3.CREATE_ROW_NUM <= T2.FJ_CNT

END;

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_SIMULATION_CALC_CST]
(
    @START_DATE DATE,
    @INPUT_DATA DBO.CABLE_PRODUCTION_BASE_DATA READONLY
  --  @OUTPUT_DATA DBO.CABLE_PROC_WORK_DATA READONLY
)
AS
BEGIN

    DECLARE @RESULT DBO.CABLE_PRODUCTION_BASE_DATA;

    INSERT INTO @RESULT 
    ( 
        ROWNUM, PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
      , 공정_SEQ, 선속_MPM
      , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수, 생산시작일
      , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
      , 시작일, 완료일
      , ATWRT02
    )
    SELECT ROWNUM, PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
         , 공정_SEQ, 선속_MPM
         , 소요기간_일, 준비기간_일, 다음공정준비_일, 주작업일수, 생산시작일
         , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
         , 시작일, 완료일
         , ATWRT02
      FROM @INPUT_DATA
	  
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
            ROWNUM,
            PRODUCTION_SEQ,
            조장_SEQ,
            조장_KM,
            FJ_SEQ,
            FJ_조장_KM,
            공정_SEQ,
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
        ORDER BY ROWNUM;

    OPEN INPUT_CURSOR;

    FETCH NEXT FROM INPUT_CURSOR INTO
        @ROWNUM, @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, @공정_SEQ, @선속_MPM,
        @소요기간_일, @준비기간_일, @다음공정준비_일, @주작업일수, @생산시작일,
        @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4,
        @ATWRT02;

    DECLARE @LEAD_TIME FLOAT;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN

	    -- 리드타임 : ((연선 조장 / 연선 일 생산량) X (7일/주작업일수) + 준비일수) X Core수 X 조장수
	    -- 코어수는 향후 파라미터화 가능할때 변수 변경 필요
	/*	PRINT '연선 리드타임 계산  조장 : ' + CAST(@FJ_조장_KM AS NVARCHAR(10)) +
		      '  일생산량 : ' + CAST(@시뮬값4 AS NVARCHAR(10)) +
		      '  준비기간_일 : ' + CAST(@준비기간_일 AS NVARCHAR(10)) +
		      '  주작업일수 : ' + CAST(@주작업일수 AS NVARCHAR(10));*/

	    IF @ATWRT02 = '3'
	    	SET @LEAD_TIME = ROUND(((@FJ_조장_KM / NULLIF(@시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(@주작업일수, 0) )  + @준비기간_일) * 3, 0);
	    ELSE IF @ATWRT02 = '1'
	        SET @LEAD_TIME = ROUND(((@FJ_조장_KM / NULLIF(@시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(@주작업일수, 0) )  + @준비기간_일) , 0);
	    
	    
        -- 이전 공정 완료일
        DECLARE @PREV_PROC_END DATE = (
            SELECT MAX(완료일)
            FROM @RESULT
            WHERE ROWNUM = @ROWNUM - 1
	          AND 완료일 IS NOT NULL
              --AND 조장_KM = @조장_KM
        );

/*
        -- 이전 LOT 동일 공정 완료일 + 다음공정준비_일
        DECLARE @PREV_LOT_PROC_END DATE = (
            SELECT MAX(DATEADD(DAY, 다음공정준비_일, 완료일))
            FROM @RESULT
            WHERE 조장_SEQ = @조장_SEQ - 1
              AND 공정_SEQ = @공정_SEQ
           --   AND PRODUCTION_SEQ = @PRODUCTION_SEQ - 1 -- 또는 구체적 LOT 구분 컬럼 필요
              AND 완료일 IS NOT NULL
        );
        -- 동일 공정에서 완료된 가장 늦은 날짜
        DECLARE @LAST_END_DATE DATE = (
            SELECT MAX(완료일)
            FROM @RESULT
            WHERE 조장_SEQ = @조장_SEQ
              AND FJ_SEQ = @FJ_SEQ
              AND 완료일 IS NOT NULL
        );*/
        -- 기준일 계산 (NULL 제외 최대 날짜)
        DECLARE @BASE_DATE DATE = (
            SELECT MAX(D)
              FROM (VALUES (@PREV_PROC_END), (@START_DATE), (@생산시작일) ) AS DATES(D)
          --  FROM (VALUES (@PREV_PROC_END), (@PREV_LOT_PROC_END), (@LAST_END_DATE), (@START_DATE)) AS DATES(D)
            WHERE D IS NOT NULL
        );

        -- 기준일 기준 준비기간 산정, 시작일과 종료일 계산
--		IF @조장_SEQ = 1 AND @FJ_SEQ = 1 
--		    SET @PROC_START_DATE = @BASE_DATE;
--		ELSE
--		    SET @PROC_START_DATE = DATEADD(DAY, @준비기간_일, @BASE_DATE );
        
        SET @PROC_START_DATE = DATEADD(DAY, @준비기간_일, COALESCE(@생산시작일, @BASE_DATE) );
        SET @PROC_END_DATE   = DATEADD(DAY, @LEAD_TIME, @PROC_START_DATE);

PRINT '연선  ROWNUM :' + CAST(@ROWNUM AS NVARCHAR(10))
      + '  ** ' + CAST(@PRODUCTION_SEQ AS NVARCHAR(10))
+ ' >> ' + CAST(@조장_SEQ AS NVARCHAR(10))
      + ' ^^ ' + CAST(@FJ_SEQ AS NVARCHAR(10))
      + ' ' + CAST(@FJ_조장_KM AS NVARCHAR(10))
      + ' @LEAD_TIME:' + CAST(@LEAD_TIME AS NVARCHAR(10))
      + ' : ' + ISNULL(CONVERT(NVARCHAR(8), @BASE_DATE, 112), '') + ' '
      + ' - ' + ISNULL(CONVERT(NVARCHAR(8), @PROC_START_DATE, 112), '') + ' , '
      + ISNULL(CONVERT(NVARCHAR(8), @PROC_END_DATE, 112), '');


        UPDATE R
           SET R.시작일 = @PROC_START_DATE,
               R.완료일 = @PROC_END_DATE,
               R.소요기간_일 = @LEAD_TIME
          FROM @RESULT R
         WHERE R.PRODUCTION_SEQ = @PRODUCTION_SEQ;
        
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

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_SIMULATION_CALC_UST]
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
            @공정_SEQ INT, @공정 NVARCHAR(10), @연합_공정_내부_SEQ INT, @시뮬호기코드 NVARCHAR(20), @선속_MPM FLOAT, 
            @소요기간_일 FLOAT, @준비기간_일 FLOAT, @다음공정준비_일 FLOAT, @주작업일수 FLOAT, @생산시작일 DATE,
            @시뮬값1 FLOAT, @시뮬값2 FLOAT, @시뮬값3 FLOAT, @시뮬값4 FLOAT,
            @ATWRT02 VARCHAR(40);;

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
            CASE WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'REW%' THEN 2
				 WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'FJT%' THEN 3
				 WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'UST%' THEN 4
				 ELSE 1
		    END AS 연합_공정_내부_SEQ,
            시뮬호기코드,
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
        ORDER BY 항차,선적순서,LOT번호,조장_SEQ,FJ_SEQ,공정_SEQ, 연합_공정_내부_SEQ;

    OPEN INPUT_CURSOR;

    FETCH NEXT FROM INPUT_CURSOR INTO
        @ROWNUM, @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, 
        @공정_SEQ, @공정, @연합_공정_내부_SEQ, @시뮬호기코드, @선속_MPM,
        @소요기간_일, @준비기간_일, @다음공정준비_일, @주작업일수, @생산시작일,
        @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4,
        @ATWRT02;

    WHILE @@FETCH_STATUS = 0
    BEGIN
	    
	    -- 리드타임 : ((연합 조장 / 연합 일 생산량) X (7일/주작업일수) + 준비일수) X 조장수
/*		PRINT '수직연합 리드타임 계산  조장 : ' + CAST(@조장_KM AS NVARCHAR(10)) +
		      '  일생산량 : ' + CAST(@시뮬값4 AS NVARCHAR(10)) +
		      '  준비기간_일 : ' + CAST(@준비기간_일 AS NVARCHAR(10)) +
		      '  주작업일수 : ' + CAST(@주작업일수 AS NVARCHAR(10));*/

	    DECLARE @LEAD_TIME FLOAT = 0;
	    IF @연합_공정_내부_SEQ = 2 -- UST 수직연합 RW공정 
	         -- 준비일수 1일 + (조장X3) / 10KM
	   		 SET @LEAD_TIME = CEILING(1 + (@FJ_조장_KM * 3) / 10000);
	    
        ELSE IF @연합_공정_내부_SEQ = 3 AND @FJ_SEQ <> 1 -- UST 수직연합 FJ공정
             -- 조장 연결부위 8일 Default
	   		 SET @LEAD_TIME = COALESCE(@주작업일수, @시뮬값1);
	    
        ELSE IF @연합_공정_내부_SEQ = 4 -- UST 수직연합 공정
             -- ((연합 조장 / 연합 일 생산량) X (7일/주작업일수) + 준비일수) X 조장수
        BEGIN
		    IF @ATWRT02 = '3'
		    	SET @LEAD_TIME = ROUND((@FJ_조장_KM / NULLIF(@시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(@주작업일수, 0) )  + @준비기간_일, 0);
		    ELSE IF @ATWRT02 = '1'
		    BEGIN
		        SET @LEAD_TIME = 0;
		        SET @준비기간_일 = 0;
		    END
        END
       
        
        -- 이전 공정 완료일
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
            FROM (VALUES (@PREV_PROC_END), (@PREV_LOT_PROC_END), (@LAST_END_DATE) ) AS DATES(D)
            WHERE D IS NOT NULL
       );

        IF @연합_공정_내부_SEQ = 3 AND @FJ_SEQ = 1
        	BEGIN
		        SET @PROC_START_DATE = NULL;
		        SET @PROC_END_DATE =  NULL;
    END
        ELSE 
	       BEGIN
	        -- 기준일 기준 준비기간 산정, 시작일과 종료일 계산
		        SET @PROC_START_DATE = DATEADD(DAY, @준비기간_일, COALESCE(@생산시작일, @BASE_DATE));
		    SET @PROC_END_DATE = DATEADD(DAY, @LEAD_TIME, @PROC_START_DATE);
	        END
        
PRINT '수직연합 ROWNUM :' + CAST(@ROWNUM AS NVARCHAR(10))
    + ' PRODUCTION_SEQ : ' + CAST(@PRODUCTION_SEQ AS NVARCHAR(10))
      + ' 조장_SEQ : ' + CAST(@조장_SEQ AS NVARCHAR(10))
      + ' FJ_SEQ : ' + CAST(@FJ_SEQ AS NVARCHAR(10))
      + ' 공정_SEQ : ' + CAST(@공정_SEQ AS NVARCHAR(10))
      + ' @LEAD_TIME:' + CAST(@LEAD_TIME AS NVARCHAR(10))
      + ' : ' + ISNULL(CONVERT(NVARCHAR(8), @BASE_DATE, 112), '') + ' '
      + ' - ' + ISNULL(CONVERT(NVARCHAR(8), @PROC_START_DATE, 112), '') + ' , '
      + ISNULL(CONVERT(NVARCHAR(8), @PROC_END_DATE, 112), '');
        -- 결과 테이블에 삽입
        INSERT INTO @RESULT
        ( 
            PRODUCTION_SEQ, 조장_SEQ, 조장_KM, FJ_SEQ, FJ_조장_KM
          , 공정_SEQ, 시뮬호기코드, 선속_MPM
          , 소요기간_일 , 준비기간_일 , 다음공정준비_일, 주작업일수
          , 시뮬값1 , 시뮬값2, 시뮬값3 , 시뮬값4
          , 시작일, 완료일
        )
        VALUES (
            @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, 
            @공정_SEQ, @시뮬호기코드, @선속_MPM,
            @LEAD_TIME, @준비기간_일, @다음공정준비_일, @주작업일수,
            @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4,
            @PROC_START_DATE, @PROC_END_DATE
        );

        FETCH NEXT FROM INPUT_CURSOR INTO       
	        @ROWNUM, @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, 
	        @공정_SEQ, @공정, @연합_공정_내부_SEQ, @시뮬호기코드, @선속_MPM, 
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
         , 시뮬호기코드
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

CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_SIMULATION_CALC_WSD]
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
            @공정_SEQ INT, @공정 NVARCHAR(10), @선속_MPM FLOAT,
            @소요기간_일 FLOAT, @준비기간_일 FLOAT, @다음공정준비_일 FLOAT, @주작업일수 FLOAT, @생산시작일 DATE,
            @시뮬값1 FLOAT, @시뮬값2 FLOAT, @시뮬값3 FLOAT, @시뮬값4 FLOAT;

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
            선속_MPM,
            소요기간_일,
            준비기간_일,
            다음공정준비_일,
            주작업일수,
            생산시작일,
            시뮬값1,
            시뮬값2,
            시뮬값3,
            시뮬값4
        FROM @INPUT_DATA
        ORDER BY ROWNUM;

    OPEN INPUT_CURSOR;
    FETCH NEXT FROM INPUT_CURSOR INTO
        @ROWNUM, @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, @공정_SEQ, @공정, @선속_MPM,
        @소요기간_일, @준비기간_일, @다음공정준비_일, @주작업일수, @생산시작일,
        @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4;

	DECLARE @LEAD_TIME FLOAT = 0;
	
    WHILE @@FETCH_STATUS = 0
    BEGIN
	    
	    -- 리드타임 : ((횡권 조장 / 횡권 일 생산량) X (7일/주작업일수) + 준비일수) X Core수 X 조장수
	    -- 코어수는 향후 파라미터화 가능할때 변수 변경 필요
		IF @공정 = 'WSD' AND NULLIF(@시뮬값4, 0) <> 0
        BEGIN
		    SET @LEAD_TIME = ROUND(( ISNULL( ( ISNULL(@FJ_조장_KM, 0) / NULLIF(@시뮬값4, 0) )  * ( CAST(7 AS FLOAT) / NULLIF(@주작업일수, 0) ) , 0) + ISNULL(@준비기간_일, 0) ) * 3 , 0);
        END
        ELSE
        BEGIN
		    SET @LEAD_TIME = 0;
        END
		    /*
			PRINT '횡권 리드타임 계산  조장 : ' + CAST(@FJ_조장_KM AS NVARCHAR(10)) +
			      '  일생산량 : ' + CAST(@시뮬값4 AS NVARCHAR(10)) +
			      '  준비기간_일 : ' + CAST(@준비기간_일 AS NVARCHAR(10)) +
			      '  주작업일수 : ' + CAST(@주작업일수 AS NVARCHAR(10)) +
			      '  리드타임 : ' + CAST(@LEAD_TIME AS NVARCHAR(10));*/
	
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
      

	PRINT '횡권 ROWNUM :' + CAST(@ROWNUM AS NVARCHAR(10))
	      + ' PRODUCTION_SEQ : ' + CAST(@PRODUCTION_SEQ AS NVARCHAR(10))
	      + ' 조장_SEQ : ' + CAST(@조장_SEQ AS NVARCHAR(10))
	      + ' FJ_SEQ : ' + CAST(@FJ_SEQ AS NVARCHAR(10))
	      + ' 공정_SEQ : ' + CAST(@공정_SEQ AS NVARCHAR(10))
	      + ' 공정 : ' + @공정 
	      + ' @LEAD_TIME: ' + CAST(@LEAD_TIME AS NVARCHAR(10)) + ' '
	 --     + ' : ' + ISNULL(CONVERT(NVARCHAR(8), @BASE_DATE, 112), '') + ' '
	  --    + ' - ' + ISNULL(CONVERT(NVARCHAR(8), @PROC_START_DATE, 112), '') + ' , '
	 --     + ISNULL(CONVERT(NVARCHAR(8), @PROC_END_DATE, 112), '') + ' , '
	      + ISNULL(CONVERT(NVARCHAR(8), @PREV_PROC_END, 112), '') + ' , '
	      + ISNULL(CONVERT(NVARCHAR(8), @PREV_LOT_PROC_END, 112), '') + ' , '
	      + ISNULL(CONVERT(NVARCHAR(8), @LAST_END_DATE, 112), '') + ' , ';
	
        -- 기준일 계산 (NULL 제외 최대 날짜)
        DECLARE @BASE_DATE DATE = (
            SELECT MAX(D)
            FROM (VALUES (@PREV_PROC_END), (@PREV_LOT_PROC_END), (@LAST_END_DATE) ) AS DATES(D)
WHERE D IS NOT NULL
        );
        -- 기준일 기준 준비기간 산정, 시작일과 종료일 계산
        SET @PROC_START_DATE = DATEADD(DAY, @준비기간_일, @BASE_DATE);
        SET @PROC_END_DATE = DATEADD(DAY, @LEAD_TIME, @PROC_START_DATE);
        --SET @PROC_START_DATE = COALESCE(@생산시작일, @BASE_DATE)
        --SET @PROC_END_DATE = @PROC_START_DATE
	
		
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
            @소요기간_일, @준비기간_일, @다음공정준비_일, @주작업일수,
            @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4,
            @PROC_START_DATE, @PROC_END_DATE
        );

	     
        FETCH NEXT FROM INPUT_CURSOR INTO            
            @ROWNUM, @PRODUCTION_SEQ, @조장_SEQ, @조장_KM, @FJ_SEQ, @FJ_조장_KM, @공정_SEQ, @공정, @선속_MPM,
            @소요기간_일, @준비기간_일, @다음공정준비_일, @주작업일수, @생산시작일,
            @시뮬값1, @시뮬값2, @시뮬값3, @시뮬값4;
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

CREATE PROCEDURE dbo.SP_CABLE_PRODUCTION_SIMULATION_INIT_DATA
    @SALE_OPP_NO NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    SET ANSI_WARNINGS OFF;



    DECLARE @TEMP_PRODUCTION_LIST dbo.CABLE_PRODUCTION_BASE_DATA;
    INSERT INTO @TEMP_PRODUCTION_LIST
    (
        프로젝트, 항차, 선적순서, LOT번호, 설계번호,
        조장_SEQ, 조장_KM, 공정_SEQ, 공정,
        시뮬호기_SEQ, 시뮬호기코드, 
        선속_MPM, 소요기간_일, 준비기간_일, 다음공정준비_일, 주작업일수, 생산시작일, PJT_생산시작일, LOT_생산시작일,
        시뮬값1, 시뮬값2, 시뮬값3, 시뮬값4, 시뮬_순서,
        ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05
        
    )
	SELECT LIST.SALE_OPP_NO
         , LIST.PJT_SHIP
         , LIST.SHIP_SEQ
         , LIST.SHIP_SEQ_LOT
         , LIST.SUL_NO
         , LIST.ASSEMBLY_SEQ
         , LIST.ASSEMBLY
         , LIST.SIMUL_PROCESS_SEQ
         , LIST.PROCESS_CODE
         , LIST.SIMUL_WORK_CNTR_SEQ
         , LIST.SIMUL_WORK_CNTR_CD
		 , DATA.EQUIP_SPEED
         , 0 AS 소요기간_일
         , LIST.SIMUL_PREPARE_DAYS
         , 1 AS 다음공정준비_일
         , LIST.SIMUL_WORK_DAYS
         , LIST.PRD_STRT_DATE
         , LIST.PJT_PRD_STRT_DATE
         , LIST.LOT_PRD_STRT_DATE
         , DATA.SIMUL_VALUE1
         , DATA.SIMUL_VALUE2
         , DATA.SIMUL_VALUE3
         , DATA.SIMUL_VALUE4
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
                 , DEF.SIMUL_PROCESS_SEQ
                 , DEF.PROCESS_CODE
                 , DEF.SIMUL_WORK_CNTR_SEQ
                 , DEF.WORK_CNTR_CD AS SIMUL_WORK_CNTR_CD
                 , DEF.WORK_DAYS    AS SIMUL_WORK_DAYS
                 , DEF.PREPARE_DAYS AS SIMUL_PREPARE_DAYS
              --   , COALESCE(LOT.WORK_CNTR_CD , PJT.WORK_CNTR_CD , DEF.WORK_CNTR_CD) AS SIMUL_WORK_CNTR_CD
              --   , COALESCE(LOT.WORK_DAYS    , PJT.WORK_DAYS    , DEF.WORK_DAYS   ) AS SIMUL_WORK_DAYS
              --   , COALESCE(LOT.PREPARE_DAYS , PJT.PREPARE_DAYS , DEF.PREPARE_DAYS) AS SIMUL_PREPARE_DAYS
                 , DEF.PRD_STRT_DATE
                 , PJT.PRD_STRT_DATE AS PJT_PRD_STRT_DATE
                 , LOT.PRD_STRT_DATE AS LOT_PRD_STRT_DATE
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
                         , DENSE_RANK() OVER (ORDER BY PJT_SHIP, SHIP_SEQ DESC, SHIP_SEQ_LOT) AS ASSEMBLY_SEQ
                         , ASSEMBLY
                         , DENSE_RANK() OVER (ORDER BY PROCESS_SEQ) AS SIMUL_PROCESS_SEQ
                         , PROCESS_CODE
                         , ROW_NUMBER() OVER (PARTITION BY  PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, ASSEMBLY ORDER BY SIMUL_SEQ) AS SIMUL_WORK_CNTR_SEQ
                         , WORK_CNTR_CD
                         , ATWRT01
                         , ATWRT02
                         , ATWRT03
                         , ATWRT04
                         , ATWRT05
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
                                 , T6.ATWRT01
                                 , T6.ATWRT02
                                 , T6.ATWRT03
                                 , T6.ATWRT04
                                 , T6.ATWRT05
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
                                          AND T2.WORK_CNTR_CD        = (CASE WHEN T2.PROCESS_CODE = 'UST' THEN T2.ATTRIBUTE2 ELSE T2.WORK_CNTR_CD END)
                                   INNER JOIN (
                                                SELECT SIMUL_PROCESS_CD, WORK_CNTR_CD, SIMUL_SEQ, DEFAULT_YN, CABLE_CORE_FLAG
                                                  FROM SOP_DB.dbo.TB_SIMUL_WORK_CENTER_MST
                                                 WHERE DEFAULT_YN = 'Y'
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
                                    OR T4.CABLE_CORE_FLAG <> CASE WHEN T6.ATWRT02 = '3' THEN 'DC' ELSE 'AC' END )
                                    -- TB_PRD_PLAN_CABLE_SPEC.ATWRT02  AC(1core)일때 'DC' 제외, DC(3core)일때 'AC' 제외
							) A
                    ) DEF
                     LEFT OUTER JOIN SOP_DB.DBO.TB_SIMUL_DATA_PJT_MODIFY PJT
                                  ON DEF.SALE_OPP_NO           = PJT.SALE_OPP_NO
                                 AND DEF.PROCESS_CODE          = PJT.PROCESS_CODE
                                 AND LEFT(DEF.WORK_CNTR_CD, 3) =  LEFT(PJT.WORK_CNTR_CD, 3)
                     LEFT OUTER JOIN SOP_DB.DBO.TB_SIMUL_DATA_LOT_MODIFY LOT
                                  ON DEF.SALE_OPP_NO   = LOT.SALE_OPP_NO
                                 AND DEF.PJT_SHIP      = LOT.PJT_SHIP
                                 AND DEF.SHIP_SEQ      = LOT.SHIP_SEQ
                                 AND DEF.SHIP_SEQ_LOT  = LOT.SHIP_SEQ_LOT
                                 AND DEF.ASSEMBLY      = LOT.ASSEMBLY
                                 AND DEF.PROCESS_CODE  = LOT.PROCESS_CODE
                                 AND LEFT(DEF.WORK_CNTR_CD, 3) =  LEFT(LOT.WORK_CNTR_CD, 3)
            ) LIST
              INNER JOIN SOP_DB.dbo.TB_SIMUL_SUL_ASSY_PROC_LIST DATA
                      ON LIST.SUL_NO             = DATA.SUL_NO
                     AND LIST.ASSEMBLY           = DATA.ASSEMBLY
                     AND LIST.PROCESS_CODE       = DATA.PROCESS_CODE
                     AND LIST.SIMUL_WORK_CNTR_CD = DATA.WORK_CNTR_CD
                 
        


    -- 분할개수 테이블
    DECLARE @FJ_CNT_CALC TABLE (
        조장_SEQ INT,
        조장_KM FLOAT,
        FJ_CNT INT
    );
    INSERT INTO @FJ_CNT_CALC(조장_SEQ, 조장_KM, FJ_CNT)
    SELECT 조장_SEQ
         , 조장_KM
         , CAST(CEILING(조장_KM / 
                        NULLIF(MAX(CASE WHEN ATWRT02 = '3' AND 공정 = 'UST' THEN 시뮬값1
                                        WHEN ATWRT02 = '1' AND 공정 = 'INS' THEN 시뮬값1
                                   END), 0 )
                        ) AS INT) AS FJ_CNT
    FROM @TEMP_PRODUCTION_LIST
    GROUP BY 조장_SEQ, 조장_KM;


    -- 동적 숫자 테이블 (1~30까지; 필요시 늘려도 됨)
    DECLARE @NUMBERS TABLE (CREATE_ROW_NUM INT);
    INSERT INTO @NUMBERS(CREATE_ROW_NUM)
    SELECT TOP (30) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS CREATE_ROW_NUM
    FROM SYS.ALL_OBJECTS;

    SELECT T.*
         , ISNULL(CALC.LEAD_TIME, 0) AS LEAD_TIME
      FROM (
            SELECT ROW_NUMBER() OVER(ORDER BY T1.항차, T1.선적순서 DESC, T1.LOT번호, T1.조장_SEQ, T3.CREATE_ROW_NUM, T1.공정_SEQ, T1.시뮬호기_SEQ) AS ROWNUM
                 , ROW_NUMBER() OVER(ORDER BY T1.항차, T1.선적순서 DESC, T1.LOT번호, T1.조장_SEQ, T3.CREATE_ROW_NUM, T1.공정_SEQ, T1.시뮬호기_SEQ) AS PRODUCTION_SEQ
                 , T1.프로젝트
                 , T1.항차
                 , T1.선적순서
                 , T1.LOT번호
                 , T1.설계번호
                 , T1.조장_SEQ
                 , T1.조장_KM
                 , T3.CREATE_ROW_NUM AS FJ_SEQ
                 , CAST(T1.조장_KM / NULLIF(T2.FJ_CNT, 0) AS INT) AS FJ_조장_KM
                 , T1.공정_SEQ
                 , T1.공정
                 , T1.시뮬호기_SEQ
                 , T1.시뮬호기코드
                 , T1.선속_MPM
                 , T1.소요기간_일
                 , T1.준비기간_일
                 , T1.다음공정준비_일
                 , T1.주작업일수
                 , CASE WHEN LOT_생산시작일 IS NOT NULL AND T3.CREATE_ROW_NUM = 1 THEN LOT_생산시작일
                        WHEN PJT_생산시작일 IS NOT NULL AND T1.조장_SEQ = 1 AND T3.CREATE_ROW_NUM = 1 THEN PJT_생산시작일
                   END AS 생산시작일
                 , T1.시뮬값1
                 , T1.시뮬값2
                 , T1.시뮬값3
                 , T1.시뮬값4
                 , T1.시뮬_순서
                 , T1.ATWRT01
                 , T1.ATWRT02
                 , T1.ATWRT03
                 , T1.ATWRT04
                 , T1.ATWRT05
              FROM @TEMP_PRODUCTION_LIST T1
                   INNER JOIN @FJ_CNT_CALC T2
                           ON T1.조장_SEQ = T2.조장_SEQ AND T1.조장_KM = T2.조장_KM
                   CROSS JOIN @NUMBERS T3
             WHERE T3.CREATE_ROW_NUM <= T2.FJ_CNT
          ) T
            CROSS APPLY (
                -- LEAD TIME 수식
                SELECT CASE
                    -- 연선,횡권
                    WHEN 공정 IN ('CST', 'WSD')                                  THEN ROUND(((FJ_조장_KM / NULLIF(시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + ISNULL(준비기간_일, 0) ) * CAST(ATWRT02 AS INT), 0)
                    -- 절연
                    WHEN 공정 = 'INS' AND ATWRT02 = '3'                          THEN ROUND(((FJ_조장_KM / NULLIF(시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + ISNULL(준비기간_일, 0) ) * CAST(ATWRT02 AS INT), 0)
                    WHEN 공정 = 'INS' AND ATWRT02 = '1'                          THEN ROUND((FJ_조장_KM / NULLIF(시뮬값1, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + ISNULL(준비기간_일, 0) , 0)
                    -- 연피쉬스
                    WHEN 공정 = 'LDS' AND 시뮬호기코드 LIKE 'LDS%'                   THEN ROUND(((FJ_조장_KM / NULLIF(시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + ISNULL(준비기간_일, 0) ) * CAST(ATWRT02 AS INT), 0)
                    WHEN 공정 = 'LDS' AND 시뮬호기코드 LIKE 'RWDL%'                  THEN 0
                    WHEN 공정 = 'LDS' AND 시뮬호기코드 LIKE 'FJYL%'                  THEN COALESCE(주작업일수, 시뮬값1)
                    -- 연합
                    WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'REW%'                   THEN CEILING(1 + (CAST(FJ_조장_KM AS FLOAT) * 3) / 10000)
                    WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'FJT%'                   THEN COALESCE(주작업일수, 시뮬값1)
                    WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'UST%' AND ATWRT02 = '3' THEN ROUND((FJ_조장_KM / NULLIF(시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + ISNULL(준비기간_일, 0) , 0)
                    WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'UST%' AND ATWRT02 = '1' THEN 0
                    -- 외장
                    WHEN 공정 = 'SMA'                                            THEN ROUND(((조장_KM / NULLIF(시뮬값4, 0) ) + ISNULL(준비기간_일, 0) ) * CAST(7 AS FLOAT) / NULLIF(주작업일수, 0), 0)
                END AS LEAD_TIME
            ) CALC

END;

CREATE PROCEDURE dbo.SP_CABLE_PRODUCTION_SIMULATION_INIT_DATA_2
    @SALE_OPP_NO NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    SET ANSI_WARNINGS OFF;



    DECLARE @TEMP_PRODUCTION_LIST dbo.CABLE_PRODUCTION_BASE_DATA;
    INSERT INTO @TEMP_PRODUCTION_LIST
    (
        프로젝트, 항차, 선적순서, LOT번호, 설계번호,
        조장_SEQ, 조장_KM, 공정_SEQ, 공정,
        시뮬호기_SEQ, 시뮬호기코드, 
        선속_MPM, 소요기간_일, 준비기간_일, 다음공정준비_일, 주작업일수, 생산시작일, PJT_생산시작일, LOT_생산시작일,
        시뮬값1, 시뮬값2, 시뮬값3, 시뮬값4, 시뮬_순서,
        ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05
        
    )---
	SELECT LIST.SALE_OPP_NO
         , LIST.PJT_SHIP
         , LIST.SHIP_SEQ
         , LIST.SHIP_SEQ_LOT
         , LIST.SUL_NO
         , LIST.ASSEMBLY_SEQ
         , LIST.ASSEMBLY
         , LIST.SIMUL_PROCESS_SEQ
         , LIST.PROCESS_CODE
         , LIST.SIMUL_WORK_CNTR_SEQ
         , LIST.SIMUL_WORK_CNTR_CD
		 , DATA.EQUIP_SPEED
         , 0 AS 소요기간_일
         , LIST.SIMUL_PREPARE_DAYS
         , 1 AS 다음공정준비_일
         , LIST.SIMUL_WORK_DAYS
         , LIST.PRD_STRT_DATE
         , LIST.PJT_PRD_STRT_DATE
         , LIST.LOT_PRD_STRT_DATE
         , DATA.SIMUL_VALUE1
         , DATA.SIMUL_VALUE2
         , DATA.SIMUL_VALUE3
         , DATA.SIMUL_VALUE4
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
                 , DEF.SIMUL_PROCESS_SEQ
                 , DEF.PROCESS_CODE
                 , DEF.SIMUL_WORK_CNTR_SEQ
                 , DEF.WORK_CNTR_CD AS SIMUL_WORK_CNTR_CD
                 , DEF.WORK_DAYS    AS SIMUL_WORK_DAYS
                 , DEF.PREPARE_DAYS AS SIMUL_PREPARE_DAYS
              --   , COALESCE(LOT.WORK_CNTR_CD , PJT.WORK_CNTR_CD , DEF.WORK_CNTR_CD) AS SIMUL_WORK_CNTR_CD
              --   , COALESCE(LOT.WORK_DAYS    , PJT.WORK_DAYS    , DEF.WORK_DAYS   ) AS SIMUL_WORK_DAYS
              --   , COALESCE(LOT.PREPARE_DAYS , PJT.PREPARE_DAYS , DEF.PREPARE_DAYS) AS SIMUL_PREPARE_DAYS
                 , DEF.PRD_STRT_DATE
                 , PJT.PRD_STRT_DATE AS PJT_PRD_STRT_DATE
                 , LOT.PRD_STRT_DATE AS LOT_PRD_STRT_DATE
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
                         , DENSE_RANK() OVER (ORDER BY PJT_SHIP, SHIP_SEQ DESC, SHIP_SEQ_LOT) AS ASSEMBLY_SEQ
                         , ASSEMBLY
                         , DENSE_RANK() OVER (ORDER BY PROCESS_SEQ) AS SIMUL_PROCESS_SEQ
                         , PROCESS_CODE
                         , ROW_NUMBER() OVER (PARTITION BY  PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, ASSEMBLY ORDER BY SIMUL_SEQ) AS SIMUL_WORK_CNTR_SEQ
                         , WORK_CNTR_CD
                         , ATWRT01
                         , ATWRT02
                         , ATWRT03
                         , ATWRT04
                         , ATWRT05
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
                                 , T6.ATWRT01
                                 , T6.ATWRT02
                                 , T6.ATWRT03
                                 , T6.ATWRT04
                                 , T6.ATWRT05
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
                                         -- AND T2.WORK_CNTR_CD        = (CASE WHEN T2.PROCESS_CODE = 'UST' THEN T2.ATTRIBUTE2 ELSE T2.WORK_CNTR_CD END)
                                   INNER JOIN (
                                                SELECT SIMUL_PROCESS_CD, WORK_CNTR_CD, SIMUL_SEQ, DEFAULT_YN, CABLE_CORE_FLAG
                                                  FROM SOP_DB.dbo.TB_SIMUL_WORK_CENTER_MST
                                         --        WHERE DEFAULT_YN = 'Y'
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
                                    OR T4.CABLE_CORE_FLAG <> CASE WHEN T6.ATWRT02 = '3' THEN 'DC' ELSE 'AC' END )
                                    -- TB_PRD_PLAN_CABLE_SPEC.ATWRT02  AC(1core)일때 'DC' 제외, DC(3core)일때 'AC' 제외
							) A
                    ) DEF
                     LEFT OUTER JOIN SOP_DB.DBO.TB_SIMUL_DATA_PJT_MODIFY PJT
                                  ON DEF.SALE_OPP_NO           = PJT.SALE_OPP_NO
                                 AND DEF.PROCESS_CODE          = PJT.PROCESS_CODE
                                 AND LEFT(DEF.WORK_CNTR_CD, 3) =  LEFT(PJT.WORK_CNTR_CD, 3)
                     LEFT OUTER JOIN SOP_DB.DBO.TB_SIMUL_DATA_LOT_MODIFY LOT
                                  ON DEF.SALE_OPP_NO   = LOT.SALE_OPP_NO
                                 AND DEF.PJT_SHIP      = LOT.PJT_SHIP
                                 AND DEF.SHIP_SEQ      = LOT.SHIP_SEQ
                                 AND DEF.SHIP_SEQ_LOT  = LOT.SHIP_SEQ_LOT
                                 AND DEF.ASSEMBLY      = LOT.ASSEMBLY
                                 AND DEF.PROCESS_CODE  = LOT.PROCESS_CODE
                                 AND LEFT(DEF.WORK_CNTR_CD, 3) =  LEFT(LOT.WORK_CNTR_CD, 3)
            ) LIST
              INNER JOIN SOP_DB.dbo.TB_SIMUL_SUL_ASSY_PROC_LIST DATA
                      ON LIST.SUL_NO             = DATA.SUL_NO
                     AND LIST.ASSEMBLY           = DATA.ASSEMBLY
                     AND LIST.PROCESS_CODE       = DATA.PROCESS_CODE
                     AND LIST.SIMUL_WORK_CNTR_CD = DATA.WORK_CNTR_CD
                 
        

/*
    -- 분할개수 테이블
    DECLARE @FJ_CNT_CALC TABLE (
        조장_SEQ INT,
        조장_KM FLOAT,
        FJ_CNT INT
    );
    INSERT INTO @FJ_CNT_CALC(조장_SEQ, 조장_KM, FJ_CNT)
    SELECT 조장_SEQ
         , 조장_KM
         , CAST(CEILING(조장_KM / 
                        NULLIF(MAX(CASE WHEN ATWRT02 = '3' AND 공정 = 'UST' THEN 시뮬값1
                                        WHEN ATWRT02 = '1' AND 공정 = 'INS' THEN 시뮬값1
                                   END), 0 )
                        ) AS INT) AS FJ_CNT
    FROM @TEMP_PRODUCTION_LIST
    GROUP BY 조장_SEQ, 조장_KM;


    -- 동적 숫자 테이블 (1~30까지; 필요시 늘려도 됨)
    DECLARE @NUMBERS TABLE (CREATE_ROW_NUM INT);
    INSERT INTO @NUMBERS(CREATE_ROW_NUM)
    SELECT TOP (30) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS CREATE_ROW_NUM
    FROM SYS.ALL_OBJECTS;
*/
    SELECT T.*
         , ISNULL(CALC.LEAD_TIME, 0) AS LEAD_TIME
      FROM (
            SELECT ROW_NUMBER() OVER(ORDER BY T1.항차, T1.선적순서 DESC, T1.LOT번호, T1.조장_SEQ, T1.공정_SEQ, T1.시뮬호기_SEQ) AS ROWNUM
                 , ROW_NUMBER() OVER(ORDER BY T1.항차, T1.선적순서 DESC, T1.LOT번호, T1.조장_SEQ, T1.공정_SEQ, T1.시뮬호기_SEQ) AS PRODUCTION_SEQ
                 , T1.프로젝트
                 , T1.항차
                 , T1.선적순서
                 , T1.LOT번호
                 , T1.설계번호
                 , T1.조장_SEQ
                 , T1.조장_KM
--                 , T3.CREATE_ROW_NUM AS FJ_SEQ
--                 , CAST(T1.조장_KM / NULLIF(T2.FJ_CNT, 0) AS INT) AS FJ_조장_KM
                 , T1.공정_SEQ
                 , T1.공정
                 , T1.시뮬호기_SEQ
                 , T1.시뮬호기코드
                 , T1.선속_MPM
                 , T1.소요기간_일
                 , T1.준비기간_일
                 , T1.다음공정준비_일
                 , T1.주작업일수
                 , T1.LOT_생산시작일
                 , T1.시뮬값1
                 , T1.시뮬값2
                 , T1.시뮬값3
                 , T1.시뮬값4
                 , T1.시뮬_순서
                 , T1.ATWRT01
                 , T1.ATWRT02
                 , T1.ATWRT03
                 , T1.ATWRT04
                 , T1.ATWRT05
              FROM @TEMP_PRODUCTION_LIST T1
/*                   INNER JOIN @FJ_CNT_CALC T2
                           ON T1.조장_SEQ = T2.조장_SEQ AND T1.조장_KM = T2.조장_KM
                   CROSS JOIN @NUMBERS T3
             WHERE T3.CREATE_ROW_NUM <= T2.FJ_CNT*/
          ) T
            CROSS APPLY (
                -- LEAD TIME 수식
                SELECT CASE
                    -- 연선,횡권
                    WHEN 공정 IN ('CST', 'WSD')                                  THEN ROUND(((조장_KM / NULLIF(시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + 준비기간_일) * CAST(ATWRT02 AS INT), 0)
                    -- 절연
                    WHEN 공정 = 'INS' AND ATWRT02 = '3'                          THEN ROUND(((조장_KM / NULLIF(시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + 준비기간_일) * CAST(ATWRT02 AS INT), 0)
                    WHEN 공정 = 'INS' AND ATWRT02 = '1'                          THEN ROUND((조장_KM / NULLIF(시뮬값1, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + 준비기간_일, 0)
                    -- 연피쉬스
                    WHEN 공정 = 'LDS' AND 시뮬호기코드 LIKE 'LDS%'                   THEN ROUND(((조장_KM / NULLIF(시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + 준비기간_일) * CAST(ATWRT02 AS INT), 0)
                    WHEN 공정 = 'LDS' AND 시뮬호기코드 LIKE 'RWDL%'                  THEN 0
                    WHEN 공정 = 'LDS' AND 시뮬호기코드 LIKE 'FJYL%'                  THEN COALESCE(주작업일수, 시뮬값1)
                    -- 연합
                    WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'REW%'                   THEN CEILING(1 + (CAST(조장_KM AS FLOAT) * 3) / 10000)
                    WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'FJT%'                   THEN COALESCE(주작업일수, 시뮬값1)
                    WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'UST%' AND ATWRT02 = '3' THEN ROUND((조장_KM / NULLIF(시뮬값4, 0) ) * (CAST(7 AS FLOAT) / NULLIF(주작업일수, 0) )  + 준비기간_일, 0)
                    WHEN 공정 = 'UST' AND 시뮬호기코드 LIKE 'UST%' AND ATWRT02 = '1' THEN 0
                    -- 외장
                    WHEN 공정 = 'SMA'                                            THEN ROUND(((조장_KM / NULLIF(시뮬값4, 0) ) + 준비기간_일) * CAST(7 AS FLOAT) / NULLIF(주작업일수, 0), 0)
                END AS LEAD_TIME
            ) CALC

END;

-- =============================================
-- Author: Franz
-- Create date: 2025-09-26
-- Description:	프로젝트 생산일정 단일 아이템 삭제
-- EXEC [SP_D365_CALL_PRODUCT_SCHEDULE_ITEM_DELETE] 
-- =============================================
CREATE PROCEDURE [dbo].[SP_D365_CALL_PRODUCT_SCHEDULE_ITEM_DELETE]
	@SALE_OPP_NO     NVARCHAR(50),
    @PJT_SHIP        DECIMAL(18,0),
    @SHIP_SEQ        DECIMAL(18,0),
    @SHIP_SEQ_LOT    DECIMAL(18,0)
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM TB_PRD_PLAN_LIST
    WHERE SALE_OPP_NO   = @SALE_OPP_NO
      AND PJT_SHIP      = @PJT_SHIP
      AND SHIP_SEQ      = @SHIP_SEQ
      AND SHIP_SEQ_LOT  = @SHIP_SEQ_LOT;

    -- 삭제된 행 수 확인
    IF @@ROWCOUNT = 0
    BEGIN
        -- 삭제된 행이 없을 경우 메시지 반환
        RAISERROR(N'삭제할 데이터가 존재하지 않습니다.', 16, 1);
    END
END;

-- =============================================
-- Author: Franz
-- Create date: 2025-12-09
-- Description:	프로젝트 생산일정 저장 후 업데이트 안된 값 삭제
-- EXEC [SP_D365_CALL_PRODUCT_SCHEDULE_ITEMS_CHECK_DELETE] 'PD25020022', '2025-12-10 09:04:40.527'
-- =============================================
CREATE PROCEDURE [dbo].[SP_D365_CALL_PRODUCT_SCHEDULE_ITEMS_CHECK_DELETE]
	@SALE_OPP_NO     NVARCHAR(50),
    @UPD_DATE        DATETIME
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM TB_PRD_PLAN_LIST
    WHERE SALE_OPP_NO   = @SALE_OPP_NO
      AND UPD_DATE      <> @UPD_DATE

END;

-- =============================================
-- Author: Franz
-- Create date: 2025-11-06
-- Description:	프로젝트 생산일정 리스트 SPEC 조회
-- EXEC [SP_D365_CALL_PRODUCT_SCHEDULE_ITEMS_SPEC_UPSERT] 
-- =============================================
CREATE PROCEDURE [dbo].[SP_D365_CALL_PRODUCT_SCHEDULE_ITEMS_SPEC_UPSERT]
    @SUL_NO          NVARCHAR(50),
    @REV_SEQ         NVARCHAR(4),
    @ATWRT01         NVARCHAR(40),
    @ATWRT02         NVARCHAR(40),
    @ATWRT03         NVARCHAR(40),
    @ATWRT04         NVARCHAR(40),
    @ATWRT05         NVARCHAR(40),
    @REG_EMP         NVARCHAR(50),
    @UPD_EMP         NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;

    
    -- Upsert 처리
    IF EXISTS (
        SELECT 1
        FROM TB_PRD_PLAN_CABLE_SPEC
        WHERE SUL_NO    = LTRIM(RTRIM(@SUL_NO))
           AND REV_SEQ  = LTRIM(RTRIM(@REV_SEQ))
    )
    BEGIN
        -- 이미 존재하면 UPDATE
        UPDATE TB_PRD_PLAN_CABLE_SPEC
        SET SUL_NO      = @SUL_NO,
            REV_SEQ     = @REV_SEQ,
            ATWRT01     = @ATWRT01,
            ATWRT02     = @ATWRT02,
            ATWRT03     = @ATWRT03,
            ATWRT04     = @ATWRT04,
            ATWRT05     = @ATWRT05,
            UPD_EMP     = @UPD_EMP,
            UPD_DATE    = GETDATE()
         WHERE SUL_NO   = LTRIM(RTRIM(@SUL_NO))
           AND REV_SEQ  = LTRIM(RTRIM(@REV_SEQ))
    END
    ELSE
    BEGIN
        -- 없으면 INSERT
        INSERT INTO TB_PRD_PLAN_CABLE_SPEC
        (
            SUL_NO, REV_SEQ, ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05,
            REG_EMP, REG_DATE, UPD_EMP, UPD_DATE            
        )
        VALUES
        (
            @SUL_NO, @REV_SEQ, @ATWRT01, @ATWRT02, @ATWRT03, @ATWRT04, @ATWRT05,
            @REG_EMP, GETDATE(), @UPD_EMP, GETDATE()
        );
    END
END;

-- =============================================
-- Author: Franz
-- Create date: 2025-09-25
-- Description:	프로젝트 생산일정 리스트 조회

-- @PRD_CNFM_STRT_DATE   NVARCHAR(8), @PRD_CNFM_END_DATE    NVARCHAR(8) 이거 두개는 AUD 에서 채워주기로했음.

-- EXEC [SP_D365_CALL_PRODUCT_SCHEDULE_ITEMS_UPSERT] 
-- =============================================
CREATE PROCEDURE [dbo].[SP_D365_CALL_PRODUCT_SCHEDULE_ITEMS_UPSERT]
	@SALE_OPP_NO            NVARCHAR(50),
    @PJT_SHIP               DECIMAL(18,0),
    @SHIP_SEQ               DECIMAL(18,0),
    @SHIP_SEQ_LOT           DECIMAL(18,0),
    @SUL_NO                 NVARCHAR(50),
    @PJT_LENGTH             DECIMAL(18,2),
    @SHIP_STRT_DATE         NVARCHAR(20),
    @SHIP_END_DATE          NVARCHAR(20),
    @PRD_STRT_DATE          NVARCHAR(20),
    @PRD_END_DATE           NVARCHAR(20),
    @NOTE                   NVARCHAR(300),
    @ATTRIBUTE1             NVARCHAR(100),
    @ATTRIBUTE2             NVARCHAR(100),
    @REG_EMP                NVARCHAR(50),
    @UPD_EMP                NVARCHAR(50),
    @UPD_DATE               DATETIME,
    @PRD_CNFM_STRT_DATE     NVARCHAR(8),
    @PRD_CNFM_END_DATE      NVARCHAR(8),
    @REV_SEQ                NVARCHAR(4)
AS
BEGIN
    SET NOCOUNT ON;


    -- Upsert 처리
    IF EXISTS (
        SELECT 1
        FROM TB_PRD_PLAN_LIST
        WHERE SALE_OPP_NO  = LTRIM(RTRIM(@SALE_OPP_NO))
           AND PJT_SHIP     = CAST(@PJT_SHIP AS INT)
           AND SHIP_SEQ     = CAST(@SHIP_SEQ AS INT)
           AND SHIP_SEQ_LOT = CAST(@SHIP_SEQ_LOT AS INT)
    )
    BEGIN
        -- 이미 존재하면 UPDATE
        UPDATE TB_PRD_PLAN_LIST
        SET SUL_NO              = @SUL_NO,
            PJT_LENGTH          = @PJT_LENGTH,
            SHIP_STRT_DATE      = @SHIP_STRT_DATE,
            SHIP_END_DATE       = @SHIP_END_DATE,
            PRD_STRT_DATE       = @PRD_STRT_DATE,
            PRD_END_DATE        = @PRD_END_DATE,
            NOTE                = @NOTE,
            ATTRIBUTE1          = @ATTRIBUTE1,
            ATTRIBUTE2          = @ATTRIBUTE2,
            UPD_EMP             = @UPD_EMP,
            UPD_DATE            = @UPD_DATE,
            REV_SEQ             = @REV_SEQ

        WHERE SALE_OPP_NO  = @SALE_OPP_NO
          AND PJT_SHIP     = @PJT_SHIP
          AND SHIP_SEQ     = @SHIP_SEQ
          AND SHIP_SEQ_LOT = @SHIP_SEQ_LOT
    END
    ELSE
    BEGIN
        -- 없으면 INSERT
        INSERT INTO TB_PRD_PLAN_LIST
        (
            SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
            SUL_NO, PJT_LENGTH,
            SHIP_STRT_DATE, SHIP_END_DATE,
            PRD_STRT_DATE, PRD_END_DATE,
            NOTE, ATTRIBUTE1, ATTRIBUTE2,
            REG_EMP, REG_DATE, UPD_EMP, UPD_DATE, REV_SEQ
        )
        VALUES
        (
            @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
            @SUL_NO, @PJT_LENGTH,
            @SHIP_STRT_DATE, @SHIP_END_DATE,
            @PRD_STRT_DATE, @PRD_END_DATE,
            @NOTE, @ATTRIBUTE1, @ATTRIBUTE2,
            @REG_EMP, @UPD_DATE, @UPD_EMP, @UPD_DATE, @REV_SEQ
        );
    END
END;

-- =============================================
-- Author: Franz
-- Create date: 2025-09-25
-- Description:	프로젝트 생산일정 리스트 조회
-- EXEC [SP_D365_CALL_PRODUCT_SCHEDULE_LIST] 'PDTEST1'
-- =============================================
CREATE PROCEDURE [dbo].[SP_D365_CALL_PRODUCT_SCHEDULE_LIST]
	 @P_SALE_OPP_NO		nvarchar(20)		-- 프로젝트 코드

AS
BEGIN

	SELECT 
		l.SALE_OPP_NO
		, l.PJT_SHIP
		, l.SHIP_SEQ
		, l.SHIP_SEQ_LOT
		, l.SUL_NO
		, l.PJT_LENGTH
		, CONVERT(char(10), TRY_CONVERT(date, LTRIM(RTRIM(l.SHIP_STRT_DATE))), 23) AS SHIP_STRT_DATE
		, CONVERT(char(10), TRY_CONVERT(date, LTRIM(RTRIM(l.SHIP_END_DATE))), 23) AS SHIP_END_DATE
		, CONVERT(char(10), TRY_CONVERT(date, LTRIM(RTRIM(l.PRD_STRT_DATE))), 23) AS PRD_STRT_DATE
		, CONVERT(char(10), TRY_CONVERT(date, LTRIM(RTRIM(l.PRD_END_DATE))), 23) AS PRD_END_DATE
		, CONVERT(char(10), TRY_CONVERT(date, LTRIM(RTRIM(l.PRD_CNFM_STRT_DATE))), 23) AS PRD_CNFM_STRT_DATE
		, CONVERT(char(10), TRY_CONVERT(date, LTRIM(RTRIM(l.PRD_CNFM_END_DATE))), 23) AS PRD_CNFM_END_DATE
		, l.NOTE
		, l.ATTRIBUTE1
		, l.ATTRIBUTE2
		, l.REG_EMP
		, l.UPD_EMP
		, l.REG_DATE
		, l.UPD_DATE
		, l.ITEM_NM
		, s.ATWRT01
		, s.ATWRT02
		, s.ATWRT03
		, s.ATWRT04
		, s.ATWRT05
		, l.REV_SEQ
	FROM TB_PRD_PLAN_LIST l
	LEFT JOIN TB_PRD_PLAN_CABLE_SPEC s ON l.SUL_NO = s.SUL_NO AND l.REV_SEQ = s.REV_SEQ
	WHERE l.SALE_OPP_NO = @P_SALE_OPP_NO
	ORDER BY PJT_SHIP ASC, SHIP_SEQ ASC

end;

-- =============================================
-- Author: Franz
-- Create date: 2025-09-26
-- Description:	프로젝트 생산일정 섹션 삭제
-- 25.12.08 김덕차장님 요청으로 LIST 에 데이터가없으면 해당 MASTER 삭제하도록 기능 추가.
-- EXEC [SP_D365_CALL_PRODUCT_SCHEDULE_SECTION_DELETE] 
-- =============================================
CREATE PROCEDURE [dbo].[SP_D365_CALL_PRODUCT_SCHEDULE_SECTION_DELETE]
	@SALE_OPP_NO     NVARCHAR(50),
    @PJT_SHIP        DECIMAL(18,0)
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM TB_PRD_PLAN_LIST
    WHERE SALE_OPP_NO   = @SALE_OPP_NO
      AND PJT_SHIP      = @PJT_SHIP

    -- 삭제된 행 수 확인
    IF @@ROWCOUNT = 0
    BEGIN
        -- 삭제된 행이 없을 경우 메시지 반환
        RAISERROR(N'삭제할 데이터가 존재하지 않습니다.', 16, 1);
    END
	BEGIN
    -- 해당 opp_no가 더 이상 존재하지 않는지 확인
    IF NOT EXISTS (
        SELECT 1
        FROM TB_PRD_PLAN_LIST
        WHERE SALE_OPP_NO = @SALE_OPP_NO
    )
    BEGIN
        DELETE FROM TB_PRD_PLAN_MASTER
        WHERE SALE_OPP_NO   = @SALE_OPP_NO
    END
END
END;

-- =============================================
-- Author: Franz
-- Create date: 2025-09-25
-- Description:	프로젝트 생산일정 리스트 조회
-- EXEC [SP_D365_CALL_PRODUCT_SCHEDULE_UPSERT] 
-- =============================================
CREATE PROCEDURE [dbo].[SP_D365_CALL_PRODUCT_SCHEDULE_UPSERT]
    @SALE_OPP_NO NVARCHAR(50),
    @SALE_OPP_NM NVARCHAR(50),
    @QUO_YM      NVARCHAR(50),
    @PJT_OWNER   NVARCHAR(50)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- 파라미터 공백 제거
    SET @SALE_OPP_NO = LTRIM(RTRIM(@SALE_OPP_NO));
    SET @QUO_YM      = LTRIM(RTRIM(@QUO_YM));

    -- PK 기준으로 존재 여부 확인
    IF EXISTS (
        SELECT 1 
        FROM TB_PRD_PLAN_MASTER WITH (UPDLOCK, HOLDLOCK)
        WHERE SALE_OPP_NO = @SALE_OPP_NO
          AND QUO_YM = @QUO_YM
    )
    BEGIN
        -- UPDATE
        UPDATE m
        SET m.PJT_STRT_DATE = CONVERT(CHAR(8), listDate.MinStartDate, 112),
            m.PJT_REND_DATE = CONVERT(CHAR(8), listDate.MaxEndDate, 112),
            m.PJT_EEND_DATE = CONVERT(CHAR(8), listDate.MaxEndDate, 112),
            m.UPD_EMP       = listEmp.LastUpdEmp,
            m.UPD_DATE      = listDate.LastUpdDt
        FROM TB_PRD_PLAN_MASTER m
        CROSS APPLY (
            SELECT 
                MIN(TRY_CONVERT(DATE, PRD_STRT_DATE)) AS MinStartDate,
                MAX(TRY_CONVERT(DATE, PRD_END_DATE))  AS MaxEndDate,
                MAX(UPD_DATE)                         AS LastUpdDt
            FROM TB_PRD_PLAN_LIST
            WHERE SALE_OPP_NO = @SALE_OPP_NO
        ) listDate
        CROSS APPLY (
            SELECT TOP 1 l.UPD_EMP AS LastUpdEmp
            FROM TB_PRD_PLAN_LIST l
            WHERE l.SALE_OPP_NO = @SALE_OPP_NO
            ORDER BY l.UPD_DATE DESC
        ) listEmp
        WHERE m.SALE_OPP_NO = @SALE_OPP_NO
          AND m.QUO_YM = @QUO_YM;
    END
    ELSE
    BEGIN
        -- INSERT (항상 1행만 PK키 중복죠심)
        INSERT INTO TB_PRD_PLAN_MASTER
        (
            QUO_YM,
            SALE_OPP_NO,
            SALE_OPP_NM,
            PJT_OWNER,
            PJT_STRT_DATE,
            PJT_EEND_DATE,
            PJT_REND_DATE,
            NOTE,
            ATTRIBUTE1,
            ATTRIBUTE2,
            REG_EMP,
            REG_DATE,
            UPD_EMP,
            UPD_DATE
        )
        SELECT 
            @QUO_YM,
            @SALE_OPP_NO,
            @SALE_OPP_NM,
            @PJT_OWNER,
            CONVERT(CHAR(8), listDate.MinStartDate, 112),
            CONVERT(CHAR(8), listDate.MaxEndDate, 112),
            CONVERT(CHAR(8), listDate.MaxEndDate, 112),
            recent.NOTE,
            recent.ATTRIBUTE1,
            recent.ATTRIBUTE2,
            listEmp.LastUpdEmp,
            listDate.LastUpdDt,
            listEmp.LastUpdEmp,
            listDate.LastUpdDt
        FROM
            (SELECT 
                MIN(TRY_CONVERT(DATE, PRD_STRT_DATE)) AS MinStartDate,
                MAX(TRY_CONVERT(DATE, PRD_END_DATE))  AS MaxEndDate,
                MAX(UPD_DATE)                         AS LastUpdDt
             FROM TB_PRD_PLAN_LIST
             WHERE SALE_OPP_NO = @SALE_OPP_NO
            ) listDate
        CROSS APPLY (
            SELECT TOP 1 l2.UPD_EMP AS LastUpdEmp
            FROM TB_PRD_PLAN_LIST l2
            WHERE l2.SALE_OPP_NO = @SALE_OPP_NO
            ORDER BY l2.UPD_DATE DESC
        ) listEmp
        CROSS APPLY (
            SELECT TOP 1 l3.NOTE, l3.ATTRIBUTE1, l3.ATTRIBUTE2
            FROM TB_PRD_PLAN_LIST l3
            WHERE l3.SALE_OPP_NO = @SALE_OPP_NO
            ORDER BY l3.UPD_DATE DESC
        ) recent;
    END
END;

CREATE PROCEDURE dbo.SP_SCHEDULER_DATA_JSON
    @PARAM_MODE    NVARCHAR(10),  -- 'SIMUL' or 'TEMP'
    @PARAM_VERSION NVARCHAR(50)   -- Version Key
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE @SQL            NVARCHAR(MAX);
    DECLARE @TableName      NVARCHAR(100);
    DECLARE @ColVersion     NVARCHAR(50);
    DECLARE @ParamDef       NVARCHAR(500);

    -- 1. 동적 테이블 및 컬럼 결정
    IF @PARAM_MODE = 'SIMUL'
    BEGIN
        SET @TableName  = 'SOP_DB.dbo.TB_SIMUL_VERSION_DATA';
        SET @ColVersion = 'SIMUL_VERSION';
    END
    ELSE
    BEGIN
        SET @TableName  = 'SOP_DB.dbo.TB_TEMP_VERSION_DATA';
        SET @ColVersion = 'TEMP_VERSION';
    END

    -- 2. 임시 테이블 생성 (데이터 캐싱용)
    -- 필요한 컬럼만 정의하여 인덱싱 효과 및 메모리 최적화
    CREATE TABLE #RawData (
        SEQ                 INT,
        SALE_OPP_NO         NVARCHAR(50),
        PJT_SHIP            NVARCHAR(10),
        SHIP_SEQ            INT,
        SHIP_SEQ_LOT        INT,
        ASSEMBLY            NVARCHAR(50),
        FJ_ASSEMBLY         NVARCHAR(50),
        FJ_ASSEMBLY_SEQ     INT,
        ASSEMBLY_SEQ        INT,
        WORK_CNTR_CD        NVARCHAR(50),
        WORK_CNTR_SEQ       INT,
        PROCESS_CODE        NVARCHAR(20),
        PRD_CNFM_STRT_DATE  DATETIME,
        PRD_CNFM_END_DATE   DATETIME,
        LEAD_TIME           INT
    );

    -- 3. 동적 쿼리로 데이터 추출하여 임시 테이블 적재
    SET @SQL = N'
        INSERT INTO #RawData
        SELECT SEQ, SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, 
               ASSEMBLY, FJ_ASSEMBLY, FJ_ASSEMBLY_SEQ, ASSEMBLY_SEQ,
               WORK_CNTR_CD, WORK_CNTR_SEQ, PROCESS_CODE,
               PRD_CNFM_STRT_DATE, PRD_CNFM_END_DATE, LEAD_TIME
          FROM ' + @TableName + N'
         WHERE ' + @ColVersion + N' = @Ver
           AND LEAD_TIME > 0;
    ';

    SET @ParamDef = N'@Ver NVARCHAR(50)';
    
    BEGIN TRY
        EXEC sp_executesql @SQL, @ParamDef, @Ver = @PARAM_VERSION;
    END TRY
    BEGIN CATCH
        THROW; -- 에러 발생 시 상위로 전파
    END CATCH

    -- 인덱스 생성 (Join 및 정렬 성능 향상)
    CREATE CLUSTERED INDEX IX_RawData_SEQ ON #RawData(SEQ);
    CREATE NONCLUSTERED INDEX IX_RawData_Process ON #RawData(PROCESS_CODE);
    CREATE NONCLUSTERED INDEX IX_RawData_Sort ON #RawData(SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT);

    -------------------------------------------------------
    -- 4. 최종 JSON 생성
    -------------------------------------------------------
    /* 
       FOR JSON 결과를 서브쿼리로 감싸고 alias를 지정합니다.
       이렇게 하면 컬럼명이 [JSON_DATA]로 고정되며, 
       결과도 여러 줄이 아닌 '한 줄'로 합쳐져서 나옵니다.
    */
    SELECT (
        SELECT 
            -- (1) Resources
            (
                SELECT 
                    A.WORK_CNTR_CD          AS [id],
                    A.SIMUL_WORK_CNTR_NM    AS [name],
                    B.PROCESS_NAME          AS [process],
                    '#FEF5E7'               AS [bgColor],
                    '#F5B041'               AS [textColor]
                FROM SOP_DB.dbo.TB_SIMUL_WORK_CENTER_MST A
                INNER JOIN SOP_DB.dbo.TB_SIMUL_FAC_PC B
                    ON A.SIMUL_PROCESS_CD = B.PROCESS_CODE
                    AND B.Q_YEAR = YEAR(GETDATE())
                ORDER BY A.SIMUL_SEQ
                FOR JSON PATH
            ) AS [resources],

            -- (2) Events
            (
                SELECT 
                    A.SEQ                   AS [id],
                    A.WORK_CNTR_CD          AS [resourceId],
                    CONVERT(VARCHAR(19), A.PRD_CNFM_STRT_DATE, 120) AS [startDate],
                    A.LEAD_TIME             AS [duration],
                    'D'                     AS [durationUnit],
                    C.COLOR_CODE            AS [eventColor],
                    PM.SALE_OPP_NM          AS [saleOppNm],
                    CAST(A.PJT_SHIP AS NVARCHAR) + '-' + CAST(A.SHIP_SEQ AS NVARCHAR) + '-' + CAST(A.SHIP_SEQ_LOT AS NVARCHAR) AS [lotInfo],
                    CAST(A.ASSEMBLY AS NVARCHAR) + ' ( ' + CAST(A.FJ_ASSEMBLY AS NVARCHAR) + ' - ' + CAST(A.FJ_ASSEMBLY_SEQ AS NVARCHAR) + ' )' AS [assemblyInfo],
                    CONCAT(A.SALE_OPP_NO, '_', CAST(A.PJT_SHIP AS NVARCHAR) + '-' + CAST(A.SHIP_SEQ AS NVARCHAR) + '-' + CAST(A.SHIP_SEQ_LOT AS NVARCHAR)) AS [groupLot]
                FROM #RawData A
                INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_MASTER PM
                    ON A.SALE_OPP_NO = PM.SALE_OPP_NO
                INNER JOIN SOP_DB.dbo.TB_SIMUL_FAC_PC F
                    ON A.PROCESS_CODE = F.PROCESS_CODE
                    AND F.Q_YEAR = YEAR(GETDATE())
                LEFT JOIN (
                    SELECT SEQ, 
                           DENSE_RANK() OVER(ORDER BY SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT) AS COLOR_SEQ
                    FROM #RawData
                ) RankData ON A.SEQ = RankData.SEQ
                LEFT JOIN SOP_DB.dbo.TB_SIMUL_COLOR_MST C
                    ON RankData.COLOR_SEQ = C.SEQ
                ORDER BY A.SEQ
                FOR JSON PATH
            ) AS [events],

            -- (3) Dependencies
            (
                SELECT 
                    SEQ      AS [id],
                    FROM_SEQ AS [from],
                    TO_SEQ   AS [to],
                    1        AS [lag],
                    'day'    AS [lagUnit]
                FROM (
                    SELECT 
                        SEQ,
                        SEQ AS FROM_SEQ,
                        LEAD(SEQ) OVER(
                            PARTITION BY SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, ASSEMBLY_SEQ, FJ_ASSEMBLY_SEQ
                            ORDER BY WORK_CNTR_SEQ
                        ) AS TO_SEQ
                    FROM #RawData
                ) A
                WHERE TO_SEQ IS NOT NULL
                ORDER BY SEQ
                FOR JSON PATH
            ) AS [dependencies],

            -- (4) DisplayDate
            (
                SELECT 
                    CONVERT(VARCHAR(19), DATEADD(MONTH, -2, MIN(PRD_CNFM_STRT_DATE)), 120) AS [startDate],
                    CONVERT(VARCHAR(19), DATEADD(MONTH,  2, MAX(PRD_CNFM_END_DATE)), 120)  AS [endDate]
                FROM #RawData
                FOR JSON PATH
            ) AS [displayDate]

        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
    ) AS [JSON_DATA]; -- << 여기가 핵심입니다. 컬럼명이 'JSON_DATA'로 고정됩니다.

    -- 임시 테이블 정리
    DROP TABLE #RawData;
END;

CREATE PROCEDURE dbo.SP_SCHEDULER_DATA_UPDATE
    @PARAM_MODE    NVARCHAR(10),   -- 'SIMUL' or 'TEMP'
    @PARAM_VERSION NVARCHAR(50),   -- Version Key
    @UPDATE_JSON      NVARCHAR(MAX),  -- 통짜 JSON 문자열
    @USER_ID          NVARCHAR(50)    -- 사용자 ID
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL            NVARCHAR(MAX);
    DECLARE @TableName      NVARCHAR(100);
    DECLARE @ColVersion     NVARCHAR(50);
    
    -- 1. 업데이트할 테이블 및 버전 컬럼 결정
    IF @PARAM_MODE = 'SIMUL'
    BEGIN
        SET @TableName  = 'SOP_DB.dbo.TB_SIMUL_VERSION_DATA';
        SET @ColVersion = 'SIMUL_VERSION';
    END
    ELSE
    BEGIN
        SET @TableName  = 'SOP_DB.dbo.TB_TEMP_VERSION_DATA';
        SET @ColVersion = 'TEMP_VERSION';
    END

    -- 2. JSON 파싱 및 임시 테이블 적재 (데이터 타입 매핑)
    -- OPENJSON을 사용하면 JSON 배열을 테이블처럼 쓸 수 있습니다.
    SELECT * INTO #JsonUpdates
    FROM OPENJSON(@UPDATE_JSON)
    WITH (
        id           INT          '$.id',
        resourceId   NVARCHAR(50) '$.resourceId',
        startDate    VARCHAR(50)  '$.startDate', -- 일단 문자열로 받고 나중에 변환
        endDate      VARCHAR(50)  '$.endDate'
    );

    -- 3. 동적 쿼리로 일괄 UPDATE 수행 (JOIN Update)
    SET @SQL = N'
        UPDATE T
           SET WORK_CNTR_CD       = J.resourceId
             , PRD_CNFM_STRT_DATE = CAST(J.startDate AS DATETIMEOFFSET)
             , PRD_CNFM_END_DATE  = CAST(J.endDate AS DATETIMEOFFSET)
             , UPD_EMP            = @P_USER_ID
             , UPD_DATE           = GETDATE()
          FROM ' + @TableName + N' T
          INNER JOIN #JsonUpdates J
             ON T.SEQ = J.id
         WHERE T.' + @ColVersion + N' = @P_VERSION;
    ';

    -- 4. 쿼리 실행
    EXEC sp_executesql @SQL, 
                       N'@P_USER_ID NVARCHAR(50), @P_VERSION NVARCHAR(50)', 
                       @P_USER_ID = @USER_ID, 
                       @P_VERSION = @PARAM_VERSION;

    -- 5. 임시 테이블 정리
    DROP TABLE #JsonUpdates;
END;