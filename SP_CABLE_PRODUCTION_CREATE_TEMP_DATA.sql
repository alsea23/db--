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