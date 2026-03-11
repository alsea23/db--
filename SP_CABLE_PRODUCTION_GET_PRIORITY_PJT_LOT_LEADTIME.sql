CREATE   PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_PRIORITY_PJT_LOT_LEADTIME]
(
    @P_PJT_LIST dbo.UDT_DATA_SALE_OPP_NO READONLY
)
AS
BEGIN
    SET NOCOUNT ON;


    DECLARE @RESULT dbo.UDT_DATA_CABLE_PJT_LOT_FJ;

    DECLARE @SALE_OPP_NO VARCHAR(20);
    DECLARE @SEQ  INT = 0;

    /* =========================================================================
       0) 커서 밖에서 #temp 1회 생성 (프로젝트마다 TRUNCATE)
    ========================================================================= */

    -- 0-1) 생산 기본 데이터(프로젝트 1건 분)
    IF OBJECT_ID('tempdb..#TEMP_PRODUCTION_LIST') IS NOT NULL DROP TABLE #TEMP_PRODUCTION_LIST;
    CREATE TABLE #TEMP_PRODUCTION_LIST
    (
        SALE_OPP_NO          VARCHAR(20)   NULL,
        PJT_SHIP             INT           NULL,
        SHIP_SEQ             INT           NULL,
        SHIP_SEQ_LOT         INT           NULL,
        SUL_NO               VARCHAR(50)   NULL,
        REV_SEQ              VARCHAR(4)    NULL,
        ASSEMBLY_SEQ         INT           NULL,
        ASSEMBLY             INT           NULL,
        PROCESS_SEQ          INT           NULL,
        PROCESS_CODE         VARCHAR(20)   NULL,
        WORK_CNTR_SEQ        INT           NULL,
        WORK_CNTR_CD         VARCHAR(20)   NULL,
        EQUIP_SPEED          FLOAT         NULL,
        LEAD_TIME            INT           NULL,
        SIMUL_PREPARE_DAYS   NUMERIC(2,1)  NULL,
        SIMUL_WORK_DAYS      NUMERIC(2,1)  NULL,
        PRD_STRT_DATE        DATE          NULL,
        PJT_PRD_STRT_DATE    DATE          NULL,
        LOT_PRD_STRT_DATE    DATE          NULL,
        SIMUL_VALUE1         FLOAT         NULL,
        SIMUL_VALUE2         FLOAT         NULL,
        SIMUL_VALUE3         FLOAT         NULL,
        SIMUL_VALUE4         FLOAT         NULL,
        ATTRIBUTE2           VARCHAR(100)  NULL,
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
        ASSEMBLY_SEQ INT  NULL,
        ASSEMBLY     INT  NULL,
        FJ_CNT       INT  NULL
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
    SELECT P.SALE_OPP_NO 
      FROM @P_PJT_LIST P
           INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_MASTER M
                   ON P.SALE_OPP_NO = M.SALE_OPP_NO
     ORDER BY M.PJT_STRT_DATE ASC; 

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
            SUL_NO, REV_SEQ, ASSEMBLY_SEQ, ASSEMBLY, 
            PROCESS_SEQ, PROCESS_CODE, WORK_CNTR_SEQ, WORK_CNTR_CD,
            EQUIP_SPEED, LEAD_TIME,
            SIMUL_PREPARE_DAYS, SIMUL_WORK_DAYS, PRD_STRT_DATE, PJT_PRD_STRT_DATE, LOT_PRD_STRT_DATE,
            SIMUL_VALUE1, SIMUL_VALUE2, SIMUL_VALUE3, SIMUL_VALUE4, ATTRIBUTE2,
            SIMUL_SEQ,
            ATWRT01, ATWRT02, ATWRT03, ATWRT04, ATWRT05
        )
        SELECT 
		       SALE_OPP_NO
		     , PJT_SHIP
		     , SHIP_SEQ
		     , SHIP_SEQ_LOT
		     , SUL_NO
			 , REV_SEQ
		     , ASSEMBLY_SEQ
		     , ASSEMBLY
		     , PROCESS_SEQ
		     , PROCESS_CODE
		     , WORK_CNTR_SEQ
		     , WORK_CNTR_CD
		     , EQUIP_SPEED
		     , 0 AS LEAD_TIME
		     , SIMUL_PREPARE_DAYS
		     , SIMUL_WORK_DAYS
		     , PRD_STRT_DATE
		     , PJT_PRD_STRT_DATE
		     , LOT_PRD_STRT_DATE
		     , COALESCE(LOT_FJ_ASSEMBLY, SIMUL_VALUE1) AS SIMUL_VALUE1 
		     , SIMUL_VALUE2
		     , SIMUL_VALUE3
		     , SIMUL_VALUE4
		     , ATTRIBUTE2
		     , SIMUL_SEQ
		     , ATWRT01
		     , ATWRT02
		     , ATWRT03
		     , ATWRT04
		     , ATWRT05
		  FROM (
		        SELECT LIST.*
		             , DATA.EQUIP_SPEED
		             , DATA.SIMUL_VALUE1
		             , DATA.SIMUL_VALUE2
		             , DATA.SIMUL_VALUE3
		             , DATA.SIMUL_VALUE4
		             , DATA.ATTRIBUTE2
	                 , MIN(LIST.PRIORITY_WORK_CNTR_CD) OVER (PARTITION BY LIST.SALE_OPP_NO
	                                                                   , LIST.PJT_SHIP
	                                                                   , LIST.SHIP_SEQ
	                                                                   , LIST.SHIP_SEQ_LOT
	                                                                   , LIST.SUL_NO
	                                                                   , LIST.ASSEMBLY_SEQ
	                                                                   , LIST.ASSEMBLY
	                                                                   , LIST.PROCESS_SEQ
	                                                                   , LIST.PROCESS_CODE) AS MIN_PRIORITY_WORK_CNTR_CD
		        FROM (
		                SELECT DEF.SALE_OPP_NO
		                     , DEF.PJT_SHIP
		                     , DEF.SHIP_SEQ
		                     , DEF.SHIP_SEQ_LOT
		                     , DEF.SUL_NO
							 , DEF.REV_SEQ
		                     , DEF.ASSEMBLY_SEQ
		                     , DEF.ASSEMBLY
		                     , DEF.PROCESS_SEQ
		                     , DEF.PROCESS_CODE
		                     , DEF.WORK_CNTR_SEQ
		                     , DEF.WORK_CNTR_CD
		                     , PJT.WORK_CNTR_CD AS PJT_WORK_CNTR_CD
		                     , LOT.WORK_CNTR_CD AS LOT_WORK_CNTR_CD
		                     , CASE WHEN LOT.WORK_CNTR_CD IS NOT NULL THEN 1
		                            WHEN PJT.WORK_CNTR_CD IS NOT NULL THEN 2
		                            ELSE 3
		                       END AS PRIORITY_WORK_CNTR_CD
		                     , LOT.FJ_ASSEMBLY AS LOT_FJ_ASSEMBLY
		                     , COALESCE(LOT.WORK_DAYS    , DEF.WORK_DAYS    ) AS SIMUL_WORK_DAYS
		                     , DEF.PREPARE_DAYS + ISNULL(LOT.PREPARE_DAYS, 0) AS SIMUL_PREPARE_DAYS
		                     , COALESCE(LOT.PRD_STRT_DATE, DEF.PRD_STRT_DATE) AS PRD_STRT_DATE
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
									 , REV_SEQ
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
											 , T1.REV_SEQ
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
		                                      INNER JOIN TB_SIMUL_WORK_CENTER_MST T4
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
		                            ON DEF.SALE_OPP_NO  = PJT.SALE_OPP_NO
		                           AND DEF.PROCESS_CODE = PJT.PROCESS_CODE
		                           AND DEF.WORK_CNTR_CD = PJT.WORK_CNTR_CD
		                    LEFT OUTER JOIN SOP_DB.DBO.TB_SIMUL_DATA_LOT_MODIFY LOT
		                            ON DEF.SALE_OPP_NO  = LOT.SALE_OPP_NO
		                           AND DEF.PJT_SHIP     = LOT.PJT_SHIP
		                           AND DEF.SHIP_SEQ     = LOT.SHIP_SEQ
		                           AND DEF.SHIP_SEQ_LOT = LOT.SHIP_SEQ_LOT
		                           AND DEF.ASSEMBLY     = LOT.ASSEMBLY
		                           AND DEF.PROCESS_CODE = LOT.PROCESS_CODE
		                           AND DEF.WORK_CNTR_CD = LOT.WORK_CNTR_CD
		            ) LIST
		            INNER JOIN SOP_DB.dbo.TB_SIMUL_SUL_ASSY_PROC_LIST DATA
		                    ON LIST.SUL_NO       = DATA.SUL_NO
		                   AND LIST.ASSEMBLY     = DATA.ASSEMBLY
		                   AND LIST.PROCESS_CODE = DATA.PROCESS_CODE
		                   AND LIST.WORK_CNTR_CD = DATA.WORK_CNTR_CD
		 
		       ) RESULT
		 WHERE SALE_OPP_NO  = SALE_OPP_NO
		   AND PJT_SHIP     = PJT_SHIP
		   AND SHIP_SEQ     = SHIP_SEQ
		   AND SHIP_SEQ_LOT = SHIP_SEQ_LOT
		   AND SUL_NO       = SUL_NO
		   AND ASSEMBLY_SEQ = ASSEMBLY_SEQ
		   AND ASSEMBLY     = ASSEMBLY
		   AND PROCESS_SEQ  = PROCESS_SEQ
		   AND PROCESS_CODE = PROCESS_CODE
		   AND PRIORITY_WORK_CNTR_CD = MIN_PRIORITY_WORK_CNTR_CD
		ORDER BY SALE_OPP_NO
		       , PJT_SHIP
		       , SHIP_SEQ DESC
		       , SHIP_SEQ_LOT
		       , ASSEMBLY_SEQ
		       , PROCESS_SEQ
		       , WORK_CNTR_SEQ;

        ---------------------------------------------------------------------
        -- (C) FJ 분할개수 계산 (프로젝트마다 재계산)
        ---------------------------------------------------------------------
        INSERT INTO #FJ_CNT_CALC(ASSEMBLY_SEQ, ASSEMBLY, FJ_CNT)
        SELECT ASSEMBLY_SEQ
             , ASSEMBLY
             , CAST(CEILING(ASSEMBLY /
                    NULLIF(MAX(CASE WHEN ATWRT02 = '3' AND PROCESS_CODE = 'UST' THEN (CASE WHEN WORK_CNTR_CD = ATTRIBUTE2 THEN SIMUL_VALUE1 ELSE SIMUL_VALUE1 END) 
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
            SUL_NO, REV_SEQ, ASSEMBLY_SEQ, ASSEMBLY, FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
            PROCESS_SEQ, PROCESS_CODE, WORK_CNTR_SEQ, WORK_CNTR_CD,
            EQUIP_SPEED, LEAD_TIME,
            PREPARE_DAYS,
            WORK_DAYS,
            PRD_STRT_DATE
        )
        SELECT 
               CAST(@SEQ + T.PRODUCTION_SEQ AS INT) AS SEQ
             , T.PRODUCTION_SEQ
             , T.SALE_OPP_NO
             , CONCAT(CAST(T.PJT_SHIP AS VARCHAR(3)), '-', CAST(T.SHIP_SEQ AS VARCHAR(3)), '-', CAST(T.SHIP_SEQ_LOT AS VARCHAR(3))) AS LOT_NO
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
             , T.WORK_CNTR_CD
             , T.EQUIP_SPEED
             , ISNULL(CALC.LEAD_TIME, 0) AS LEAD_TIME
             , T.SIMUL_PREPARE_DAYS
             , T.SIMUL_WORK_DAYS
             , T.PRD_STRT_DATE
        FROM (
            SELECT 
--                   RANK () OVER(ORDER BY PRD_CNFM_STRT_DATE ASC) PRODUCTION_SEQ
                   ROW_NUMBER() OVER(
                       ORDER BY T1.SALE_OPP_NO, T1.PJT_SHIP, T1.SHIP_SEQ DESC, T1.SHIP_SEQ_LOT, T1.ASSEMBLY_SEQ,
                                N.CREATE_FJ_NUMBER, T1.PROCESS_SEQ, T1.WORK_CNTR_SEQ
                   ) AS PRODUCTION_SEQ
                 , T1.SALE_OPP_NO
                 , T1.PJT_SHIP
                 , T1.SHIP_SEQ
                 , T1.SHIP_SEQ_LOT
                 , T1.SUL_NO
				 , T1.REV_SEQ
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
                 , CASE WHEN T1.LOT_PRD_STRT_DATE IS NOT NULL AND N.CREATE_FJ_NUMBER = 1                         THEN T1.LOT_PRD_STRT_DATE
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
	                 -- 연선, 횡권 
	                 WHEN T.PROCESS_CODE IN ('CST', 'WSD')
	                      THEN ROUND(((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(T.ATWRT02 AS INT) ,0 )
	
	                 -- 절연
  	                 WHEN T.PROCESS_CODE = 'INS' AND T.ATWRT02 = '3'
	                      THEN ROUND(((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(T.ATWRT02 AS INT) ,0 )
	                 WHEN T.PROCESS_CODE = 'INS' AND T.ATWRT02 = '1'
	                      THEN ROUND((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE1, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0) ,0 )
	
	                 -- 연피쉬스
	                 WHEN T.PROCESS_CODE = 'LDS' AND T.WORK_CNTR_CD LIKE 'LDS%'
	                      THEN ROUND(((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(T.ATWRT02 AS INT) ,0 )
	                
	                 WHEN T.PROCESS_CODE = 'LDS' AND T.WORK_CNTR_CD LIKE 'RWDL%' THEN 0
	                 
	                 WHEN T.PROCESS_CODE = 'LDS' AND T.WORK_CNTR_CD LIKE 'FJYL%' THEN COALESCE(T.SIMUL_WORK_DAYS, T.SIMUL_VALUE1)
	
	                 -- 수직연합
	                 WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'REW%' THEN CEILING(1 + (CAST(T.FJ_ASSEMBLY AS FLOAT) * 3) / 10000)
	                
	                 WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'FJT%' THEN COALESCE(T.SIMUL_WORK_DAYS, T.SIMUL_VALUE1)
	                
	                 WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'UST%' AND T.ATWRT02 = '3'
	                      THEN ROUND((T.FJ_ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) * (CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0) ,0 )
	                     
	                 WHEN T.PROCESS_CODE = 'UST' AND T.WORK_CNTR_CD LIKE 'UST%' AND T.ATWRT02 = '1' THEN 0
	
	                 -- 외장
	                 WHEN T.PROCESS_CODE = 'SMA'
	                      THEN ROUND(((T.ASSEMBLY / NULLIF(T.SIMUL_VALUE4, 0)) + ISNULL(T.SIMUL_PREPARE_DAYS, 0)) * CAST(7 AS FLOAT) / NULLIF(T.SIMUL_WORK_DAYS, 0) ,0 )
                END AS LEAD_TIME
        ) CALC;

        SET @SEQ = @SEQ + @@ROWCOUNT;
        
        FETCH NEXT FROM CUR_PJT INTO @SALE_OPP_NO;
    END

    CLOSE CUR_PJT;
    DEALLOCATE CUR_PJT;

    SELECT 'P' AS DATA_KIND
         , SEQ
         , PRODUCTION_SEQ
         , SALE_OPP_NO
         , LOT_NO
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
         , PREPARE_DAYS
         , WORK_DAYS
         , PRD_STRT_DATE
         , NULL
         , NULL
         , NULL
    FROM @RESULT
    WHERE LEAD_TIME > 0
    ORDER BY SEQ,PRODUCTION_SEQ;
END;
