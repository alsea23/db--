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

END
