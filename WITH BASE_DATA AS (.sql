WITH BASE_DATA AS (
    /* 1. 실제 스케줄링된 데이터 및 총조장(ASSEMBLY), 전체 작업일수 추출 */
    SELECT
          T.SALE_OPP_NO
        , T.PJT_SHIP
        , T.SHIP_SEQ
        , T.SHIP_SEQ_LOT
        -- ASSEMBLY(총조장) 컬럼의 콤마 제거 후 숫자형 변환
        , T.ASSEMBLY
        , T.FJ_ASSEMBLY 
        , T.PROCESS_CODE
        , T.WORK_CNTR_CD
        , T.EQUIP_SPEED
        , T.PRD_CNFM_STRT_DATE AS START_DATE
        , T.PRD_CNFM_END_DATE  AS END_DATE
        -- 전체 작업 기간 (분모로 사용)
--        , T.LEAD_TIME AS TOTAL_WORK_DAYS
        , DATEDIFF(DAY, T.PRD_CNFM_STRT_DATE, T.PRD_CNFM_END_DATE) + 1 AS TOTAL_WORK_DAYS
        , MIN(T.PRD_CNFM_STRT_DATE) OVER() AS MIN_WORK_DATE
        , MAX(T.PRD_CNFM_END_DATE)  OVER() AS MAX_WORK_DATE
    FROM SOP_DB.dbo.TB_SIMUL_VERSION_DATA T
    WHERE T.SIMUL_VERSION = :VS_SIMUL_VERSION
      AND T.LEAD_TIME > 0
),

MONTH_CALENDAR AS (
    /* 2. 전체 기간에 대한 월별 달력 생성 */
    SELECT
           DATEFROMPARTS(YEAR(MIN_WORK_DATE), MONTH(MIN_WORK_DATE), 1) AS YM
         , DATEFROMPARTS(YEAR(MAX_WORK_DATE), MONTH(MAX_WORK_DATE), 1) AS MAX_YM
      FROM BASE_DATA
     GROUP BY MIN_WORK_DATE, MAX_WORK_DATE
    
     UNION ALL
    
    SELECT DATEADD(MONTH, 1, YM), MAX_YM
      FROM MONTH_CALENDAR
     WHERE DATEADD(MONTH, 1, YM) <= MAX_YM
),

ACTUAL_WORK_DAYS AS (
    /* 3. LOT/공정/월별 실제 점유 일수 계산 */
    SELECT 
           B.SALE_OPP_NO
         , B.PJT_SHIP
         , B.SHIP_SEQ
         , B.SHIP_SEQ_LOT
         , B.ASSEMBLY
         , B.FJ_ASSEMBLY
         , B.TOTAL_WORK_DAYS
         , B.PROCESS_CODE
         , B.WORK_CNTR_CD
         , B.EQUIP_SPEED
         , C.YM
         , SUM(CASE 
               WHEN CASE WHEN B.START_DATE > C.YM THEN B.START_DATE ELSE C.YM END
                    > CASE WHEN B.END_DATE < EOMONTH(C.YM) THEN B.END_DATE ELSE EOMONTH(C.YM) END
               THEN 0
               ELSE DATEDIFF(DAY, 
                             CASE WHEN B.START_DATE > C.YM THEN B.START_DATE ELSE C.YM END,
                             CASE WHEN B.END_DATE < EOMONTH(C.YM) THEN B.END_DATE ELSE EOMONTH(C.YM) END
                    ) + 1
               END) AS MONTHLY_WORK_DAYS
         , SUM(SUM(CASE 
                   WHEN CASE WHEN B.START_DATE > C.YM THEN B.START_DATE ELSE C.YM END
                        > CASE WHEN B.END_DATE < EOMONTH(C.YM) THEN B.END_DATE ELSE EOMONTH(C.YM) END
                   THEN 0
                   ELSE DATEDIFF(DAY, 
                                 CASE WHEN B.START_DATE > C.YM THEN B.START_DATE ELSE C.YM END,
                                 CASE WHEN B.END_DATE < EOMONTH(C.YM) THEN B.END_DATE ELSE EOMONTH(C.YM) END
                        ) + 1
                   END)
              ) OVER( PARTITION BY B.SALE_OPP_NO
                                 , B.PJT_SHIP
                                 , B.SHIP_SEQ
                                 , B.SHIP_SEQ_LOT
                                 , B.ASSEMBLY
                                 , B.FJ_ASSEMBLY
                                 , B.PROCESS_CODE
                                 , B.WORK_CNTR_CD
                    ) AS GROUP_TOTAL_WORK_DAYS
      FROM BASE_DATA B
           INNER JOIN MONTH_CALENDAR C  
                   ON B.START_DATE <= EOMONTH(C.YM) 
                  AND B.END_DATE >= C.YM
      GROUP BY B.SALE_OPP_NO
             , B.PJT_SHIP
             , B.SHIP_SEQ
             , B.SHIP_SEQ_LOT
             , B.ASSEMBLY
             , B.FJ_ASSEMBLY
             , B.TOTAL_WORK_DAYS
             , B.PROCESS_CODE
             , B.WORK_CNTR_CD
             , B.EQUIP_SPEED
             , C.YM
),

MASTER_INFO AS (
    /* 4. 공정/설비 마스터 정보 */
    SELECT
           WC.SIMUL_PROCESS_CD
         , PC.PROCESS_NAME
         , WC.WORK_CNTR_CD
         , WC.SIMUL_WORK_CNTR_NM
         , WC.SIMUL_SEQ
         , PC.STANDARD_PERSON
         , PC.PROCESS_PRICE
      FROM SOP_DB.dbo.TB_SIMUL_WORK_CENTER_MST WC
           INNER JOIN (
               SELECT T1.PROCESS_CODE
                    , T1.PROCESS_NAME
                    , T1.STANDARD_PERSON
                    , T2.PROCESS_PRICE
                 FROM SOP_DB.dbo.TB_SIMUL_FAC_PC T1
                      INNER JOIN QUOTATION_DATA.dbo.TB_FAC_PC T2 
                              ON T1.ATTRIBUTE1 = T2.PROCESS_CODE 
                             AND T1.Q_YEAR = T2.Q_YEAR
                WHERE T1.Q_YEAR = YEAR(GETDATE())
                  AND T2.PL_CENTER = '해저케이블'
           ) PC ON WC.SIMUL_PROCESS_CD = PC.PROCESS_CODE
    WHERE WC.CATEGORY = '1' 
      AND WC.PLANT_CD = '1802'
)

/* 5. 최종 결과: 월별 생산량(ASSEMBLY 배분) 및 가공비 산출 */
SELECT
      M.SIMUL_PROCESS_CD
    , M.PROCESS_NAME
    , M.WORK_CNTR_CD
    , M.SIMUL_SEQ
    , M.SIMUL_WORK_CNTR_NM
    , CONVERT(VARCHAR(7), C.YM, 120) AS YM
    , W.SALE_OPP_NO
    
    , CONCAT(CAST(W.PJT_SHIP AS VARCHAR(3)), '-', CAST(W.SHIP_SEQ AS VARCHAR(3)), '-', CAST(W.SHIP_SEQ_LOT AS VARCHAR(3))) AS LOT_NO
--    , W.PJT_SHIP
--    , W.SHIP_SEQ
--    , W.SHIP_SEQ_LOT
    
    /* 1) 월별 생산량 계산: 총조장 * (월 작업일 / 전체 작업일) */
    , ROUND(ISNULL(W.FJ_ASSEMBLY * ((CAST(W.MONTHLY_WORK_DAYS AS FLOAT)) / GROUP_TOTAL_WORK_DAYS), 0), 2) AS PRD_ASSEMBLY
    
    /* 1) 월별 생산량 계산: 총조장 * (월 작업일 / 22) */
    --, ROUND(ISNULL(W.FJ_ASSEMBLY * ((CAST(W.MONTHLY_WORK_DAYS AS FLOAT)) / 22), 0), 2) AS [월_생산량_ASSEMBLY2]
    /* 1) 월별 생산량 계산: 총조장 * (월 작업일 / 전체 작업일) */
    --, ROUND(ISNULL(W.FJ_ASSEMBLY * (CAST(W.MONTHLY_WORK_DAYS AS FLOAT) / NULLIF(W.TOTAL_WORK_DAYS, 0)), 0), 2) AS [월_생산량_ASSEMBLY]
    /* 2) 가동률 계산: 월 작업일 / 표준 영업일(22일) */
    --, ROUND(CAST(ISNULL(W.MONTHLY_WORK_DAYS, 0) AS FLOAT) / 22 * 100, 2) AS [가동률_PERCENT]
    
    /* 3) 가공비 계산: 월 작업일 * 인원 * 단가 */
    , CAST(ISNULL(W.MONTHLY_WORK_DAYS, 0) AS FLOAT) * M.STANDARD_PERSON * M.PROCESS_PRICE AS TOTAL_PROC_COST
    , 22 AS WEEK_DAYS
    , W.MONTHLY_WORK_DAYS
    , W.TOTAL_WORK_DAYS
    , W.GROUP_TOTAL_WORK_DAYS
    , W.FJ_ASSEMBLY
    , W.ASSEMBLY
    , W.EQUIP_SPEED
   
FROM MASTER_INFO M
     CROSS JOIN MONTH_CALENDAR C
     LEFT JOIN ACTUAL_WORK_DAYS W 
            ON M.SIMUL_PROCESS_CD = W.PROCESS_CODE 
           AND M.WORK_CNTR_CD     = W.WORK_CNTR_CD 
           AND C.YM               = W.YM
ORDER BY M.SIMUL_SEQ, C.YM