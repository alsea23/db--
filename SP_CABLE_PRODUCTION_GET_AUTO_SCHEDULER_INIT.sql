CREATE   PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER_INIT]
(
      @TIMESTAMP_BASE_DATE            DATE
    , @TB_PROCESS_WORK_CNTR_TIMESTAMP dbo.UDT_MST_PROCESS_WORK_CNTR_TIMESTAMP READONLY
    , @TB_PRIORITY_PJT_LOT_LEADTIME   dbo.UDT_DATA_CABLE_PJT_LOT_FJ READONLY
    , @TB_FIXED_PJT_LOT_TIMESTAMP     dbo.UDT_DATA_CABLE_PJT_LOT_FJ READONLY
)
AS
BEGIN
    SET NOCOUNT ON;

    /* =====================================================================================
       0) TEMP: WORK_CNTR_TIMESTAMP
    ===================================================================================== */

    INSERT INTO #WORK_CNTR_TIMESTAMP(SEQ, PROCESS_CODE, WORK_CNTR_CD, TIMESTAMP_POINT)
    SELECT SEQ, PROCESS_CODE, WORK_CNTR_CD, TIMESTAMP_POINT 
    FROM @TB_PROCESS_WORK_CNTR_TIMESTAMP;

    CREATE UNIQUE CLUSTERED INDEX CX_WCT ON #WORK_CNTR_TIMESTAMP(PROCESS_CODE, SEQ);
    CREATE INDEX IX_WCT_CD ON #WORK_CNTR_TIMESTAMP(PROCESS_CODE, WORK_CNTR_CD);

    /* =====================================================================================
       1) TEMP: FIXED_PJT_LOT_DATA
    ===================================================================================== */
 
    INSERT INTO #FIXED_PJT_LOT_DATA
    (
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        PROCESS_SEQ, PROCESS_CODE,
        WORK_CNTR_SEQ, WORK_CNTR_CD,
        FIX_START_TS, FIX_END_TS
    )
    SELECT
          F.SALE_OPP_NO
        , F.PJT_SHIP
        , F.SHIP_SEQ
        , F.SHIP_SEQ_LOT
        , F.PROCESS_SEQ
        , F.PROCESS_CODE
        , F.WORK_CNTR_SEQ
        , F.WORK_CNTR_CD
        , MIN(
              COALESCE(
                  F.FIXED_TIMESTAMP_POINT,
                  CASE WHEN F.PRD_CNFM_STRT_DATE IS NULL THEN 0
                       ELSE DATEDIFF(DAY, @TIMESTAMP_BASE_DATE, F.PRD_CNFM_STRT_DATE)
                  END
              )
          ) AS FIX_START_TS
        , MIN(
              COALESCE(
                  F.FIXED_TIMESTAMP_POINT,
                  CASE WHEN F.PRD_CNFM_STRT_DATE IS NULL THEN 0
                       ELSE DATEDIFF(DAY, @TIMESTAMP_BASE_DATE, F.PRD_CNFM_STRT_DATE)
                  END
              )
          )
          + SUM(CAST(ISNULL(F.LEAD_TIME,0) AS INT)) AS FIX_END_TS
    FROM @TB_FIXED_PJT_LOT_TIMESTAMP F
    GROUP BY
          F.SALE_OPP_NO, F.PJT_SHIP, F.SHIP_SEQ, F.SHIP_SEQ_LOT,
          F.PROCESS_SEQ, F.PROCESS_CODE, F.WORK_CNTR_SEQ, F.WORK_CNTR_CD;

    CREATE INDEX IX_FIX ON #FIXED_PJT_LOT_DATA(PROCESS_CODE, WORK_CNTR_CD, FIX_START_TS, FIX_END_TS);

    /* =====================================================================================
       2) TEMP: ASSIGN_BLOCK_DATA
    ===================================================================================== */

    INSERT INTO #ASSIGN_BLOCK_DATA
    (
        ASSIGN_SEQ, SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, SUL_NO, REV_SEQ, ATWRT02, 
		ASSEMBLY_SEQ, ASSEMBLY,
        PROCESS_SEQ, PROCESS_CODE
    )
    SELECT
          MIN(P.SEQ) AS ASSIGN_SEQ
        , P.SALE_OPP_NO
        , P.PJT_SHIP
        , P.SHIP_SEQ
        , P.SHIP_SEQ_LOT
        , P.LOT_NO
		, P.SUL_NO
		, PL.REV_SEQ
        , CS.ATWRT02
        , P.ASSEMBLY_SEQ
        , P.ASSEMBLY
        , P.PROCESS_SEQ
        , P.PROCESS_CODE
    FROM @TB_PRIORITY_PJT_LOT_LEADTIME P                         
         INNER JOIN SOP_DB.dbo.TB_PRD_PLAN_LIST PL
                 ON P.SALE_OPP_NO  = PL.SALE_OPP_NO
                AND P.PJT_SHIP     = PL.PJT_SHIP
                AND P.SHIP_SEQ     = PL.SHIP_SEQ
                AND P.SHIP_SEQ_LOT = PL.SHIP_SEQ_LOT
                AND P.SUL_NO       = PL.SUL_NO
         LEFT OUTER JOIN SOP_DB.dbo.TB_PRD_PLAN_CABLE_SPEC CS
                      ON PL.SUL_NO  = CS.SUL_NO
                     AND PL.REV_SEQ = CS.REV_SEQ
    GROUP BY
          P.SALE_OPP_NO, P.PJT_SHIP, P.SHIP_SEQ, P.SHIP_SEQ_LOT,
          P.LOT_NO, P.SUL_NO, PL.REV_SEQ, CS.ATWRT02,
		  P.ASSEMBLY_SEQ, P.ASSEMBLY,
          P.PROCESS_SEQ, P.PROCESS_CODE
    ORDER BY ASSIGN_SEQ;

    CREATE CLUSTERED INDEX CX_ASSIGN_BLOCK ON #ASSIGN_BLOCK_DATA(ASSIGN_SEQ);

    /* =====================================================================================
       3) TEMP: LOT_ORDER
    ===================================================================================== */

    INSERT INTO #LOT_ORDER
    (
        ASSIGN_SEQ,
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, SUL_NO, REV_SEQ, ATWRT02, 
		ASSEMBLY_SEQ, ASSEMBLY,
        PROCESS_SEQ, PROCESS_CODE
    )
    SELECT
        A.ASSIGN_SEQ,
        A.SALE_OPP_NO, A.PJT_SHIP, A.SHIP_SEQ, A.SHIP_SEQ_LOT,
        A.LOT_NO, A.SUL_NO, A.REV_SEQ, A.ATWRT02, 
		A.ASSEMBLY_SEQ, A.ASSEMBLY,
        A.PROCESS_SEQ, A.PROCESS_CODE
    FROM #ASSIGN_BLOCK_DATA A
    ORDER BY A.ASSIGN_SEQ;

    CREATE UNIQUE CLUSTERED INDEX CX_LOT_ORDER ON #LOT_ORDER(LOT_ORDER_ID);

    /* =====================================================================================
       4) TEMP: SRC_FJ
    ===================================================================================== */


    INSERT INTO #SRC_FJ
    (
        SEQ, SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, SUL_NO, ASSEMBLY_SEQ, ASSEMBLY,
        FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
        PROCESS_SEQ, PROCESS_CODE,
        WORK_CNTR_SEQ, WORK_CNTR_CD,
        EQUIP_SPEED, LEAD_TIME_DAYS,
        PREPARE_DAYS, WORK_DAYS, PRD_STRT_DATE
    )
    SELECT
          P.SEQ
        , P.SALE_OPP_NO, P.PJT_SHIP, P.SHIP_SEQ, P.SHIP_SEQ_LOT
        , P.LOT_NO, P.SUL_NO, P.ASSEMBLY_SEQ, P.ASSEMBLY
        , P.FJ_ASSEMBLY_SEQ, P.FJ_ASSEMBLY
        , P.PROCESS_SEQ, P.PROCESS_CODE
        , P.WORK_CNTR_SEQ, P.WORK_CNTR_CD
        , P.EQUIP_SPEED
        , CAST(ISNULL(P.LEAD_TIME,0) AS INT) AS LEAD_TIME_DAYS
        , P.PREPARE_DAYS
        , P.WORK_DAYS
        , P.PRD_STRT_DATE
    FROM @TB_PRIORITY_PJT_LOT_LEADTIME P
    ORDER BY P.SEQ;

    CREATE INDEX IX_SRCFJ ON #SRC_FJ
    (
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, ASSEMBLY_SEQ, FJ_ASSEMBLY_SEQ,
        PROCESS_CODE, WORK_CNTR_CD
    );


    /* =====================================================================================
       6) TEMP: FJ_READY_TS
    ===================================================================================== */


    INSERT INTO #FJ_READY_TS
    (
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, ASSEMBLY_SEQ, FJ_ASSEMBLY_SEQ, READY_TS
    )
    SELECT DISTINCT
          S.SALE_OPP_NO
        , S.PJT_SHIP
        , S.SHIP_SEQ
        , S.SHIP_SEQ_LOT
        , S.LOT_NO
        , S.ASSEMBLY_SEQ
        , S.FJ_ASSEMBLY_SEQ
        , 0
    FROM #SRC_FJ S
    ORDER BY SALE_OPP_NO, PJT_SHIP, SHIP_SEQ DESC, SHIP_SEQ_LOT, ASSEMBLY_SEQ, FJ_ASSEMBLY_SEQ;
    

    /* =====================================================================================
       7) TEMP: WC_GRP
    ===================================================================================== */
	INSERT INTO #WC_GRP(SIMUL_PROCESS_CD, GRP_WORK_CNTR_CD, WORK_CNTR_CD)
	SELECT SIMUL_PROCESS_CD, GRP_WORK_CNTR_CD, WORK_CNTR_CD
	FROM TB_SIMUL_WORK_CENTER_GRP;
	
	CREATE INDEX IX_WC_GRP_1 ON #WC_GRP(SIMUL_PROCESS_CD, GRP_WORK_CNTR_CD);
	CREATE INDEX IX_WC_GRP_2 ON #WC_GRP(SIMUL_PROCESS_CD, WORK_CNTR_CD);

END
