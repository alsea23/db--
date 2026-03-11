CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_FIXED_PJT_LOT_TIMESTAMP]
(
      @P_VERSION            VARCHAR(50)  -- FIX 데이터를 읽을 VersionCode
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

    SELECT 'F' AS DATA_KIND
          -- ✅ 전체 데이터 전역 순번
         , ROW_NUMBER() OVER
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
        , CONCAT(CAST(T1.PJT_SHIP AS VARCHAR(3)), '-', CAST(T1.SHIP_SEQ AS VARCHAR(3)), '-', CAST(T1.SHIP_SEQ_LOT AS VARCHAR(3))) AS LOT_NO
        , T1.PJT_SHIP
        , T1.SHIP_SEQ
        , T1.SHIP_SEQ_LOT
        , T1.SUL_NO
		, T1.REV_SEQ
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
        , NULL
        , NULL
        , NULL
        , T1.PRD_CNFM_STRT_DATE
        , T1.PRD_CNFM_END_DATE
        , DATEDIFF(DAY, @TIMESTAMP_BASE_DATE, T1.PRD_CNFM_STRT_DATE) AS FIXED_TIMESTAMP_POINT -- ✅ BaseDate 기준 FIX Timestamp (Day offset)
    FROM SOP_DB.dbo.TB_SIMUL_VERSION_DATA T1
         INNER JOIN @P_FIXED_PJT_LIST F
                ON T1.SALE_OPP_NO = F.SALE_OPP_NO
    WHERE T1.SIMUL_VERSION = @P_VERSION
      AND T1.LEAD_TIME > 0
    ORDER BY SEQ, PRODUCTION_SEQ;

END;