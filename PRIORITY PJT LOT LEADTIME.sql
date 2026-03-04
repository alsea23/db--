SELECT 
       SALE_OPP_NO
     , PJT_SHIP
     , SHIP_SEQ
     , SHIP_SEQ_LOT
     , SUL_NO
     , ASSEMBLY_SEQ
     , ASSEMBLY
     , PROCESS_SEQ
     , PROCESS_CODE
     , WORK_CNTR_SEQ
     , WORK_CNTR_CD
     , PJT_WORK_CNTR_CD
     , LOT_WORK_CNTR_CD
     , EQUIP_SPEED
     , 0 AS LEAD_TIME
     , SIMUL_PREPARE_DAYS
     , SIMUL_WORK_DAYS
     , PRD_STRT_DATE
     , PJT_PRD_STRT_DATE
     , LOT_PRD_STRT_DATE
     , SIMUL_VALUE1
     , SIMUL_VALUE2
     , SIMUL_VALUE3
     , SIMUL_VALUE4
     , SIMUL_SEQ
     , ATWRT01
     , ATWRT02
     , ATWRT03
     , ATWRT04
     , ATWRT05
  FROM (
        SELECT LIST.*
             , DATA.*
	         , MIN(LIST.PRIORITY_WORK_CNTR_CD) OVER (PARTITION BY LIST.SALE_OPP_NO
	                                                            , LIST.PJT_SHIP
	                                                            , LIST.SHIP_SEQ
	                                                            , LIST.SHIP_SEQ_LOT
	                                                            , LIST.SUL_NO
	                                                            , LIST.ASSEMBLY_SEQ
	                                                            , LIST.ASSEMBLY
	                                                            , LIST.PROCESS_SEQ
	                                                            , LIST.PROCESS_CODE) AS MIN_PRIORITY_WORK_CNTR_CD
            --    LIST.SALE_OPP_NO
            --  , LIST.PJT_SHIP
            --  , LIST.SHIP_SEQ
            --  , LIST.SHIP_SEQ_LOT
            --  , LIST.SUL_NO
            --  , LIST.ASSEMBLY_SEQ
            --  , LIST.ASSEMBLY
            --  , LIST.PROCESS_SEQ
            --  , LIST.PROCESS_CODE
            --  , LIST.WORK_CNTR_SEQ
            --  , LIST.WORK_CNTR_CD
            --  , LIST.PJT_WORK_CNTR_CD
            --  , LIST.LOT_WORK_CNTR_CD
            --  , DATA.EQUIP_SPEED
            --  , 0 AS LEAD_TIME
            --  , LIST.SIMUL_PREPARE_DAYS
            --  , LIST.SIMUL_WORK_DAYS
            --  , LIST.PRD_STRT_DATE
            --  , LIST.PJT_PRD_STRT_DATE
            --  , LIST.LOT_PRD_STRT_DATE
            --  , DATA.SIMUL_VALUE1
            --  , DATA.SIMUL_VALUE2
            --  , DATA.SIMUL_VALUE3
            --  , DATA.SIMUL_VALUE4
            --  , LIST.SIMUL_SEQ
            --  , LIST.ATWRT01
            --  , LIST.ATWRT02
            --  , LIST.ATWRT03
            --  , LIST.ATWRT04
            --  , LIST.ATWRT05
        FROM (
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
                     , PJT.WORK_CNTR_CD AS PJT_WORK_CNTR_CD
                     , LOT.WORK_CNTR_CD AS LOT_WORK_CNTR_CD
                     , CASE WHEN PJT.WORK_CNTR_CD IS NOT NULL THEN 1
                            WHEN LOT.WORK_CNTR_CD IS NOT NULL THEN 2
                            ELSE 3
                       END AS PRIORITY_WORK_CNTR_CD
                     , COALESCE(LOT.WORK_DAYS    , PJT.WORK_DAYS    , DEF.WORK_DAYS    ) AS SIMUL_WORK_DAYS
                     , COALESCE(LOT.PREPARE_DAYS , PJT.PREPARE_DAYS , DEF.PREPARE_DAYS ) AS SIMUL_PREPARE_DAYS
                     , COALESCE(LOT.PRD_STRT_DATE, PJT.PRD_STRT_DATE, DEF.PRD_STRT_DATE) AS PRD_STRT_DATE
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
                                WHERE T1.SALE_OPP_NO = 'P2301029' -- @SALE_OPP_NO
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
 WHERE SALE_OPP_NO = SALE_OPP_NO
   AND PJT_SHIP = PJT_SHIP
   AND SHIP_SEQ = SHIP_SEQ
   AND SHIP_SEQ_LOT = SHIP_SEQ_LOT
   AND SUL_NO = SUL_NO
   AND ASSEMBLY_SEQ = ASSEMBLY_SEQ
   AND ASSEMBLY = ASSEMBLY
   AND PROCESS_SEQ = PROCESS_SEQ
   AND PROCESS_CODE = PROCESS_CODEPRIORITY_WORK_CNTR_CD = MIN_PRIORITY_WORK_CNTR_CD
ORDER BY SALE_OPP_NO
       , PJT_SHIP
       , SHIP_SEQ DESC
       , SHIP_SEQ_LOT
       , ASSEMBLY_SEQ
       , PROCESS_SEQ
       , WORK_CNTR_SEQ