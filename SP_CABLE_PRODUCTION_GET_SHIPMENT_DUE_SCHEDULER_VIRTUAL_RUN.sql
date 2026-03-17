CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_SHIPMENT_DUE_SCHEDULER_VIRTUAL_RUN]
(
      @P_SALE_OPP_NO    VARCHAR(20)
    , @P_PJT_SHIP       INT
    , @P_PREP_PROC_DAYS INT = 1
    , @P_PREP_FJ_DAYS   INT = 1
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE
          @SALE_OPP_NO    VARCHAR(20)
        , @PJT_SHIP       INT
        , @SHIP_SEQ       INT
        , @SHIP_SEQ_LOT   INT
        , @LOT_NO         VARCHAR(50)
        , @ATWRT02        VARCHAR(50) -- ✅ 케이블 코어
        , @ASSEMBLY_SEQ   INT
        , @ASSEMBLY       FLOAT

        , @FJ_ASM_SEQ     INT
        , @FJ_ASSEMBLY    FLOAT
        , @FJ_LAST_ASM_SEQ INT   -- ✅ SMA 공정에서 "마지막 FJ만" 처리할 때 사용

        , @SUL_NO         VARCHAR(50)
        , @REV_SEQ        VARCHAR(4)
        , @EQUIP_SPEED    FLOAT
        , @LEAD_TIME      INT
        , @READY_TS       INT     -- 공정 시작 가능 시점(이전 공정 종료 + 준비일)
        , @START_TS       INT     -- 실제 시작 TS
        , @END_TS         INT     -- 실제 종료 TS
        , @WORK_CNTR_SEQ  INT

        , @CHOSEN_GRP_EQUIP VARCHAR(20) -- LDS/UST 그룹 대표 설비
        , @CUR_GRP_TS       INT          -- 그룹 대표 설비의 현재 TS(선택 기준)

        , @PROCESS_SEQ    INT
        , @PROCESS_CODE   VARCHAR(20)
        , @CHOSEN_EQUIP   VARCHAR(20)   -- 실제 배정 설비 (단일 or 그룹 내부 서브 설비)
        , @CUR_TS         INT            -- 해당 설비의 현재 TS
        
    /* =================================================================================
       임시 테이블 생성
    ================================================================================= */
    IF OBJECT_ID('tempdb..#PROC_LIST') IS NOT NULL DROP TABLE #PROC_LIST;
    CREATE TABLE #PROC_LIST
    (
        RN            INT IDENTITY(1,1) NOT NULL,
        PROCESS_SEQ   INT               NULL,
        PROCESS_CODE  VARCHAR(20)       NULL
    );

    IF OBJECT_ID('tempdb..#FJ_LIST') IS NOT NULL DROP TABLE #FJ_LIST;
    CREATE TABLE #FJ_LIST
    (
        RN              INT IDENTITY(1,1) NOT NULL,
        FJ_ASSEMBLY_SEQ INT               NOT NULL
    );

    IF OBJECT_ID('tempdb..#SUB_EQUIP_LIST') IS NOT NULL DROP TABLE #SUB_EQUIP_LIST;
    CREATE TABLE #SUB_EQUIP_LIST
    (
        RN            INT IDENTITY(1,1) NOT NULL,
        WORK_CNTR_SEQ INT NULL,
        WORK_CNTR_CD  NVARCHAR(20) NOT NULL
    );
        
    IF OBJECT_ID('tempdb..#LOT_WORK_CNTR_LIST') IS NOT NULL DROP TABLE #LOT_WORK_CNTR_LIST;
    CREATE TABLE #LOT_WORK_CNTR_LIST
    (
        LOT_NO        VARCHAR(50) NULL,
        PROCESS_CODE  VARCHAR(20) NULL,
        WORK_CNTR_CD  VARCHAR(20) NULL
    );
    /* 1. 명세서 규칙에 따라 변수 추출 및 업데이트 수행
    */

    -- 변수 선언 및 값 할당
    DECLARE @SHIPMENT_TIMESTAMP INT;

    SELECT @SHIPMENT_TIMESTAMP = SHIPMENT_TIMESTAMP 
      FROM #SHIPMENT_TIMESTAMP
    WHERE SALE_OPP_NO = @P_SALE_OPP_NO
      AND PJT_SHIP    = @P_PJT_SHIP;

    -- #WORK_CNTR_TIMESTAMP 업데이트
    UPDATE T
       SET T.TIMESTAMP_POINT = @SHIPMENT_TIMESTAMP
      FROM #WORK_CNTR_TIMESTAMP AS T

    /* =====================================================================================
       [CUR_LOT]
       - #LOT_ORDER에서 (SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, LOT_NO, ASSEMBLY_SEQ) 별로 1건만 뽑아
         LOT 단위 루프를 돈다.
       - ORDER BY로 처리 순서를 고정 (SHIP_SEQ DESC 포함)
    ===================================================================================== */
    DECLARE CUR_LOT CURSOR LOCAL FAST_FORWARD FOR
    SELECT
           SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, LOT_NO, SUL_NO, REV_SEQ, ATWRT02, ASSEMBLY_SEQ
    FROM #LOT_ORDER
    WHERE SALE_OPP_NO = @P_SALE_OPP_NO
      AND PJT_SHIP    = @P_PJT_SHIP
    GROUP BY
           SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, LOT_NO, SUL_NO, REV_SEQ, ATWRT02, ASSEMBLY_SEQ
    ORDER BY MIN(ASSIGN_SEQ) ASC; -- 그룹 내 가장 작은 ASSIGN_SEQ 기준으로 정렬!

    OPEN CUR_LOT;

    FETCH NEXT FROM CUR_LOT
    INTO @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT, @LOT_NO, @SUL_NO, @REV_SEQ, @ATWRT02, @ASSEMBLY_SEQ;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        /* =================================================================================
           1) 공정 리스트 만들기 (#PROC_LIST)
           - 해당 LOT/ASSEMBLY에서 수행해야 하는 공정들을 PROCESS_SEQ 기준으로 정렬해 RN 부여
        ================================================================================= */
        TRUNCATE TABLE #PROC_LIST;

        INSERT INTO #PROC_LIST(PROCESS_SEQ, PROCESS_CODE)        
        SELECT DISTINCT
               L.PROCESS_SEQ
             , L.PROCESS_CODE
          FROM #LOT_ORDER L
         WHERE L.SALE_OPP_NO  = @SALE_OPP_NO
           AND L.LOT_NO       = @LOT_NO
           AND L.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
         ORDER BY L.PROCESS_SEQ;

        /* =================================================================================
           2) LOT별 공정-설비 리스트 만들기 (#LOT_WORK_CNTR_LIST)
           - 해당 LOT/ASSEMBLY에서 수행해야 하는 공정-설비 리스트
        ================================================================================= */
        TRUNCATE TABLE #LOT_WORK_CNTR_LIST;

        INSERT INTO #LOT_WORK_CNTR_LIST(LOT_NO, PROCESS_CODE, WORK_CNTR_CD)      
        SELECT DISTINCT
               S.LOT_NO
             , S.PROCESS_CODE
             , S.WORK_CNTR_CD
          FROM #SRC_FJ S
         WHERE S.SALE_OPP_NO  = @SALE_OPP_NO
           AND S.LOT_NO       = @LOT_NO
           AND S.ASSEMBLY_SEQ = @ASSEMBLY_SEQ;
         
         
        /* =================================================================================
           2) FJ 리스트 만들기 (#FJ_LIST)
           - 해당 LOT/ASSEMBLY에서 흘려야 하는 FJ_ASSEMBLY_SEQ 목록
           - 이후 공정별로 이 FJ들을 순서대로 처리
        ================================================================================= */
        TRUNCATE TABLE #FJ_LIST;

        INSERT INTO #FJ_LIST(FJ_ASSEMBLY_SEQ)
        SELECT DISTINCT S.FJ_ASSEMBLY_SEQ
        FROM #SRC_FJ S
        WHERE S.SALE_OPP_NO  = @SALE_OPP_NO
          AND S.LOT_NO       = @LOT_NO
          AND S.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
        ORDER BY S.FJ_ASSEMBLY_SEQ;

        SELECT @FJ_LAST_ASM_SEQ = MAX(FJ_ASSEMBLY_SEQ)
        FROM #FJ_LIST;
                
        
        /* =================================================================================
           3) 파이프라인 실행: 공정(바깥) -> FJ(안쪽)
        ================================================================================= */
        DECLARE @P_RN INT = 1;
        DECLARE @P_CNT INT = (SELECT COUNT(*) FROM #PROC_LIST);

        WHILE @P_RN <= @P_CNT
        BEGIN
            SELECT
                  @PROCESS_SEQ  = PROCESS_SEQ
                , @PROCESS_CODE = PROCESS_CODE
            FROM #PROC_LIST
            WHERE RN = @P_RN;


            
            /* =============================================================================
               (A) 공정별 설비 선택
               - 기본은 #WORK_CNTR_TIMESTAMP에서 TS가 가장 작은 설비를 선택
               - [필터 적용] #LOT_WORK_CNTR_LIST에 존재하는 설비 중에서만 선택
                 (자동배정 대상 설비 제어)
               - LDS/UST는 그룹 설비 선택 로직
               - INS는 ATWRT02(코어) 값으로 후보 설비를 제한
            ============================================================================= */
            -- 변수를 미리 초기화하여 불필요한 NULL 체크 제거
            SET @CHOSEN_EQUIP     = '';
            SET @CUR_TS           = 0;
            SET @CHOSEN_GRP_EQUIP = '';
            SET @CUR_GRP_TS       = 0;

            IF @PROCESS_CODE IN ('LDS','UST')
            BEGIN
                SELECT TOP (1)
                      @CHOSEN_EQUIP = W.WORK_CNTR_CD
                    , @CUR_TS       = W.TIMESTAMP_POINT
                FROM #WORK_CNTR_TIMESTAMP W
                WHERE W.PROCESS_CODE = @PROCESS_CODE
                AND EXISTS (
                    SELECT 1 FROM #WC_GRP G
                     WHERE G.SIMUL_PROCESS_CD = @PROCESS_CODE
                       AND G.GRP_WORK_CNTR_CD = W.WORK_CNTR_CD
                       AND G.WORK_CNTR_CD     = W.WORK_CNTR_CD
                )
                -- [전술 1] Leadtime 데이터가 계산된(필터링된) 설비만 통과!
                AND EXISTS (
                    SELECT 1 FROM #LOT_WORK_CNTR_LIST LT   -- ※ 실제 Leadtime 임시테이블명으로 변경
                     WHERE LT.LOT_NO       = @LOT_NO       -- ※ 실제 LOT 식별 변수명으로 변경
                       AND LT.PROCESS_CODE = W.PROCESS_CODE
                       AND LT.WORK_CNTR_CD = W.WORK_CNTR_CD
                )
                ORDER BY W.TIMESTAMP_POINT ASC, W.SEQ ASC;

                -- 기존 로직 호환을 위해 GRP 변수에도 동일하게 세팅
                SET @CHOSEN_GRP_EQUIP = @CHOSEN_EQUIP;
                SET @CUR_GRP_TS       = @CUR_TS;
            END
            ELSE IF @PROCESS_CODE = 'INS'
            BEGIN
                /* ✅ INS 공정: ATWRT02(코어) 값으로 설비 후보군 제한 + Leadtime 필터 적용 */
                SELECT TOP (1)
                       @CHOSEN_EQUIP = W.WORK_CNTR_CD
                     , @CUR_TS       = W.TIMESTAMP_POINT
                FROM #WORK_CNTR_TIMESTAMP W
                WHERE W.PROCESS_CODE = @PROCESS_CODE
                AND (
                       (ISNULL(@ATWRT02, '') = '3'  AND W.WORK_CNTR_CD IN ('INS044', 'INS048', 'INS049'))
                    OR (ISNULL(@ATWRT02, '') = '1'  AND W.WORK_CNTR_CD IN ('INS050', 'INS051', 'INS052'))
                )
                -- [전술 1] Leadtime 데이터가 계산된(필터링된) 설비만 통과!
                AND EXISTS (
                    SELECT 1 FROM #LOT_WORK_CNTR_LIST LT   -- ※ 실제 Leadtime 임시테이블명으로 변경
                     WHERE LT.LOT_NO       = @LOT_NO       -- ※ 실제 LOT 식별 변수명으로 변경
                       AND LT.PROCESS_CODE = W.PROCESS_CODE
                       AND LT.WORK_CNTR_CD = W.WORK_CNTR_CD
                )
                ORDER BY W.TIMESTAMP_POINT ASC, W.SEQ ASC;
            END
            ELSE
            BEGIN
                /* ✅ 일반 공정: Leadtime 필터 적용 */
                SELECT TOP (1)
                    @CHOSEN_EQUIP = W.WORK_CNTR_CD
                    , @CUR_TS       = W.TIMESTAMP_POINT
                FROM #WORK_CNTR_TIMESTAMP W
                WHERE W.PROCESS_CODE = @PROCESS_CODE
                -- [전술 1] Leadtime 데이터가 계산된(필터링된) 설비만 통과!
                AND EXISTS (
                    SELECT 1 FROM #LOT_WORK_CNTR_LIST LT   -- ※ 실제 Leadtime 임시테이블명으로 변경
                    WHERE LT.LOT_NO       = @LOT_NO       -- ※ 실제 LOT 식별 변수명으로 변경
                        AND LT.PROCESS_CODE = W.PROCESS_CODE
                        AND LT.WORK_CNTR_CD = W.WORK_CNTR_CD
                )
                ORDER BY W.TIMESTAMP_POINT ASC, W.SEQ ASC;
            END

            /* =============================================================================
               (B) 이 공정에서 FJ들을 순서대로 처리
               - 공정은 고정(@PROCESS_CODE), FJ별로 READY/TS/충돌체크/누적을 진행
            ============================================================================= */
            DECLARE @FJ_RN INT = 1;
            DECLARE @FJ_CNT INT = (SELECT COUNT(*) FROM #FJ_LIST);

            WHILE @FJ_RN <= @FJ_CNT
            BEGIN
                SELECT @FJ_ASM_SEQ = FJ_ASSEMBLY_SEQ
                FROM #FJ_LIST
                WHERE RN = @FJ_RN;
            
                /* ------------------------------------------------------------
                   [Skip Rule 1] FJY/FJT 설비는 FJ_ASSEMBLY_SEQ >= 2부터만 처리
                   - (LDS/UST 그룹일 때만 의미있지만, 공정 레벨에서 한번 더 안전하게)
                ------------------------------------------------------------ */
                IF (@PROCESS_CODE IN ('LDS','UST'))
                   AND (LEFT(ISNULL(@CHOSEN_EQUIP, ''), 3) IN ('FJY', 'FJT')) 
                   AND (@FJ_ASM_SEQ < 2 )				
                BEGIN
                    SET @FJ_RN += 1;
                    CONTINUE;  -- 이 FJ는 통째로 스킵 (TS/결과/누적 모두 안 함)
                END

                /* ------------------------------------------------------------
                   [Skip Rule 2] SMA 공정은 마지막 FJ만 처리
                ------------------------------------------------------------ */
                IF (@PROCESS_CODE = 'SMA')
                   AND (ISNULL(@FJ_ASM_SEQ, 0) <> ISNULL(@FJ_LAST_ASM_SEQ, 0))
                BEGIN
                    SET @FJ_RN += 1;
                    CONTINUE;
                END

                
                /* =============================================================================
                   [핵심] LDS/UST 그룹 설비 전개
                   - 그룹 대표(@CHOSEN_GRP_EQUIP) 안의 서브 설비들을 WORK_CNTR_SEQ 순으로 "직렬" 처리
                   - 그룹 내부 직렬 READY는 @STAGE_READY_TS 로 유지
                   - 서브 설비별 TS, FIX 체크, 결과 insert, TS 누적을 각각 수행
                   - 그룹 공정이 끝나면 #FJ_READY_TS를 마지막 END 기준으로 갱신 후 CONTINUE
                ============================================================================= */
                IF @PROCESS_CODE IN ('LDS','UST')
                BEGIN
                    /* ------------------------------------------------------------
                       0) 현재 FJ에 대해, 선택된 그룹(@CHOSEN_GRP_EQUIP)에 속한 서브 설비 목록 구성
                          - 기본 정렬: WORK_CNTR_SEQ
                          - (옵션) UST 단계 순서를 prefix로 고정하고 싶으면 CASE ORDER BY 사용 가능
                    ------------------------------------------------------------ */
                    TRUNCATE TABLE #SUB_EQUIP_LIST;
                    INSERT INTO #SUB_EQUIP_LIST(WORK_CNTR_SEQ, WORK_CNTR_CD)
                    SELECT
                          S.WORK_CNTR_SEQ
                        , S.WORK_CNTR_CD
                    FROM #SRC_FJ S
                    WHERE S.SALE_OPP_NO = @SALE_OPP_NO
                      AND S.LOT_NO       = @LOT_NO
                      AND S.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                      AND S.FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ
                      AND S.PROCESS_CODE = @PROCESS_CODE
                      AND EXISTS
                      (
                          SELECT 1
                          FROM #WC_GRP G
                          WHERE G.SIMUL_PROCESS_CD = @PROCESS_CODE
                            AND G.GRP_WORK_CNTR_CD = @CHOSEN_GRP_EQUIP
                            AND G.WORK_CNTR_CD     = S.WORK_CNTR_CD
                      )
                    ORDER BY
                      S.WORK_CNTR_SEQ;
                
                    DECLARE @SUB_RN INT = 1;
                    DECLARE @SUB_CNT INT = (SELECT COUNT(*) FROM #SUB_EQUIP_LIST);
                
                    /* ------------------------------------------------------------
                       1) 그룹 공정 내부 "직렬 체인" READY
                          - 최초는 FJ READY(#FJ_READY_TS)로 시작
                          - 이후는 직전 단계 END로 갱신
                    ------------------------------------------------------------ */
                    DECLARE @STAGE_READY_TS INT = NULL;
                    DECLARE @LAST_END_TS    INT = NULL;
                
                    /* FJ READY 로드 (그룹 내부 최초 READY의 기준) */
                    SELECT @READY_TS = READY_TS
                    FROM #FJ_READY_TS
                    WHERE SALE_OPP_NO = @SALE_OPP_NO
                      AND LOT_NO       = @LOT_NO
                      AND ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                      AND FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ;
                
                    IF @READY_TS IS NULL SET @READY_TS = 0;
                    SET @STAGE_READY_TS = @READY_TS;
                
                    /* ------------------------------------------------------------
                       2) 서브 설비를 호기 순서대로 직렬 처리
                    ------------------------------------------------------------ */
                    WHILE @SUB_RN <= @SUB_CNT
                    BEGIN
                        SELECT
                              @CHOSEN_EQUIP  = WORK_CNTR_CD
                            , @WORK_CNTR_SEQ = WORK_CNTR_SEQ
                        FROM #SUB_EQUIP_LIST
                        WHERE RN = @SUB_RN;
                
                        /* ------------------------------------------------------------
                           2-1) FJY/FJT 설비 스킵 규칙
                                - FJ_ASSEMBLY_SEQ < 2 인 경우 해당 서브 설비만 스킵
                        ------------------------------------------------------------ */
                        IF (LEFT(ISNULL(@CHOSEN_EQUIP, ''), 3) IN ('FJY', 'FJT'))
                           AND ISNULL(@FJ_ASM_SEQ, 0) < 2
                        BEGIN
                            SET @SUB_RN += 1;
                            CONTINUE;
                        END
                
                        /* ------------------------------------------------------------
                           2-2) 리드타임/속도/기타 정보 로드 (해당 서브 설비 기준)
                        ------------------------------------------------------------ */
                        SET @LEAD_TIME = NULL;
                
                        SELECT TOP (1)
                              @LEAD_TIME     = S.LEAD_TIME_DAYS
                            , @EQUIP_SPEED   = S.EQUIP_SPEED
                            , @ASSEMBLY      = S.ASSEMBLY
                            , @FJ_ASSEMBLY   = S.FJ_ASSEMBLY
                            , @WORK_CNTR_SEQ = S.WORK_CNTR_SEQ
                        FROM #SRC_FJ S
                        WHERE S.SALE_OPP_NO = @SALE_OPP_NO
                          AND S.LOT_NO       = @LOT_NO
                          AND S.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                          AND S.FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ
                          AND S.PROCESS_CODE = @PROCESS_CODE
                          AND S.WORK_CNTR_CD = @CHOSEN_EQUIP;
                
                        IF @LEAD_TIME IS NULL SET @LEAD_TIME = 0;
                
                        /* ------------------------------------------------------------
                           2-3) 설비 TS 로드 (해당 서브 설비 기준)
                        ------------------------------------------------------------ */
                        SELECT @CUR_TS = TIMESTAMP_POINT
                        FROM #WORK_CNTR_TIMESTAMP
                        WHERE PROCESS_CODE = @PROCESS_CODE
                          AND WORK_CNTR_CD  = @CHOSEN_EQUIP;
                
                        IF @CUR_TS IS NULL SET @CUR_TS = 0;
                
                        /* ------------------------------------------------------------
                           2-4) 직렬 START/END 계산
                                - START = max(설비 TS, 직렬 체인 READY)
                        ------------------------------------------------------------ */
                        SET @START_TS = CASE WHEN @CUR_TS > @STAGE_READY_TS THEN @CUR_TS ELSE @STAGE_READY_TS END;
                        SET @END_TS   = @START_TS + ISNULL(@LEAD_TIME,0);
                
                
                        INSERT INTO #VIRTUAL_RESULT (
                            SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
                            LOT_NO, SUL_NO, REV_SEQ,
                            ASSEMBLY_SEQ, ASSEMBLY,
                            FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
                            PROCESS_SEQ, PROCESS_CODE,
                            WORK_CNTR_SEQ, WORK_CNTR_CD,
                            EQUIP_SPEED, LEAD_TIME,
                            START_TS, END_TS
                        )
                        VALUES
                        (
                            @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
                            @LOT_NO, @SUL_NO, @REV_SEQ,
                            @ASSEMBLY_SEQ, @ASSEMBLY,
                            @FJ_ASM_SEQ, @FJ_ASSEMBLY,
                            @PROCESS_SEQ, @PROCESS_CODE,
                            @WORK_CNTR_SEQ, @CHOSEN_EQUIP,
                            @EQUIP_SPEED, ISNULL(@LEAD_TIME,0),
                            @START_TS, @END_TS
                        );
                        /* ------------------------------------------------------------
                           2-6) 결과 저장
                        ------------------------------------------------------------ */
                        -- INSERT INTO #RESULT_STEP
                        -- (
                        --     SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
                        --     LOT_NO, SUL_NO, REV_SEQ,
                        --     ASSEMBLY_SEQ, ASSEMBLY,
                        --     FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
                        --     PROCESS_SEQ, PROCESS_CODE,
                        --     WORK_CNTR_SEQ, WORK_CNTR_CD,
                        --     EQUIP_SPEED, LEAD_TIME,
                        --     START_TS, END_TS,
                        --     HAS_CONFLICT, OVERLAP_DAYS,
                        --     CONFLICT_FIX_START_TS, CONFLICT_FIX_END_TS,
                        --     FIX_BLOCK_START_TS, FIX_BLOCK_END_TS
                        -- )
                        -- VALUES
                        -- (
                        --     @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
                        --     @LOT_NO, @SUL_NO, @REV_SEQ,
                        --     @ASSEMBLY_SEQ, @ASSEMBLY,
                        --     @FJ_ASM_SEQ, @FJ_ASSEMBLY,
                        --     @PROCESS_SEQ, @PROCESS_CODE,
                        --     @WORK_CNTR_SEQ, @CHOSEN_EQUIP,
                        --     @EQUIP_SPEED, ISNULL(@LEAD_TIME,0),
                        --     @START_TS, @END_TS,
                        --     @HAS_CONFLICT, @OVERLAP_DAYS,
                        --     @CFX_S, @CFX_E,
                        --     @CBLOCK_S, @CBLOCK_E
                        -- );
                
                        /* ------------------------------------------------------------
                           2-7) 설비 TS 누적 (해당 설비만)
                                - 다음 작업 가능 시점 = END + FJ 준비일수
                        ------------------------------------------------------------ */
                        UPDATE #WORK_CNTR_TIMESTAMP
                        SET TIMESTAMP_POINT = @END_TS + @P_PREP_FJ_DAYS
                        WHERE PROCESS_CODE = @PROCESS_CODE
                          AND WORK_CNTR_CD  = @CHOSEN_EQUIP;
                
                        /* ------------------------------------------------------------
                           2-8) 그룹 내부 직렬 체인 READY 갱신
                                - 다음 서브 설비는 현재 END 이후에만 시작 가능
                        ------------------------------------------------------------ */
                        SET @STAGE_READY_TS = @END_TS;
                
                        /* (공정간 READY 업데이트용) 마지막 END 보관 */
                        SET @LAST_END_TS = CASE WHEN @LAST_END_TS IS NULL OR @END_TS > @LAST_END_TS THEN @END_TS ELSE @LAST_END_TS END;
                
                        SET @SUB_RN += 1;
                    END -- WHILE SUB_EQUIP
                
                    /* ------------------------------------------------------------
                       3) 공정간 READY 갱신 (해당 FJ의 다음 공정 시작 가능 시점)
                          - 그룹 공정의 마지막 종료 기준 + 공정 준비일수
                    ------------------------------------------------------------ */
                    IF @LAST_END_TS IS NOT NULL
                    BEGIN
                        UPDATE #FJ_READY_TS
                        SET READY_TS = @LAST_END_TS + @P_PREP_PROC_DAYS
                        WHERE SALE_OPP_NO = @SALE_OPP_NO
                          AND LOT_NO       = @LOT_NO
                          AND ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                          AND FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ;
                    END
                
                    SET @FJ_RN += 1;
                    CONTINUE;  -- ✅ 그룹 공정은 여기서 끝. 아래 단일 설비 로직 스킵
                END


                /* =============================================================================
                   [기본] 단일 설비 공정 처리 로직
                   - (LDS/UST가 아니거나, 그룹 처리로 넘어가지 않는 경우)
                   - START = max(설비 TS, FJ READY)
                   - FIX 충돌 검사 후 결과 저장
                   - 설비 TS, FJ READY 갱신
                ============================================================================= */

                /* 이 FJ+공정의 리드타임/속도/기타 */
                SET @WORK_CNTR_SEQ = NULL;
                SET @LEAD_TIME = NULL;

                SELECT TOP (1)
                      @LEAD_TIME     = S.LEAD_TIME_DAYS
                    , @EQUIP_SPEED   = S.EQUIP_SPEED
                    , @ASSEMBLY      = S.ASSEMBLY
                    , @FJ_ASSEMBLY   = S.FJ_ASSEMBLY
                    , @WORK_CNTR_SEQ = S.WORK_CNTR_SEQ
                FROM #SRC_FJ S
                WHERE S.SALE_OPP_NO = @SALE_OPP_NO
                  AND S.LOT_NO       = @LOT_NO
                  AND S.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                  AND S.FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ
                  AND S.PROCESS_CODE = @PROCESS_CODE
                  AND S.WORK_CNTR_CD = @CHOSEN_EQUIP;

                /* 설비 TS */
                SELECT @CUR_TS = TIMESTAMP_POINT
                FROM #WORK_CNTR_TIMESTAMP
                WHERE PROCESS_CODE = @PROCESS_CODE
                  AND WORK_CNTR_CD = @CHOSEN_EQUIP;
                IF @CUR_TS IS NULL SET @CUR_TS = 0;

                /* FJ READY */
                SELECT @READY_TS = READY_TS
                FROM #FJ_READY_TS
                WHERE SALE_OPP_NO = @SALE_OPP_NO
                  AND LOT_NO       = @LOT_NO
                  AND ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                  AND FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ;
                IF @READY_TS IS NULL SET @READY_TS = 0;

                SET @START_TS = CASE WHEN @CUR_TS > @READY_TS THEN @CUR_TS ELSE @READY_TS END;
                SET @END_TS   = @START_TS + ISNULL(@LEAD_TIME,0);


                INSERT INTO #VIRTUAL_RESULT (
                    SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
                    LOT_NO, SUL_NO, REV_SEQ,
                    ASSEMBLY_SEQ, ASSEMBLY,
                    FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
                    PROCESS_SEQ, PROCESS_CODE,
                    WORK_CNTR_SEQ, WORK_CNTR_CD,
                    EQUIP_SPEED, LEAD_TIME,
                    START_TS, END_TS
                )
                VALUES
                (
                    @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
                    @LOT_NO, @SUL_NO, @REV_SEQ,
                    @ASSEMBLY_SEQ, @ASSEMBLY,
                    @FJ_ASM_SEQ, @FJ_ASSEMBLY,
                    @PROCESS_SEQ, @PROCESS_CODE,
                    @WORK_CNTR_SEQ, @CHOSEN_EQUIP,
                    @EQUIP_SPEED, ISNULL(@LEAD_TIME,0),
                    @START_TS, @END_TS
                );
                -- INSERT INTO #RESULT_STEP
                -- (
                --     SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
                --     LOT_NO, SUL_NO, REV_SEQ,
                --     ASSEMBLY_SEQ, ASSEMBLY,
                --     FJ_ASSEMBLY_SEQ, FJ_ASSEMBLY,
                --     PROCESS_SEQ, PROCESS_CODE,
                --     WORK_CNTR_SEQ, WORK_CNTR_CD,
                --     EQUIP_SPEED, LEAD_TIME,
                --     START_TS, END_TS,
                --     HAS_CONFLICT, OVERLAP_DAYS,
                --     CONFLICT_FIX_START_TS, CONFLICT_FIX_END_TS,
                --     FIX_BLOCK_START_TS, FIX_BLOCK_END_TS
                -- )
                -- VALUES
                -- (
                --     @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
                --     @LOT_NO, @SUL_NO, @REV_SEQ,
                --     @ASSEMBLY_SEQ, @ASSEMBLY,
                --     @FJ_ASM_SEQ, @FJ_ASSEMBLY,
                --     @PROCESS_SEQ, @PROCESS_CODE,
                --     @WORK_CNTR_SEQ, @CHOSEN_EQUIP,
                --     @EQUIP_SPEED, ISNULL(@LEAD_TIME,0),
                --     @START_TS, @END_TS,
                --     @HAS_CONFLICT, @OVERLAP_DAYS,
                --     @CFX_S, @CFX_E,
                --     @CBLOCK_S, @CBLOCK_E
                -- );

                UPDATE #WORK_CNTR_TIMESTAMP
                SET TIMESTAMP_POINT = @END_TS + @P_PREP_FJ_DAYS
                WHERE PROCESS_CODE = @PROCESS_CODE
                  AND WORK_CNTR_CD  = @CHOSEN_EQUIP;

                UPDATE #FJ_READY_TS
                SET READY_TS = @END_TS + @P_PREP_PROC_DAYS
                WHERE SALE_OPP_NO = @SALE_OPP_NO
                  AND LOT_NO       = @LOT_NO
                  AND ASSEMBLY_SEQ = @ASSEMBLY_SEQ
                  AND FJ_ASSEMBLY_SEQ = @FJ_ASM_SEQ;

                SET @FJ_RN += 1;
            END -- WHILE FJ

            SET @P_RN += 1;
        END -- WHILE PROC

        FETCH NEXT FROM CUR_LOT

        INTO @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT, @LOT_NO, @SUL_NO, @REV_SEQ, @ATWRT02, @ASSEMBLY_SEQ;

    END -- WHILE LOT

    CLOSE CUR_LOT;
    DEALLOCATE CUR_LOT;

    IF OBJECT_ID('tempdb..#PROC_LIST') IS NOT NULL DROP TABLE #PROC_LIST;
    IF OBJECT_ID('tempdb..#LOT_WORK_CNTR_LIST') IS NOT NULL DROP TABLE #LOT_WORK_CNTR_LIST;
    IF OBJECT_ID('tempdb..#FJ_LIST') IS NOT NULL DROP TABLE #FJ_LIST;
    IF OBJECT_ID('tempdb..#SUB_EQUIP_LIST') IS NOT NULL DROP TABLE #SUB_EQUIP_LIST;
END