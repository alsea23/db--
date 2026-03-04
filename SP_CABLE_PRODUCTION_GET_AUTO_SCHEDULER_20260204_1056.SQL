CREATE   PROCEDURE dbo.SP_CABLE_PRODUCTION_GET_AUTO_SCHEDULER
(
      @TIMESTAMP_BASE_DATE            DATE
    , @TB_PROCESS_WORK_CNTR_TIMESTAMP dbo.UDT_MST_PROCESS_WORK_CNTR_TIMESTAMP READONLY
    , @TB_PRIORITY_PJT_LOT_LEADTIME   dbo.UDT_DATA_CABLE_PJT_LOT_FJ READONLY
    , @TB_FIXED_PJT_LOT_TIMESTAMP     dbo.UDT_DATA_CABLE_PJT_LOT_FJ READONLY
    
    -- RESULT SELECT에 사용
    , @P_TEMP_VERSION                 NVARCHAR(20)
    , @P_USER_CODE                    NVARCHAR(50)

    -- FIX 민감도/오버랩
    , @P_SENS_BEFORE                  INT = 0
    , @P_SENS_AFTER                   INT = 0
    , @P_ALLOW_OVERLAP                INT = 0

    -- ✅ 준비일수(둘 다 적용)
    , @P_PREP_PROC_DAYS               INT = 1   -- 공정 사이 준비일수(LOT+ASSEMBLY 기준)
    , @P_PREP_FJ_DAYS                 INT = 1   -- FJ 사이 준비일수(블록 내부)
)
AS
BEGIN
    SET NOCOUNT ON;

    /* =====================================================================================
       [핵심 정의 - 최소 선후행]
       - 다른 LOT/다른 작업 때문에 뒤로 밀리는 것: 설비별 TIMESTAMP_POINT(@CUR_TS)로 해결됨
       - 필요한 선후행만 유지:
         (LOT키 + ASSEMBLY_SEQ) 내에서 PROCESS_SEQ 순으로 직렬
         => #LOT_ASM_READY_TS 로 READY_TS 체인 유지 (공정준비일수 포함)
         => START_TS = MAX(설비가능시각(@CUR_TS), LOT+ASM 준비완료(@READY_TS))
       - FJ 전개: 블록 START부터 FJ_ASSEMBLY_SEQ 순서로 전개 (FJ 준비일수 포함)
    ===================================================================================== */


    /* =====================================================================================
       0) TEMP: WORK_CNTR_TIMESTAMP (공정/설비별 현재 TS)
    ===================================================================================== */
    IF OBJECT_ID('tempdb..#WORK_CNTR_TIMESTAMP') IS NOT NULL DROP TABLE #WORK_CNTR_TIMESTAMP;
    CREATE TABLE #WORK_CNTR_TIMESTAMP
    (
        SEQ             INT NOT NULL,
        PROCESS_CODE    NVARCHAR(20) NOT NULL,
        WORK_CNTR_CD    NVARCHAR(20) NOT NULL,
        TIMESTAMP_POINT INT NOT NULL
    );

    INSERT INTO #WORK_CNTR_TIMESTAMP(SEQ, PROCESS_CODE, WORK_CNTR_CD, TIMESTAMP_POINT)
    SELECT SEQ, PROCESS_CODE, WORK_CNTR_CD, ISNULL(TIMESTAMP_POINT,0)
    FROM @TB_PROCESS_WORK_CNTR_TIMESTAMP;

    CREATE UNIQUE CLUSTERED INDEX CX_WCT ON #WORK_CNTR_TIMESTAMP(PROCESS_CODE, SEQ);

--    PRINT 'AUTO_SCHEDULER  WORK_CNTR_TIMESTAMP  END';

    /* =====================================================================================
       1) TEMP: FIXED_PJT_LOT_DATA (설비 점유 블록)
    ===================================================================================== */
    IF OBJECT_ID('tempdb..#FIXED_PJT_LOT_DATA') IS NOT NULL DROP TABLE #FIXED_PJT_LOT_DATA;
    CREATE TABLE #FIXED_PJT_LOT_DATA
    (
        SALE_OPP_NO   NVARCHAR(20) NOT NULL,
        PJT_SHIP      INT          NULL,
        SHIP_SEQ      INT          NULL,
        SHIP_SEQ_LOT  INT          NULL,
        PROCESS_SEQ   INT          NULL,
        PROCESS_CODE  NVARCHAR(20) NOT NULL,
        WORK_CNTR_SEQ INT          NULL,
        WORK_CNTR_CD  NVARCHAR(20) NOT NULL,
        FIX_START_TS  INT          NOT NULL,
        FIX_END_TS    INT          NOT NULL
    );

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
          + SUM(CAST(CEILING(ISNULL(F.LEAD_TIME,0)) AS INT)) AS FIX_END_TS
    FROM @TB_FIXED_PJT_LOT_TIMESTAMP F
    GROUP BY
          F.SALE_OPP_NO, F.PJT_SHIP, F.SHIP_SEQ, F.SHIP_SEQ_LOT,
          F.PROCESS_SEQ, F.PROCESS_CODE, F.WORK_CNTR_SEQ, F.WORK_CNTR_CD;

    CREATE INDEX IX_FIX ON #FIXED_PJT_LOT_DATA(PROCESS_CODE, WORK_CNTR_CD, FIX_START_TS, FIX_END_TS);


--    PRINT 'AUTO_SCHEDULER  FIXED_PJT_LOT_DATA  END';
    
    /* =====================================================================================
       2) TEMP: 블록 대상(LOT+ASSEMBLY+PROCESS) - 너 수정 반영
          - 주의: INSERT..SELECT에서 ORDER BY는 의미/보장 없음 => 제거
    ===================================================================================== */
    IF OBJECT_ID('tempdb..#ASSIGN_BLOCK_DATA') IS NOT NULL DROP TABLE #ASSIGN_BLOCK_DATA;
    CREATE TABLE #ASSIGN_BLOCK_DATA
    (
        ASSIGN_SEQ      INT          NOT NULL,
        SALE_OPP_NO     NVARCHAR(20) NOT NULL,
        PJT_SHIP        INT          NULL,
        SHIP_SEQ        INT          NULL,
        SHIP_SEQ_LOT    INT          NULL,

        LOT_NO          NVARCHAR(50) NOT NULL,
        ASSEMBLY_SEQ    INT          NOT NULL,
        ASSEMBLY        FLOAT        NULL,

        PROCESS_SEQ     INT          NULL,
        PROCESS_CODE    NVARCHAR(20) NOT NULL
    );

    INSERT INTO #ASSIGN_BLOCK_DATA
    (
        ASSIGN_SEQ, SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, ASSEMBLY_SEQ, ASSEMBLY,
        PROCESS_SEQ, PROCESS_CODE
    )
    SELECT
          MIN(P.SEQ) AS ASSIGN_SEQ
        , P.SALE_OPP_NO
        , P.PJT_SHIP
        , P.SHIP_SEQ
        , P.SHIP_SEQ_LOT
        , P.LOT_NO
        , P.ASSEMBLY_SEQ
        , P.ASSEMBLY
        , P.PROCESS_SEQ
        , P.PROCESS_CODE
    FROM @TB_PRIORITY_PJT_LOT_LEADTIME P
    GROUP BY
          P.SALE_OPP_NO
        , P.PJT_SHIP
        , P.SHIP_SEQ
        , P.SHIP_SEQ_LOT
        , P.LOT_NO
        , P.ASSEMBLY_SEQ
        , P.ASSEMBLY
        , P.PROCESS_SEQ
        , P.PROCESS_CODE;

    CREATE CLUSTERED INDEX CX_ASSIGN_BLOCK ON #ASSIGN_BLOCK_DATA(ASSIGN_SEQ);

--    PRINT 'AUTO_SCHEDULER  ASSIGN_BLOCK_DATA  END'

    /* =====================================================================================
       3) TEMP: LOT_ORDER
          - LOT키 → ASSEMBLY_SEQ → PROCESS_SEQ 순으로 "LOT(+ASM) 내부 직렬"을 보장
    ===================================================================================== */
    IF OBJECT_ID('tempdb..#LOT_ORDER') IS NOT NULL DROP TABLE #LOT_ORDER;
    CREATE TABLE #LOT_ORDER
    (
        LOT_ORDER_ID  INT IDENTITY(1,1) NOT NULL,
        ASSIGN_SEQ    INT NOT NULL,

        SALE_OPP_NO   NVARCHAR(20) NOT NULL,
        PJT_SHIP      INT NULL,
        SHIP_SEQ      INT NULL,
        SHIP_SEQ_LOT  INT NULL,

        LOT_NO        NVARCHAR(50) NOT NULL,
        ASSEMBLY_SEQ  INT NOT NULL,
        ASSEMBLY      FLOAT NULL,

        PROCESS_SEQ   INT NULL,
        PROCESS_CODE  NVARCHAR(20) NOT NULL
    );

    INSERT INTO #LOT_ORDER
    (
        ASSIGN_SEQ,
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        LOT_NO, ASSEMBLY_SEQ, ASSEMBLY,
        PROCESS_SEQ, PROCESS_CODE
    )
    SELECT
        A.ASSIGN_SEQ,
        A.SALE_OPP_NO, A.PJT_SHIP, A.SHIP_SEQ, A.SHIP_SEQ_LOT,
        A.LOT_NO, A.ASSEMBLY_SEQ, A.ASSEMBLY,
        A.PROCESS_SEQ, A.PROCESS_CODE
    FROM #ASSIGN_BLOCK_DATA A
    ORDER BY
        A.SALE_OPP_NO,
        A.PJT_SHIP, A.SHIP_SEQ, A.SHIP_SEQ_LOT,
        A.ASSEMBLY_SEQ,
        A.PROCESS_SEQ,
        A.ASSIGN_SEQ;

    CREATE UNIQUE CLUSTERED INDEX CX_LOT_ORDER ON #LOT_ORDER(LOT_ORDER_ID);

--    PRINT 'AUTO_SCHEDULER  LOT_ORDER  END'

    /* =====================================================================================
       4) TEMP: LOT+ASSEMBLY 선후행 체인 (READY_TS)  <-- 필요한 선후행의 전부
          - 동일 LOT키 + ASSEMBLY_SEQ 내에서 공정이 PROCESS_SEQ 순으로만 진행되도록 강제
          - READY_TS = 다음 공정 시작 가능 시각(공정 준비일수 포함)
    ===================================================================================== */
    IF OBJECT_ID('tempdb..#LOT_ASM_READY_TS') IS NOT NULL DROP TABLE #LOT_ASM_READY_TS;
    CREATE TABLE #LOT_ASM_READY_TS
    (
        SALE_OPP_NO   NVARCHAR(20) NOT NULL,
        PJT_SHIP      INT          NOT NULL,
        SHIP_SEQ      INT          NOT NULL,
        SHIP_SEQ_LOT  INT          NOT NULL,
        ASSEMBLY_SEQ  INT          NOT NULL,
        READY_TS      INT          NULL,
        CONSTRAINT PK_LOT_ASM_READY PRIMARY KEY (SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, ASSEMBLY_SEQ)
    );

    INSERT INTO #LOT_ASM_READY_TS(SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, ASSEMBLY_SEQ, READY_TS)
    SELECT DISTINCT
        L.SALE_OPP_NO, L.PJT_SHIP, L.SHIP_SEQ, L.SHIP_SEQ_LOT, L.ASSEMBLY_SEQ, 0
    FROM #LOT_ORDER L;

--    PRINT 'AUTO_SCHEDULER  LOT_ASM_READY_TS  END'

    /* =====================================================================================
       5) TEMP: 블록 리드타임 사전 집계(선택 설비 기준)
          - 블록 점유시간 = SUM(FJ_LT) + @P_PREP_FJ_DAYS * (FJ_COUNT-1)
    ===================================================================================== */
    IF OBJECT_ID('tempdb..#BLOCK_EQUIP_LT') IS NOT NULL DROP TABLE #BLOCK_EQUIP_LT;
    CREATE TABLE #BLOCK_EQUIP_LT
    (
        SALE_OPP_NO     NVARCHAR(20) NOT NULL,
        PJT_SHIP        INT          NOT NULL,
        SHIP_SEQ        INT          NOT NULL,
        SHIP_SEQ_LOT    INT          NOT NULL,
        ASSEMBLY_SEQ    INT          NOT NULL,
        PROCESS_CODE    NVARCHAR(20) NOT NULL,
        WORK_CNTR_CD    NVARCHAR(20) NOT NULL,

        FJ_COUNT        INT          NOT NULL,
        SUM_FJ_LT       INT          NOT NULL,
        TOTAL_LT        INT          NOT NULL,

        CONSTRAINT PK_BLOCK_EQUIP_LT PRIMARY KEY
        (
            SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, ASSEMBLY_SEQ, PROCESS_CODE, WORK_CNTR_CD
        )
    );

    INSERT INTO #BLOCK_EQUIP_LT
    (
        SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
        ASSEMBLY_SEQ, PROCESS_CODE, WORK_CNTR_CD,
        FJ_COUNT, SUM_FJ_LT, TOTAL_LT
    )
    SELECT
        P.SALE_OPP_NO,
        P.PJT_SHIP, P.SHIP_SEQ, P.SHIP_SEQ_LOT,
        P.ASSEMBLY_SEQ,
        P.PROCESS_CODE,
        P.WORK_CNTR_CD,
        COUNT(*) AS FJ_COUNT,
        SUM(CAST(CEILING(ISNULL(P.LEAD_TIME,0)) AS INT)) AS SUM_FJ_LT,
        SUM(CAST(CEILING(ISNULL(P.LEAD_TIME,0)) AS INT))
          + (CASE WHEN COUNT(*) > 1 THEN @P_PREP_FJ_DAYS * (COUNT(*) - 1) ELSE 0 END) AS TOTAL_LT
    FROM @TB_PRIORITY_PJT_LOT_LEADTIME P
    GROUP BY
        P.SALE_OPP_NO, P.PJT_SHIP, P.SHIP_SEQ, P.SHIP_SEQ_LOT,
        P.ASSEMBLY_SEQ,
        P.PROCESS_CODE, P.WORK_CNTR_CD;

--    PRINT 'AUTO_SCHEDULER  BLOCK_EQUIP_LT  END'

    /* =====================================================================================
       6) RESULT: 블록 배치 결과
    ===================================================================================== */
    DECLARE @RESULT_BLOCK TABLE
    (
        INSERT_ORDER          INT IDENTITY(1,1) NOT NULL,

        SALE_OPP_NO           NVARCHAR(20) NOT NULL,
        PJT_SHIP              INT          NULL,
        SHIP_SEQ              INT          NULL,
        SHIP_SEQ_LOT          INT          NULL,

        LOT_NO                NVARCHAR(50) NOT NULL,
        ASSEMBLY_SEQ          INT          NOT NULL,
        ASSEMBLY              FLOAT        NULL,

        PROCESS_SEQ           INT          NULL,
        PROCESS_CODE          NVARCHAR(20) NOT NULL,

        WORK_CNTR_CD          NVARCHAR(20) NOT NULL,

        START_TS              INT NOT NULL,
        END_TS                INT NOT NULL,
        LEAD_TIME_DAYS        INT NOT NULL,

        HAS_CONFLICT          BIT NOT NULL,
        OVERLAP_DAYS          INT NOT NULL,
        CONFLICT_FIX_START_TS INT NULL,
        CONFLICT_FIX_END_TS   INT NULL,
        FIX_BLOCK_START_TS    INT NULL,
        FIX_BLOCK_END_TS      INT NULL
    );


    /* =====================================================================================
       7) 일정 계산
          - START_TS = MAX(설비가능시각(@CUR_TS), LOT+ASM READY_TS(@READY_TS))
    ===================================================================================== */
    DECLARE
          @LOT_ORDER_ID   INT
        , @ASSIGN_SEQ     INT
        , @SALE_OPP_NO    NVARCHAR(20)
        , @PJT_SHIP       INT
        , @SHIP_SEQ       INT
        , @SHIP_SEQ_LOT   INT
        , @LOT_NO         NVARCHAR(50)
        , @ASSEMBLY_SEQ   INT
        , @ASSEMBLY       FLOAT
        , @PROCESS_SEQ    INT
        , @PROCESS_CODE   NVARCHAR(20);

    DECLARE
          @CHOSEN_EQUIP    NVARCHAR(20)
        , @CUR_TS          INT
        , @READY_TS        INT
        , @START_TS        INT
        , @END_TS          INT
        , @TOTAL_LEAD_TIME INT

        , @FX_S            INT
        , @FX_E            INT
        , @BLOCK_S         INT
        , @BLOCK_E         INT
        , @OV_S            INT
        , @OV_E            INT
        , @OV_D            INT
        , @HAS_CONFLICT    BIT
        , @OVERLAP_DAYS    INT
        , @CFX_S           INT
        , @CFX_E           INT
        , @CBLOCK_S        INT
        , @CBLOCK_E        INT;

    DECLARE CUR_CALC CURSOR LOCAL FAST_FORWARD FOR
    SELECT
          LOT_ORDER_ID, ASSIGN_SEQ,
          SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
          LOT_NO, ASSEMBLY_SEQ, ASSEMBLY,
          PROCESS_SEQ, PROCESS_CODE
    FROM #LOT_ORDER
    ORDER BY LOT_ORDER_ID;

    OPEN CUR_CALC;

    FETCH NEXT FROM CUR_CALC
    INTO @LOT_ORDER_ID, @ASSIGN_SEQ,
         @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
         @LOT_NO, @ASSEMBLY_SEQ, @ASSEMBLY,
         @PROCESS_SEQ, @PROCESS_CODE;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        /* 1) 공정 내 가장 TIMESTAMP_POINT 낮은 설비 선택 */
        SELECT TOP (1)
              @CHOSEN_EQUIP = M.WORK_CNTR_CD
            , @CUR_TS       = M.TIMESTAMP_POINT
        FROM #WORK_CNTR_TIMESTAMP M
        WHERE M.PROCESS_CODE = @PROCESS_CODE
        ORDER BY M.TIMESTAMP_POINT ASC, M.SEQ ASC;

        IF @CHOSEN_EQUIP IS NULL
        BEGIN
            SET @CHOSEN_EQUIP = N'';
            SET @CUR_TS = 0;
        END

        /* 2) LOT+ASSEMBLY 선후행 READY_TS 조회 */
        SELECT @READY_TS = R.READY_TS
        FROM #LOT_ASM_READY_TS R
        WHERE R.SALE_OPP_NO  = @SALE_OPP_NO
          AND ISNULL(R.PJT_SHIP,-1)     = ISNULL(@PJT_SHIP,-1)
          AND ISNULL(R.SHIP_SEQ,-1)     = ISNULL(@SHIP_SEQ,-1)
          AND ISNULL(R.SHIP_SEQ_LOT,-1) = ISNULL(@SHIP_SEQ_LOT,-1)
          AND R.ASSEMBLY_SEQ = @ASSEMBLY_SEQ;

        IF @READY_TS IS NULL SET @READY_TS = 0;

        /* 3) 선택 설비 기준 블록 리드타임 조회 */
        SELECT @TOTAL_LEAD_TIME = B.TOTAL_LT
        FROM #BLOCK_EQUIP_LT B
        WHERE B.SALE_OPP_NO = @SALE_OPP_NO
          AND ISNULL(B.PJT_SHIP,-1)     = ISNULL(@PJT_SHIP,-1)
          AND ISNULL(B.SHIP_SEQ,-1)     = ISNULL(@SHIP_SEQ,-1)
          AND ISNULL(B.SHIP_SEQ_LOT,-1) = ISNULL(@SHIP_SEQ_LOT,-1)
          AND B.ASSEMBLY_SEQ = @ASSEMBLY_SEQ
          AND B.PROCESS_CODE = @PROCESS_CODE
          AND B.WORK_CNTR_CD = @CHOSEN_EQUIP;

        IF @TOTAL_LEAD_TIME IS NULL SET @TOTAL_LEAD_TIME = 0;

        /* 4) start/end 산출 (필요한 선후행만) */
        SET @START_TS = CASE WHEN ISNULL(@CUR_TS,0) > ISNULL(@READY_TS,0) THEN ISNULL(@CUR_TS,0) ELSE ISNULL(@READY_TS,0) END;
        SET @END_TS   = @START_TS + @TOTAL_LEAD_TIME;

        /* 5) FIX 체크 (기존 로직 유지)
              - FIX로 START 밀 때마다 LOT+ASM READY_TS 다시 MAX 정규화
        */
        SET @HAS_CONFLICT = 0;
        SET @OVERLAP_DAYS = 0;
        SET @CFX_S = NULL; SET @CFX_E = NULL;
        SET @CBLOCK_S = NULL; SET @CBLOCK_E = NULL;

        WHILE 1 = 1
        BEGIN
            SELECT TOP (1)
                  @FX_S = F.FIX_START_TS
                , @FX_E = F.FIX_END_TS
            FROM #FIXED_PJT_LOT_DATA F
            WHERE F.PROCESS_CODE = @PROCESS_CODE
              AND F.WORK_CNTR_CD = @CHOSEN_EQUIP
              AND (F.FIX_END_TS + @P_SENS_AFTER) > @START_TS
            ORDER BY F.FIX_START_TS;

            IF @@ROWCOUNT = 0
            BEGIN
                SET @HAS_CONFLICT = 0;
                SET @OVERLAP_DAYS = 0;
                SET @CFX_S = NULL; SET @CFX_E = NULL;
                SET @CBLOCK_S = NULL; SET @CBLOCK_E = NULL;
                BREAK;
            END

            SET @BLOCK_S = @FX_S - @P_SENS_BEFORE;
            SET @BLOCK_E = @FX_E + @P_SENS_AFTER;

            SET @OV_S = CASE WHEN @START_TS > @BLOCK_S THEN @START_TS ELSE @BLOCK_S END;
            SET @OV_E = CASE WHEN @END_TS   < @BLOCK_E THEN @END_TS   ELSE @BLOCK_E END;
            SET @OV_D = CASE WHEN @OV_E > @OV_S THEN (@OV_E - @OV_S) ELSE 0 END;

            SET @HAS_CONFLICT = CASE WHEN @OV_D > 0 THEN 1 ELSE 0 END;
            SET @OVERLAP_DAYS = @OV_D;

            SET @CFX_S = @FX_S; SET @CFX_E = @FX_E;
            SET @CBLOCK_S = @BLOCK_S; SET @CBLOCK_E = @BLOCK_E;

            IF (@OV_D <= @P_ALLOW_OVERLAP)
                BREAK;

            -- FIX 블록 뒤로 밀기
            SET @START_TS = @BLOCK_E;

            -- ✅ LOT+ASM READY_TS 다시 반영 (필요한 선후행)
            SET @START_TS = CASE WHEN @START_TS > @READY_TS THEN @START_TS ELSE @READY_TS END;
            SET @END_TS   = @START_TS + @TOTAL_LEAD_TIME;
        END

        /* 6) 블록 결과 저장 */
        INSERT INTO @RESULT_BLOCK
        (
            SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT,
            LOT_NO, ASSEMBLY_SEQ, ASSEMBLY,
            PROCESS_SEQ, PROCESS_CODE,
            WORK_CNTR_CD,
            START_TS, END_TS, LEAD_TIME_DAYS,
            HAS_CONFLICT, OVERLAP_DAYS, CONFLICT_FIX_START_TS, CONFLICT_FIX_END_TS, FIX_BLOCK_START_TS, FIX_BLOCK_END_TS
        )
        VALUES
        (
            @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
            @LOT_NO, @ASSEMBLY_SEQ, @ASSEMBLY,
            @PROCESS_SEQ, @PROCESS_CODE,
            @CHOSEN_EQUIP,
            @START_TS, @END_TS, @TOTAL_LEAD_TIME,
            @HAS_CONFLICT, @OVERLAP_DAYS, @CFX_S, @CFX_E, @CBLOCK_S, @CBLOCK_E
        );
        

        /* 7) 설비 TS 누적 */
        UPDATE #WORK_CNTR_TIMESTAMP
        SET TIMESTAMP_POINT = @END_TS
        WHERE PROCESS_CODE = @PROCESS_CODE
          AND WORK_CNTR_CD = @CHOSEN_EQUIP;

        /* 8) LOT+ASM READY_TS 갱신 (공정 사이 준비일수 포함) */
        UPDATE #LOT_ASM_READY_TS
        SET READY_TS = @END_TS + @P_PREP_PROC_DAYS
        WHERE SALE_OPP_NO = @SALE_OPP_NO
          AND ISNULL(PJT_SHIP,-1)     = ISNULL(@PJT_SHIP,-1)
          AND ISNULL(SHIP_SEQ,-1)     = ISNULL(@SHIP_SEQ,-1)
          AND ISNULL(SHIP_SEQ_LOT,-1) = ISNULL(@SHIP_SEQ_LOT,-1)
          AND ASSEMBLY_SEQ = @ASSEMBLY_SEQ;

       -- PRINT  @LOT_NO + ' LOOP END'
        
        
        FETCH NEXT FROM CUR_CALC
        INTO @LOT_ORDER_ID, @ASSIGN_SEQ,
             @SALE_OPP_NO, @PJT_SHIP, @SHIP_SEQ, @SHIP_SEQ_LOT,
             @LOT_NO, @ASSEMBLY_SEQ, @ASSEMBLY,
             @PROCESS_SEQ, @PROCESS_CODE;
        

    END

    CLOSE CUR_CALC;
    DEALLOCATE CUR_CALC;


    /* =====================================================================================
       9) 결과 2: FJ 전개 결과 (네 코드 그대로)
    ===================================================================================== */
    ;WITH SRC AS
    (
        SELECT
              P.SALE_OPP_NO
            , P.PJT_SHIP, P.SHIP_SEQ, P.SHIP_SEQ_LOT
            , P.LOT_NO
            , P.SUL_NO
            , P.ASSEMBLY_SEQ
            , P.ASSEMBLY
            , P.FJ_ASSEMBLY_SEQ
            , P.FJ_ASSEMBLY
            , P.PROCESS_SEQ
            , P.PROCESS_CODE
            , P.WORK_CNTR_SEQ
            , P.WORK_CNTR_CD
            , P.EQUIP_SPEED
            , P.LEAD_TIME
        FROM @TB_PRIORITY_PJT_LOT_LEADTIME P
    ),
    JOINED AS
    (
        SELECT
              B.INSERT_ORDER
            , B.SALE_OPP_NO
            , B.PJT_SHIP, B.SHIP_SEQ, B.SHIP_SEQ_LOT
            , B.LOT_NO
            , S.SUL_NO
            , B.ASSEMBLY_SEQ
            , B.ASSEMBLY
            , S.FJ_ASSEMBLY_SEQ
            , S.FJ_ASSEMBLY
            , B.PROCESS_SEQ
            , B.PROCESS_CODE
            , S.WORK_CNTR_SEQ
            , B.WORK_CNTR_CD
            , S.EQUIP_SPEED
            , S.LEAD_TIME
            , B.START_TS AS BLOCK_START_TS
            , B.END_TS   AS BLOCK_END_TS
        FROM @RESULT_BLOCK B
        INNER JOIN SRC S
          ON  S.SALE_OPP_NO = B.SALE_OPP_NO
          AND S.LOT_NO = B.LOT_NO
          AND S.ASSEMBLY_SEQ = B.ASSEMBLY_SEQ
          AND S.PROCESS_CODE = B.PROCESS_CODE
          AND S.WORK_CNTR_CD = B.WORK_CNTR_CD
    ),
    CALC AS
    (
        SELECT
              J.*
            , ROW_NUMBER() OVER
              (
                  PARTITION BY
                      J.SALE_OPP_NO, J.PJT_SHIP, J.SHIP_SEQ, J.SHIP_SEQ_LOT,
                      J.LOT_NO, J.ASSEMBLY_SEQ, J.PROCESS_CODE, J.WORK_CNTR_CD
                  ORDER BY J.FJ_ASSEMBLY_SEQ
              ) AS RN
            , SUM(J.LEAD_TIME) OVER
              (
                  PARTITION BY
                      J.SALE_OPP_NO, J.PJT_SHIP, J.SHIP_SEQ, J.SHIP_SEQ_LOT,
                      J.LOT_NO, J.ASSEMBLY_SEQ, J.PROCESS_CODE, J.WORK_CNTR_CD
                  ORDER BY J.FJ_ASSEMBLY_SEQ
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
              ) AS SUM_PREV_LEAD_TIME
        FROM JOINED J
    )
    
    
    SELECT
          @P_TEMP_VERSION
        , ROW_NUMBER() OVER(
              ORDER BY C.PJT_SHIP, C.SHIP_SEQ DESC, C.SHIP_SEQ_LOT,
                       C.ASSEMBLY_SEQ, C.FJ_ASSEMBLY_SEQ, C.PROCESS_SEQ, C.INSERT_ORDER
          ) AS SEQ
        , C.SALE_OPP_NO
        , C.PJT_SHIP
        , C.SHIP_SEQ
        , C.SHIP_SEQ_LOT
        , C.SUL_NO
        , C.ASSEMBLY_SEQ
        , C.ASSEMBLY
        , C.FJ_ASSEMBLY_SEQ
        , C.FJ_ASSEMBLY
        , C.PROCESS_SEQ
        , C.PROCESS_CODE
        , ROW_NUMBER() OVER (PARTITION BY C.SALE_OPP_NO, C.PJT_SHIP, C.SHIP_SEQ , C.SHIP_SEQ_LOT, C.ASSEMBLY, C.FJ_ASSEMBLY_SEQ ORDER BY C.INSERT_ORDER) AS WORK_CNTR_SEQ
        , C.WORK_CNTR_CD
        , C.EQUIP_SPEED
        , C.LEAD_TIME
        , DATEADD(DAY,
            (C.BLOCK_START_TS
             + ISNULL(C.SUM_PREV_LEAD_TIME,0)
             + (CASE WHEN C.RN > 1 THEN @P_PREP_FJ_DAYS * (C.RN - 1) ELSE 0 END)
            ),
            @TIMESTAMP_BASE_DATE
          ) AS PRD_CNFM_STRT_DATE
        , DATEADD(DAY,
            (C.BLOCK_START_TS
             + ISNULL(C.SUM_PREV_LEAD_TIME,0)
             + (CASE WHEN C.RN > 1 THEN @P_PREP_FJ_DAYS * (C.RN - 1) ELSE 0 END)
             + C.LEAD_TIME
            ),
            @TIMESTAMP_BASE_DATE
          ) AS PRD_CNFM_END_DATE
        , @P_USER_CODE AS REG_EMP
        , GETDATE() AS REG_DATE
--        , (C.BLOCK_START_TS
--           + ISNULL(C.SUM_PREV_LEAD_TIME,0)
--           + (CASE WHEN C.RN > 1 THEN @P_PREP_FJ_DAYS * (C.RN - 1) ELSE 0 END)
--          ) AS FJ_START_TS
--        , (C.BLOCK_START_TS
--           + ISNULL(C.SUM_PREV_LEAD_TIME,0)
--           + (CASE WHEN C.RN > 1 THEN @P_PREP_FJ_DAYS * (C.RN - 1) ELSE 0 END)
--           + C.LEAD_TIME
--          ) AS FJ_END_TS
--        , C.BLOCK_START_TS
--        , C.BLOCK_END_TS
    FROM CALC C
    ORDER BY SEQ;
    
END
