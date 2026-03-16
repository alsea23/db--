CREATE PROCEDURE [dbo].[SP_CABLE_PRODUCTION_GET_SHIPMENT_START_SCHEDULER_RUN]
AS
BEGINSET NOCOUNT ON;

    -------------------------------------------------------------------------
    -- Step 1. 가상 설비 가용 시간 테이블 생성
    -- 각 항차(PJT_SHIP)별로 설비들이 언제 비는지 독립적으로 관리합니다.
    -------------------------------------------------------------------------
    IF OBJECT_ID('tempdb..#VIRTUAL_MACHINE_TIME') IS NOT NULL DROP TABLE #VIRTUAL_MACHINE_TIME;
    CREATE TABLE #VIRTUAL_MACHINE_TIME (
        PJT_SHIP        INT,
        WORK_CNTR_CD    VARCHAR(20),
        AVAILABLE_TS    INT,
        PRIMARY KEY (PJT_SHIP, WORK_CNTR_CD)
    );

    -- 각 항차별 설비의 최초 가용 시간은 해당 항차의 SHIP_TS_POINT입니다.
    INSERT INTO #VIRTUAL_MACHINE_TIME (PJT_SHIP, WORK_CNTR_CD, AVAILABLE_TS)
    SELECT DISTINCT PJT_SHIP, WORK_CNTR_CD, SHIP_TS_POINT
    FROM #SRC_FJ;

    -------------------------------------------------------------------------
    -- Step 2. Virtual Scheduling 실행 (항차별 독립 Forward Fill)
    -- 기존 Auto Scheduler의 테트리스 방식을 가상 타임라인에 적용
    -------------------------------------------------------------------------
    
    -- LOT별 마지막 공정 종료 시간을 기록하기 위한 변수 및 테이블
    DECLARE @CUR_LOT VARCHAR(20), @CUR_PJT_SHIP INT, @CUR_SHIP_TS_POINT INT;
    DECLARE @LAST_PROC_END_TS INT;

    -- 우선순위: PJT_SHIP(항차)별로 돌리되, 내부에서는 SHIP_SEQ DESC, SHIP_SEQ_LOT ASC
    DECLARE LOT_V_CURSOR CURSOR FOR 
    SELECT DISTINCT SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, LOT_NO, SHIP_TS_POINT
    FROM #SRC_FJ 
    ORDER BY PJT_SHIP ASC, SHIP_SEQ DESC, SHIP_SEQ_LOT ASC;

    OPEN LOT_V_CURSOR;
    FETCH NEXT FROM LOT_V_CURSOR INTO @CUR_PJT, @CUR_PJT_SHIP, @CUR_S_SEQ, @CUR_S_LOT, @CUR_LOT, @CUR_SHIP_TS_POINT;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @LAST_PROC_END_TS = @CUR_SHIP_TS_POINT; -- LOT의 첫 공정은 항차 시작점에서 시작 가능

        -- 해당 LOT의 공정들을 순서대로 가상 배정
        DECLARE PROC_V_CURSOR CURSOR FOR
        SELECT PROCESS_CODE, WORK_CNTR_CD, LEAD_TIME
        FROM #SRC_FJ
        WHERE LOT_NO = @CUR_LOT
        ORDER BY PROCESS_SEQ ASC;

        OPEN PROC_V_CURSOR;
        DECLARE @V_PROC_CODE VARCHAR(20), @V_WC_CD VARCHAR(20), @V_LEAD INT;
        FETCH NEXT FROM PROC_V_CURSOR INTO @V_PROC_CODE, @V_WC_CD, @V_LEAD;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @MC_AVAIL_TS INT;
            
            -- 1. 해당 항차 내 이 설비가 비는 시간 확인
            SELECT @MC_AVAIL_TS = AVAILABLE_TS 
            FROM #VIRTUAL_MACHINE_TIME 
            WHERE PJT_SHIP = @CUR_PJT_SHIP AND WORK_CNTR_CD = @V_WC_CD;

            -- 2. 가상 시작/종료 계산 (Forward 로직)
            -- 시작점 = MAX(이전 공정 종료일, 설비 가용 시간)
            DECLARE @V_STRT INT = CASE WHEN @LAST_PROC_END_TS > @MC_AVAIL_TS THEN @LAST_PROC_END_TS ELSE @MC_AVAIL_TS END;
            DECLARE @V_END INT = @V_STRT + @V_LEAD - 1;

            -- 3. 가상 결과 저장
            INSERT INTO #VIRTUAL_RESULT (SALE_OPP_NO, PJT_SHIP, LOT_NO, PROCESS_CODE, WORK_CNTR_CD, V_STRT_TS, V_END_TS)
            VALUES (@CUR_PJT, @CUR_PJT_SHIP, @CUR_LOT, @V_PROC_CODE, @V_WC_CD, @V_STRT, @V_END);

            -- 4. 다음 공정을 위해 상태 업데이트
            SET @LAST_PROC_END_TS = @V_END + 1; -- 공정 간 간격은 일단 1 (또는 0)
            
            UPDATE #VIRTUAL_MACHINE_TIME 
            SET AVAILABLE_TS = @V_END + 1
            WHERE PJT_SHIP = @CUR_PJT_SHIP AND WORK_CNTR_CD = @V_WC_CD;

            FETCH NEXT FROM PROC_V_CURSOR INTO @V_PROC_CODE, @V_WC_CD, @V_LEAD;
        END
        CLOSE PROC_V_CURSOR;
        DEALLOCATE PROC_V_CURSOR;

        FETCH NEXT FROM LOT_V_CURSOR INTO @CUR_PJT, @CUR_PJT_SHIP, @CUR_S_SEQ, @CUR_S_LOT, @CUR_LOT, @CUR_SHIP_TS_POINT;
    END
    CLOSE LOT_V_CURSOR;
    DEALLOCATE LOT_V_CURSOR;

    -------------------------------------------------------------------------
    -- Step 2. Merge & Conflict Adjustment (우선순위 기반 병합 및 조정)
    -------------------------------------------------------------------------
    -- 사용자 요청 우선순위: PJT_SHIP ASC, SHIP_SEQ DESC, SHIP_SEQ_LOT ASC
    DECLARE @CUR_LOT VARCHAR(20), @CUR_PROC VARCHAR(20), @CUR_WC VARCHAR(20);
    DECLARE @V_STRT INT, @V_END INT, @SHIFT_OFFSET INT;

    DECLARE LOT_CURSOR CURSOR FOR 
    SELECT DISTINCT SALE_OPP_NO, PJT_SHIP, SHIP_SEQ, SHIP_SEQ_LOT, LOT_NO 
    FROM #SRC_FJ 
    ORDER BY PJT_SHIP ASC, SHIP_SEQ DESC, SHIP_SEQ_LOT ASC;

    OPEN LOT_CURSOR;
    FETCH NEXT FROM LOT_CURSOR INTO @CUR_PJT, @CUR_SHIP, @CUR_S_SEQ, @CUR_S_LOT, @CUR_LOT;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @SHIFT_OFFSET = 0; -- 새로운 LOT 배정 시마다 쉬프트 초기화

        -- 해당 LOT의 모든 공정을 순서대로 처리
        DECLARE PROC_CURSOR CURSOR FOR
        SELECT PROCESS_CODE, WORK_CNTR_CD, V_STRT_TS, V_END_TS
        FROM #VIRTUAL_RESULT
        WHERE LOT_NO = @CUR_LOT
        ORDER BY V_STRT_TS ASC; -- 공정 순서대로

        OPEN PROC_CURSOR;
        FETCH NEXT FROM PROC_CURSOR INTO @CUR_PROC, @CUR_WC, @V_STRT, @V_END;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            -- 1. 현재 공정이 들어갈 위치 계산 (이전 공정에서 밀린 만큼 + @SHIFT_OFFSET)
            DECLARE @ADJ_STRT INT = @V_STRT + @SHIFT_OFFSET;
            DECLARE @ADJ_END INT = @V_END + @SHIFT_OFFSET;
            DECLARE @LEAD INT = @V_END - @V_STRT + 1;

            -- 2. 설비 충돌 체크 (Conflict Adjustment)
            -- Main Timeline(#WORK_CNTR_TIMESTAMP)에서 빈자리가 나올 때까지 찾음
            WHILE EXISTS (
                SELECT 1 FROM #WORK_CNTR_TIMESTAMP 
                WHERE WORK_CNTR_CD = @CUR_WC AND TS_IDX BETWEEN @ADJ_STRT AND @ADJ_END AND IS_ASSIGNED = 1
            )
            BEGIN
                -- 충돌 발생 시 뒤로 1일씩 이동 (Backward Adjustment)
                SET @ADJ_STRT = @ADJ_STRT + 1;
                SET @ADJ_END = @ADJ_STRT + @LEAD - 1;
                SET @SHIFT_OFFSET = @SHIFT_OFFSET + 1; -- 후속 공정들도 똑같이 밀리도록 저장
            END

            -- 3. 확정된 시간으로 Main Timeline 점유
            UPDATE #WORK_CNTR_TIMESTAMP
            SET IS_ASSIGNED = 1, SALE_OPP_NO = @CUR_PJT, LOT_NO = @CUR_LOT, PROCESS_CODE = @CUR_PROC
            WHERE WORK_CNTR_CD = @CUR_WC AND TS_IDX BETWEEN @ADJ_STRT AND @ADJ_END;

            -- 4. 최종 결과 테이블 저장
            INSERT INTO #FINAL_ASSIGN_DATA (SALE_OPP_NO, PJT_SHIP, LOT_NO, PROCESS_CODE, WORK_CNTR_CD, STRT_TS, END_TS, RESULT_STEP)
            VALUES (@CUR_PJT, @CUR_SHIP, @CUR_LOT, @CUR_PROC, @CUR_WC, @ADJ_STRT, @ADJ_END, 0);

            FETCH NEXT FROM PROC_CURSOR INTO @CUR_PROC, @CUR_WC, @V_STRT, @V_END;
        END
        CLOSE PROC_CURSOR;
        DEALLOCATE PROC_CURSOR;

        FETCH NEXT FROM LOT_CURSOR INTO @CUR_PJT, @CUR_SHIP, @CUR_S_SEQ, @CUR_S_LOT, @CUR_LOT;
    END
    CLOSE LOT_CURSOR;
    DEALLOCATE LOT_CURSOR;
END