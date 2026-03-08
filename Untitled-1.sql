/* =============================================================================
   (A) 공정별 설비 선택 [최적화 & 전술1 적용 완료]
   - 진입 전 변수를 기본값으로 초기화하여 불필요한 NULL 체크를 제거함.
   - 모든 공정 선택 시 #TEMP_LOT_LEADTIME 에 데이터가 존재하는 설비만 선택 (전술 1)
============================================================================= */
-- 1. 기본값 사전 세팅 (후보가 없을 경우를 대비한 튼튼한 방어막)
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
          SELECT 1 FROM #TEMP_LOT_LEADTIME LT   -- ※ 실제 Leadtime 임시테이블명으로 변경
          WHERE LT.LOT_ID       = @LOT_ID       -- ※ 실제 LOT 식별 변수명으로 변경
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
          SELECT 1 FROM #TEMP_LOT_LEADTIME LT   -- ※ 실제 Leadtime 임시테이블명으로 변경
          WHERE LT.LOT_ID       = @LOT_ID       -- ※ 실제 LOT 식별 변수명으로 변경
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
          SELECT 1 FROM #TEMP_LOT_LEADTIME LT   -- ※ 실제 Leadtime 임시테이블명으로 변경
          WHERE LT.LOT_ID       = @LOT_ID       -- ※ 실제 LOT 식별 변수명으로 변경
            AND LT.PROCESS_CODE = W.PROCESS_CODE
            AND LT.WORK_CNTR_CD = W.WORK_CNTR_CD
      )
    ORDER BY W.TIMESTAMP_POINT ASC, W.SEQ ASC;
END