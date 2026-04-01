/**
 * 파일명: CustomSchedulerLogic.js
 * 설명: 메인 스크립트에서 import하여 실행하는 Bryntum Scheduler 로직 (JSON 데이터 로드 버전)
 */

import * as SchedulerModule from '../build/scheduler.module.js';

// =================================================================
// [핵심] 외부에서 호출 가능한 초기화 함수
// =================================================================
export async function initScheduler(containerId, jsonData) {
    const {
        DateHelper,
        Scheduler,
        Mask,
        AsyncHelper,
        VersionHelper
    } = SchedulerModule;

    const STYLE_ID = 'bryntum-custom-selection-style';
    const CLS_ROW_OVERLAP = 'row-overlap-highlight';
    const CLS_GROUP_SELECTED = 'group-selected';

    const DELAY = {
        INITIAL_LOAD : 100,
        AFTER_RENDER : 50,
        COMBO_SYNC   : 10
    };

    let scheduler = null;
    let isCascading = false;

    // ---------------------------------------------------------
    // 공통 유틸
    // ---------------------------------------------------------
    function hasValue(value) {
        return value !== undefined && value !== null && value !== '';
    }

    function normalizeString(value) {
        return String(value ?? '').trim();
    }

    function isSameArray(a, b) {
        return JSON.stringify(a || []) === JSON.stringify(b || []);
    }

    function uniqueSorted(values) {
        return [...new Set((values || []).filter(hasValue))].sort();
    }

    function sortEventsByStart(events) {
        return (events || []).slice().sort((a, b) => {
            const diff = a.startDate - b.startDate;
            if (diff !== 0) return diff;
            return (a.id || 0) - (b.id || 0);
        });
    }

    function updateRecordClass(record, clsName, shouldAdd) {
        const current = (record.cls || '').split(/\s+/).filter(Boolean);
        const hasCls = current.includes(clsName);

        if (shouldAdd && !hasCls) {
            current.push(clsName);
            record.cls = current.join(' ');
        }
        else if (!shouldAdd && hasCls) {
            record.cls = current.filter(cls => cls !== clsName).join(' ');
        }
    }

    function syncComboValue(combo, nextValue, delay = DELAY.COMBO_SYNC) {
        clearTimeout(combo._updateTimer);
        combo._updateTimer = setTimeout(() => {
            combo.value = nextValue;

            if (combo.picker) {
                combo.picker.refresh();
            }
        }, delay);
    }

    function setRowTopBorder(row, enabled) {
        if (!row || !row.elements) return;

        Object.values(row.elements).forEach(el => {
            if (enabled) {
                el.style.setProperty('border-top', '1px solid #dbdbdbff', 'important');
            }
            else {
                el.style.borderTop = 'none';
            }
        });
    }

    function clearEventFilters() {
        if (!scheduler) return;
        scheduler.eventStore.clearFilters();
    }

    // ---------------------------------------------------------
    // 스타일 주입
    // ---------------------------------------------------------
    function injectSelectionStyle() {
        if (document.getElementById(STYLE_ID)) return;

        const style = document.createElement('style');
        style.id = STYLE_ID;
        style.textContent = `
            /* 1. 왼쪽 그리드 영역 행 테두리 구분선 */
            .b-scheduler .b-grid-row.b-odd.group-start-line,
            .b-scheduler .b-grid-row.b-odd.group-start-line .b-sch-timeaxis-row,
            .b-scheduler .b-grid-row.b-odd.group-start-line .b-sch-time-axis-cell,
            .b-scheduler .b-grid-row.b-odd.group-start-line .b-time-axis-cell {
                border-top: 1px solid #dbdbdbff !important;
            }

            /* 2. 이벤트 선택 시 강조 효과 */
            .b-scheduler .b-sch-event-wrap.b-selected .b-sch-event,
            .b-scheduler .b-sch-event-wrap.b-sch-event-selected .b-sch-event,
            .b-scheduler .b-sch-event-wrap.b-active .b-sch-event {
                border: 2px solid rgba(255, 255, 255, 0.7) !important;
                z-index: 9999 !important;
                box-shadow: 0 0 10px rgba(0, 0, 0, 0.8) !important;
            }

            /* 3. 중복 발생 시 행 배경색 강조 */
            .b-scheduler .b-grid-row.${CLS_ROW_OVERLAP} .b-grid-cell,
            .b-scheduler .b-grid-row.${CLS_ROW_OVERLAP} {
                background-color: #ffe0e0 !important;
            }

            /* 4. 같은 그룹 선택 하이라이트 */
            .b-scheduler .b-sch-event-wrap.${CLS_GROUP_SELECTED} .b-sch-event,
            .b-scheduler .b-sch-event.${CLS_GROUP_SELECTED} {
                z-index: 9999 !important;
                box-shadow:
                    0 6px 6px rgba(0,0,0,0.5),
                    0 10px 18px rgba(0,0,0,0.7),
                    0 0 0 2px #ffffff !important;
                transform: translateY(-2px);
                transition: all 0.2s ease;
            }
        `;

        document.head.appendChild(style);
    }

    injectSelectionStyle();

    // ---------------------------------------------------------
    // 툴팁 관련
    // ---------------------------------------------------------
    function showEventTooltipOnClick({ eventRecord, eventElement }) {
        const ttFeature = scheduler?.features?.eventTooltip;
        if (!ttFeature || !eventRecord || !eventElement) return;

        const tooltip = ttFeature.tooltip || ttFeature._tooltip;

        if (tooltip) {
            if (typeof ttFeature.showTooltip === 'function') {
                ttFeature.showTooltip(eventRecord);
            }
            else {
                tooltip.showBy({
                    target     : eventElement,
                    anchor     : true,
                    forElement : eventElement
                });
            }
        }
    }

    function hideEventTooltip() {
        const tt = scheduler?.features?.eventTooltip;
        if (tt?.isVisible) tt.hide();

        const inner = tt?.tooltip || tt?._tooltip;
        if (inner?.isVisible) inner.hide();
    }

    // ---------------------------------------------------------
    // 콤보박스 아이템 갱신
    // ---------------------------------------------------------
    function updateDistinctComboItems({ ref, field, allText }) {
        if (!scheduler) return;

        const values = uniqueSorted(
            scheduler.eventStore.map(event => event[field])
        );

        const items = values.map(value => ({
            id   : value,
            text : value
        }));

        const combo = scheduler.widgetMap[ref];
        if (combo) {
            combo.items = [{ id : 'ALL', text : allText }, ...items];
        }
    }

    function updateProjectComboItems() {
        updateDistinctComboItems({
            ref     : 'projectCombo',
            field   : 'saleOppInfo',
            allText : '전체 프로젝트'
        });
    }

    function updateShipmentComboItems() {
        updateDistinctComboItems({
            ref     : 'shipmentCombo',
            field   : 'ShipmentInfo',
            allText : '전체 항차'
        });
    }

    // ---------------------------------------------------------
    // 필터 적용
    // ※ 기존 동작 유지: 프로젝트/항차 필터는 각각 단독 적용 방식 유지
    // ---------------------------------------------------------
    function applyProjectFilter(selectedValues) {
        if (!scheduler) return;

        scheduler.eventStore.filter({
            filters : event => selectedValues.includes(event.saleOppInfo),
            replace : true
        });
    }

    function applyShipmentFilter(selectedValues) {
        if (!scheduler) return;

        clearEventFilters();

        if (selectedValues.length > 0 && !selectedValues.includes('ALL')) {
            const searchValues = selectedValues.map(v => normalizeString(v));

            scheduler.eventStore.filterBy(task => {
                const rawValue = task.get?.('ShipmentInfo') ?? task.data?.ShipmentInfo;
                const taskValue = normalizeString(rawValue);

                return searchValues.includes(taskValue);
            });
        }
    }

    // ---------------------------------------------------------
    // 선택된 이벤트와 같은 groupLot 이벤트들 전체 하이라이트
    // ---------------------------------------------------------
    function highlightSameGroupSelected() {
        if (!scheduler || !scheduler.element) return;

        scheduler.element
            .querySelectorAll(`.b-sch-event-wrap.${CLS_GROUP_SELECTED}`)
            .forEach(el => el.classList.remove(CLS_GROUP_SELECTED));

        const selected = scheduler.selectedEvents || scheduler.eventStore?.selectedRecords || [];
        if (!selected || selected.length === 0) return;

        const groupIds = new Set(
            selected
                .map(ev => ev.groupLot)
                .filter(hasValue)
        );

        if (groupIds.size === 0) return;

        scheduler.eventStore.forEach(ev => {
            if (!groupIds.has(ev.groupLot)) return;

            const elements = scheduler.getElementsFromEventRecord?.(ev);

            if (elements && elements.length) {
                elements.forEach(el => {
                    const wrap = el.closest('.b-sch-event-wrap') || el;
                    wrap.classList.add(CLS_GROUP_SELECTED);
                });
            }
            else {
                const el = scheduler.getElementFromEventRecord?.(ev);
                if (el) {
                    const wrap = el.closest('.b-sch-event-wrap') || el;
                    wrap.classList.add(CLS_GROUP_SELECTED);
                }
            }
        });
    }

    // ---------------------------------------------------------
    // 다른 그룹 간 중복 검사 및 하이라이트
    // ---------------------------------------------------------
    function checkAndHighlightOverlap(resourceRecord) {
        if (!resourceRecord) return;

        const events = resourceRecord.events;

        if (!events || events.length < 2) {
            updateRecordClass(resourceRecord, CLS_ROW_OVERLAP, false);
            return;
        }

        const sortedEvents = sortEventsByStart(events);

        let hasOverlap = false;
        let prev = sortedEvents[0];

        for (let i = 1; i < sortedEvents.length; i++) {
            const current = sortedEvents[i];

            if (current.startDate < prev.endDate) {
                hasOverlap = true;
                break;
            }

            if (current.endDate > prev.endDate) {
                prev = current;
            }
        }

        updateRecordClass(resourceRecord, CLS_ROW_OVERLAP, hasOverlap);
    }

    // ---------------------------------------------------------
    // Cascade 함수 (연쇄 이동 로직)
    // ---------------------------------------------------------
    function cascadeEvents(schedulerInstance, predecessorRecord) {
        if (isCascading) return;

        // 1. Dependency(화살표 연결)에 의한 연쇄 이동
        const dependencies = schedulerInstance.dependencyStore.query(
            dep => dep.from === predecessorRecord.id
        );

        const updates = [];

        dependencies.forEach(dep => {
            const successorRecord = schedulerInstance.eventStore.getById(dep.to);
            if (!successorRecord) return;

            const lag = dep.lag !== undefined ? dep.lag : 0;
            const newStartDate = DateHelper.add(predecessorRecord.endDate, lag, 'days');

            if (+successorRecord.startDate !== +newStartDate) {
                updates.push({
                    record   : successorRecord,
                    newStart : newStartDate
                });
            }
        });

        if (updates.length > 0) {
            isCascading = true;
            try {
                updates.forEach(item => {
                    item.record.setStartDate(item.newStart);
                });
            }
            finally {
                isCascading = false;
            }
        }

        // 2. 같은 행(설비) 내 밀어내기 처리
        cascadeEventsInRow(schedulerInstance, predecessorRecord);
    }

    function cascadeEventsInRow(schedulerInstance, changedRecord) {
        if (isCascading) return;

        const resource = changedRecord.resource;
        if (!resource?.events?.length) return;

        const eventsInRow = sortEventsByStart(resource.events);
        const changedIndex = eventsInRow.findIndex(ev => ev.id === changedRecord.id);

        if (changedIndex === -1) return;

        isCascading = true;
        try {
            let current = eventsInRow[changedIndex];

            for (let i = changedIndex + 1; i < eventsInRow.length; i++) {
                const next = eventsInRow[i];

                if (current.endDate > next.startDate) {
                    const newStartDate = DateHelper.add(current.endDate, 1, 'day');

                    if (+next.startDate !== +newStartDate) {
                        next.setStartDate(newStartDate, true);
                    }
                }

                current = next;
            }
        }
        finally {
            isCascading = false;
        }

        checkAndHighlightOverlap(resource);
    }

    // ---------------------------------------------------------
    // 툴바 콤보 설정
    // ---------------------------------------------------------
    function buildProjectComboConfig() {
        return {
            type            : 'combo',
            ref             : 'projectCombo',
            label           : '프로젝트',
            placeholder     : '프로젝트 선택...',
            width           : '50em',
            height          : 35,
            editable        : true,
            multiSelect     : true,
            value           : [],
            items           : [{ id : 'ALL', text : '전체 프로젝트' }],
            filterParamName : null,
            primaryFilter   : {
                operator : '*'
            },

            onChange({ value, userAction }) {
                const me = this;
                if (!userAction) return;

                const newValueStr = JSON.stringify(value);
                if (me._lastValue === newValueStr) return;
                me._lastValue = newValueStr;

                let newValue = [...value];
                const hasAll = value.includes('ALL');
                const wasAllSelected = me._prevValue && me._prevValue.includes('ALL');

                if (hasAll && !wasAllSelected) {
                    newValue = ['ALL'];
                    clearEventFilters();
                }
                else if (hasAll && value.length > 1) {
                    newValue = value.filter(val => val !== 'ALL');
                    applyProjectFilter(newValue);
                }
                else if (value.length === 0) {
                    clearEventFilters();
                }
                else {
                    applyProjectFilter(newValue);
                }

                me._prevValue = newValue;

                if (!isSameArray(value, newValue)) {
                    syncComboValue(me, newValue, DELAY.COMBO_SYNC);
                }
            }
        };
    }

    function buildShipmentComboConfig() {
        return {
            type            : 'combo',
            ref             : 'shipmentCombo',
            label           : '항차',
            placeholder     : '항차 선택...',
            width           : '18em',
            editable        : true,
            multiSelect     : true,
            value           : [],
            items           : [{ id : 'ALL', text : '전체 항차' }],
            validateOnBlur  : false,
            filterParamName : null,
            primaryFilter   : {
                operator : '*'
            },

            onCollapse() {
                this.input.value = '';
            },

            onChange({ value, userAction }) {
                if (!userAction) return;

                const me = this;
                let newValue = [...value];

                const hasAll = value.includes('ALL');
                const wasAllSelected = me._prevShipment && me._prevShipment.includes('ALL');

                if (hasAll && !wasAllSelected) {
                    newValue = ['ALL'];
                }
                else if (hasAll && value.length > 1) {
                    newValue = value.filter(val => val !== 'ALL');
                }

                me._prevShipment = newValue;

                if (newValue.length > 0 && !newValue.includes('ALL')) {
                    applyShipmentFilter(newValue);
                }
                else {
                    clearEventFilters();
                }

                if (!isSameArray(value, newValue)) {
                    syncComboValue(me, newValue, 0);
                }
            }
        };
    }

    // ---------------------------------------------------------
    // Scheduler 생성
    // ---------------------------------------------------------
    scheduler = new Scheduler({
        appendTo : containerId,

        eventStyle       : 'colored',
        rowHeight        : 25,
        barMargin        : 3,
        sort             : false,
        multiEventSelect : true,

        // 가로 스크롤 관련 핵심 설정
        forceFit   : false,
        scrollable : true,
        overflow   : 'auto',

        detectCSSCompatibilityIssues : false,
        createEventOnDblClick        : false,
        eventLayout                  : 'stack',

        viewPreset : {
            base              : 'monthAndYear',
            displayDateFormat : 'YYYY-MM-DD',
            tickWidth         : 50,
            headers           : [
                { unit : 'year',  dateFormat : 'YYYY년도' },
                { unit : 'month', dateFormat : 'M월' }
            ]
        },

        eventRenderer({ eventRecord, renderData }) {
            const borderColor = '#1B4F72';
            const borderWidth = '1px';
            const backgroundColor = renderData.eventColor || '#336699';

            renderData.style = `
                background: ${backgroundColor};
                box-shadow: inset 0 0 0 1px rgba(255,255,255,0.2);
                border: ${borderWidth} solid ${borderColor};
                color: #fff;
                white-space: pre-line;
                text-align: left;
            `;

            return eventRecord.name;
        },

        // ---------------------------------------------------------
        // STM (Undo / Redo)
        // ---------------------------------------------------------
        project : {
            stm : {
                autoRecord : true
            }
        },

        keyMap : {
            'ctrl+z' : () => scheduler.project.stm.canUndo && scheduler.project.stm.undo(),
            'ctrl+y' : () => scheduler.project.stm.canRedo && scheduler.project.stm.redo()
        },

        features : {
            print     : true,
            pdfExport : {
                exportServer : ''
            },
            dependencies : {
                showTooltip           : true,
                highlightOnEventClick : true,
                allowCreate           : false
            },
            stripe             : true,
            resourceTimeRanges : true,

            eventTooltip : {
                maxWidth : 420,
                minWidth : 250,
                template({ eventRecord }) {
                    const resourceNames = eventRecord.resources.map(r => r.name);

                    return `
                        <div style="white-space: normal; text-align: left; width: 800px; max-width: 900px;">
                            <div style="
                                display: block;
                                padding: 10px 20px;
                                margin:-20px;
                                background-color: #474c53;
                                color: white;
                                border-radius: 6px;
                                box-shadow: 0 2px 4px rgba(0,0,0,0.2), inset 0 1px 1px rgba(255,255,255,0.3);
                                border: 1px solid rgba(0,0,0,0.2);
                                min-height: 150px;
                                line-height: 1.7;
                                font-size: 1.1em;
                            ">
                                <b>프로젝트 :</b> ${eventRecord.saleOppInfo || 'N/A'}<br>
                                <b>반영날짜 :</b> ${DateHelper.format(eventRecord.startDate, 'YYYY-MM-DD')} ~ ${DateHelper.format(eventRecord.endDate, 'YYYY-MM-DD') || ''} (${eventRecord.duration || ''}일)<br>
                                <b>공정 :</b> ${resourceNames.join(', ') || '없음'}<br>
                                <b>항차/선적/LOT :</b> ${eventRecord.lotInfo || ''}<br>
                                <b>설계번호 (core) :</b> ${eventRecord.sulNoInfo || ''}<br>
                                <b>조장 (FJ) :</b> ${eventRecord.assemblyInfo || ''}
                            </div>
                        </div>
                    `;
                }
            },

            eventDragCreate : false,
            eventDrag : {
                copyKey : null,

                // 다중 선택 + 상대 위치 계산 로직
                validatorFn({ eventRecords, newResource }) {
                    if (!newResource || eventRecords.length === 0) return true;

                    const anchorEvent = eventRecords[0];
                    const anchorResource = anchorEvent.resource;
                    if (!anchorResource) return true;

                    const resourceStore = anchorEvent.resourceStore || scheduler.resourceStore;
                    const anchorResIndex = resourceStore.indexOf(anchorResource);
                    const targetResIndex = resourceStore.indexOf(newResource);
                    const rowDelta = targetResIndex - anchorResIndex;

                    for (const task of eventRecords) {
                        const currentRes = task.resource;
                        if (!currentRes) continue;

                        const currentResIndex = resourceStore.indexOf(currentRes);
                        const predictedTargetIndex = currentResIndex + rowDelta;

                        if (predictedTargetIndex < 0 || predictedTargetIndex >= resourceStore.count) {
                            continue;
                        }

                        const predictedResource = resourceStore.getAt(predictedTargetIndex);

                        if (predictedResource && predictedResource !== currentRes) {
                            if (currentRes.process !== predictedResource.process) {
                                return {
                                    valid   : false,
                                    message : '다른 공정으로 이동할 수 없습니다.'
                                };
                            }
                        }
                    }

                    return true;
                },

                tooltipTemplate(data) {
                    const start = DateHelper.format(data.startDate, 'YYYY-MM-DD');
                    const end = DateHelper.format(data.endDate, 'YYYY-MM-DD');

                    return `
                        <div class="b-sch-drag-tooltip">
                            <div class="b-sch-clock-info">
                                <i class="b-icon b-icon-calendar"></i>
                                <span>${start} - ${end}</span>
                            </div>
                        </div>
                    `;
                }
            },

            eventEdit          : false,
            eventMenu          : false,
            scheduleMenu       : false,
            headerMenu         : false,
            timeAxisHeaderMenu : false,
            cellMenu           : false,
            eventResize        : false
        },

        columns : [
            {
                text       : '공정',
                field      : 'process',
                width      : 100,
                align      : 'center',
                sortable   : false,
                filterable : false,
                editor     : false,

                renderer({ record, grid }) {
                    const isFirst = record === grid.store.find(r => r.process === record.process);
                    const row = grid.rowManager.getRowById(record.id);

                    setRowTopBorder(row, isFirst);

                    return isFirst ? record.process : '';
                }
            },
            {
                text       : '설비호기',
                field      : 'name',
                width      : 130,
                align      : 'center',
                sortable   : false,
                filterable : false,
                editor     : false
            }
        ],

        tbar : [
            buildProjectComboConfig(),
            buildShipmentComboConfig()
        ]
    });

    // ---------------------------------------------------------
    // STM 활성화
    // ---------------------------------------------------------
    const stm = scheduler.project.stm;
    stm.enable();

    // ---------------------------------------------------------
    // 데이터 생성 및 로드 함수
    // ---------------------------------------------------------
    async function loadRealData() {
        console.log('Version:', VersionHelper.getVersion('scheduler'));

        const mask = Mask.mask('Loading Data from JSON...', scheduler.element);

        try {
            const data = JSON.parse(jsonData);
            console.log('Data loaded successfully:', data);

            scheduler.suspendRefresh();
            scheduler.forceFit = false;

            if (data.displayDate) {
                const displayDate = Array.isArray(data.displayDate)
                    ? data.displayDate[0]
                    : data.displayDate;

                if (displayDate?.startDate && displayDate?.endDate) {
                    scheduler.setTimeSpan(
                        new Date(displayDate.startDate),
                        new Date(displayDate.endDate)
                    );
                    console.log(`Time span set to: ${displayDate.startDate} - ${displayDate.endDate}`);
                }
            }

            if (data.resources) {
                scheduler.resourceStore.data = data.resources;
            }
            if (data.events) {
                scheduler.eventStore.data = data.events;
            }
            if (data.dependencies) {
                scheduler.dependencyStore.data = data.dependencies;
            }

            console.log('JSON load End');

            scheduler.resumeRefresh(true);

            updateProjectComboItems();
            updateShipmentComboItems();

            await AsyncHelper.sleep(DELAY.AFTER_RENDER);

            // 로딩 완료 후 다시 활성화 및 큐 초기화
            stm.enable();
            stm.resetQueue();

            scheduler.resourceStore.forEach(resource => {
                checkAndHighlightOverlap(resource);
            });

            console.log('Overlap Checked After Render');
            console.log('📍 실제 적용된 Scheduler StartDate:', scheduler.startDate);
            console.log('📍 실제 적용된 Scheduler EndDate:', scheduler.endDate);
        }
        catch (error) {
            console.error('Failed to load JSON data:', error);

            if (scheduler?.element) {
                Mask.unmask(scheduler.element);
            }

            alert('데이터 로드 실패: ' + error.message);
        }
        finally {
            if (mask) mask.close();
        }
    }

    // ---------------------------------------------------------
    // 전역 공유
    // ---------------------------------------------------------
    if (top.window) {
        top.window.scheduler = scheduler;
    }
    else {
        window.scheduler = scheduler;
    }

    // ---------------------------------------------------------
    // 이벤트 바인딩
    // ---------------------------------------------------------
    scheduler.eventStore.on('update', ({ record, changes }) => {
        // 다른 설비로 이동 시, 이전 설비 중복 배경색 갱신
        if (changes.resourceId && changes.resourceId.oldValue) {
            const oldResource = scheduler.resourceStore.getById(changes.resourceId.oldValue);
            if (oldResource) {
                checkAndHighlightOverlap(oldResource);
            }
        }

        // 날짜/리소스 변경 시 연쇄 이동 및 중복 체크
        if (!isCascading && (changes.startDate || changes.endDate || changes.resourceId)) {
            cascadeEvents(scheduler, record);
        }
    });

    scheduler.on('selectionChange', highlightSameGroupSelected);
    scheduler.on('eventSelectionChange', highlightSameGroupSelected);

    scheduler.on('paint', () => {
        highlightSameGroupSelected();
    }, { once : true });

    scheduler.on('beforeEventAdd', () => {
        console.log('Event add blocked.');
        return false;
    });

    scheduler.on('beforeEventCopy', () => {
        console.log('Event copy blocked.');
        return false;
    });

    scheduler.on('eventClick', showEventTooltipOnClick);

    scheduler.on('scheduleClick', () => {
        hideEventTooltip();
    });

    // ---------------------------------------------------------
    // 초기 데이터 로드
    // ---------------------------------------------------------
    await AsyncHelper.sleep(DELAY.INITIAL_LOAD);
    loadRealData();

    console.log('Scheduler Initialized via Module with JSON Data');
}

// =================================================================
// [필수] 전역 변수(window)에 함수 등록
// =================================================================
window.initScheduler = initScheduler;

if (top.window) {
    top.window.initScheduler = initScheduler;
}

if (window.parent) {
    window.parent.initScheduler = initScheduler;
}

console.log('CustomSchedulerLogic Loaded & Function Registered.');