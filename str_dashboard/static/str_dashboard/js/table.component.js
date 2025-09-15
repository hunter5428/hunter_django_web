
// str_dashboard/static/str_dashboard/js/table.component.js

/**
 * 통합 테이블 렌더링 컴포넌트
 * 모든 섹션의 테이블 렌더링을 표준화
 * - 통합 고객 정보 테이블
 * - 기존 person_info, person_detail 관련 코드 제거
 */
(function(window) {
    'use strict';

    /**
     * 테이블 렌더링 옵션
     */
    const TableOptions = {
        DEFAULT: {
            showHeader: true,
            emptyMessage: '결과가 없습니다.',
            hoverEffect: true,
            className: 'table',
            wrapperClassName: 'table-wrap'
        },
        KEY_VALUE: {
            showHeader: false,
            orientation: 'vertical',
            className: 'table table-kv'
        },
        KEY_VALUE_2COL: {
            showHeader: false,
            orientation: 'vertical-2col',
            className: 'table-kv-2col',
            skipNull: true
        },
        HIGHLIGHTED: {
            highlightRow: null,
            highlightClass: 'rep-row'
        }
    };

    /**
     * 테이블 컴포넌트 클래스
     */
    class TableComponent {
        constructor(containerId, options = {}) {
            this.container = document.getElementById(containerId);
            this.options = { ...TableOptions.DEFAULT, ...options };
            this.data = null;
            this.columns = null;
            this.rows = null;
        }

        setData(columns, rows) {
            this.columns = columns || [];
            this.rows = rows || [];
            return this;
        }

        renderEmpty() {
            if (!this.container) return;
            
            this.container.innerHTML = `
                <div class="card empty-row">
                    ${this.escapeHtml(this.options.emptyMessage)}
                </div>
            `;
        }

        render() {
            if (!this.container) {
                console.error('Container not found');
                return;
            }

            if (!this.rows || this.rows.length === 0) {
                this.renderEmpty();
                return;
            }

            if (this.options.orientation === 'vertical') {
                this.renderVerticalTable();
            } else if (this.options.orientation === 'vertical-2col') {
                this.renderVertical2ColTable();
            } else {
                this.renderHorizontalTable();
            }
        }

        renderHorizontalTable() {
            const table = document.createElement('table');
            table.className = this.options.className;

            if (this.options.showHeader && this.columns.length > 0) {
                const thead = this.createTableHeader();
                table.appendChild(thead);
            }

            const tbody = this.createTableBody();
            table.appendChild(tbody);

            this.container.innerHTML = '';
            this.container.appendChild(table);
        }

        renderVerticalTable() {
            if (this.rows.length === 0) {
                this.renderEmpty();
                return;
            }

            const table = document.createElement('table');
            table.className = this.options.className;
            
            const tbody = document.createElement('tbody');
            const row = this.rows[0];
            
            this.columns.forEach((col, idx) => {
                const value = row[idx];
                
                if (this.options.skipNull && (value == null || value === '')) {
                    return;
                }
                
                const tr = document.createElement('tr');
                
                const th = document.createElement('th');
                th.textContent = col;
                tr.appendChild(th);
                
                const td = document.createElement('td');
                td.innerHTML = this.formatCellValue(value, col, idx);
                tr.appendChild(td);
                
                tbody.appendChild(tr);
            });
            
            table.appendChild(tbody);
            this.container.innerHTML = '';
            this.container.appendChild(table);
        }

        renderVertical2ColTable() {
            if (this.rows.length === 0) {
                this.renderEmpty();
                return;
            }

            const table = document.createElement('table');
            table.className = this.options.className;
            
            const tbody = document.createElement('tbody');
            const row = this.rows[0];
            
            const validFields = [];
            this.columns.forEach((col, idx) => {
                const value = row[idx];
                if (!this.options.skipNull || (value != null && value !== '')) {
                    validFields.push({ col, value, idx });
                }
            });
            
            for (let i = 0; i < validFields.length; i += 2) {
                const tr = document.createElement('tr');
                
                const field1 = validFields[i];
                const th1 = document.createElement('th');
                th1.textContent = field1.col;
                tr.appendChild(th1);
                
                const td1 = document.createElement('td');
                td1.innerHTML = this.formatCellValue(field1.value, field1.col, field1.idx);
                tr.appendChild(td1);
                
                if (i + 1 < validFields.length) {
                    const field2 = validFields[i + 1];
                    const th2 = document.createElement('th');
                    th2.textContent = field2.col;
                    tr.appendChild(th2);
                    
                    const td2 = document.createElement('td');
                    td2.innerHTML = this.formatCellValue(field2.value, field2.col, field2.idx);
                    tr.appendChild(td2);
                } else {
                    const th2 = document.createElement('th');
                    th2.innerHTML = '&nbsp;';
                    tr.appendChild(th2);
                    
                    const td2 = document.createElement('td');
                    td2.innerHTML = '&nbsp;';
                    tr.appendChild(td2);
                }
                
                tbody.appendChild(tr);
            }
            
            table.appendChild(tbody);
            this.container.innerHTML = '';
            this.container.appendChild(table);
        }

        createTableHeader() {
            const thead = document.createElement('thead');
            const tr = document.createElement('tr');
            
            const visibleColumns = this.options.visibleColumns || this.columns;
            
            visibleColumns.forEach(col => {
                const th = document.createElement('th');
                th.textContent = col;
                
                if (this.options.sortable) {
                    th.className = 'sortable';
                    th.style.cursor = 'pointer';
                    th.addEventListener('click', () => this.sortByColumn(col));
                }
                
                tr.appendChild(th);
            });
            
            thead.appendChild(tr);
            return thead;
        }

        createTableBody() {
            const tbody = document.createElement('tbody');
            
            this.rows.forEach((row, rowIndex) => {
                const tr = document.createElement('tr');
                
                if (this.options.highlightRow && this.options.highlightRow(row, rowIndex)) {
                    tr.className = this.options.highlightClass || 'highlighted';
                }
                
                if (this.options.hoverEffect) {
                    tr.addEventListener('mouseenter', () => tr.classList.add('hover'));
                    tr.addEventListener('mouseleave', () => tr.classList.remove('hover'));
                }
                
                const visibleColumns = this.options.visibleColumns || this.columns;
                
                visibleColumns.forEach(col => {
                    const colIndex = this.columns.indexOf(col);
                    const td = document.createElement('td');
                    const value = colIndex >= 0 ? row[colIndex] : '';
                    td.innerHTML = this.formatCellValue(value, col, colIndex, row);
                    tr.appendChild(td);
                });
                
                tbody.appendChild(tr);
            });
            
            return tbody;
        }

        formatCellValue(value, columnName, columnIndex, row) {
            if (value == null) {
                return this.options.nullDisplay || '';
            }
            
            if (this.options.formatters && this.options.formatters[columnName]) {
                return this.options.formatters[columnName](value, row);
            }
            
            if (typeof value === 'boolean') {
                return value ? 'Y' : 'N';
            }
            
            if (value instanceof Date) {
                return this.formatDate(value);
            }
            
            if (typeof value === 'number') {
                return this.formatNumber(value, columnName);
            }
            
            return this.escapeHtml(String(value));
        }

        formatDate(date) {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        }

        formatNumber(value, columnName) {
            if (columnName && columnName.toLowerCase().includes('amount')) {
                return value.toLocaleString('ko-KR');
            }
            return String(value);
        }

        escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        sortByColumn(columnName) {
            const colIndex = this.columns.indexOf(columnName);
            if (colIndex < 0) return;
            
            this.sortDirection = this.sortDirection === 'asc' ? 'desc' : 'asc';
            
            this.rows.sort((a, b) => {
                const valA = a[colIndex];
                const valB = b[colIndex];
                
                if (valA == null) return 1;
                if (valB == null) return -1;
                
                let comparison = 0;
                if (valA > valB) comparison = 1;
                if (valA < valB) comparison = -1;
                
                return this.sortDirection === 'asc' ? comparison : -comparison;
            });
            
            this.render();
        }

        refresh() {
            this.render();
        }

        clear() {
            if (this.container) {
                this.container.innerHTML = '';
            }
            this.data = null;
            this.columns = null;
            this.rows = null;
        }
    }

    /**
     * 특화된 테이블 컴포넌트들
     */
    
    // ==================== 통합 고객 정보 테이블 ====================
    // CustomerUnifiedTable 클래스 수정 부분
    class CustomerUnifiedTable extends TableComponent {
        constructor(containerId) {
            super(containerId, {
                ...TableOptions.KEY_VALUE_2COL,
                emptyMessage: '고객 정보가 없습니다.',
                className: 'customer-unified-table',
                skipNull: true
            });
        }
        
        render() {
            if (!this.rows || this.rows.length === 0) {
                this.renderEmpty();
                return;
            }
            
            const table = document.createElement('table');
            table.className = 'customer-unified-table';
            
            const tbody = document.createElement('tbody');
            const row = this.rows[0];
            
            // NULL이 아닌 필드만 필터링
            const validFields = [];
            this.columns.forEach((col, idx) => {
                const value = row[idx];
                if (value != null && value !== '') {
                    validFields.push({ col, value, idx });
                }
            });
            
            // 2열 레이아웃으로 렌더링
            for (let i = 0; i < validFields.length; i += 2) {
                const tr = document.createElement('tr');
                
                // 첫 번째 열
                const field1 = validFields[i];
                const th1 = document.createElement('th');
                th1.textContent = field1.col;
                tr.appendChild(th1);
                
                const td1 = document.createElement('td');
                td1.innerHTML = this.formatCellValue(field1.value, field1.col, field1.idx);
                tr.appendChild(td1);
                
                // 두 번째 열
                if (i + 1 < validFields.length) {
                    const field2 = validFields[i + 1];
                    const th2 = document.createElement('th');
                    th2.textContent = field2.col;
                    tr.appendChild(th2);
                    
                    const td2 = document.createElement('td');
                    td2.innerHTML = this.formatCellValue(field2.value, field2.col, field2.idx);
                    tr.appendChild(td2);
                } else {
                    // 마지막 행이 홀수인 경우 빈 셀 추가
                    const th2 = document.createElement('th');
                    th2.innerHTML = '&nbsp;';
                    tr.appendChild(th2);
                    
                    const td2 = document.createElement('td');
                    td2.innerHTML = '&nbsp;';
                    tr.appendChild(td2);
                }
                
                tbody.appendChild(tr);
            }
            
            table.appendChild(tbody);
            this.container.innerHTML = '';
            this.container.appendChild(table);
        }
    }   

    // 법인 관련인 테이블 클래스
    class CorpRelatedPersonsTable extends TableComponent {
        constructor(containerId) {
            super(containerId, {
                className: 'table corp-related-table',
                emptyMessage: '등록된 관련인 정보가 없습니다.',
                showHeader: true
            });
        }
    }

    // DuplicatePersonsTable 클래스 전체 재작성
    class DuplicatePersonsTable extends TableComponent {
        constructor(containerId, matchCriteria) {
            super(containerId, {
                className: 'duplicate-persons-container',
                emptyMessage: '동일한 E-mail, 휴대폰 번호, 거주주소, 직장명, 직장주소를 가진 회원이 조회되지 않습니다.',
                showHeader: false
            });
            this.matchCriteria = matchCriteria || {};
            this.fieldsToShow = [
                'MATCH_TYPES', '고객ID', 'MID', '성명', 
                '영문명', '실명번호', '생년월일', 'E-mail',
                '휴대폰 번호', '거주주소국가', '거주주소', '직장명', 
                '직장주소', '직업/업종'
            ];
        }
        
        render() {
            if (!this.rows || this.rows.length === 0) {
                this.renderEmpty();
                return;
            }
            
            const container = document.createElement('div');
            container.className = 'duplicate-persons-wrapper';
            
            // 각 행(사람)별로 렌더링
            this.rows.forEach((row, rowIndex) => {
                const personGroup = document.createElement('div');
                personGroup.className = 'duplicate-person-group';
                
                // 헤더 생성 (이름과 CID)
                const nameIdx = this.columns.indexOf('성명');
                const cidIdx = this.columns.indexOf('고객ID');
                const matchTypesIdx = this.columns.indexOf('MATCH_TYPES');
                
                const name = row[nameIdx] || 'N/A';
                const cid = row[cidIdx] || 'N/A';
                const matchTypes = row[matchTypesIdx] || '';
                
                const header = document.createElement('div');
                header.className = 'duplicate-person-header';
                header.textContent = `${name} (CID: ${cid})`;
                personGroup.appendChild(header);
                
                // 그리드 생성 (최대 8개씩)
                const grid = document.createElement('div');
                grid.className = 'duplicate-person-grid';
                
                // 표시할 필드 렌더링
                let fieldCount = 0;
                this.fieldsToShow.forEach(fieldName => {
                    const colIdx = this.columns.indexOf(fieldName);
                    //if (colIdx >= 0 && fieldCount < 8) {
                    if (colIdx >= 0) {
                        const value = row[colIdx];
                        
                        // MATCH_TYPES는 표시하지 않음 (내부적으로만 사용)
                        //if (fieldName === 'MATCH_TYPES') return;
                        
                        // NULL이나 빈 값은 건너뛰기
                        if (value == null || value === '') return;
                        
                        const field = document.createElement('div');
                        field.className = 'duplicate-field';
                        
                        // 매칭 여부 확인
                        const isMatched = this.checkIfFieldMatched(fieldName, value, matchTypes);
                        if (isMatched) {
                            field.classList.add('matched');
                        }
                        
                        const label = document.createElement('div');
                        label.className = 'duplicate-field-label';
                        label.textContent = fieldName;
                        
                        const valueDiv = document.createElement('div');
                        valueDiv.className = 'duplicate-field-value';
                        valueDiv.textContent = value || '';
                        
                        field.appendChild(label);
                        field.appendChild(valueDiv);
                        grid.appendChild(field);
                        
                        fieldCount++;
                    }
                });
                
                personGroup.appendChild(grid);
                container.appendChild(personGroup);
            });
            
            this.container.innerHTML = '';
            this.container.appendChild(container);
        }
        
        checkIfFieldMatched(fieldName, value, matchTypes) {
            if (!matchTypes || !value) return false;
            
            // 이메일 매칭
            if (fieldName === 'E-mail' && matchTypes.includes('EMAIL')) {
                return true;
            }
            
            // 주소 매칭
            if (fieldName === '거주주소' && matchTypes.includes('ADDRESS')) {
                return true;
            }
            
            // 직장명 매칭
            if (fieldName === '직장명' && matchTypes.includes('WORKPLACE_NAME')) {
                return true;
            }
            
            // 직장주소 매칭
            if (fieldName === '직장주소' && matchTypes.includes('WORKPLACE_ADDRESS')) {
                return true;
            }
            
            // 전화번호 매칭 (뒷 4자리)
            if (fieldName === '휴대폰 번호' && this.matchCriteria.phone_suffix) {
                const phone = String(value);
                if (phone.slice(-4) === this.matchCriteria.phone_suffix) {
                    return true;
                }
            }
            
            return false;
        }
    }

    // Rule 히스토리 테이블
    class RuleHistoryTable extends TableComponent {
        constructor(containerId) {
            super(containerId, {
                emptyMessage: '일치하는 히스토리 리스트가 없습니다.',
                className: 'table rule-history-table',
                formatters: {
                    'STR_RULE_ID_NO_COUNT': (value) => {
                        return `<strong>${value}</strong>건`;
                    }
                }
            });
            this.searchedRule = null;
            this.similarList = null;
        }
        
        setSearchInfo(searchedRule, similarList) {
            this.searchedRule = searchedRule;
            this.similarList = similarList;
            return this;
        }
        
        renderEmpty() {
            if (!this.container) return;
            
            let html = '';
            
            if (this.searchedRule) {
                html = `<div class="card empty-row" style="text-align: left;">
                    <p style="margin-bottom: 10px;">
                        <strong>해당 RULE 조합 (${this.escapeHtml(this.searchedRule)})에 대한 히스토리가 없습니다.</strong>
                    </p>`;
                
                if (this.similarList && this.similarList.length > 0) {
                    const similarity = Math.round(this.similarList[0].similarity * 100);
                    const count = this.similarList.length;
                    
                    html += `
                    <div style="margin-top: 15px; padding: 12px; background: #222; border-radius: 8px; border: 1px solid #333;">
                        <p style="margin-bottom: 8px; color: #4fc3f7;">
                            <strong>가장 유사한 RULE 조합`;
                    
                    if (count > 1) {
                        html += ` ${count}개`;
                    }
                    
                    html += ` (유사도: ${similarity}%)</strong>
                        </p>
                        <table class="table" style="margin-top: 10px;">
                            <thead>
                                <tr>
                                    <th>STR_RULE_ID_LIST</th>
                                    <th>STR_RULE_ID_NO_COUNT</th>
                                    <th>STR_SSPC_UPER</th>
                                    <th>STR_SSPC_LWER</th>
                                </tr>
                            </thead>
                            <tbody>`;
                    
                    this.similarList.forEach((similar, index) => {
                        html += `
                                <tr${index === 0 ? ' style="background: #2a2a2a;"' : ''}>
                                    <td>${this.escapeHtml(similar.rule_list)}</td>
                                    <td><strong>${similar.count}</strong>건</td>
                                    <td>${this.escapeHtml(similar.uper || '')}</td>
                                    <td>${this.escapeHtml(similar.lwer || '')}</td>
                                </tr>`;
                    });
                    
                    html += `
                            </tbody>
                        </table>
                    </div>`;
                } else {
                    html += `
                    <p style="margin-top: 10px; color: #bdbdbd;">
                        유사한 RULE 조합도 존재하지 않습니다.
                    </p>`;
                }
                
                html += '</div>';
            } else {
                html = `<div class="card empty-row">
                    ${this.escapeHtml(this.options.emptyMessage)}
                </div>`;
            }
            
            this.container.innerHTML = html;
        }
    }

    // Alert 히스토리 테이블
    class AlertHistoryTable extends TableComponent {
        constructor(containerId, alertId) {
            super(containerId, {
                className: 'table alert-history-table',
                highlightRow: (row, index) => {
                    const alertIdCol = this.columns?.indexOf('STR_ALERT_ID');
                    return alertIdCol >= 0 && String(row[alertIdCol]) === String(alertId);
                },
                highlightClass: 'rep-row'
            });
        }
    }

    // 의심거래 객관식 테이블
    class ObjectivesTable extends TableComponent {
        constructor(containerId, ruleObjMap, repRuleId) {
            super(containerId, {
                className: 'table objectives-table',
                emptyMessage: '객관식 정보가 없습니다.',
                formatters: {
                    '객관식정보': (value) => {
                        return value ? value.replace(/\n/g, '<br>') : '-';
                    }
                },
                highlightRow: (row) => {
                    const ruleIdCol = this.columns?.indexOf('STR_RULE_ID');
                    return ruleIdCol >= 0 && String(row[ruleIdCol]) === String(repRuleId);
                },
                highlightClass: 'rep-row'
            });
            
            this.ruleObjMap = ruleObjMap;
            this.repRuleId = repRuleId;
        }
        
        renderFromAlertData(cols, rows, canonicalIds) {
            const idxRuleId = cols.indexOf('STR_RULE_ID');
            const idxRuleName = cols.indexOf('STR_RULE_NM');
            
            if (idxRuleId < 0 || idxRuleName < 0) {
                this.renderEmpty();
                return;
            }
            
            const ruleNameMap = new Map();
            rows.forEach(row => {
                const ruleId = row[idxRuleId];
                if (!ruleNameMap.has(ruleId)) {
                    ruleNameMap.set(ruleId, row[idxRuleName]);
                }
            });
            
            const tableColumns = ['STR_RULE_ID', 'STR_RULE_NM', '객관식정보'];
            const tableRows = canonicalIds.map(ruleId => {
                const ruleName = ruleNameMap.get(ruleId) || '';
                const objectives = this.ruleObjMap[ruleId] ? this.ruleObjMap[ruleId].join('\n') : '-';
                return [ruleId, ruleName, objectives];
            });
            
            this.setData(tableColumns, tableRows);
            this.render();
        }
    }

    // Rule 설명 테이블 (DISTINCT)
    class RuleDescriptionTable extends TableComponent {
        constructor(containerId, repRuleId) {
            super(containerId, {
                className: 'table rule-desc-table',
                visibleColumns: ['STR_RULE_ID', 'STR_RULE_DTL_EXP', 'STR_RULE_EXTR_COND_CTNT', 'AML_BSS_CTNT'],
                highlightRow: (row) => {
                    const ruleIdCol = this.columns?.indexOf('STR_RULE_ID');
                    return ruleIdCol >= 0 && String(row[ruleIdCol]) === String(repRuleId);
                },
                highlightClass: 'rep-row'
            });
            
            this.repRuleId = repRuleId;
        }
        
        renderFromAlertData(cols, rows, canonicalIds) {
            const indices = {
                STR_RULE_ID: cols.indexOf('STR_RULE_ID'),
                STR_RULE_DTL_EXP: cols.indexOf('STR_RULE_DTL_EXP'),
                STR_RULE_EXTR_COND_CTNT: cols.indexOf('STR_RULE_EXTR_COND_CTNT'),
                AML_BSS_CTNT: cols.indexOf('AML_BSS_CTNT')
            };
            
            if (Object.values(indices).some(v => v < 0)) {
                this.renderEmpty();
                return;
            }
            
            const uniqueRules = new Map();
            rows.forEach(row => {
                const ruleId = row[indices.STR_RULE_ID];
                if (!uniqueRules.has(ruleId)) {
                    uniqueRules.set(ruleId, [
                        ruleId,
                        row[indices.STR_RULE_DTL_EXP],
                        row[indices.STR_RULE_EXTR_COND_CTNT],
                        row[indices.AML_BSS_CTNT]
                    ]);
                }
            });
            
            const tableRows = canonicalIds
                .map(id => uniqueRules.get(id))
                .filter(Boolean);
            
            this.setData(this.options.visibleColumns, tableRows);
            this.render();
        }
    }

    class IPAccessHistoryTable extends TableComponent {
        constructor(containerId) {
            super(containerId, {
                className: 'table ip-history-table',
                emptyMessage: 'IP 접속 이력이 없습니다.',
                showHeader: true,
                // 해외 IP 행 강조 (회색 배경)
                highlightRow: (row, index) => {
                    // 국가한글명 컬럼 인덱스 찾기
                    const countryCol = this.columns?.indexOf('국가한글명');
                    if (countryCol >= 0 && row[countryCol]) {
                        const country = String(row[countryCol]).trim();
                        // 대한민국이 아닌 경우 강조
                        return country !== '대한민국' && country !== '한국' && country !== '';
                    }
                    return false;
                },
                highlightClass: 'rep-row',  // Alert 테이블과 동일한 클래스 사용
                formatters: {
                    '접속일시': (value) => {
                        // 날짜 포맷팅
                        return value ? value : '-';
                    },
                    'IP주소': (value) => {
                        // IP 주소는 그대로 표시
                        return value || '-';
                    },
                    '채널': (value) => {
                        // 채널 타입 표시
                        return value || '-';
                    },
                    '국가한글명': (value) => {
                        // 국가명 표시 (해외는 강조될 예정)
                        if (!value || value === '대한민국' || value === '한국') {
                            return value || '-';
                        }
                        // 해외 국가는 굵게 표시
                        return `<strong>${value}</strong>`;
                    }
                }
            });
        }
    }

 

    // ==================== 섹션 상태 관리 ====================
    // table.component.js의 initSectionToggle() 함수 수정
    function initSectionToggle() {
        // 페이지 로드 시 저장된 상태 복원
        restoreSectionStates();
        
        // 이벤트 위임 방식으로 변경 (동적으로 생성되는 섹션도 처리)
        document.addEventListener('click', function(e) {
            if (e.target.matches('.section h3')) {
                const section = e.target.parentElement;
                section.classList.toggle('collapsed');
                
                // 상태 저장
                saveSectionState(section.id, section.classList.contains('collapsed'));
            }
        });
    }

    // 섹션 상태 저장
    function saveSectionState(sectionId, isCollapsed) {
        if (!sectionId) return;  // ID가 없는 섹션은 저장하지 않음
        
        const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
        states[sectionId] = isCollapsed;
        localStorage.setItem('sectionStates', JSON.stringify(states));
    }

    // 페이지 로드 시 복원
    function restoreSectionStates() {
        const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
        
        Object.entries(states).forEach(([sectionId, isCollapsed]) => {
            const section = document.getElementById(sectionId);
            if (section) {
                if (isCollapsed) {
                    section.classList.add('collapsed');
                } else {
                    section.classList.remove('collapsed');
                }
            }
        });
        
        // 특정 섹션의 기본 상태 설정 (최초 방문 시)
        if (Object.keys(states).length === 0) {
            // IP 접속 이력은 기본적으로 접힘
            const ipSection = document.getElementById('section_ip_access_history');
            if (ipSection) {
                ipSection.classList.add('collapsed');
                saveSectionState('section_ip_access_history', true);
            }
            
            // 일자별 현황도 기본적으로 접힘
            const dailySection = document.getElementById('section_orderbook_daily');
            if (dailySection) {
                dailySection.classList.add('collapsed');
                saveSectionState('section_orderbook_daily', true);
            }
            
            // 구간별 상세도 기본적으로 접힘
            const segmentSection = document.getElementById('section_orderbook_segments');
            if (segmentSection) {
                segmentSection.classList.add('collapsed');
                saveSectionState('section_orderbook_segments', true);
            }
        }
    }




    /**
     * 전역 렌더링 함수들
     */
    
    // 통합 고객 정보 렌더링
    window.renderCustomerUnifiedSection = function(columns, rows) {
        const section = document.getElementById('section_customer_unified');
        if (section) section.style.display = 'block';
        
        const table = new CustomerUnifiedTable('result_table_customer_unified');
        table.setData(columns, rows);
        table.render();
    };
    
    // 법인 관련인 정보 렌더링
    window.renderCorpRelatedSection = function(columns, rows) {
        const section = document.getElementById('section_corp_related');
        if (section) {
            section.style.display = 'block';
            const table = new CorpRelatedPersonsTable('result_table_corp_related');
            table.setData(columns, rows);
            table.render();
        }
    };
    
    // Rule 히스토리 렌더링
    window.renderRuleHistorySection = function(columns, rows, searchedRule, similarList) {
        const section = document.getElementById('section_rule_hist');
        if (section) section.style.display = 'block';
        
        const table = new RuleHistoryTable('result_table_rule_hist');
        table.setSearchInfo(searchedRule, similarList);
        table.setData(columns, rows);
        table.render();
    };  
    
    // Alert 히스토리 렌더링
    window.renderAlertHistSection = function(cols, rows, alertId) {
        const section = document.getElementById('section_alert_rule');
        if (section) section.style.display = 'block';
        
        const table = new AlertHistoryTable('result_table_alert_rule', alertId);
        const visibleCols = ['STDS_DTM', 'CUST_ID', 'STR_RULE_ID', 'STR_ALERT_ID', 'STR_RPT_MNGT_NO', 'STR_RULE_NM', 'TRAN_STRT', 'TRAN_END'];
        const colIndices = visibleCols.map(col => cols.indexOf(col));
        
        if (colIndices.some(idx => idx < 0)) {
            const availableCols = [];
            const availableIndices = [];
            visibleCols.forEach((col, i) => {
                if (colIndices[i] >= 0) {
                    availableCols.push(col);
                    availableIndices.push(colIndices[i]);
                }
            });
            
            if (availableCols.length === 0) {
                table.renderEmpty();
                return;
            }
            
            const filteredRows = rows.map(row => 
                availableIndices.map(idx => row[idx])
            );
            
            table.setData(availableCols, filteredRows);
        } else {
            const filteredRows = rows.map(row => 
                colIndices.map(idx => row[idx])
            );
            
            table.setData(visibleCols, filteredRows);
        }
        
        table.render();
    };
    
    // 의심거래 객관식 렌더링
    window.renderObjectivesSection = function(cols, rows, ruleObjMap, canonicalIds, repRuleId) {
        const section = document.getElementById('section_objectives');
        if (section) section.style.display = 'block';
        
        const table = new ObjectivesTable('result_table_objectives', ruleObjMap, repRuleId);
        table.renderFromAlertData(cols, rows, canonicalIds);
    };
    
    // Rule 설명 렌더링
    window.renderRuleDescSection = function(cols, rows, canonicalIds, repRuleId) {
        const section = document.getElementById('section_rule_distinct');
        if (section) section.style.display = 'block';
        
        const table = new RuleDescriptionTable('result_table_rule_distinct', repRuleId);
        table.renderFromAlertData(cols, rows, canonicalIds);
    };

    // 동일_차명의심_상세 정보 렌더링
    window.renderDuplicatePersonsSection = function(columns, rows, matchCriteria) {
        const section = document.getElementById('section_duplicate_persons');
        if (section) section.style.display = 'block';
        
        const table = new DuplicatePersonsTable('result_table_duplicate_persons', matchCriteria);
        table.setData(columns, rows);
        table.render();
    };

    // 개인 관련인 정보 렌더링 (테이블 형태로 변경)
    window.renderPersonRelatedSection = function(relatedPersonsData) {
        const section = document.getElementById('section_person_related');
        const container = document.getElementById('result_table_person_related');
        
        if (!section || !container) {
            console.warn('Person related section or container not found');
            return;
        }
        
        section.style.display = 'block';
        
        // relatedPersonsData가 문자열인 경우 (기존 텍스트 형태)는 무시하고
        // 새로운 구조화된 데이터 형태를 기대
        if (!relatedPersonsData || typeof relatedPersonsData === 'string') {
            container.innerHTML = `
                <div class="card empty-row">
                    내부입출금 거래 관련인 정보가 없습니다.
                </div>
            `;
            return;
        }
        
        // 관련인 정보를 테이블로 렌더링
        let html = '';
        let personIndex = 0;
        
        for (const [custId, data] of Object.entries(relatedPersonsData)) {
            const info = data.info;
            const transactions = data.transactions || [];
            
            if (!info) continue;
            
            personIndex++;
            
            // 관련인 헤더
            html += `<div class="related-person-header">◆ 관련인 ${personIndex}: ${info.name || 'N/A'} (CID: ${custId})</div>`;
            
            // 기본 정보 테이블
            html += `
            <table class="person-related-table">
                <tbody>
                    <tr>
                        <th>실명번호</th>
                        <td>${info.id_number || 'N/A'}</td>
                        <th>생년월일</th>
                        <td>${info.birth_date || 'N/A'} (만 ${info.age || 'N/A'}세)</td>
                    </tr>
                    <tr>
                        <th>성별</th>
                        <td>${info.gender || 'N/A'}</td>
                        <th>거주지</th>
                        <td>${info.address || 'N/A'}</td>
                    </tr>`;
            
            if (info.job || info.workplace) {
                html += `
                    <tr>
                        <th>직업</th>
                        <td>${info.job || 'N/A'}</td>
                        <th>직장명</th>
                        <td>${info.workplace || 'N/A'}</td>
                    </tr>`;
            }
            
            if (info.workplace_addr) {
                html += `
                    <tr>
                        <th>직장주소</th>
                        <td colspan="3">${info.workplace_addr}</td>
                    </tr>`;
            }
            
            html += `
                    <tr>
                        <th>자금의 원천</th>
                        <td>${info.income_source || 'N/A'}</td>
                        <th>거래목적</th>
                        <td>${info.tran_purpose || 'N/A'}</td>
                    </tr>
                    <tr>
                        <th>위험등급</th>
                        <td>${info.risk_grade || 'N/A'}</td>
                        <th>총 거래횟수</th>
                        <td>${info.total_tran_count || 0}회</td>
                    </tr>
                </tbody>
            </table>`;
            
            // 거래 내역 테이블
            if (transactions.length > 0) {
                // 내부입고/출고 분리
                const deposits = transactions.filter(t => t.tran_type === '내부입고');
                const withdrawals = transactions.filter(t => t.tran_type === '내부출고');
                
                html += `
                <table class="person-related-table transaction-table">
                    <thead>
                        <tr>
                            <th colspan="4">내부입고</th>
                            <th colspan="4">내부출고</th>
                        </tr>
                        <tr>
                            <th>종목</th>
                            <th>수량</th>
                            <th>금액</th>
                            <th>건수</th>
                            <th>종목</th>
                            <th>수량</th>
                            <th>금액</th>
                            <th>건수</th>
                        </tr>
                    </thead>
                    <tbody>`;
                
                const maxRows = Math.max(deposits.length, withdrawals.length);
                
                for (let i = 0; i < maxRows; i++) {
                    html += '<tr>';
                    
                    // 내부입고
                    if (i < deposits.length) {
                        const d = deposits[i];
                        const qty = parseFloat(d.tran_qty || 0);
                        const amt = parseFloat(d.tran_amt || 0);
                        const cnt = parseInt(d.tran_cnt || 0);
                        
                        html += `
                            <td>${d.coin_symbol || '-'}</td>
                            <td>${qty.toLocaleString('ko-KR', {minimumFractionDigits: 4})}</td>
                            <td>${amt.toLocaleString('ko-KR')}원</td>
                            <td>${cnt}건</td>`;
                    } else {
                        html += '<td>-</td><td>-</td><td>-</td><td>-</td>';
                    }
                    
                    // 내부출고
                    if (i < withdrawals.length) {
                        const w = withdrawals[i];
                        const qty = parseFloat(w.tran_qty || 0);
                        const amt = parseFloat(w.tran_amt || 0);
                        const cnt = parseInt(w.tran_cnt || 0);
                        
                        html += `
                            <td>${w.coin_symbol || '-'}</td>
                            <td>${qty.toLocaleString('ko-KR', {minimumFractionDigits: 4})}</td>
                            <td>${amt.toLocaleString('ko-KR')}원</td>
                            <td>${cnt}건</td>`;
                    } else {
                        html += '<td>-</td><td>-</td><td>-</td><td>-</td>';
                    }
                    
                    html += '</tr>';
                }
                
                html += `
                    </tbody>
                </table>`;
            }
            
            // 관련인 간 구분선
            if (personIndex < Object.keys(relatedPersonsData).length) {
                html += '<hr style="margin: 20px 0; border: none; border-top: 1px solid #2a2a2a;">';
            }
        }
        
        container.innerHTML = html || `
            <div class="card empty-row">
                내부입출금 거래 관련인 정보가 없습니다.
            </div>
        `;
    };

    // 전역 렌더링 함수 추가 (다른 render 함수들 다음에)
    window.renderIPAccessHistorySection = function(columns, rows) {
        const section = document.getElementById('section_ip_access_history');
        if (section) {
            section.style.display = 'block';
            // IP 접속 이력은 기본적으로 접힌 상태로 시작
            if (!section.classList.contains('collapsed')) {
                section.classList.add('collapsed');
            }
            
            const table = new IPAccessHistoryTable('result_table_ip_access_history');
            
            // 필요한 컬럼만 표시 (선택적)
            const visibleColumns = [
                '접속일시', '국가한글명', '채널', 'IP주소', 
                '접속위치', 'OS정보', '브라우저정보'
            ];
            
            // 보이는 컬럼에 해당하는 인덱스 찾기
            const visibleIndices = visibleColumns.map(col => columns.indexOf(col)).filter(idx => idx >= 0);
            
            if (visibleIndices.length > 0) {
                const filteredColumns = visibleIndices.map(idx => columns[idx]);
                const filteredRows = rows.map(row => visibleIndices.map(idx => row[idx]));
                
                // 해외 접속 건수 계산 (로깅용)
                const countryIdx = columns.indexOf('국가한글명');
                if (countryIdx >= 0) {
                    const foreignAccess = rows.filter(row => {
                        const country = row[countryIdx];
                        return country && country !== '대한민국' && country !== '한국';
                    }).length;
                    
                    if (foreignAccess > 0) {
                        console.log(`Found ${foreignAccess} foreign IP access records (highlighted)`);
                    }
                }
                
                table.setData(filteredColumns, filteredRows);
            } else {
                // 모든 컬럼 표시
                table.setData(columns, rows);
            }
            
            table.render();
        }
    };

    // 전역으로 노출 (디버깅용)
    window.clearSectionStates = function() {
        localStorage.removeItem('sectionStates');
        console.log('Section states cleared');
    };


    // Orderbook 분석 결과 렌더링 함수
    // 기존 코드의 섹션 생성 부분을 다음과 같이 수정
    window.renderOrderbookAnalysis = function(analysisResult) {
        // 1. 거래원장 개요 섹션
        let patternSection = document.getElementById('section_orderbook_patterns');
        if (!patternSection) {
            patternSection = document.createElement('div');
            patternSection.id = 'section_orderbook_patterns';
            patternSection.className = 'section';
            patternSection.innerHTML = `
                <h3>거래원장(Orderbook) 개요</h3>
                <div class="table-wrap" id="result_orderbook_patterns"></div>
            `;
            
            const ipSection = document.getElementById('section_ip_access_history');
            if (ipSection && ipSection.parentNode) {
                ipSection.parentNode.insertBefore(patternSection, ipSection.nextSibling);
            } else {
                document.querySelector('.app-main').appendChild(patternSection);
            }
        }
        
        // 2. 일자별 현황 섹션 (새로 추가)
        let dailySection = document.getElementById('section_orderbook_daily');
        if (!dailySection) {
            dailySection = document.createElement('div');
            dailySection.id = 'section_orderbook_daily';
            dailySection.className = 'section collapsed'; // 기본 접힘
            dailySection.innerHTML = `
                <h3>일자별 매수/매도, 입출금 현황</h3>
                <div class="table-wrap" id="result_orderbook_daily"></div>
            `;
            
            if (patternSection && patternSection.parentNode) {
                patternSection.parentNode.insertBefore(dailySection, patternSection.nextSibling);
            }
        }
        
        // 3. 구간별 상세 섹션
        let segmentSection = document.getElementById('section_orderbook_segments');
        if (!segmentSection) {
            segmentSection = document.createElement('div');
            segmentSection.id = 'section_orderbook_segments';
            segmentSection.className = 'section collapsed';
            segmentSection.innerHTML = `
                <h3>구간별 상세 내역</h3>
                <div class="table-wrap" id="result_orderbook_segments"></div>
            `;
            
            if (dailySection && dailySection.parentNode) {
                dailySection.parentNode.insertBefore(segmentSection, dailySection.nextSibling);
            }
        }
        
        // 섹션 표시
        patternSection.style.display = 'block';
        dailySection.style.display = 'block';
        segmentSection.style.display = 'block';
        
        // 렌더링 컴포넌트 생성
        const patternRenderer = new OrderbookPatternRenderer('result_orderbook_patterns');
        const dailyRenderer = new OrderbookDailyRenderer('result_orderbook_daily'); // 새로운 렌더러
        const segmentRenderer = new OrderbookSegmentRenderer('result_orderbook_segments');
        
        // 데이터 렌더링
        patternRenderer.render(analysisResult);
        dailyRenderer.render(analysisResult); // 일자별 현황은 별도 렌더러로
        segmentRenderer.render(analysisResult);
        
        // 전역 데이터 저장
        window.orderbookAnalysisData = analysisResult;
    };


    // Orderbook 패턴 렌더러
    class OrderbookPatternRenderer {
        constructor(containerId) {
            this.container = document.getElementById(containerId);
        }
        
        render(data) {
            if (!this.container) return;
            
            let html = '';
            
            // 패턴 분석 카드
            if (data.patterns) {
                html += this.renderPatternCards(data.patterns);
            }
            
            // 일자별 요약 테이블
            //if (data.daily_summary && data.daily_summary.length > 0) {
            //    html += this.renderDailySummary(data.daily_summary);
            //}
            
            this.container.innerHTML = html;
            this.attachEventListeners();
        }
        
        renderPatternCards(patterns) {
            let html = `<div class="orderbook-patterns-grid">`;
            
            const items = [
                { key: 'buy', label: '총 매수', amount: patterns.total_buy_amount, count: patterns.total_buy_count },
                { key: 'sell', label: '총 매도', amount: patterns.total_sell_amount, count: patterns.total_sell_count },
                { key: 'deposit_krw', label: '원화 입금', amount: patterns.total_deposit_krw, count: patterns.total_deposit_krw_count },
                { key: 'withdraw_krw', label: '원화 출금', amount: patterns.total_withdraw_krw, count: patterns.total_withdraw_krw_count },
                { key: 'deposit_crypto', label: '가상자산 입금', amount: patterns.total_deposit_crypto, count: patterns.total_deposit_crypto_count },
                { key: 'withdraw_crypto', label: '가상자산 출금', amount: patterns.total_withdraw_crypto, count: patterns.total_withdraw_crypto_count }
            ];
            
            items.forEach(item => {
                if (item.amount > 0 || item.count > 0) {
                    html += `
                        <div class="pattern-stat-card" data-action="${item.key}">
                            <div class="pattern-stat-label">${item.label}</div>
                            <div class="pattern-stat-value">${formatAmountWithUnit(item.amount)}</div>
                            <div class="pattern-stat-count">${item.count.toLocaleString('ko-KR')}건</div>
                            <div class="pattern-detail" id="pattern-detail-${item.key}"></div>
                        </div>`;
                }
            });
            
            html += `</div>`;
            return html;
        }
        
        renderDailySummary(dailySummary) {
            let html = `
            <div class="card" style="margin-top: 15px;">
                <h4 style="margin: 0 0 10px 0; font-size: 14px;">일자별 매수/매도, 입출금 현황</h4>
                <table class="table daily-summary-table">
                    <thead>
                        <tr>
                            <th>날짜</th>
                            <th>매수</th>
                            <th>매도</th>
                            <th>원화입금</th>
                            <th>원화출금</th>
                            <th>가상자산<br>내부입금</th>
                            <th>가상자산<br>내부출금</th>
                            <th>가상자산<br>외부입금</th>
                            <th>가상자산<br>외부출금</th>
                        </tr>
                    </thead>
                    <tbody>`;
            
            dailySummary.forEach((day, idx) => {
                html += `
                    <tr>
                        <td>${day['날짜']}</td>
                        <td class="clickable-cell" data-day="${idx}" data-type="매수">
                            ${day['매수'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="매도">
                            ${day['매도'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="원화입금">
                            ${day['원화입금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="원화출금">
                            ${day['원화출금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="가상자산내부입금">
                            ${day['가상자산내부입금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="가상자산내부출금">
                            ${day['가상자산내부출금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="가상자산외부입금">
                            ${day['가상자산외부입금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="가상자산외부출금">
                            ${day['가상자산외부출금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                    </tr>
                    <tr class="daily-detail-row" id="daily-detail-${idx}">
                        <td colspan="9"><div class="daily-detail-content"></div></td>
                    </tr>`;
            });
            
            html += `
                    </tbody>
                </table>
            </div>`;
            
            return html;
        }
        
        attachEventListeners() {
            // 패턴 카드 클릭 이벤트
            document.querySelectorAll('.pattern-stat-card').forEach(card => {
                card.addEventListener('click', this.handlePatternCardClick.bind(this));
            });
            
            // 일자별 셀 클릭 이벤트
            document.querySelectorAll('.clickable-cell').forEach(cell => {
                cell.addEventListener('click', this.handleDailyCellClick.bind(this));
            });
        }
        
        handlePatternCardClick(e) {
            const card = e.currentTarget;
            const action = card.dataset.action;
            const detailDiv = card.querySelector('.pattern-detail');
            
            if (!detailDiv) return;
            
            if (detailDiv.classList.contains('show')) {
                detailDiv.classList.remove('show');
                card.classList.remove('expanded');
            } else {
                // 상세 내역 생성
                const patterns = window.orderbookAnalysisData.patterns;
                const details = patterns[`${action}_details`];
                
                if (details && details.length > 0) {
                    detailDiv.innerHTML = this.formatPatternDetails(details);
                } else {
                    detailDiv.innerHTML = '<div style="color: #666;">상세 내역이 없습니다.</div>';
                }
                
                detailDiv.classList.add('show');
                card.classList.add('expanded');
            }
        }
        
        handleDailyCellClick(e) {
            const cell = e.currentTarget;
            const dayIdx = parseInt(cell.dataset.day);
            const type = cell.dataset.type;
            const detailRow = document.getElementById(`daily-detail-${dayIdx}`);
            
            if (!detailRow) return;
            
            const detailContent = detailRow.querySelector('.daily-detail-content');
            const dayData = window.orderbookAnalysisData.daily_summary[dayIdx][type];
            
            // 이미 열려있고 같은 타입이면 닫기
            if (detailRow.style.display === 'table-row' && detailRow.dataset.currentType === type) {
                detailRow.style.display = 'none';
                cell.classList.remove('active');
                return;
            }
            
            // 다른 셀들의 active 제거
            document.querySelectorAll(`.clickable-cell[data-day="${dayIdx}"]`).forEach(c => {
                c.classList.remove('active');
            });
            
            // 상세 내역 생성
            let html = `<strong>${type} 상세내역</strong><br><br>`;
            
            if (dayData && dayData.details && dayData.details.length > 0) {
                html += this.formatDailyDetails(dayData.details, type);
            } else {
                html += '<div style="color: #666;">상세 내역이 없습니다.</div>';
            }
            
            detailContent.innerHTML = html;
            detailRow.style.display = 'table-row';
            detailRow.dataset.currentType = type;
            cell.classList.add('active');
        }
        
        formatPatternDetails(details) {
            let html = '<div class="pattern-detail-item">';
            details.forEach(([ticker, data]) => {
                const amount = parseInt(data.amount_krw || 0);
                const quantity = parseFloat(data.quantity || 0);
                const count = parseInt(data.count || 0);
                
                html += `<div><strong>${ticker}</strong>: `;
                html += `금액 ${amount.toLocaleString('ko-KR')}원, `;
                html += `수량 ${quantity.toLocaleString('ko-KR', {maximumFractionDigits: 4})}개, `;
                html += `횟수 ${count}건</div>`;
            });
            html += '</div>';
            return html;
        }
        
        formatDailyDetails(details, type) {
            let html = '<div style="line-height: 1.6;">';
            
            if (type === '매수' || type === '매도') {
                details.forEach(([ticker, data]) => {
                    const amount = parseInt(data.amount_krw || 0);
                    const quantity = parseFloat(data.quantity || 0);
                    const count = parseInt(data.count || 0);
                    
                    html += `${ticker}: 금액 ${amount.toLocaleString('ko-KR')}원, `;
                    html += `수량 ${quantity.toLocaleString('ko-KR', {maximumFractionDigits: 4})}개, `;
                    html += `횟수 ${count}건<br>`;
                });
            } else if (type.includes('가상자산')) {
                details.forEach(item => {
                    const datetime = item.datetime || '';
                    const ticker = item.ticker || 'Unknown';
                    const amount = parseInt(item.amount_krw || 0);
                    const quantity = parseFloat(item.quantity || 0);
                    
                    html += `${datetime} - ${ticker}: `;
                    html += `금액 ${amount.toLocaleString('ko-KR')}원, `;
                    html += `수량 ${quantity.toLocaleString('ko-KR', {maximumFractionDigits: 4})}개<br>`;
                });
            } else {
                // 원화입출금
                details.forEach(([ticker, data]) => {
                    const amount = parseInt(data.amount_krw || 0);
                    const count = parseInt(data.count || 0);
                    
                    html += `KRW: 금액 ${amount.toLocaleString('ko-KR')}원, `;
                    html += `횟수 ${count}건<br>`;
                });
            }
            
            html += '</div>';
            return html;
        }
    }


    // OrderbookDailyRenderer 클래스 - table.component.js의 OrderbookSegmentRenderer 클래스 바로 앞에 추가
    class OrderbookDailyRenderer {
        constructor(containerId) {
            this.container = document.getElementById(containerId);
        }
        
        render(data) {
            if (!this.container || !data.daily_summary || data.daily_summary.length === 0) {
                if (this.container) {
                    this.container.innerHTML = '<div class="card empty-row">일자별 현황 데이터가 없습니다.</div>';
                }
                return;
            }
            
            let html = this.renderDailySummaryTable(data.daily_summary);
            this.container.innerHTML = html;
            this.attachEventListeners();
        }
        
        renderDailySummaryTable(dailySummary) {
            let html = `
            <table class="table daily-summary-table">
                <thead>
                    <tr>
                        <th>날짜</th>
                        <th>매수</th>
                        <th>매도</th>
                        <th>원화입금</th>
                        <th>원화출금</th>
                        <th>가상자산<br>내부입금</th>
                        <th>가상자산<br>내부출금</th>
                        <th>가상자산<br>외부입금</th>
                        <th>가상자산<br>외부출금</th>
                    </tr>
                </thead>
                <tbody>`;
            
            dailySummary.forEach((day, idx) => {
                html += `
                    <tr>
                        <td>${day['날짜']}</td>
                        <td class="clickable-cell" data-day="${idx}" data-type="매수">
                            ${day['매수'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="매도">
                            ${day['매도'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="원화입금">
                            ${day['원화입금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="원화출금">
                            ${day['원화출금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="가상자산내부입금">
                            ${day['가상자산내부입금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="가상자산내부출금">
                            ${day['가상자산내부출금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="가상자산외부입금">
                            ${day['가상자산외부입금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                        <td class="clickable-cell" data-day="${idx}" data-type="가상자산외부출금">
                            ${day['가상자산외부출금'].total_amount.toLocaleString('ko-KR')}
                        </td>
                    </tr>
                    <tr class="daily-detail-row" id="daily-detail-${idx}" style="display: none;">
                        <td colspan="9"><div class="daily-detail-content"></div></td>
                    </tr>`;
            });
            
            html += `
                </tbody>
            </table>`;
            
            return html;
        }
        
        attachEventListeners() {
            // 일자별 셀 클릭 이벤트
            document.querySelectorAll('.clickable-cell').forEach(cell => {
                cell.addEventListener('click', this.handleDailyCellClick.bind(this));
            });
        }
        
        handleDailyCellClick(e) {
            const cell = e.currentTarget;
            const dayIdx = parseInt(cell.dataset.day);
            const type = cell.dataset.type;
            const detailRow = document.getElementById(`daily-detail-${dayIdx}`);
            
            if (!detailRow) return;
            
            const detailContent = detailRow.querySelector('.daily-detail-content');
            const dayData = window.orderbookAnalysisData.daily_summary[dayIdx][type];
            
            // 이미 열려있고 같은 타입이면 닫기
            if (detailRow.style.display === 'table-row' && detailRow.dataset.currentType === type) {
                detailRow.style.display = 'none';
                cell.classList.remove('active');
                return;
            }
            
            // 같은 행의 다른 셀들의 active 제거
            document.querySelectorAll(`.clickable-cell[data-day="${dayIdx}"]`).forEach(c => {
                c.classList.remove('active');
            });
            
            // 상세 내역 생성
            let html = `<strong>${type} 상세내역</strong><br><br>`;
            
            if (dayData && dayData.details && dayData.details.length > 0) {
                html += this.formatDailyDetails(dayData.details, type);
            } else {
                html += '<div style="color: #666;">상세 내역이 없습니다.</div>';
            }
            
            detailContent.innerHTML = html;
            detailRow.style.display = 'table-row';
            detailRow.dataset.currentType = type;
            cell.classList.add('active');
        }
        
        formatDailyDetails(details, type) {
            let html = '<div style="line-height: 1.6;">';
            
            if (type === '매수' || type === '매도' || type === '원화입금' || type === '원화출금') {
                // 튜플 형태의 데이터 처리
                details.forEach(item => {
                    if (Array.isArray(item) && item.length >= 2) {
                        const [ticker, data] = item;
                        const amount = parseInt(data.amount_krw || 0);
                        const quantity = parseFloat(data.quantity || 0);
                        const count = parseInt(data.count || 0);
                        
                        if (type === '원화입금' || type === '원화출금') {
                            html += `${ticker}: 금액 ${amount.toLocaleString('ko-KR')}원, `;
                            html += `횟수 ${count}건<br>`;
                        } else {
                            html += `${ticker}: 금액 ${amount.toLocaleString('ko-KR')}원, `;
                            html += `수량 ${quantity.toLocaleString('ko-KR', {maximumFractionDigits: 4})}개, `;
                            html += `횟수 ${count}건<br>`;
                        }
                    }
                });
            } else if (type.includes('가상자산')) {
                // 딕셔너리 형태의 데이터 처리
                details.forEach(item => {
                    const datetime = item.datetime || '';
                    const ticker = item.ticker || 'Unknown';
                    const amount = parseInt(item.amount_krw || 0);
                    const quantity = parseFloat(item.quantity || 0);
                    
                    html += `${datetime} - ${ticker}: `;
                    
                    if (quantity > 0) {
                        html += `수량 ${quantity.toLocaleString('ko-KR', {maximumFractionDigits: 4})}개`;
                    }
                    
                    if (amount > 0) {
                        if (quantity > 0) html += ', ';
                        html += `금액 ${amount.toLocaleString('ko-KR')}원`;
                    }
                    
                    html += '<br>';
                });
            }
            
            html += '</div>';
            return html;
        }
    }


    // Orderbook 구간 렌더러
    class OrderbookSegmentRenderer {
        constructor(containerId) {
            this.container = document.getElementById(containerId);
        }
        
        render(data) {
            if (!this.container || !data.summary_data) return;
            
            let html = `
            <table class="table segments-table">
                <thead>
                    <tr>
                        <th>구간</th>
                        <th>Cat</th>
                        <th>행동</th>
                        <th>시작시간</th>
                        <th>종료시간</th>
                        <th>소요시간</th>
                        <th>건수</th>
                        <th>종목수</th>
                        <th>주요종목</th>
                        <th>총금액(원)</th>
                    </tr>
                </thead>
                <tbody>`;
            
            data.summary_data.forEach((segment, idx) => {
                const amount = segment['총금액(KRW)'] || 0;
                const amountFormatted = amount > 0 ? Number(amount).toLocaleString('ko-KR') : '-';
                const countFormatted = Number(segment['건수'] || 0).toLocaleString('ko-KR');
                
                html += `
                    <tr class="segment-row" data-segment="${idx}">
                        <td>${segment['구간']}</td>
                        <td>${segment['trans_cat'] || '-'}</td>
                        <td style="font-weight: 600;">${segment['행동']}</td>
                        <td style="font-size: 11px;">${segment['시작시간']}</td>
                        <td style="font-size: 11px;">${segment['종료시간']}</td>
                        <td>${segment['소요시간']}</td>
                        <td>${countFormatted}</td>
                        <td>${segment['종목수']}</td>
                        <td style="font-size: 11px;">${segment['주요종목'] || '-'}</td>
                        <td style="text-align: right; font-weight: 600;">${amountFormatted}</td>
                    </tr>`;
                
                if (segment['상세내역']) {
                    html += `
                    <tr class="segment-detail-row" id="segment-detail-${idx}">
                        <td colspan="10">
                            <pre class="segment-detail-content">${segment['상세내역']}</pre>
                        </td>
                    </tr>`;
                }
            });
            
            html += `
                </tbody>
            </table>`;
            
            this.container.innerHTML = html;
            this.attachEventListeners();
        }
        
        attachEventListeners() {
            document.querySelectorAll('.segment-row').forEach(row => {
                row.addEventListener('click', this.handleSegmentRowClick.bind(this));
            });
        }
        
        handleSegmentRowClick(e) {
            const row = e.currentTarget;
            const segmentIdx = row.dataset.segment;
            const detailRow = document.getElementById(`segment-detail-${segmentIdx}`);
            
            if (detailRow) {
                if (detailRow.style.display === 'table-row') {
                    detailRow.style.display = 'none';
                    row.classList.remove('expanded');
                } else {
                    detailRow.style.display = 'table-row';
                    row.classList.add('expanded');
                }
            }
        }
    }

    // HTML 이스케이프 헬퍼 함수
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }
    // formatAmount 헬퍼 함수 추가 (클래스 외부에)
    // formatAmountWithUnit 헬퍼 함수 수정
    function formatAmountWithUnit(amount) {
        const absAmount = Math.abs(amount);
        
        if (absAmount === 0) {
            return '0원';
        }
        
        // 한국 숫자 단위 배열
        const units = [
            { value: 1000000000000, name: '조' },
            { value: 100000000, name: '억' },
            { value: 10000, name: '만' }
        ];
        
        let result = [];
        let remaining = absAmount;
        
        // 각 단위별로 처리
        for (const unit of units) {
            if (remaining >= unit.value) {
                const unitAmount = Math.floor(remaining / unit.value);
                result.push(`${unitAmount.toLocaleString('ko-KR')}${unit.name}`);
                remaining = remaining % unit.value;
            }
        }
        
        // 남은 원 단위 처리 (1만원 미만)
        if (remaining > 0 && result.length > 0) {
            // 억 단위 이상일 때만 남은 금액 표시
            if (absAmount >= 100000000) {
                result.push(`${Math.floor(remaining).toLocaleString('ko-KR')}원`);
            }
        } else if (result.length === 0) {
            // 1만원 미만은 그냥 원으로 표시
            result.push(`${absAmount.toLocaleString('ko-KR')}원`);
        }
        
        // 최종 조합
        const mainText = result.join(' ');
        
        // 괄호 안에 전체 금액 표시 (1억원 이상일 때만)
        if (absAmount >= 100000000) {
            return `${mainText} <span style="font-size: 0.85em; color: #999;">(${absAmount.toLocaleString('ko-KR')}원)</span>`;
        }
        
        return mainText;
    }

    // DOM 준비 후 섹션 토글 초기화 (중복 방지)
    if (!window.sectionToggleInitialized) {
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', initSectionToggle);
        } else {
            initSectionToggle();
        }
        window.sectionToggleInitialized = true;
    }

    // 내보내기
    window.TableComponent = TableComponent;
    window.CustomerUnifiedTable = CustomerUnifiedTable;

})(window);