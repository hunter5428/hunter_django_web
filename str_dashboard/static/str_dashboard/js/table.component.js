// str_dashboard/static/str_dashboard/js/table.component.js

/**
 * 통합 테이블 렌더링 컴포넌트
 * 모든 섹션의 테이블 렌더링을 표준화
 * - 통합 고객 정보 테이블 추가
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
            orientation: 'vertical', // vertical: th/td, horizontal: 일반 테이블
            className: 'table table-kv'
        },
        KEY_VALUE_2COL: {
            showHeader: false,
            orientation: 'vertical-2col',
            className: 'table-kv-2col',
            skipNull: true
        },
        HIGHLIGHTED: {
            highlightRow: null, // 함수 또는 조건
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

        /**
         * 데이터 설정
         */
        setData(columns, rows) {
            this.columns = columns || [];
            this.rows = rows || [];
            return this;
        }

        /**
         * 빈 상태 렌더링
         */
        renderEmpty() {
            if (!this.container) return;
            
            this.container.innerHTML = `
                <div class="card empty-row">
                    ${this.escapeHtml(this.options.emptyMessage)}
                </div>
            `;
        }

        /**
         * 테이블 렌더링
         */
        render() {
            if (!this.container) {
                console.error('Container not found');
                return;
            }

            // 데이터 확인
            if (!this.rows || this.rows.length === 0) {
                this.renderEmpty();
                return;
            }

            // 테이블 타입에 따른 렌더링
            if (this.options.orientation === 'vertical') {
                this.renderVerticalTable();
            } else if (this.options.orientation === 'vertical-2col') {
                this.renderVertical2ColTable();
            } else {
                this.renderHorizontalTable();
            }
        }

        /**
         * 일반 테이블 렌더링 (가로형)
         */
        renderHorizontalTable() {
            const table = document.createElement('table');
            table.className = this.options.className;

            // 헤더
            if (this.options.showHeader && this.columns.length > 0) {
                const thead = this.createTableHeader();
                table.appendChild(thead);
            }

            // 바디
            const tbody = this.createTableBody();
            table.appendChild(tbody);

            // 컨테이너에 추가
            this.container.innerHTML = '';
            this.container.appendChild(table);
        }

        /**
         * 세로형 테이블 렌더링 (Key-Value 형태)
         */
        renderVerticalTable() {
            if (this.rows.length === 0) {
                this.renderEmpty();
                return;
            }

            const table = document.createElement('table');
            table.className = this.options.className;
            
            const tbody = document.createElement('tbody');
            
            // 첫 번째 행만 사용 (주로 단일 레코드용)
            const row = this.rows[0];
            
            this.columns.forEach((col, idx) => {
                const value = row[idx];
                
                // NULL 값 스킵 옵션
                if (this.options.skipNull && (value == null || value === '')) {
                    return;
                }
                
                const tr = document.createElement('tr');
                
                // 컬럼명 (th)
                const th = document.createElement('th');
                th.textContent = col;
                tr.appendChild(th);
                
                // 값 (td)
                const td = document.createElement('td');
                td.innerHTML = this.formatCellValue(value, col, idx);
                tr.appendChild(td);
                
                tbody.appendChild(tr);
            });
            
            table.appendChild(tbody);
            
            this.container.innerHTML = '';
            this.container.appendChild(table);
        }

        /**
         * 2열 세로형 테이블 렌더링
         */
        renderVertical2ColTable() {
            if (this.rows.length === 0) {
                this.renderEmpty();
                return;
            }

            const table = document.createElement('table');
            table.className = this.options.className;
            
            const tbody = document.createElement('tbody');
            
            // 첫 번째 행만 사용
            const row = this.rows[0];
            
            // NULL이 아닌 필드만 필터링
            const validFields = [];
            this.columns.forEach((col, idx) => {
                const value = row[idx];
                if (!this.options.skipNull || (value != null && value !== '')) {
                    validFields.push({ col, value, idx });
                }
            });
            
            // 2열씩 배치
            for (let i = 0; i < validFields.length; i += 2) {
                const tr = document.createElement('tr');
                
                // 첫 번째 컬럼
                const field1 = validFields[i];
                const th1 = document.createElement('th');
                th1.textContent = field1.col;
                tr.appendChild(th1);
                
                const td1 = document.createElement('td');
                td1.innerHTML = this.formatCellValue(field1.value, field1.col, field1.idx);
                tr.appendChild(td1);
                
                // 두 번째 컬럼 (있는 경우)
                if (i + 1 < validFields.length) {
                    const field2 = validFields[i + 1];
                    const th2 = document.createElement('th');
                    th2.textContent = field2.col;
                    tr.appendChild(th2);
                    
                    const td2 = document.createElement('td');
                    td2.innerHTML = this.formatCellValue(field2.value, field2.col, field2.idx);
                    tr.appendChild(td2);
                } else {
                    // 빈 셀 추가
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

        /**
         * 테이블 헤더 생성
         */
        createTableHeader() {
            const thead = document.createElement('thead');
            const tr = document.createElement('tr');
            
            // 컬럼 필터링 옵션이 있는 경우
            const visibleColumns = this.options.visibleColumns || this.columns;
            
            visibleColumns.forEach(col => {
                const th = document.createElement('th');
                th.textContent = col;
                
                // 정렬 기능 (옵션)
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

        /**
         * 테이블 바디 생성
         */
        createTableBody() {
            const tbody = document.createElement('tbody');
            
            this.rows.forEach((row, rowIndex) => {
                const tr = document.createElement('tr');
                
                // 하이라이트 처리
                if (this.options.highlightRow && this.options.highlightRow(row, rowIndex)) {
                    tr.className = this.options.highlightClass || 'highlighted';
                }
                
                // hover 효과
                if (this.options.hoverEffect) {
                    tr.addEventListener('mouseenter', () => tr.classList.add('hover'));
                    tr.addEventListener('mouseleave', () => tr.classList.remove('hover'));
                }
                
                // 컬럼 필터링
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

        /**
         * 셀 값 포맷팅
         */
        formatCellValue(value, columnName, columnIndex, row) {
            // null/undefined 처리
            if (value == null) {
                return this.options.nullDisplay || '';
            }
            
            // 커스텀 포매터
            if (this.options.formatters && this.options.formatters[columnName]) {
                return this.options.formatters[columnName](value, row);
            }
            
            // 기본 포맷팅
            if (typeof value === 'boolean') {
                return value ? 'Y' : 'N';
            }
            
            if (value instanceof Date) {
                return this.formatDate(value);
            }
            
            if (typeof value === 'number') {
                return this.formatNumber(value, columnName);
            }
            
            // HTML 이스케이프
            return this.escapeHtml(String(value));
        }

        /**
         * 날짜 포맷팅
         */
        formatDate(date) {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            return `${year}-${month}-${day}`;
        }

        /**
         * 숫자 포맷팅
         */
        formatNumber(value, columnName) {
            // 금액 관련 컬럼
            if (columnName && columnName.toLowerCase().includes('amount')) {
                return value.toLocaleString('ko-KR');
            }
            return String(value);
        }

        /**
         * HTML 이스케이프
         */
        escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        /**
         * 정렬 기능
         */
        sortByColumn(columnName) {
            const colIndex = this.columns.indexOf(columnName);
            if (colIndex < 0) return;
            
            // 정렬 방향 토글
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

        /**
         * 테이블 새로고침
         */
        refresh() {
            this.render();
        }

        /**
         * 테이블 초기화
         */
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
    class CustomerUnifiedTable extends TableComponent {
        constructor(containerId) {
            super(containerId, {
                ...TableOptions.KEY_VALUE_2COL,
                emptyMessage: '고객 정보가 없습니다.',
                className: 'table-kv-2col customer-unified-table',
                skipNull: true,
                groupedSections: [
                    {
                        title: '기본 정보',
                        fields: ['고객ID', 'MID', '성명', '영문명', '고객구분', '실명번호구분', '실명번호', '생년월일/설립일', '성별']
                    },
                    {
                        title: '연락처 정보',
                        fields: ['연락처', '이메일']
                    },
                    {
                        title: '거주지 정보',
                        fields: ['국적', '국적코드', '거주지국가', '거주지우편번호', '거주지주소', '거주지상세주소']
                    },
                    {
                        title: '직장 정보',
                        fields: ['직업/업종', '직위', '직장명', '직장국가', '직장우편번호', '직장주소', '직장상세주소']
                    },
                    {
                        title: '거래 정보',
                        fields: ['거래목적', '주요소득원천', '월평균소득/매출', '매매거래자금원천', '총자산규모', '월평균예상거래횟수', '월평균예상거래규모_입출금', '월평균예상거래규모_가상자산']
                    },
                    {
                        title: '리스크 정보',
                        fields: ['RA등급', '위험등급', '고액자산가', 'STR보고건수', '최종STR보고일', 'Alert건수']
                    },
                    {
                        title: '법인 정보',
                        fields: ['법인유형', '업종', '시장참여단계', '상장여부', '국내신고대상VASP여부'],
                        condition: (data) => data['고객구분'] === '법인'
                    },
                    {
                        title: 'KYC 심사 정보',
                        fields: ['KYC완료일시', '심사상태']
                    }
                ]
            });
        }
        
        render() {
            if (!this.rows || this.rows.length === 0) {
                this.renderEmpty();
                return;
            }
            
            const container = document.createElement('div');
            container.className = 'customer-info-sections';
            
            const row = this.rows[0];
            const dataMap = {};
            
            // 컬럼과 값 매핑
            this.columns.forEach((col, idx) => {
                dataMap[col] = row[idx];
            });
            
            // 각 섹션별로 렌더링
            this.options.groupedSections.forEach(section => {
                // 조건 체크 (법인 정보 등)
                if (section.condition && !section.condition(dataMap)) {
                    return;
                }
                
                // 해당 섹션의 필드 중 값이 있는 것만 필터링
                const sectionFields = section.fields.filter(field => 
                    dataMap.hasOwnProperty(field) && dataMap[field] != null && dataMap[field] !== ''
                );
                
                if (sectionFields.length === 0) {
                    return;
                }
                
                // 섹션 제목
                const sectionTitle = document.createElement('h4');
                sectionTitle.className = 'section-subtitle';
                sectionTitle.textContent = section.title;
                container.appendChild(sectionTitle);
                
                // 섹션 테이블
                const table = document.createElement('table');
                table.className = 'table-kv-2col';
                const tbody = document.createElement('tbody');
                
                // 2열씩 배치
                for (let i = 0; i < sectionFields.length; i += 2) {
                    const tr = document.createElement('tr');
                    
                    // 첫 번째 컬럼
                    const field1 = sectionFields[i];
                    const th1 = document.createElement('th');
                    th1.textContent = field1;
                    tr.appendChild(th1);
                    
                    const td1 = document.createElement('td');
                    td1.setAttribute('data-field', field1);
                    td1.innerHTML = this.formatCellValue(dataMap[field1], field1);
                    tr.appendChild(td1);
                    
                    // 두 번째 컬럼 (있는 경우)
                    if (i + 1 < sectionFields.length) {
                        const field2 = sectionFields[i + 1];
                        const th2 = document.createElement('th');
                        th2.textContent = field2;
                        tr.appendChild(th2);
                        
                        const td2 = document.createElement('td');
                        td2.setAttribute('data-field', field2);
                        td2.innerHTML = this.formatCellValue(dataMap[field2], field2);
                        tr.appendChild(td2);
                    } else {
                        // 빈 셀
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
                container.appendChild(table);
            });
            
            this.container.innerHTML = '';
            this.container.appendChild(container);
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

    // DuplicatePersonsTable 클래스 - NULL 컬럼 제외 처리
    class DuplicatePersonsTable extends TableComponent {
        constructor(containerId, matchCriteria) {
            super(containerId, {
                className: 'table duplicate-persons-table',
                emptyMessage: '동일한 E-mail, 휴대폰 번호, 거주주소, 직장명, 직장주소를 가진 회원이 조회되지 않습니다.',
                showHeader: true
            });
            this.matchCriteria = matchCriteria || {};
            this.visibleColumnIndices = [];
        }
        
        setData(columns, rows) {
            this.columns = columns || [];
            this.rows = rows || [];
            
            // NULL이 아닌 컬럼 찾기
            this.visibleColumnIndices = [];
            if (this.rows.length > 0) {
                this.columns.forEach((col, idx) => {
                    // MATCH_TYPES는 항상 제외
                    if (col === 'MATCH_TYPES') return;
                    
                    // 모든 행에서 해당 컬럼이 NULL인지 체크
                    const hasNonNullValue = this.rows.some(row => {
                        const value = row[idx];
                        return value != null && value !== '';
                    });
                    
                    if (hasNonNullValue) {
                        this.visibleColumnIndices.push(idx);
                    }
                });
            } else {
                // 데이터가 없으면 MATCH_TYPES 제외한 모든 컬럼 표시
                this.columns.forEach((col, idx) => {
                    if (col !== 'MATCH_TYPES') {
                        this.visibleColumnIndices.push(idx);
                    }
                });
            }
            
            return this;
        }
        
        createTableHeader() {
            const thead = document.createElement('thead');
            const tr = document.createElement('tr');
            
            this.visibleColumnIndices.forEach(idx => {
                const th = document.createElement('th');
                th.textContent = this.columns[idx];
                tr.appendChild(th);
            });
            
            thead.appendChild(tr);
            return thead;
        }
        
        createTableBody() {
            const tbody = document.createElement('tbody');
            
            // 매칭 컬럼 인덱스
            const emailIdx = this.columns.indexOf('E-mail');
            const phoneIdx = this.columns.indexOf('휴대폰 번호');
            const addressIdx = this.columns.indexOf('거주주소');
            const detailAddressIdx = this.columns.indexOf('거주상세주소');
            const workplaceNameIdx = this.columns.indexOf('직장명');
            const workplaceAddressIdx = this.columns.indexOf('직장주소');
            const workplaceDetailAddressIdx = this.columns.indexOf('직장상세주소');
            const matchTypesIdx = this.columns.indexOf('MATCH_TYPES');
            
            this.rows.forEach((row, rowIndex) => {
                const tr = document.createElement('tr');
                const matchTypes = matchTypesIdx >= 0 ? row[matchTypesIdx] : '';
                
                // hover 효과
                if (this.options.hoverEffect) {
                    tr.addEventListener('mouseenter', () => tr.classList.add('hover'));
                    tr.addEventListener('mouseleave', () => tr.classList.remove('hover'));
                }
                
                // 표시할 컬럼만 셀 생성
                this.visibleColumnIndices.forEach(colIndex => {
                    const td = document.createElement('td');
                    const value = row[colIndex];
                    
                    // 매칭된 컬럼 강조 (복수 매칭 처리)
                    if (matchTypes && (
                        (matchTypes.includes('EMAIL') && colIndex === emailIdx) ||
                        (matchTypes.includes('ADDRESS') && (colIndex === addressIdx || colIndex === detailAddressIdx)) ||
                        (matchTypes.includes('WORKPLACE_NAME') && colIndex === workplaceNameIdx) ||
                        (matchTypes.includes('WORKPLACE_ADDRESS') && (colIndex === workplaceAddressIdx || colIndex === workplaceDetailAddressIdx))
                    )) {
                        td.style.backgroundColor = '#383838';
                        td.style.fontWeight = '600';
                    }
                    
                    // 휴대폰 번호는 항상 체크
                    if (colIndex === phoneIdx && this.matchCriteria.phone_suffix) {
                        const phone = String(value || '');
                        if (phone.slice(-4) === this.matchCriteria.phone_suffix) {
                            td.style.backgroundColor = '#383838';
                            td.style.fontWeight = '600';
                        }
                    }
                    
                    td.innerHTML = this.formatCellValue(value, this.columns[colIndex], colIndex, row);
                    tr.appendChild(td);
                });
                
                tbody.appendChild(tr);
            });
            
            return tbody;
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
            
            // Rule ID -> Name 매핑
            const ruleNameMap = new Map();
            rows.forEach(row => {
                const ruleId = row[idxRuleId];
                if (!ruleNameMap.has(ruleId)) {
                    ruleNameMap.set(ruleId, row[idxRuleName]);
                }
            });
            
            // 테이블 데이터 구성
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
            
            // DISTINCT 처리
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
            
            // canonicalIds 순서로 정렬
            const tableRows = canonicalIds
                .map(id => uniqueRules.get(id))
                .filter(Boolean);
            
            this.setData(this.options.visibleColumns, tableRows);
            this.render();
        }
    }

    /**
     * 섹션 토글 기능 초기화
     */
    function initSectionToggle() {
        document.querySelectorAll('.section h3').forEach(header => {
            header.addEventListener('click', function() {
                const section = this.parentElement;
                section.classList.toggle('collapsed');
            });
        });
    }

    /**
     * 팩토리 함수 - 전역 렌더링 함수들
     */
    window.TableRenderer = {
        /**
         * 통합 고객 정보 렌더링
         */
        renderCustomerUnified(containerId, columns, rows) {
            const table = new CustomerUnifiedTable(containerId);
            table.setData(columns, rows);
            table.render();
        },
        
        /**
         * Rule 히스토리 렌더링
         */
        renderRuleHistory(containerId, columns, rows) {
            const table = new RuleHistoryTable(containerId);
            table.setData(columns, rows);
            table.render();
        },
        
        /**
         * Alert 히스토리 렌더링
         */
        renderAlertHistory(containerId, columns, rows, alertId) {
            const table = new AlertHistoryTable(containerId, alertId);
            // 필요한 컬럼만 필터링
            const visibleCols = ['STDS_DTM', 'CUST_ID', 'STR_RULE_ID', 'STR_ALERT_ID', 'STR_RPT_MNGT_NO', 'STR_RULE_NM', 'TRAN_STRT', 'TRAN_END'];
            const colIndices = visibleCols.map(col => columns.indexOf(col));
            
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
        },
        
        /**
         * 의심거래 객관식 렌더링
         */
        renderObjectives(containerId, cols, rows, ruleObjMap, canonicalIds, repRuleId) {
            const table = new ObjectivesTable(containerId, ruleObjMap, repRuleId);
            table.renderFromAlertData(cols, rows, canonicalIds);
        },
        
        /**
         * Rule 설명 렌더링
         */
        renderRuleDescription(containerId, cols, rows, canonicalIds, repRuleId) {
            const table = new RuleDescriptionTable(containerId, repRuleId);
            table.renderFromAlertData(cols, rows, canonicalIds);
        }
    };
    
    // ==================== 전역 렌더링 함수들 ====================
    
    // 통합 고객 정보 렌더링
    window.renderCustomerUnifiedSection = function(columns, rows) {
        const section = document.getElementById('section_customer_unified');
        if (section) section.style.display = 'block';
        
        // 기존 섹션들 숨기기 (하위 호환성)
        const oldSections = ['section_person_info', 'section_person_detail'];
        oldSections.forEach(id => {
            const el = document.getElementById(id);
            if (el) el.style.display = 'none';
        });
        
        window.TableRenderer.renderCustomerUnified('result_table_customer_unified', columns, rows);
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
        window.TableRenderer.renderAlertHistory('result_table_alert_rule', cols, rows, alertId);
    };
    
    // 의심거래 객관식 렌더링
    window.renderObjectivesSection = function(cols, rows, ruleObjMap, canonicalIds, repRuleId) {
        const section = document.getElementById('section_objectives');
        if (section) section.style.display = 'block';
        window.TableRenderer.renderObjectives('result_table_objectives', cols, rows, ruleObjMap, canonicalIds, repRuleId);
    };
    
    // Rule 설명 렌더링
    window.renderRuleDescSection = function(cols, rows, canonicalIds, repRuleId) {
        const section = document.getElementById('section_rule_distinct');
        if (section) section.style.display = 'block';
        window.TableRenderer.renderRuleDescription('result_table_rule_distinct', cols, rows, canonicalIds, repRuleId);
    };

    // 동일_차명의심_상세 정보 렌더링
    window.renderDuplicatePersonsSection = function(columns, rows, matchCriteria) {
        const section = document.getElementById('section_duplicate_persons');
        if (section) section.style.display = 'block';
        
        const table = new DuplicatePersonsTable('result_table_duplicate_persons', matchCriteria);
        table.setData(columns, rows);
        table.render();
    };

    // 개인 관련인 정보 렌더링
    window.renderPersonRelatedSection = function(summaryText) {
        const section = document.getElementById('section_person_related');
        const container = document.getElementById('result_table_person_related');
        
        if (!section || !container) {
            console.warn('Person related section or container not found');
            return;
        }
        
        section.style.display = 'block';
        
        if (!summaryText) {
            container.innerHTML = `
                <div class="card empty-row">
                    내부입출금 거래 관련인 정보가 없습니다.
                </div>
            `;
            return;
        }
        
        // 텍스트 형식의 관련인 정보 표시
        container.innerHTML = `
            <div class="person-related-summary">
                <pre class="summary-text">${escapeHtml(summaryText)}</pre>
            </div>
        `;
        
        // 스타일 적용
        const style = document.createElement('style');
        style.textContent = `
            .person-related-summary {
                background: #1a1a1a;
                border: 1px solid #2a2a2a;
                border-radius: 12px;
                padding: 16px;
                margin-top: 10px;
            }
            
            .person-related-summary .summary-text {
                font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
                font-size: 13px;
                line-height: 1.5;
                color: #eaeaea;
                white-space: pre-wrap;
                word-wrap: break-word;
                margin: 0;
                background: transparent;
                border: none;
            }
            
            .person-related-summary .summary-text strong {
                color: #4fc3f7;
            }
            
            /* 통합 고객 정보 섹션 스타일 */
            .customer-info-sections {
                padding: 10px;
            }
            
            .customer-info-sections .section-subtitle {
                margin: 20px 0 10px 0;
                padding: 8px 12px;
                background: linear-gradient(90deg, #2a2a2a 0%, transparent 100%);
                border-left: 3px solid #4fc3f7;
                color: #4fc3f7;
                font-size: 14px;
                font-weight: 700;
                letter-spacing: 0.5px;
            }
            
            .customer-info-sections .section-subtitle:first-child {
                margin-top: 0;
            }
            
            .customer-info-sections .table-kv-2col {
                margin-bottom: 15px;
                background: #1a1a1a;
                border-radius: 8px;
            }
            
            .customer-info-sections .table-kv-2col th {
                background: #151515;
                font-size: 12px;
                color: #9a9a9a;
            }
            
            .customer-info-sections .table-kv-2col td {
                font-size: 13px;
                color: #eaeaea;
            }
            
            .customer-info-sections .table-kv-2col td[data-field="고객ID"],
            .customer-info-sections .table-kv-2col td[data-field="성명"],
            .customer-info-sections .table-kv-2col td[data-field="위험등급"] {
                font-weight: 600;
                color: #4fc3f7;
            }
            
            .customer-info-sections .table-kv-2col td[data-field="STR보고건수"]:not(:empty),
            .customer-info-sections .table-kv-2col td[data-field="Alert건수"]:not(:empty) {
                color: #ffa726;
                font-weight: 600;
            }
        `;
        
        // 스타일이 이미 추가되지 않았다면 추가
        if (!document.getElementById('person-related-style')) {
            style.id = 'person-related-style';
            document.head.appendChild(style);
        }
    };
    
    // HTML 이스케이프 헬퍼 함수
    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // DOM 준비 후 섹션 토글 초기화
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initSectionToggle);
    } else {
        initSectionToggle();
    }

    // 내보내기
    window.TableComponent = TableComponent;
    window.CustomerUnifiedTable = CustomerUnifiedTable;

})(window);