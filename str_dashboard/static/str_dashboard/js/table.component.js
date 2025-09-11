// str_dashboard/static/str_dashboard/js/table.component.js

/**
 * 통합 테이블 렌더링 컴포넌트
 * 모든 섹션의 테이블 렌더링을 표준화
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
                const tr = document.createElement('tr');
                
                // 컬럼명 (th)
                const th = document.createElement('th');
                th.textContent = col;
                tr.appendChild(th);
                
                // 값 (td)
                const td = document.createElement('td');
                const value = row[idx];
                td.innerHTML = this.formatCellValue(value, col, idx);
                tr.appendChild(td);
                
                tbody.appendChild(tr);
            });
            
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
                
                // 행 하이라이트 처리
                if (this.options.highlightRow) {
                    const shouldHighlight = typeof this.options.highlightRow === 'function' 
                        ? this.options.highlightRow(row, rowIndex)
                        : this.options.highlightRow === rowIndex;
                    
                    if (shouldHighlight) {
                        tr.className = this.options.highlightClass || 'highlighted';
                    }
                }
                
                // hover 효과
                if (this.options.hoverEffect) {
                    tr.addEventListener('mouseenter', () => tr.classList.add('hover'));
                    tr.addEventListener('mouseleave', () => tr.classList.remove('hover'));
                }
                
                // 셀 생성
                const visibleColumns = this.options.visibleColumns || this.columns;
                visibleColumns.forEach(col => {
                    const colIndex = this.columns.indexOf(col);
                    const td = document.createElement('td');
                    const value = row[colIndex];
                    
                    td.innerHTML = this.formatCellValue(value, col, colIndex, row);
                    
                    // 셀 클릭 이벤트 (옵션)
                    if (this.options.onCellClick) {
                        td.style.cursor = 'pointer';
                        td.addEventListener('click', () => {
                            this.options.onCellClick(value, col, row, rowIndex);
                        });
                    }
                    
                    tr.appendChild(td);
                });
                
                // 행 클릭 이벤트 (옵션)
                if (this.options.onRowClick) {
                    tr.style.cursor = 'pointer';
                    tr.addEventListener('click', () => {
                        this.options.onRowClick(row, rowIndex);
                    });
                }
                
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
    
    // 고객 정보 테이블 (Key-Value 형태)
    class PersonInfoTable extends TableComponent {
        constructor(containerId) {
            super(containerId, {
                ...TableOptions.KEY_VALUE,
                emptyMessage: '고객 정보가 없습니다.',
                className: 'table person-info-table'
            });
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
        }
    }

    // Alert 발생 내역 테이블
    class AlertHistoryTable extends TableComponent {
        constructor(containerId, alertId) {
            super(containerId, {
                className: 'table alert-history-table',
                highlightRow: (row, index) => {
                    // alertId와 일치하는 행 하이라이트
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
                        // 줄바꿈 처리
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
        
        /**
         * 객관식 데이터로 변환하여 렌더링
         */
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
        
        /**
         * Alert 데이터에서 DISTINCT Rule 추출하여 렌더링
         */
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
     * 팩토리 함수 - 전역 렌더링 함수들을 대체
     */
    window.TableRenderer = {
        /**
         * 고객 정보 렌더링
         */
        renderPersonInfo(containerId, columns, rows) {
            const table = new PersonInfoTable(containerId);
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
            const visibleCols = ['STDS_DTM', 'CUST_ID', 'STR_RULE_ID', 'STR_ALERT_ID', 'STR_RPT_MNGT_NO', 'STR_RULE_NM'];
            const colIndices = visibleCols.map(col => columns.indexOf(col));
            
            if (colIndices.some(idx => idx < 0)) {
                table.renderEmpty();
                return;
            }
            
            const filteredRows = rows.map(row => 
                colIndices.map(idx => row[idx])
            );
            
            table.setData(visibleCols, filteredRows);
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
    
    // 기존 전역 함수들과의 호환성 유지
    window.renderPersonInfoSection = function(columns, rows) {
        const section = document.getElementById('section_person_info');
        if (section) section.style.display = 'block';
        window.TableRenderer.renderPersonInfo('result_table_person_info', columns, rows);
    };
    
    window.renderRuleHistorySection = function(columns, rows) {
        const section = document.getElementById('section_rule_hist');
        if (section) section.style.display = 'block';
        window.TableRenderer.renderRuleHistory('result_table_rule_hist', columns, rows);
    };
    
    window.renderAlertHistSection = function(cols, rows, alertId) {
        const section = document.getElementById('section_alert_rule');
        if (section) section.style.display = 'block';
        window.TableRenderer.renderAlertHistory('result_table_alert_rule', cols, rows, alertId);
    };
    
    window.renderObjectivesSection = function(cols, rows, ruleObjMap, canonicalIds, repRuleId) {
        const section = document.getElementById('section_objectives');
        if (section) section.style.display = 'block';
        window.TableRenderer.renderObjectives('result_table_objectives', cols, rows, ruleObjMap, canonicalIds, repRuleId);
    };
    
    window.renderRuleDescSection = function(cols, rows, canonicalIds, repRuleId) {
        const section = document.getElementById('section_rule_distinct');
        if (section) section.style.display = 'block';
        window.TableRenderer.renderRuleDescription('result_table_rule_distinct', cols, rows, canonicalIds, repRuleId);
    };
    
    // 내보내기
    window.TableComponent = TableComponent;

})(window);