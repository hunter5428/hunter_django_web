// str_dashboard/static/str_dashboard/js/table_renderer.js
// 테이블 렌더링 전용 모듈 - ALERT ID별 매매/입출고 현황 추가

(function(window) {
    'use strict';

    // ==================== 기본 테이블 클래스 ====================
    class BaseTable {
        constructor(containerId, options = {}) {
            this.container = document.getElementById(containerId);
            this.options = {
                emptyMessage: '데이터가 없습니다.',
                showHeader: true,
                className: 'table',
                ...options
            };
        }

        setData(columns, rows) {
            this.columns = columns || [];
            this.rows = rows || [];
            return this;
        }

        render() {
            if (!this.container) return;
            
            if (!this.rows || this.rows.length === 0) {
                this.renderEmpty();
                return;
            }
            
            this.renderTable();
        }

        renderEmpty() {
            this.container.innerHTML = `
                <div class="card empty-row">
                    ${this.escapeHtml(this.options.emptyMessage)}
                </div>
            `;
        }

        renderTable() {
            // 서브클래스에서 구현
        }

        escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text || '';
            return div.innerHTML;
        }

        formatNumber(value) {
            if (typeof value !== 'number') return String(value || '');
            return value.toLocaleString('ko-KR');
        }
    }

    // ==================== 특화 테이블 클래스들 ====================
    
    // 2열 Key-Value 테이블
    class KeyValueTable extends BaseTable {
        renderTable() {
            const table = document.createElement('table');
            table.className = this.options.className;
            
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
            
            // 2열 레이아웃
            for (let i = 0; i < validFields.length; i += 2) {
                const tr = document.createElement('tr');
                
                // 첫 번째 열
                this.addKeyValuePair(tr, validFields[i]);
                
                // 두 번째 열
                if (i + 1 < validFields.length) {
                    this.addKeyValuePair(tr, validFields[i + 1]);
                } else {
                    this.addEmptyPair(tr);
                }
                
                tbody.appendChild(tr);
            }
            
            table.appendChild(tbody);
            this.container.innerHTML = '';
            this.container.appendChild(table);
        }

        addKeyValuePair(tr, field) {
            const th = document.createElement('th');
            th.textContent = field.col;
            tr.appendChild(th);
            
            const td = document.createElement('td');
            td.innerHTML = this.escapeHtml(field.value);
            tr.appendChild(td);
        }

        addEmptyPair(tr) {
            const th = document.createElement('th');
            th.innerHTML = '&nbsp;';
            tr.appendChild(th);
            
            const td = document.createElement('td');
            td.innerHTML = '&nbsp;';
            tr.appendChild(td);
        }
    }

    // 일반 데이터 테이블
    class DataTable extends BaseTable {
        renderTable() {
            const table = document.createElement('table');
            table.className = this.options.className;
            
            // 헤더
            if (this.options.showHeader && this.columns.length > 0) {
                const thead = document.createElement('thead');
                const tr = document.createElement('tr');
                
                const visibleColumns = this.options.visibleColumns || this.columns;
                visibleColumns.forEach(col => {
                    const th = document.createElement('th');
                    th.textContent = col;
                    tr.appendChild(th);
                });
                
                thead.appendChild(tr);
                table.appendChild(thead);
            }
            
            // 바디
            const tbody = document.createElement('tbody');
            
            this.rows.forEach((row, rowIndex) => {
                const tr = document.createElement('tr');
                
                // 하이라이트 처리
                if (this.options.highlightRow && this.options.highlightRow(row, rowIndex)) {
                    tr.className = this.options.highlightClass || 'highlighted';
                }
                
                const visibleColumns = this.options.visibleColumns || this.columns;
                visibleColumns.forEach(col => {
                    const colIndex = this.columns.indexOf(col);
                    const td = document.createElement('td');
                    const value = colIndex >= 0 ? row[colIndex] : '';
                    td.innerHTML = this.formatCellValue(value, col);
                    tr.appendChild(td);
                });
                
                tbody.appendChild(tr);
            });
            
            table.appendChild(tbody);
            this.container.innerHTML = '';
            this.container.appendChild(table);
        }

        formatCellValue(value, columnName) {
            if (value == null) return '';
            
            if (this.options.formatters && this.options.formatters[columnName]) {
                return this.options.formatters[columnName](value);
            }
            
            if (typeof value === 'number') {
                return this.formatNumber(value);
            }
            
            return this.escapeHtml(String(value));
        }
    }

    // ==================== 섹션별 렌더러 ====================
    class TableRenderer {
        
        // 통합 고객 정보
        static renderCustomerUnified(columns, rows) {
            const section = document.getElementById('section_customer_unified');
            if (section) section.style.display = 'block';
            
            const table = new KeyValueTable('result_table_customer_unified', {
                className: 'customer-unified-table',
                emptyMessage: '고객 정보가 없습니다.'
            });
            table.setData(columns, rows).render();
        }

        // 법인 관련인
        static renderCorpRelated(columns, rows) {
            const section = document.getElementById('section_corp_related');
            if (section) {
                section.style.display = 'block';
                const table = new DataTable('result_table_corp_related', {
                    className: 'table corp-related-table',
                    emptyMessage: '등록된 관련인 정보가 없습니다.'
                });
                table.setData(columns, rows).render();
            }
        }

        // 개인 관련인
        static renderPersonRelated(relatedPersonsData) {
            const section = document.getElementById('section_person_related');
            const container = document.getElementById('result_table_person_related');
            
            if (!section || !container) return;
            
            section.style.display = 'block';
            
            if (!relatedPersonsData || Object.keys(relatedPersonsData).length === 0) {
                container.innerHTML = '<div class="card empty-row">내부입출금 거래 관련인 정보가 없습니다.</div>';
                return;
            }
            
            // 개인 관련인 HTML 직접 생성 (구조가 복잡)
            let html = '';
            let personIndex = 0;
            
            for (const [custId, data] of Object.entries(relatedPersonsData)) {
                const info = data.info;
                if (!info) continue;
                
                personIndex++;
                html += this._renderPersonRelatedHTML(personIndex, custId, info, data.transactions || []);
            }
            
            container.innerHTML = html || '<div class="card empty-row">내부입출금 거래 관련인 정보가 없습니다.</div>';
        }

        static _renderPersonRelatedHTML(index, custId, info, transactions) {
            let html = `<div class="related-person-header">◆ 관련인 ${index}: ${info.name || 'N/A'} (CID: ${custId})</div>`;
            
            // 기본 정보 테이블
            html += `<table class="person-related-table"><tbody>`;
            html += `<tr>
                <th>실명번호</th><td>${info.id_number || 'N/A'}</td>
                <th>생년월일</th><td>${info.birth_date || 'N/A'} (만 ${info.age || 'N/A'}세)</td>
            </tr>`;
            html += `<tr>
                <th>성별</th><td>${info.gender || 'N/A'}</td>
                <th>거주지</th><td>${info.address || 'N/A'}</td>
            </tr>`;
            
            if (info.job || info.workplace) {
                html += `<tr>
                    <th>직업</th><td>${info.job || 'N/A'}</td>
                    <th>직장명</th><td>${info.workplace || 'N/A'}</td>
                </tr>`;
            }
            
            html += `<tr>
                <th>위험등급</th><td>${info.risk_grade || 'N/A'}</td>
                <th>총 거래횟수</th><td>${info.total_tran_count || 0}회</td>
            </tr>`;
            html += `</tbody></table>`;
            
            // 거래 내역 (생략 가능)
            if (transactions.length > 0) {
                html += this._renderTransactionTable(transactions);
            }
            
            return html;
        }

        static _renderTransactionTable(transactions) {
            const deposits = transactions.filter(t => t.tran_type === '내부입고');
            const withdrawals = transactions.filter(t => t.tran_type === '내부출고');
            
            let html = `<table class="person-related-table transaction-table">
                <thead><tr>
                    <th colspan="4">내부입고</th>
                    <th colspan="4">내부출고</th>
                </tr><tr>
                    <th>종목</th><th>수량</th><th>금액</th><th>건수</th>
                    <th>종목</th><th>수량</th><th>금액</th><th>건수</th>
                </tr></thead><tbody>`;
            
            const maxRows = Math.max(deposits.length, withdrawals.length);
            for (let i = 0; i < maxRows; i++) {
                html += '<tr>';
                
                // 내부입고
                if (i < deposits.length) {
                    const d = deposits[i];
                    html += `<td>${d.coin_symbol || '-'}</td>
                        <td>${parseFloat(d.tran_qty || 0).toLocaleString('ko-KR', {minimumFractionDigits: 4})}</td>
                        <td>${parseFloat(d.tran_amt || 0).toLocaleString('ko-KR')}원</td>
                        <td>${parseInt(d.tran_cnt || 0)}건</td>`;
                } else {
                    html += '<td>-</td><td>-</td><td>-</td><td>-</td>';
                }
                
                // 내부출고
                if (i < withdrawals.length) {
                    const w = withdrawals[i];
                    html += `<td>${w.coin_symbol || '-'}</td>
                        <td>${parseFloat(w.tran_qty || 0).toLocaleString('ko-KR', {minimumFractionDigits: 4})}</td>
                        <td>${parseFloat(w.tran_amt || 0).toLocaleString('ko-KR')}원</td>
                        <td>${parseInt(w.tran_cnt || 0)}건</td>`;
                } else {
                    html += '<td>-</td><td>-</td><td>-</td><td>-</td>';
                }
                
                html += '</tr>';
            }
            
            html += '</tbody></table>';
            return html;
        }

        // Rule 히스토리
        static renderRuleHistory(columns, rows, searchedRule, similarList) {
            const section = document.getElementById('section_rule_hist');
            if (section) section.style.display = 'block';
            
            const container = document.getElementById('result_table_rule_hist');
            if (!container) return;
            
            if (rows.length === 0) {
                // 커스텀 빈 메시지 (유사 조합 포함)
                container.innerHTML = this._renderRuleHistoryEmpty(searchedRule, similarList);
            } else {
                const table = new DataTable('result_table_rule_hist', {
                    className: 'table rule-history-table',
                    formatters: {
                        'STR_RULE_ID_NO_COUNT': (value) => `<strong>${value}</strong>건`
                    }
                });
                table.setData(columns, rows).render();
            }
        }

        static _renderRuleHistoryEmpty(searchedRule, similarList) {
            if (!searchedRule) {
                return '<div class="card empty-row">일치하는 히스토리가 없습니다.</div>';
            }
            
            let html = `<div class="card empty-row" style="text-align: left;">
                <p><strong>해당 RULE 조합 (${searchedRule})에 대한 히스토리가 없습니다.</strong></p>`;
            
            if (similarList && similarList.length > 0) {
                const similarity = Math.round(similarList[0].similarity * 100);
                html += `<div style="margin-top: 15px; padding: 12px; background: #222; border-radius: 8px;">
                    <p style="color: #4fc3f7;"><strong>유사한 RULE 조합 (유사도: ${similarity}%)</strong></p>
                    <table class="table" style="margin-top: 10px;"><thead><tr>
                        <th>STR_RULE_ID_LIST</th><th>COUNT</th><th>UPER</th><th>LWER</th>
                    </tr></thead><tbody>`;
                
                similarList.forEach(item => {
                    html += `<tr>
                        <td>${item.rule_list}</td>
                        <td><strong>${item.count}</strong>건</td>
                        <td>${item.uper || ''}</td>
                        <td>${item.lwer || ''}</td>
                    </tr>`;
                });
                
                html += '</tbody></table></div>';
            }
            
            html += '</div>';
            return html;
        }

        // Alert 히스토리
        static renderAlertHistory(cols, rows, alertId) {
            const section = document.getElementById('section_alert_rule');
            if (section) section.style.display = 'block';
            
            const visibleCols = ['STDS_DTM', 'CUST_ID', 'STR_RULE_ID', 'STR_ALERT_ID', 
                                'STR_RPT_MNGT_NO', 'STR_RULE_NM', 'TRAN_STRT', 'TRAN_END'];
            
            const table = new DataTable('result_table_alert_rule', {
                className: 'table alert-history-table',
                visibleColumns: visibleCols,
                highlightRow: (row) => {
                    const alertIdCol = cols.indexOf('STR_ALERT_ID');
                    return alertIdCol >= 0 && String(row[alertIdCol]) === String(alertId);
                },
                highlightClass: 'rep-row'
            });
            
            // 필터링된 데이터 생성
            const colIndices = visibleCols.map(col => cols.indexOf(col));
            const filteredRows = rows.map(row => colIndices.map(idx => idx >= 0 ? row[idx] : ''));
            
            table.setData(visibleCols, filteredRows).render();
        }

        // 의심거래 객관식
        static renderObjectives(cols, rows, ruleObjMap, canonicalIds, repRuleId) {
            const section = document.getElementById('section_objectives');
            if (section) section.style.display = 'block';
            
            const idxRuleId = cols.indexOf('STR_RULE_ID');
            const idxRuleName = cols.indexOf('STR_RULE_NM');
            
            if (idxRuleId < 0 || idxRuleName < 0) {
                const table = new DataTable('result_table_objectives');
                table.renderEmpty();
                return;
            }
            
            // 데이터 준비
            const ruleNameMap = new Map();
            rows.forEach(row => {
                const ruleId = row[idxRuleId];
                if (!ruleNameMap.has(ruleId)) {
                    ruleNameMap.set(ruleId, row[idxRuleName]);
                }
            });
            
            const tableColumns = ['STR_RULE_ID', 'STR_RULE_NM', '객관식정보'];
            const tableRows = canonicalIds.map(ruleId => [
                ruleId,
                ruleNameMap.get(ruleId) || '',
                ruleObjMap[ruleId] ? ruleObjMap[ruleId].join('\n') : '-'
            ]);
            
            const table = new DataTable('result_table_objectives', {
                className: 'table objectives-table',
                highlightRow: (row) => String(row[0]) === String(repRuleId),
                highlightClass: 'rep-row',
                formatters: {
                    '객관식정보': (value) => value ? value.replace(/\n/g, '<br>') : '-'
                }
            });
            
            table.setData(tableColumns, tableRows).render();
        }

        // Rule 설명
        static renderRuleDesc(cols, rows, canonicalIds, repRuleId) {
            const section = document.getElementById('section_rule_distinct');
            if (section) section.style.display = 'block';
            
            const visibleCols = ['STR_RULE_ID', 'STR_RULE_DTL_EXP', 'STR_RULE_EXTR_COND_CTNT', 'AML_BSS_CTNT'];
            const indices = {};
            
            visibleCols.forEach(col => {
                indices[col] = cols.indexOf(col);
            });
            
            if (Object.values(indices).some(v => v < 0)) {
                const table = new DataTable('result_table_rule_distinct');
                table.renderEmpty();
                return;
            }
            
            // DISTINCT 처리
            const uniqueRules = new Map();
            rows.forEach(row => {
                const ruleId = row[indices.STR_RULE_ID];
                if (!uniqueRules.has(ruleId)) {
                    uniqueRules.set(ruleId, visibleCols.map(col => row[indices[col]]));
                }
            });
            
            const tableRows = canonicalIds.map(id => uniqueRules.get(id)).filter(Boolean);
            
            const table = new DataTable('result_table_rule_distinct', {
                className: 'table rule-desc-table',
                visibleColumns: visibleCols,
                highlightRow: (row) => String(row[0]) === String(repRuleId),
                highlightClass: 'rep-row'
            });
            
            table.setData(visibleCols, tableRows).render();
        }

        // 중복 회원
        static renderDuplicatePersons(columns, rows, matchCriteria) {
            const section = document.getElementById('section_duplicate_persons');
            if (section) section.style.display = 'block';
            
            const container = document.getElementById('result_table_duplicate_persons');
            if (!container) return;
            
            if (!rows || rows.length === 0) {
                container.innerHTML = '<div class="card empty-row">동일한 정보를 가진 회원이 조회되지 않습니다.</div>';
                return;
            }
            
            // 중복 회원 HTML 직접 생성
            let html = '<div class="duplicate-persons-wrapper">';
            
            rows.forEach(row => {
                html += this._renderDuplicatePersonHTML(columns, row, matchCriteria);
            });
            
            html += '</div>';
            container.innerHTML = html;
        }

        static _renderDuplicatePersonHTML(columns, row, matchCriteria) {
            const getValue = (colName) => {
                const idx = columns.indexOf(colName);
                return idx >= 0 ? row[idx] : '';
            };
            
            const name = getValue('성명') || 'N/A';
            const cid = getValue('고객ID') || 'N/A';
            const matchTypes = getValue('MATCH_TYPES') || '';
            
            let html = `<div class="duplicate-person-group">
                <div class="duplicate-person-header">${name} (CID: ${cid})</div>
                <div class="duplicate-person-grid">`;
            
            const fields = [
                'MATCH_TYPES', '고객ID', 'MID', '성명', '영문명', 
                '실명번호', '생년월일', 'E-mail', '휴대폰 번호',
                '거주주소', '직장명', '직장주소'
            ];
            
            fields.forEach(field => {
                const value = getValue(field);
                if (!value || field === 'MATCH_TYPES') return;
                
                const isMatched = this._checkIfMatched(field, value, matchTypes, matchCriteria);
                html += `<div class="duplicate-field ${isMatched ? 'matched' : ''}">
                    <div class="duplicate-field-label">${field}</div>
                    <div class="duplicate-field-value">${value}</div>
                </div>`;
            });
            
            html += '</div></div>';
            return html;
        }

        static _checkIfMatched(field, value, matchTypes, criteria) {
            if (!matchTypes || !value) return false;
            
            const checkMap = {
                'E-mail': () => matchTypes.includes('EMAIL'),
                '거주주소': () => matchTypes.includes('ADDRESS'),
                '직장명': () => matchTypes.includes('WORKPLACE_NAME'),
                '직장주소': () => matchTypes.includes('WORKPLACE_ADDRESS'),
                '휴대폰 번호': () => criteria.phone_suffix && value.slice(-4) === criteria.phone_suffix
            };
            
            return checkMap[field] ? checkMap[field]() : false;
        }

        // IP 접속 이력
        static renderIPHistory(columns, rows) {
            const section = document.getElementById('section_ip_access_history');
            if (section) {
                section.style.display = 'block';
                section.classList.add('collapsed'); // 기본 접힘
                
                const visibleColumns = ['접속일시', '국가한글명', '채널', 'IP주소', '접속위치', 'OS정보', '브라우저정보'];
                
                const table = new DataTable('result_table_ip_access_history', {
                    className: 'table ip-history-table',
                    visibleColumns: visibleColumns,
                    emptyMessage: 'IP 접속 이력이 없습니다.',
                    highlightRow: (row) => {
                        const countryIdx = columns.indexOf('국가한글명');
                        if (countryIdx >= 0) {
                            const country = String(row[countryIdx] || '').trim();
                            return country && country !== '대한민국' && country !== '한국';
                        }
                        return false;
                    },
                    highlightClass: 'rep-row'
                });
                
                // 필터링
                const visibleIndices = visibleColumns.map(col => columns.indexOf(col)).filter(idx => idx >= 0);
                const filteredColumns = visibleIndices.map(idx => columns[idx]);
                const filteredRows = rows.map(row => visibleIndices.map(idx => row[idx]));
                
                table.setData(filteredColumns, filteredRows).render();
            }
        }

        // Orderbook 분석 - ALERT ID별 현황 추가
        static renderOrderbookAnalysis(analysisResult, alertData) {
            // 동적으로 섹션 생성
            this._createOrderbookSections();
            
            // 기간 정보 추출
            const periodInfo = analysisResult.period_info || {};
            
            // 각 섹션 렌더링
            this._renderOrderbookPatterns(analysisResult, periodInfo);
            this._renderAlertOrderbook(analysisResult, alertData);  // 새로운 섹션
            this._renderOrderbookDaily(analysisResult);
        }

        static _createOrderbookSections() {
            const sections = [
                { id: 'section_orderbook_patterns', title: '거래원장(Orderbook) 개요', collapsed: false },
                { id: 'section_alert_orderbook', title: 'ALERT ID별 매매/입출고 현황', collapsed: false },  // 새로운 섹션
                { id: 'section_orderbook_daily', title: '일자별 매수/매도, 입출금 현황', collapsed: true }
            ];
            
            const ipSection = document.getElementById('section_ip_access_history');
            const insertPoint = ipSection ? ipSection.nextSibling : document.querySelector('.app-main');
            
            sections.forEach(config => {
                if (!document.getElementById(config.id)) {
                    const section = document.createElement('div');
                    section.id = config.id;
                    section.className = `section ${config.collapsed ? 'collapsed' : ''}`;
                    section.innerHTML = `
                        <h3>${config.title}</h3>
                        <div class="table-wrap" id="result_${config.id.replace('section_', '')}"></div>
                    `;
                    
                    if (insertPoint && insertPoint.parentNode) {
                        insertPoint.parentNode.insertBefore(section, insertPoint);
                    } else if (insertPoint) {
                        insertPoint.appendChild(section);
                    }
                }
            });
        }

        static _renderOrderbookPatterns(data, periodInfo) {
            const container = document.getElementById('result_orderbook_patterns');
            if (!container || !data.patterns) return;
            
            const section = document.getElementById('section_orderbook_patterns');
            if (section) {
                section.style.display = 'block';
                
                // 제목에 기간 정보 추가
                const h3 = section.querySelector('h3');
                if (h3 && periodInfo.start_date && periodInfo.end_date) {
                    h3.textContent = `거래원장(Orderbook) 개요 (${periodInfo.start_date} ~ ${periodInfo.end_date})`;
                }
            }
            
            // 패턴 카드 HTML 생성
            const items = [
                { key: 'buy', label: '총 매수', amount: data.patterns.total_buy_amount, count: data.patterns.total_buy_count, details: data.patterns.buy_details },
                { key: 'sell', label: '총 매도', amount: data.patterns.total_sell_amount, count: data.patterns.total_sell_count, details: data.patterns.sell_details },
                { key: 'deposit_krw', label: '원화 입금', amount: data.patterns.total_deposit_krw, count: data.patterns.total_deposit_krw_count, details: data.patterns.deposit_krw_details },
                { key: 'withdraw_krw', label: '원화 출금', amount: data.patterns.total_withdraw_krw, count: data.patterns.total_withdraw_krw_count, details: data.patterns.withdraw_krw_details },
                { key: 'deposit_crypto', label: '가상자산 입금', amount: data.patterns.total_deposit_crypto, count: data.patterns.total_deposit_crypto_count, details: data.patterns.deposit_crypto_details },
                { key: 'withdraw_crypto', label: '가상자산 출금', amount: data.patterns.total_withdraw_crypto, count: data.patterns.total_withdraw_crypto_count, details: data.patterns.withdraw_crypto_details }
            ];
            
            let html = '<div class="orderbook-patterns-grid">';
            items.forEach(item => {
                if (item.amount > 0 || item.count > 0) {
                    const formattedAmount = this._formatAmount(item.amount);
                    const actualAmount = item.amount.toLocaleString('ko-KR');
                    
                    html += `<div class="pattern-stat-card" data-action="${item.key}">
                        <div class="pattern-stat-label">${item.label}</div>
                        <div class="pattern-stat-value">
                            ${formattedAmount}
                            <span class="actual-amount">(${actualAmount}원)</span>
                        </div>
                        <div class="pattern-stat-count">${item.count.toLocaleString('ko-KR')}건</div>
                        <div class="pattern-detail" id="detail-${item.key}">
                            ${this._renderPatternDetail(item.details)}
                        </div>
                    </div>`;
                }
            });
            html += '</div>';
            
            container.innerHTML = html;
            
            // 이벤트 리스너 추가
            container.querySelectorAll('.pattern-stat-card').forEach(card => {
                card.addEventListener('click', (e) => {
                    // 상세 영역이 아닌 경우에만 토글
                    if (!e.target.closest('.pattern-detail')) {
                        const detail = card.querySelector('.pattern-detail');
                        detail.classList.toggle('show');
                        card.classList.toggle('expanded');
                    }
                });
            });
        }

        // 새로운 메서드: ALERT ID별 매매/입출고 현황
        static _renderAlertOrderbook(analysisResult, alertData) {
            const container = document.getElementById('result_alert_orderbook');
            if (!container) return;
            
            const section = document.getElementById('section_alert_orderbook');
            if (section) section.style.display = 'block';
            
            // alertData가 없으면 빈 메시지
            if (!alertData || !alertData.rows || alertData.rows.length === 0) {
                container.innerHTML = '<div class="card empty-row">ALERT 데이터가 없습니다.</div>';
                return;
            }
            
            const cols = alertData.cols;
            const rows = alertData.rows;
            const repAlertId = alertData.repAlertId;
            
            // 필요한 컬럼 인덱스
            const idxAlertId = cols.indexOf('STR_ALERT_ID');
            const idxRuleId = cols.indexOf('STR_RULE_ID');
            const idxStdsDtm = cols.indexOf('STDS_DTM');
            const idxTranStart = cols.indexOf('TRAN_STRT');
            const idxTranEnd = cols.indexOf('TRAN_END');
            
            let html = '';
            
            // 대표 ALERT ID 상세내역 먼저 표시
            if (repAlertId) {
                const repRow = rows.find(r => String(r[idxAlertId]) === String(repAlertId));
                if (repRow && analysisResult.alert_details && analysisResult.alert_details[repAlertId]) {
                    html += `<div class="alert-detail-section">
                        <h4 style="color: #4fc3f7; margin-bottom: 10px;">◆ 대표 ALERT ID (${repAlertId}) 상세내역</h4>
                        ${this._renderAlertDetail(analysisResult.alert_details[repAlertId])}
                    </div>`;
                }
            }
            
            // ALERT ID 목록 테이블
            html += '<div style="margin-top: 20px;">';
            html += '<table class="table alert-orderbook-table"><thead><tr>';
            html += '<th>STDS_DTM</th><th>STR_ALERT_ID</th><th>STR_RULE_ID</th>';
            html += '<th>TRAN_STRT</th><th>TRAN_END</th><th>조회기간</th>';
            html += '</tr></thead><tbody>';
            
            // 각 ALERT ID별로 행 생성
            const uniqueAlerts = new Map();
            rows.forEach(row => {
                const alertId = row[idxAlertId];
                if (!uniqueAlerts.has(alertId)) {
                    uniqueAlerts.set(alertId, row);
                }
            });
            
            uniqueAlerts.forEach((row, alertId) => {
                const ruleId = row[idxRuleId];
                const tranStart = row[idxTranStart];
                const tranEnd = row[idxTranEnd];
                
                // 조회 기간 계산 (특정 RULE ID는 12개월, 나머지는 3개월)
                const monthsBack = (ruleId === 'IO000' || ruleId === 'IO111') ? 12 : 3;
                const queryPeriod = this._calculateQueryPeriod(tranStart, tranEnd, monthsBack);
                
                const isRep = String(alertId) === String(repAlertId);
                html += `<tr class="${isRep ? 'rep-row' : ''}" data-alert-id="${alertId}" 
                         data-rule-id="${ruleId}" data-period-start="${queryPeriod.start}" 
                         data-period-end="${queryPeriod.end}">`;
                html += `<td>${row[idxStdsDtm] || ''}</td>`;
                html += `<td class="clickable-alert" style="cursor: pointer; color: #4fc3f7; text-decoration: underline;">
                         ${alertId}</td>`;
                html += `<td>${ruleId || ''}</td>`;
                html += `<td>${tranStart || ''}</td>`;
                html += `<td>${tranEnd || ''}</td>`;
                html += `<td style="font-size: 11px; color: #9a9a9a;">${queryPeriod.display}</td>`;
                html += '</tr>';
                
                // 상세 행 (숨김 상태)
                html += `<tr class="alert-detail-row" id="detail-row-${alertId}" style="display: none;">`;
                html += `<td colspan="6"><div id="alert-detail-${alertId}" class="alert-detail-content">
                         <div style="text-align: center; color: #9a9a9a;">로딩 중...</div></div></td>`;
                html += '</tr>';
            });
            
            html += '</tbody></table></div>';
            
            container.innerHTML = html;
            
            // 클릭 이벤트 추가
            container.querySelectorAll('.clickable-alert').forEach(el => {
                el.addEventListener('click', (e) => {
                    const row = e.target.closest('tr');
                    const alertId = row.dataset.alertId;
                    const detailRow = document.getElementById(`detail-row-${alertId}`);
                    
                    if (detailRow.style.display === 'none') {
                        // 상세 정보 표시
                        detailRow.style.display = 'table-row';
                        this._loadAlertDetail(alertId, row.dataset.periodStart, row.dataset.periodEnd, analysisResult);
                    } else {
                        // 숨기기
                        detailRow.style.display = 'none';
                    }
                });
            });
        }

        static _calculateQueryPeriod(tranStart, tranEnd, monthsBack) {
            if (!tranStart || !tranEnd) {
                return { start: '', end: '', display: 'N/A' };
            }
            
            try {
                const startDate = new Date(tranStart);
                startDate.setMonth(startDate.getMonth() - monthsBack);
                
                const startStr = startDate.toISOString().split('T')[0];
                const endStr = tranEnd.split(' ')[0];
                
                return {
                    start: startStr,
                    end: endStr,
                    display: `${startStr} ~ ${endStr} (-${monthsBack}개월)`
                };
            } catch (e) {
                return { start: '', end: '', display: 'Error' };
            }
        }

        static _loadAlertDetail(alertId, periodStart, periodEnd, analysisResult) {
            const container = document.getElementById(`alert-detail-${alertId}`);
            if (!container) return;
            
            // analysisResult에서 해당 ALERT의 상세 정보 찾기
            if (analysisResult.alert_details && analysisResult.alert_details[alertId]) {
                container.innerHTML = this._renderAlertDetail(analysisResult.alert_details[alertId]);
            } else {
                // 상세 정보가 없으면 서버에서 로드
                container.innerHTML = '<div class="alert-loading">분석 중...</div>';
                
                // AJAX 요청으로 서버에서 상세 정보 로드
                fetch(window.URLS.analyze_alert_orderbook, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': this._getCookie('csrftoken')
                    },
                    body: new URLSearchParams({
                        alert_id: alertId,
                        start_date: periodStart,
                        end_date: periodEnd,
                        cache_key: analysisResult.cache_key || ''
                    })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        container.innerHTML = this._renderAlertDetail(data.detail);
                    } else {
                        container.innerHTML = '<div style="color: #ff6b6b;">분석 실패: ' + (data.message || '알 수 없는 오류') + '</div>';
                    }
                })
                .catch(error => {
                    console.error('Error loading alert detail:', error);
                    container.innerHTML = '<div style="color: #ff6b6b;">데이터 로드 실패</div>';
                });
            }
        }
        
        static _getCookie(name) {
            const value = `; ${document.cookie}`;
            const parts = value.split(`; ${name}=`);
            return parts.length === 2 ? decodeURIComponent(parts.pop().split(';').shift()) : undefined;
        }

        static _renderAlertDetail(detail) {
            if (!detail) return '<div>상세 정보가 없습니다.</div>';
            
            let html = '<div class="alert-detail-wrapper" style="padding: 10px; background: #0a0a0a; border-radius: 8px;">';
            
            // 요약 정보
            html += '<div style="margin-bottom: 15px; padding: 10px; background: #1a1a1a; border-radius: 6px;">';
            html += `<div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px;">`;
            
            if (detail.summary) {
                const s = detail.summary;
                html += `<div><strong>매수:</strong> ${this._formatAmount(s.buy_amount || 0)} (${s.buy_count || 0}건)</div>`;
                html += `<div><strong>매도:</strong> ${this._formatAmount(s.sell_amount || 0)} (${s.sell_count || 0}건)</div>`;
                html += `<div><strong>원화입금:</strong> ${this._formatAmount(s.deposit_krw || 0)} (${s.deposit_krw_count || 0}건)</div>`;
                html += `<div><strong>원화출금:</strong> ${this._formatAmount(s.withdraw_krw || 0)} (${s.withdraw_krw_count || 0}건)</div>`;
                html += `<div><strong>가상자산입금:</strong> ${this._formatAmount(s.deposit_crypto || 0)} (${s.deposit_crypto_count || 0}건)</div>`;
                html += `<div><strong>가상자산출금:</strong> ${this._formatAmount(s.withdraw_crypto || 0)} (${s.withdraw_crypto_count || 0}건)</div>`;
            }
            
            html += '</div></div>';
            
            // 종목별 상세
            if (detail.by_ticker) {
                html += '<div style="margin-top: 10px;">';
                html += '<h5 style="color: #bdbdbd; margin-bottom: 8px;">종목별 상세</h5>';
                html += '<div style="display: grid; grid-template-columns: repeat(2, 1fr); gap: 10px;">';
                
                ['buy', 'sell', 'deposit', 'withdraw'].forEach(action => {
                    if (detail.by_ticker[action] && detail.by_ticker[action].length > 0) {
                        const actionLabel = {
                            buy: '매수', sell: '매도', 
                            deposit: '입금', withdraw: '출금'
                        }[action];
                        
                        html += `<div style="padding: 8px; background: #151515; border-radius: 4px;">`;
                        html += `<strong style="color: #4fc3f7;">${actionLabel}:</strong><br>`;
                        
                        detail.by_ticker[action].slice(0, 5).forEach(item => {
                            html += `<div style="margin-left: 10px; font-size: 12px;">`;
                            html += `${item.ticker}: ${this._formatAmount(item.amount)} (${item.count}건)`;
                            html += `</div>`;
                        });
                        
                        if (detail.by_ticker[action].length > 5) {
                            html += `<div style="margin-left: 10px; font-size: 11px; color: #7a7a7a;">`;
                            html += `... 외 ${detail.by_ticker[action].length - 5}개 종목`;
                            html += `</div>`;
                        }
                        
                        html += '</div>';
                    }
                });
                
                html += '</div></div>';
            }
            
            html += '</div>';
            return html;
        }

        static _renderPatternDetail(details) {
            if (!details || details.length === 0) {
                return '<div class="pattern-detail-item">상세 내역이 없습니다.</div>';
            }
            
            let html = '';
            const maxItems = Math.min(details.length, 20); // 최대 20개
            
            for (let i = 0; i < maxItems; i++) {
                const [ticker, data] = details[i];
                const amount = Math.abs(data.amount_krw);
                const formattedAmount = amount.toLocaleString('ko-KR');
                
                html += `<div class="pattern-detail-item">
                    <span class="pattern-detail-ticker">${ticker}</span>: 
                    ${formattedAmount}원 (${data.count}건)
                </div>`;
            }
            
            // 20개 이상인 경우 메시지 추가
            if (details.length > 20) {
                const remaining = details.length - 20;
                html += `<div class="pattern-detail-more">
                    ... 외 ${remaining}개 종목 더보기 (상위 20개만 표시)
                </div>`;
            }
            
            return html;
        }

        static _renderOrderbookDaily(data) {
            const container = document.getElementById('result_orderbook_daily');
            if (!container || !data.daily_summary) return;
            
            document.getElementById('section_orderbook_daily').style.display = 'block';
            
            // 일자별 테이블 생성
            let html = '<table class="table daily-summary-table"><thead><tr>';
            html += '<th>날짜</th><th>매수</th><th>매도</th><th>원화입금</th><th>원화출금</th>';
            html += '<th>가상자산<br>내부입금</th><th>가상자산<br>내부출금</th>';
            html += '<th>가상자산<br>외부입금</th><th>가상자산<br>외부출금</th>';
            html += '</tr></thead><tbody>';
            
            data.daily_summary.forEach(day => {
                html += `<tr>
                    <td>${day['날짜']}</td>
                    <td>${day['매수'].total_amount.toLocaleString('ko-KR')}</td>
                    <td>${day['매도'].total_amount.toLocaleString('ko-KR')}</td>
                    <td>${day['원화입금'].total_amount.toLocaleString('ko-KR')}</td>
                    <td>${day['원화출금'].total_amount.toLocaleString('ko-KR')}</td>
                    <td>${day['가상자산내부입금'].total_amount.toLocaleString('ko-KR')}</td>
                    <td>${day['가상자산내부출금'].total_amount.toLocaleString('ko-KR')}</td>
                    <td>${day['가상자산외부입금'].total_amount.toLocaleString('ko-KR')}</td>
                    <td>${day['가상자산외부출금'].total_amount.toLocaleString('ko-KR')}</td>
                </tr>`;
            });
            
            html += '</tbody></table>';
            container.innerHTML = html;
        }

        static _formatAmount(amount) {
            const absAmount = Math.abs(amount);
            if (absAmount === 0) return '0원';
            
            const units = [
                { value: 1000000000000, name: '조' },
                { value: 100000000, name: '억' },
                { value: 10000, name: '만' }
            ];
            
            let result = [];
            let remaining = absAmount;
            
            for (const unit of units) {
                if (remaining >= unit.value) {
                    const unitAmount = Math.floor(remaining / unit.value);
                    result.push(`${unitAmount.toLocaleString('ko-KR')}${unit.name}`);
                    remaining = remaining % unit.value;
                }
            }
            
            if (result.length === 0) {
                return `${absAmount.toLocaleString('ko-KR')}원`;
            }
            
            // 남은 금액이 있으면 추가
            if (remaining >= 10000) {
                const manAmount = Math.floor(remaining / 10000);
                result.push(`${manAmount.toLocaleString('ko-KR')}만`);
            }
            
            return result.join(' ') + '원';
        }
    }

    // ==================== 섹션 토글 기능 ====================
    function initSectionToggle() {
        document.addEventListener('click', function(e) {
            if (e.target.matches('.section h3')) {
                const section = e.target.parentElement;
                section.classList.toggle('collapsed');
                
                // 상태 저장
                const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
                states[section.id] = section.classList.contains('collapsed');
                localStorage.setItem('sectionStates', JSON.stringify(states));
            }
        });
        
        // 저장된 상태 복원
        const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
        Object.entries(states).forEach(([sectionId, isCollapsed]) => {
            const section = document.getElementById(sectionId);
            if (section) {
                section.classList.toggle('collapsed', isCollapsed);
            }
        });
    }

    // 초기화
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initSectionToggle);
    } else {
        initSectionToggle();
    }

    // 전역 노출
    window.TableRenderer = TableRenderer;

})(window);