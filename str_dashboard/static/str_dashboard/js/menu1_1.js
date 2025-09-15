// str_dashboard/static/str_dashboard/js/menu1_1.js
// ALERT ID 조회 페이지 메인 로직 - 리팩토링 버전

(function() {
    'use strict';

    // ==================== 유틸리티 ====================
    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);
    
    const getCookie = (name) => {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        return parts.length === 2 ? decodeURIComponent(parts.pop().split(';').shift()) : undefined;
    };

    // ==================== API 호출 모듈 ====================
    class APIClient {
        constructor(baseHeaders) {
            this.headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-CSRFToken': getCookie('csrftoken'),
                ...baseHeaders
            };
        }

        async post(url, data) {
            try {
                const response = await fetch(url, {
                    method: 'POST',
                    headers: this.headers,
                    body: new URLSearchParams(data)
                });
                
                // HTML 응답 체크 (로그인 페이지 리다이렉트 등)
                const contentType = response.headers.get('content-type');
                if (!contentType || !contentType.includes('application/json')) {
                    throw new Error('서버 응답이 JSON 형식이 아닙니다. 세션이 만료되었을 수 있습니다.');
                }
                
                return await response.json();
            } catch (error) {
                console.error('API call failed:', error);
                throw error;
            }
        }
    }

    // ==================== 상태 관리 ====================
    class SearchState {
        constructor() {
            this.reset();
        }

        reset() {
            this.currentAlertId = null;
            this.alertData = null;
            this.customerData = null;
            this.isSearching = false;
            this.abortController = null;
        }

        setSearching(value) {
            this.isSearching = value;
        }

        setAlertData(data) {
            this.alertData = data;
        }

        abort() {
            if (this.abortController) {
                this.abortController.abort();
                this.abortController = null;
            }
        }
    }

    // ==================== UI 관리 ====================
    class UIManager {
        static hideAllSections() {
            $$('.section').forEach(section => {
                section.style.display = 'none';
                // 기존 내용 초기화
                const container = section.querySelector('.table-wrap');
                if (container) {
                    container.innerHTML = '';
                }
            });
        }

        static showSection(sectionId) {
            const section = document.getElementById(sectionId);
            if (section) {
                section.style.display = 'block';
            }
        }

        static showLoading(show = true) {
            const btn = $('#alert_id_search_btn');
            if (btn) {
                btn.disabled = show;
                btn.textContent = show ? '조회 중...' : '조회';
            }
        }

        static showError(message) {
            alert(message || '조회 중 오류가 발생했습니다.');
        }
    }

    // ==================== 데이터 처리 ====================
    class DataProcessor {
        static processAlertData(cols, rows, alertId) {
            const idxAlert = cols.indexOf('STR_ALERT_ID');
            const idxRule = cols.indexOf('STR_RULE_ID');
            const idxCust = cols.indexOf('CUST_ID');

            let repRuleId = null;
            let custIdForPerson = null;
            const canonicalIds = [];

            if (idxAlert >= 0 && idxRule >= 0) {
                const repRow = rows.find(r => String(r[idxAlert]) === alertId);
                repRuleId = repRow ? String(repRow[idxRule]) : null;
                if (repRow && idxCust >= 0) {
                    custIdForPerson = repRow[idxCust];
                }
            }

            if (!custIdForPerson && rows.length && idxCust >= 0) {
                custIdForPerson = rows[0][idxCust];
            }

            if (idxRule >= 0) {
                const seen = new Set();
                rows.forEach(row => {
                    const ruleId = row[idxRule];
                    if (ruleId != null) {
                        const strId = String(ruleId).trim();
                        if (!seen.has(strId)) {
                            seen.add(strId);
                            canonicalIds.push(strId);
                        }
                    }
                });
            }

            return { repRuleId, custIdForPerson, canonicalIds };
        }

        static extractTransactionPeriod(cols, rows) {
            const idxTranStart = cols.indexOf('TRAN_STRT');
            const idxTranEnd = cols.indexOf('TRAN_END');
            
            if (idxTranStart < 0 || idxTranEnd < 0) {
                return { start: null, end: null };
            }
            
            let minStart = null;
            let maxEnd = null;
            
            rows.forEach(row => {
                const startDate = row[idxTranStart];
                const endDate = row[idxTranEnd];
                
                if (startDate && /^\d{4}-\d{2}-\d{2}/.test(startDate)) {
                    if (!minStart || startDate < minStart) {
                        minStart = startDate;
                    }
                }
                
                if (endDate && /^\d{4}-\d{2}-\d{2}/.test(endDate)) {
                    if (!maxEnd || endDate > maxEnd) {
                        maxEnd = endDate;
                    }
                }
            });
            
            // 3개월 이전 날짜 계산
            if (minStart) {
                const startDateObj = new Date(minStart);
                startDateObj.setMonth(startDateObj.getMonth() - 3);
                minStart = startDateObj.toISOString().split('T')[0] + ' 00:00:00.000000000';
            }
            
            if (maxEnd) {
                maxEnd = maxEnd.includes(' ') ? maxEnd : maxEnd + ' 23:59:59.999999999';
            }
            
            return { start: minStart, end: maxEnd };
        }
    }

    // ==================== ALERT 검색 매니저 ====================
    class AlertSearchManager {
        constructor() {
            this.api = new APIClient();
            this.state = new SearchState();
            this.searchBtn = $('#alert_id_search_btn');
            this.inputField = $('#alert_id_input');
            this.init();
        }

        init() {
            this.searchBtn?.addEventListener('click', () => this.search());
            this.inputField?.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') this.search();
            });
        }

        async search() {
            // 이미 검색 중이면 중단
            if (this.state.isSearching) {
                console.log('Already searching, aborting previous search');
                this.state.abort();
                return;
            }

            // DB 연결 확인
            if (!window.dualDBManager?.isOracleConnected()) {
                UIManager.showError('먼저 Oracle DB 연결을 완료해 주세요.');
                $('#btn-open-db-modal')?.click();
                return;
            }
            
            const alertId = this.inputField?.value?.trim();
            if (!alertId) {
                UIManager.showError('ALERT ID를 입력하세요.');
                return;
            }

            // 이전 검색과 동일한 경우 무시
            if (this.state.currentAlertId === alertId && this.state.alertData) {
                console.log('Same alert ID, skipping search');
                return;
            }

            // 상태 초기화 및 UI 초기화
            this.state.reset();
            this.state.currentAlertId = alertId;
            this.state.setSearching(true);
            UIManager.hideAllSections();
            UIManager.showLoading(true);

            try {
                // 1. ALERT 정보 조회
                const alertData = await this.api.post(window.URLS.query_alert, { alert_id: alertId });
                
                if (!alertData.success) {
                    throw new Error(alertData.message || '조회 실패');
                }

                const cols = alertData.columns || [];
                const rows = alertData.rows || [];
                
                if (rows.length === 0) {
                    throw new Error('해당 ALERT ID에 대한 데이터가 없습니다.');
                }

                // 데이터 처리
                const processedData = DataProcessor.processAlertData(cols, rows, alertId);
                this.state.setAlertData({ cols, rows, ...processedData });

                // 2. 모든 섹션 렌더링
                await this.renderAllSections();
                
            } catch (error) {
                console.error('Alert search error:', error);
                UIManager.showError(error.message || '조회 중 오류가 발생했습니다.');
                UIManager.hideAllSections();
            } finally {
                this.state.setSearching(false);
                UIManager.showLoading(false);
            }
        }

        async renderAllSections() {
            const { cols, rows, repRuleId, custIdForPerson, canonicalIds } = this.state.alertData;
            
            // Promise 배열로 병렬 처리
            const promises = [];

            // 1. 고객 정보 조회
            if (custIdForPerson) {
                promises.push(this.fetchCustomerInfo(custIdForPerson));
            }

            // 2. Rule 히스토리 조회
            if (canonicalIds.length > 0) {
                promises.push(this.fetchRuleHistory(canonicalIds));
            }

            // 병렬 실행
            await Promise.allSettled(promises);

            // 3. 동기 렌더링 (Alert 데이터 기반)
            this.renderSyncSections(cols, rows, repRuleId, canonicalIds);
        }

        async fetchCustomerInfo(custId) {
            try {
                const data = await this.api.post(window.URLS.query_customer_unified, { 
                    cust_id: String(custId) 
                });
                
                if (data.success) {
                    // 고객 정보 렌더링
                    window.TableRenderer.renderCustomerUnified(data.columns || [], data.rows || []);
                    
                    // 고객 유형별 추가 조회
                    const customerType = data.customer_type;
                    const subPromises = [];
                    
                    if (customerType === '법인') {
                        subPromises.push(this.fetchCorpRelated(custId));
                    } else if (customerType === '개인') {
                        const tranPeriod = DataProcessor.extractTransactionPeriod(
                            this.state.alertData.cols, 
                            this.state.alertData.rows
                        );
                        if (tranPeriod.start && tranPeriod.end) {
                            subPromises.push(this.fetchPersonRelated(custId, tranPeriod));
                        }
                    }
                    
                    // 중복 회원 검색
                    if (data.rows && data.rows.length > 0) {
                        subPromises.push(this.fetchDuplicatePersons(custId, data.columns, data.rows[0], customerType));
                    }
                    
                    // IP 접속 이력 및 Orderbook
                    const memId = this.extractMID(data.columns, data.rows);
                    if (memId) {
                        const tranPeriod = DataProcessor.extractTransactionPeriod(
                            this.state.alertData.cols, 
                            this.state.alertData.rows
                        );
                        if (tranPeriod.start && tranPeriod.end) {
                            subPromises.push(this.fetchIPHistory(memId, tranPeriod));
                            
                            if (window.dualDBManager?.isRedshiftConnected()) {
                                subPromises.push(this.fetchOrderbook(memId, tranPeriod));
                            }
                        }
                    }
                    
                    await Promise.allSettled(subPromises);
                }
            } catch (error) {
                console.error('Customer info fetch failed:', error);
                window.TableRenderer.renderCustomerUnified([], []);
            }
        }

        async fetchRuleHistory(canonicalIds) {
            try {
                const ruleKey = canonicalIds.slice().sort().join(',');
                const data = await this.api.post(window.URLS.rule_history, { rule_key: ruleKey });
                
                if (data.success) {
                    window.TableRenderer.renderRuleHistory(
                        data.columns || [], 
                        data.rows || [],
                        data.searched_rule || ruleKey,
                        data.similar_list || null
                    );
                }
            } catch (error) {
                console.error('Rule history fetch failed:', error);
                window.TableRenderer.renderRuleHistory([], [], '', null);
            }
        }

        async fetchCorpRelated(custId) {
            try {
                const data = await this.api.post(window.URLS.query_corp_related_persons, { 
                    cust_id: String(custId) 
                });
                if (data.success) {
                    window.TableRenderer.renderCorpRelated(data.columns || [], data.rows || []);
                }
            } catch (error) {
                console.error('Corp related fetch failed:', error);
            }
        }

        async fetchPersonRelated(custId, tranPeriod) {
            try {
                const data = await this.api.post(window.URLS.query_person_related_summary, {
                    cust_id: String(custId),
                    start_date: tranPeriod.start,
                    end_date: tranPeriod.end
                });
                if (data.success) {
                    window.TableRenderer.renderPersonRelated(data.related_persons);
                }
            } catch (error) {
                console.error('Person related fetch failed:', error);
            }
        }

        async fetchDuplicatePersons(custId, columns, row, custType) {
            try {
                // 컬럼 인덱스 매핑
                const params = this.extractDuplicateParams(columns, row);
                params.current_cust_id = String(custId);
                
                const data = await this.api.post(window.URLS.query_duplicate_unified, params);
                if (data.success) {
                    const matchCriteria = this.buildMatchCriteria(params, custType);
                    window.TableRenderer.renderDuplicatePersons(data.columns, data.rows, matchCriteria);
                }
            } catch (error) {
                console.error('Duplicate persons fetch failed:', error);
            }
        }

        async fetchIPHistory(memId, tranPeriod) {
            try {
                const data = await this.api.post(window.URLS.query_ip_access_history, {
                    mem_id: String(memId),
                    start_date: tranPeriod.start.split(' ')[0],
                    end_date: tranPeriod.end.split(' ')[0]
                });
                if (data.success) {
                    window.TableRenderer.renderIPHistory(data.columns || [], data.rows || []);
                }
            } catch (error) {
                console.error('IP history fetch failed:', error);
            }
        }

        async fetchOrderbook(memId, tranPeriod) {
            try {
                // 1. Orderbook 조회 및 캐싱
                const response = await this.api.post(window.URLS.query_redshift_orderbook, {
                    user_id: String(memId),
                    tran_start: tranPeriod.start.split(' ')[0],
                    tran_end: tranPeriod.end.split(' ')[0]
                });
                
                if (response.success && response.rows_count > 0) {
                    // 2. 분석 실행
                    const analysis = await this.api.post(window.URLS.analyze_cached_orderbook, {
                        cache_key: response.cache_key
                    });
                    
                    if (analysis.success) {
                        window.TableRenderer.renderOrderbookAnalysis(analysis);
                    }
                }
            } catch (error) {
                console.error('Orderbook fetch/analysis failed:', error);
            }
        }

        renderSyncSections(cols, rows, repRuleId, canonicalIds) {
            const ruleObjMap = window.RULE_OBJ_MAP || {};
            const alertId = this.state.currentAlertId;
            
            // Alert 히스토리
            window.TableRenderer.renderAlertHistory(cols, rows, alertId);
            
            // 의심거래 객관식
            window.TableRenderer.renderObjectives(cols, rows, ruleObjMap, canonicalIds, repRuleId);
            
            // Rule 설명
            window.TableRenderer.renderRuleDesc(cols, rows, canonicalIds, repRuleId);
        }

        // === 헬퍼 메서드 ===
        extractMID(columns, rows) {
            if (!rows || rows.length === 0) return null;
            const midIdx = columns.indexOf('MID');
            return midIdx >= 0 ? rows[0][midIdx] : null;
        }

        extractDuplicateParams(columns, row) {
            const getColumnValue = (colName) => {
                const idx = columns.indexOf(colName);
                return idx >= 0 ? (row[idx] || '') : '';
            };
            
            const phone = getColumnValue('연락처');
            const phoneSuffix = phone.length >= 4 ? phone.slice(-4) : '';
            
            return {
                full_email: getColumnValue('이메일'),
                phone_suffix: phoneSuffix,
                address: getColumnValue('거주지주소'),
                detail_address: getColumnValue('거주지상세주소'),
                workplace_name: getColumnValue('직장명'),
                workplace_address: getColumnValue('직장주소'),
                workplace_detail_address: getColumnValue('직장상세주소') || ''
            };
        }

        buildMatchCriteria(params, custType) {
            return {
                email_prefix: params.full_email ? params.full_email.split('@')[0] : null,
                full_email: params.full_email || null,
                phone_suffix: params.phone_suffix || null,
                address: params.address || null,
                detail_address: params.detail_address || null,
                workplace_name: params.workplace_name || null,
                workplace_address: params.workplace_address || null,
                workplace_detail_address: params.workplace_detail_address || null,
                customer_type: custType
            };
        }
    }

    // ==================== 초기화 ====================
    document.addEventListener('DOMContentLoaded', function() {
        // TableRenderer가 로드될 때까지 대기
        const initInterval = setInterval(() => {
            if (window.TableRenderer) {
                clearInterval(initInterval);
                window.alertManager = new AlertSearchManager();
                
                // 초기 상태: 섹션 숨김
                UIManager.hideAllSections();
                
                console.log('Menu1_1 initialized with refactored architecture');
            }
        }, 100);
    });

})();