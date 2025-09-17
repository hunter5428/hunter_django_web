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


    // ==================== TOML 저장 관리 ====================
    class TomlExportManager {
        constructor() {
            this.collectedData = {};
            this.init();
        }

        init() {
            // TOML 저장 버튼 이벤트
            const tomlBtn = document.getElementById('toml_save_btn');
            if (tomlBtn) {
                tomlBtn.addEventListener('click', () => this.showConfigModal());
            }
            
            // 모달 이벤트
            this.setupModalEvents();
        }

        setupModalEvents() {
            const modal = document.getElementById('toml-config-modal');
            if (!modal) return;
            
            // 취소 버튼
            const cancelBtn = modal.querySelector('.toml-cancel-btn');
            if (cancelBtn) {
                cancelBtn.addEventListener('click', () => this.closeModal());
            }
            
            // 다운로드 버튼
            const downloadBtn = modal.querySelector('.toml-download-btn');
            if (downloadBtn) {
                downloadBtn.addEventListener('click', () => this.downloadToml());
            }
            
            // 모달 외부 클릭
            modal.addEventListener('click', (e) => {
                if (e.target === modal) this.closeModal();
            });
        }

        showConfigModal() {
            const modal = document.getElementById('toml-config-modal');
            if (modal) {
                modal.classList.add('show');
            }
        }

        closeModal() {
            const modal = document.getElementById('toml-config-modal');
            if (modal) {
                modal.classList.remove('show');
            }
        }

        async downloadToml() {
            const tomlBtn = document.getElementById('toml_save_btn');
            if (tomlBtn) {
                tomlBtn.disabled = true;
                tomlBtn.textContent = '처리 중...';
            }
            
            try {
                // 데이터 준비 요청
                const response = await fetch(window.URLS.prepare_toml_data, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    }
                });
                
                const result = await response.json();
                
                if (result.success) {
                    // 다운로드 실행
                    window.location.href = window.URLS.download_toml;
                    
                    setTimeout(() => {
                        this.closeModal();
                        alert('TOML 파일이 다운로드되었습니다.');
                    }, 1000);
                } else {
                    alert('TOML 데이터 준비 실패: ' + result.message);
                }
            } catch (error) {
                console.error('TOML export error:', error);
                alert('TOML 저장 중 오류가 발생했습니다.');
            } finally {
                if (tomlBtn) {
                    tomlBtn.disabled = false;
                    tomlBtn.textContent = 'TOML 저장';
                }
            }
        }
    }


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

        static extractTransactionPeriod(cols, rows, kycDatetime = null) {
            const idxTranStart = cols.indexOf('TRAN_STRT');
            const idxTranEnd = cols.indexOf('TRAN_END');
            const idxRuleId = cols.indexOf('STR_RULE_ID');
            
            if (idxTranStart < 0 || idxTranEnd < 0) {
                return { start: null, end: null };
            }
            
            let minStart = null;
            let maxEnd = null;
            let hasSpecialRule = false;
            
            // 특정 RULE ID 체크
            if (idxRuleId >= 0) {
                rows.forEach(row => {
                    const ruleId = row[idxRuleId];
                    if (ruleId) {
                        // RuleUtils를 사용하여 동적으로 특별 규칙 확인
                        if (window.RuleUtils && typeof window.RuleUtils.getDayCountForRule === 'function') {
                            const ruleInfo = window.RuleUtils.getDayCountForRule(ruleId);
                            if (ruleInfo.isSpecial) {
                                hasSpecialRule = true;
                            }
                        } else {
                            // RuleUtils가 로드되지 않은 경우 기존 방식으로 체크
                            if (ruleId === 'IO000' || ruleId === 'IO111') {
                                hasSpecialRule = true;
                            }
                        }
                    }
                });
            }
            
            // 1. 먼저 원본 TRAN_STRT/TRAN_END의 MIN/MAX 값을 추출
            let originalMinStart = null;
            let originalMaxEnd = null;
            
            rows.forEach(row => {
                const startDate = row[idxTranStart];
                const endDate = row[idxTranEnd];
                
                if (startDate && /^\d{4}-\d{2}-\d{2}/.test(startDate)) {
                    if (!originalMinStart || startDate < originalMinStart) {
                        originalMinStart = startDate;
                    }
                }
                
                if (endDate && /^\d{4}-\d{2}-\d{2}/.test(endDate)) {
                    if (!originalMaxEnd || endDate > originalMaxEnd) {
                        originalMaxEnd = endDate;
                    }
                }
            });
            
            // 2. 특정 RULE ID가 있으면 12개월, 없으면 3개월 이전
            const monthsBack = hasSpecialRule ? 12 : 3;
            
            // 3. monthsBack 기반으로 가장 넓은 범위의 시작일 계산
            let calculatedStart = null;
            
            if (originalMaxEnd) {
                // MAX(TRAN_END)로부터 몇 개월 이전 날짜
                const endDateObj = new Date(originalMaxEnd.split(' ')[0]);
                calculatedStart = new Date(endDateObj);
                calculatedStart.setMonth(calculatedStart.getMonth() - monthsBack);
                calculatedStart = calculatedStart.toISOString().split('T')[0];
            }
            
            // 4. 최종 쿼리용 날짜 결정: 
            // - 시작일: MIN(TRAN_STRT)와 계산된 이전 날짜 중 더 빠른 날짜
            // - 종료일: MAX(TRAN_END)
            let finalStartDate = null;
            if (originalMinStart && calculatedStart) {
                const minStartDate = originalMinStart.split(' ')[0];
                finalStartDate = minStartDate < calculatedStart ? minStartDate : calculatedStart;
            } else if (originalMinStart) {
                finalStartDate = originalMinStart.split(' ')[0];
            } else if (calculatedStart) {
                finalStartDate = calculatedStart;
            }
            
            // KYC 완료시점 처리
            let kycDate = null;
            let useKycDate = false;
            
            if (kycDatetime && kycDatetime.trim() !== '') {
                // KYC 완료시점 추출 (YYYY-MM-DD HH24:MI:SS 형식)
                kycDate = kycDatetime.split(' ')[0]; // 날짜 부분만 추출
                
                // 최종 시작일보다 KYC 완료시점이 더 최근인 경우, KYC 완료시점을 사용
                if (finalStartDate && kycDate > finalStartDate) {
                    finalStartDate = kycDate;
                    useKycDate = true;
                }
            }
            
            const finalEndDate = originalMaxEnd ? originalMaxEnd.split(' ')[0] : null;
            
            // 5. 시간 정보 추가
            minStart = finalStartDate ? finalStartDate + ' 00:00:00.000000000' : null;
            maxEnd = finalEndDate ? finalEndDate + ' 23:59:59.999999999' : null;
            
            return { 
                start: minStart, 
                end: maxEnd, 
                monthsBack,
                original_min_start: originalMinStart ? originalMinStart.split(' ')[0] : null,
                original_max_end: originalMaxEnd ? originalMaxEnd.split(' ')[0] : null,
                kyc_date: kycDate,
                used_kyc_date: useKycDate,
                has_special_rule: hasSpecialRule  // 특정 RULE ID 정보 추가
            };
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
                this.state.setAlertData({ 
                    cols, 
                    rows, 
                    currentAlertId: alertId,  // 현재 ALERT ID 명시적으로 추가
                    ...processedData 
                });
                
                this.saveToSession('current_alert_data', {
                    alert_id: alertId,
                    cols, 
                    rows, 
                    ...processedData
                });
                this.saveToSession('current_alert_id', alertId);

                // 2. 모든 섹션 렌더링
                await this.renderAllSections();
                // TOML 저장 버튼 표시
                const tomlBtn = document.getElementById('toml_save_btn');
                if (tomlBtn) {
                    tomlBtn.style.display = 'inline-flex';
                }
                
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
                    const columns = data.columns || [];
                    const rows = data.rows || [];
                    
                    // KYC 완료시점 추출
                    const kycDatetime = this.extractKYCDatetime(columns, rows);
                    
                    // 세션에 저장 (TOML 저장용) - 서버로 전송
                    this.saveToSession('current_customer_data', {
                        columns: columns,
                        rows: rows,
                        customer_type: data.customer_type || null,
                        kyc_datetime: kycDatetime
                    });
                    
                    // 고객 정보 렌더링
                    window.TableRenderer.renderCustomerUnified(columns, rows);
                    
                    // 고객 유형별 추가 조회
                    const customerType = data.customer_type;
                    const subPromises = [];
                    
                    if (customerType === '법인') {
                        subPromises.push(this.fetchCorpRelated(custId));
                    } else if (customerType === '개인') {
                        const tranPeriod = DataProcessor.extractTransactionPeriod(
                            this.state.alertData.cols, 
                            this.state.alertData.rows,
                            kycDatetime
                        );
                        if (tranPeriod.start && tranPeriod.end) {
                            subPromises.push(this.fetchPersonRelated(custId, tranPeriod));
                        }
                    }
                    
                    // 중복 회원 검색
                    if (rows.length > 0) {
                        subPromises.push(this.fetchDuplicatePersons(custId, columns, rows[0], customerType));
                    }
                    
                    // IP 접속 이력 및 Orderbook
                    const memId = this.extractMID(columns, rows);
                    if (memId) {
                        const tranPeriod = DataProcessor.extractTransactionPeriod(
                            this.state.alertData.cols, 
                            this.state.alertData.rows,
                            kycDatetime
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

        saveToSession(key, data) {
            // 서버 세션에 저장 (비동기 처리, 에러는 무시)
            fetch('/api/save_to_session/', {  // URL은 나중에 추가할 예정
                method: 'POST',
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'X-CSRFToken': getCookie('csrftoken')
                },
                body: new URLSearchParams({
                    key: key,
                    data: JSON.stringify(data)
                })
            }).catch(error => {
                console.error('Session save error:', error);
            });
        }
        
        // KYC 완료시점을 추출하는 함수
        extractKYCDatetime(columns, rows) {
            if (!rows || rows.length === 0) return null;
            const kycDatetimeIdx = columns.indexOf('KYC완료일시');
            return kycDatetimeIdx >= 0 ? rows[0][kycDatetimeIdx] : null;
        }


        async fetchRuleHistory(canonicalIds) {
            try {
                const ruleKey = canonicalIds.slice().sort().join(',');
                const data = await this.api.post(window.URLS.rule_history, { rule_key: ruleKey });
                
                if (data.success) {
                    // 세션에 저장
                    this.saveToSession('current_rule_history_data', {
                        columns: data.columns || [],
                        rows: data.rows || []
                    });
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
                        // monthsBack 정보 추가
                        analysis.monthsBack = tranPeriod.monthsBack;
                        
                        // 세션에 저장 (TOML 저장용)
                        this.saveToSession('current_orderbook_analysis', {
                            patterns: analysis.patterns,
                            period_info: analysis.period_info,
                            text_summary: analysis.text_summary,
                            cache_key: response.cache_key
                        });
                        
                        // tranPeriod 정보를 alertData에 추가하여 전달
                        const alertDataWithTranPeriod = {
                            ...this.state.alertData,
                            tranPeriod: tranPeriod
                        };
                        
                        // ALERT 데이터와 함께 전달
                        window.TableRenderer.renderOrderbookAnalysis(analysis, alertDataWithTranPeriod);
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
                window.tomlExporter = new TomlExportManager();  // 새로 추가

                // 초기 상태: 섹션 숨김
                UIManager.hideAllSections();
                
                console.log('Menu1_1 initialized with refactored architecture');
            }
        }, 100);
    });

})();