// str_dashboard/static/str_dashboard/js/menu1_1.js
// ALERT ID 조회 페이지 - 간소화 버전 (데이터 표시 제거, 콘솔 로깅 추가)

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
            this.init();
        }

        init() {
            const tomlBtn = document.getElementById('toml_save_btn');
            if (tomlBtn) {
                tomlBtn.addEventListener('click', () => this.showConfigModal());
            }
            
            this.setupModalEvents();
        }

        setupModalEvents() {
            const modal = document.getElementById('toml-config-modal');
            if (!modal) return;
            
            const cancelBtn = modal.querySelector('.toml-cancel-btn');
            if (cancelBtn) {
                cancelBtn.addEventListener('click', () => this.closeModal());
            }
            
            const downloadBtn = modal.querySelector('.toml-download-btn');
            if (downloadBtn) {
                downloadBtn.addEventListener('click', () => this.downloadToml());
            }
            
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
                const response = await fetch(window.URLS.prepare_toml_data, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    }
                });
                
                const result = await response.json();
                
                if (result.success) {
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

    // ==================== ALERT 검색 매니저 (간소화) ====================
    class AlertSearchManager {
        constructor() {
            this.api = new APIClient();
            this.searchBtn = $('#alert_id_search_btn');
            this.inputField = $('#alert_id_input');
            this.isSearching = false;
            this.collectedData = {}; // 수집된 모든 데이터 저장
            this.init();
        }

        init() {
            this.searchBtn?.addEventListener('click', () => this.search());
            this.inputField?.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') this.search();
            });
        }

        async search() {
            if (this.isSearching) {
                return;
            }

            // DB 연결 확인
            if (!window.dualDBManager?.isOracleConnected()) {
                alert('먼저 Oracle DB 연결을 완료해 주세요.');
                $('#btn-open-db-modal')?.click();
                return;
            }
            
            const alertId = this.inputField?.value?.trim();
            if (!alertId) {
                alert('ALERT ID를 입력하세요.');
                return;
            }

            this.isSearching = true;
            this.setLoading(true);
            this.collectedData = {}; // 데이터 초기화

            console.group(`%c🔍 ALERT ID: ${alertId} 조회 시작`, 'color: #4fc3f7; font-size: 16px; font-weight: bold;');
            console.time('전체 조회 시간');

            try {
                // 모든 필요한 데이터 조회
                await this.fetchAllData(alertId);
                
                // 조회 완료 메시지 표시
                this.showCompleteMessage(alertId);
                
                // TOML 저장 버튼 표시
                const tomlBtn = document.getElementById('toml_save_btn');
                if (tomlBtn) {
                    tomlBtn.style.display = 'inline-flex';
                }
                
                console.timeEnd('전체 조회 시간');
                console.log('%c✅ 모든 데이터 조회 완료', 'color: #4caf50; font-size: 14px; font-weight: bold;');
                console.log('%c📊 수집된 전체 데이터:', 'color: #ffa726; font-size: 14px; font-weight: bold;');
                console.log(this.collectedData);
                console.groupEnd();
                
                // 전역 변수로도 노출 (개발자가 콘솔에서 접근 가능)
                window.COLLECTED_DATA = this.collectedData;
                console.log('%c💡 Tip: window.COLLECTED_DATA로 전체 데이터에 접근 가능합니다.', 'color: #29b6f6; font-style: italic;');
                
            } catch (error) {
                console.error('❌ Alert search error:', error);
                alert(error.message || '조회 중 오류가 발생했습니다.');
                console.groupEnd();
            } finally {
                this.isSearching = false;
                this.setLoading(false);
            }
        }

        async fetchAllData(alertId) {
            console.group('📋 1. ALERT 정보 조회');
            console.time('ALERT 정보 조회');
            
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

            console.log('✓ ALERT 정보:', { columns: cols, rows: rows, row_count: rows.length });
            console.timeEnd('ALERT 정보 조회');

            // 데이터 처리
            const processedData = this.processAlertData(cols, rows, alertId);
            console.log('✓ 처리된 데이터:', processedData);
            console.groupEnd();
            
            // 수집된 데이터 저장
            this.collectedData.alert_info = {
                alert_id: alertId,
                columns: cols,
                rows: rows,
                processed: processedData
            };
            
            // 세션에 저장
            await this.api.post(window.URLS.save_to_session, {
                key: 'current_alert_data',
                data: JSON.stringify({
                    alert_id: alertId,
                    cols, 
                    rows, 
                    ...processedData
                })
            });
            
            await this.api.post(window.URLS.save_to_session, {
                key: 'current_alert_id',
                data: JSON.stringify(alertId)
            });

            // 2. 병렬로 추가 데이터 조회
            console.group('📋 2. 추가 데이터 병렬 조회');
            const promises = [];

            // 고객 정보
            if (processedData.custIdForPerson) {
                console.log(`👤 고객 정보 조회 시작 (CUST_ID: ${processedData.custIdForPerson})`);
                promises.push(this.fetchCustomerData(processedData.custIdForPerson, cols, rows));
            }

            // Rule 히스토리
            if (processedData.canonicalIds.length > 0) {
                console.log(`📜 Rule 히스토리 조회 시작 (Rules: ${processedData.canonicalIds.join(', ')})`);
                promises.push(this.fetchRuleHistory(processedData.canonicalIds));
            }

            await Promise.allSettled(promises);
            console.groupEnd();
        }

        processAlertData(cols, rows, alertId) {
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

        async fetchCustomerData(custId, alertCols, alertRows) {
            console.group(`👤 고객 정보 상세 조회`);
            console.time('고객 정보 조회');
            
            try {
                const data = await this.api.post(window.URLS.query_customer_unified, { 
                    cust_id: String(custId) 
                });
                
                if (data.success) {
                    const columns = data.columns || [];
                    const rows = data.rows || [];
                    
                    console.log('✓ 고객 기본 정보:', { 
                        customer_type: data.customer_type,
                        columns: columns, 
                        rows: rows 
                    });
                    
                    // KYC 완료시점 추출
                    const kycDatetime = this.extractKYCDatetime(columns, rows);
                    console.log('✓ KYC 완료시점:', kycDatetime);
                    
                    // 수집된 데이터 저장
                    this.collectedData.customer_info = {
                        columns: columns,
                        rows: rows,
                        customer_type: data.customer_type,
                        kyc_datetime: kycDatetime
                    };
                    
                    // 세션에 저장
                    await this.api.post(window.URLS.save_to_session, {
                        key: 'current_customer_data',
                        data: JSON.stringify({
                            columns: columns,
                            rows: rows,
                            customer_type: data.customer_type || null,
                            kyc_datetime: kycDatetime
                        })
                    });
                    
                    // 추가 관련 데이터 조회
                    console.group('📋 고객 관련 추가 데이터 조회');
                    const subPromises = [];
                    
                    if (data.customer_type === '법인') {
                        console.log('🏢 법인 관련인 조회 시작');
                        subPromises.push(this.fetchCorpRelated(custId));
                    } else if (data.customer_type === '개인') {
                        console.log('👥 개인 관련인 조회 시작');
                        const tranPeriod = this.extractTransactionPeriod(alertCols, alertRows, kycDatetime);
                        console.log('✓ 거래 기간:', tranPeriod);
                        if (tranPeriod.start && tranPeriod.end) {
                            subPromises.push(this.fetchPersonRelated(custId, tranPeriod));
                        }
                    }
                    
                    // 중복 회원 검색
                    if (rows.length > 0) {
                        console.log('🔍 중복 회원 조회 시작');
                        subPromises.push(this.fetchDuplicatePersons(custId, columns, rows[0]));
                    }
                    
                    // IP 접속 이력 및 Orderbook
                    const memId = this.extractMID(columns, rows);
                    if (memId) {
                        console.log(`📡 IP 접속 이력 조회 시작 (MID: ${memId})`);
                        const tranPeriod = this.extractTransactionPeriod(alertCols, alertRows, kycDatetime);
                        if (tranPeriod.start && tranPeriod.end) {
                            subPromises.push(this.fetchIPHistory(memId, tranPeriod));
                            
                            if (window.dualDBManager?.isRedshiftConnected()) {
                                console.log('📊 Orderbook 조회 시작');
                                subPromises.push(this.fetchOrderbook(memId, tranPeriod));
                            }
                        }
                    }
                    
                    await Promise.allSettled(subPromises);
                    console.groupEnd();
                }
                
                console.timeEnd('고객 정보 조회');
            } catch (error) {
                console.error('❌ Customer info fetch failed:', error);
            } finally {
                console.groupEnd();
            }
        }

        async fetchRuleHistory(canonicalIds) {
            console.group('📜 Rule 히스토리 조회');
            console.time('Rule 히스토리 조회');
            
            try {
                const ruleKey = canonicalIds.slice().sort().join(',');
                console.log('✓ Rule Key:', ruleKey);
                
                const data = await this.api.post(window.URLS.rule_history, { rule_key: ruleKey });
                
                if (data.success) {
                    console.log('✓ Rule 히스토리:', {
                        columns: data.columns,
                        rows: data.rows,
                        row_count: data.rows?.length || 0
                    });
                    
                    this.collectedData.rule_history = {
                        columns: data.columns || [],
                        rows: data.rows || [],
                        rule_key: ruleKey
                    };
                    
                    await this.api.post(window.URLS.save_to_session, {
                        key: 'current_rule_history_data',
                        data: JSON.stringify({
                            columns: data.columns || [],
                            rows: data.rows || []
                        })
                    });
                }
                
                console.timeEnd('Rule 히스토리 조회');
            } catch (error) {
                console.error('❌ Rule history fetch failed:', error);
            } finally {
                console.groupEnd();
            }
        }

        async fetchCorpRelated(custId) {
            console.group('🏢 법인 관련인 조회');
            console.time('법인 관련인 조회');
            
            try {
                const data = await this.api.post(window.URLS.query_corp_related_persons, { 
                    cust_id: String(custId) 
                });
                if (data.success) {
                    console.log('✓ 법인 관련인:', {
                        columns: data.columns,
                        rows: data.rows,
                        row_count: data.rows?.length || 0
                    });
                    
                    this.collectedData.corp_related = {
                        columns: data.columns || [],
                        rows: data.rows || []
                    };
                    
                    await this.api.post(window.URLS.save_to_session, {
                        key: 'current_corp_related_data',
                        data: JSON.stringify({
                            columns: data.columns || [],
                            rows: data.rows || []
                        })
                    });
                }
                
                console.timeEnd('법인 관련인 조회');
            } catch (error) {
                console.error('❌ Corp related fetch failed:', error);
            } finally {
                console.groupEnd();
            }
        }

        async fetchPersonRelated(custId, tranPeriod) {
            console.group('👥 개인 관련인 조회');
            console.time('개인 관련인 조회');
            
            try {
                const data = await this.api.post(window.URLS.query_person_related_summary, {
                    cust_id: String(custId),
                    start_date: tranPeriod.start,
                    end_date: tranPeriod.end
                });
                if (data.success) {
                    console.log('✓ 개인 관련인:', {
                        related_persons: data.related_persons,
                        person_count: Object.keys(data.related_persons || {}).length
                    });
                    
                    this.collectedData.person_related = data.related_persons;
                    
                    await this.api.post(window.URLS.save_to_session, {
                        key: 'current_person_related_data',
                        data: JSON.stringify(data.related_persons)
                    });
                }
                
                console.timeEnd('개인 관련인 조회');
            } catch (error) {
                console.error('❌ Person related fetch failed:', error);
            } finally {
                console.groupEnd();
            }
        }

        async fetchDuplicatePersons(custId, columns, row) {
            console.group('🔍 중복 회원 조회');
            console.time('중복 회원 조회');
            
            try {
                const params = this.extractDuplicateParams(columns, row);
                params.current_cust_id = String(custId);
                console.log('✓ 검색 파라미터:', params);
                
                const data = await this.api.post(window.URLS.query_duplicate_unified, params);
                if (data.success) {
                    console.log('✓ 중복 회원:', {
                        columns: data.columns,
                        rows: data.rows,
                        duplicate_count: data.rows?.length || 0
                    });
                    
                    this.collectedData.duplicate_persons = {
                        columns: data.columns || [],
                        rows: data.rows || [],
                        search_params: params
                    };
                    
                    await this.api.post(window.URLS.save_to_session, {
                        key: 'duplicate_persons_data',
                        data: JSON.stringify({
                            columns: data.columns || [],
                            rows: data.rows || []
                        })
                    });
                }
                
                console.timeEnd('중복 회원 조회');
            } catch (error) {
                console.error('❌ Duplicate persons fetch failed:', error);
            } finally {
                console.groupEnd();
            }
        }

        async fetchIPHistory(memId, tranPeriod) {
            console.group('📡 IP 접속 이력 조회');
            console.time('IP 접속 이력 조회');
            
            try {
                const data = await this.api.post(window.URLS.query_ip_access_history, {
                    mem_id: String(memId),
                    start_date: tranPeriod.start.split(' ')[0],
                    end_date: tranPeriod.end.split(' ')[0]
                });
                if (data.success) {
                    console.log('✓ IP 접속 이력:', {
                        columns: data.columns,
                        rows: data.rows,
                        access_count: data.rows?.length || 0
                    });
                    
                    this.collectedData.ip_history = {
                        columns: data.columns || [],
                        rows: data.rows || []
                    };
                    
                    await this.api.post(window.URLS.save_to_session, {
                        key: 'ip_history_data',
                        data: JSON.stringify({
                            columns: data.columns || [],
                            rows: data.rows || []
                        })
                    });
                }
                
                console.timeEnd('IP 접속 이력 조회');
            } catch (error) {
                console.error('❌ IP history fetch failed:', error);
            } finally {
                console.groupEnd();
            }
        }

        async fetchOrderbook(memId, tranPeriod) {
            console.group('📊 Orderbook 조회 및 분석');
            console.time('Orderbook 전체 처리');
            
            try {
                console.log('✓ 조회 파라미터:', {
                    user_id: memId,
                    start: tranPeriod.start.split(' ')[0],
                    end: tranPeriod.end.split(' ')[0]
                });
                
                const response = await this.api.post(window.URLS.query_redshift_orderbook, {
                    user_id: String(memId),
                    tran_start: tranPeriod.start.split(' ')[0],
                    tran_end: tranPeriod.end.split(' ')[0]
                });
                
                console.log('✓ Orderbook 조회 결과:', {
                    success: response.success,
                    rows_count: response.rows_count,
                    cache_key: response.cache_key
                });
                
                if (response.success && response.rows_count > 0) {
                    console.log('📈 Orderbook 분석 시작...');
                    const analysis = await this.api.post(window.URLS.analyze_cached_orderbook, {
                        cache_key: response.cache_key
                    });
                    
                    if (analysis.success) {
                        console.log('✓ Orderbook 분석 완료:', {
                            patterns: analysis.patterns,
                            period_info: analysis.period_info
                        });
                        
                        this.collectedData.orderbook_analysis = {
                            patterns: analysis.patterns,
                            period_info: analysis.period_info,
                            text_summary: analysis.text_summary,
                            cache_key: response.cache_key
                        };
                        
                        await this.api.post(window.URLS.save_to_session, {
                            key: 'current_orderbook_analysis',
                            data: JSON.stringify({
                                patterns: analysis.patterns,
                                period_info: analysis.period_info,
                                text_summary: analysis.text_summary,
                                cache_key: response.cache_key
                            })
                        });
                    }
                }
                
                console.timeEnd('Orderbook 전체 처리');
            } catch (error) {
                console.error('❌ Orderbook fetch/analysis failed:', error);
            } finally {
                console.groupEnd();
            }
        }

        // 헬퍼 메서드들
        extractKYCDatetime(columns, rows) {
            if (!rows || rows.length === 0) return null;
            const kycDatetimeIdx = columns.indexOf('KYC완료일시');
            return kycDatetimeIdx >= 0 ? rows[0][kycDatetimeIdx] : null;
        }

        extractMID(columns, rows) {
            if (!rows || rows.length === 0) return null;
            const midIdx = columns.indexOf('MID');
            return midIdx >= 0 ? rows[0][midIdx] : null;
        }

        extractTransactionPeriod(cols, rows, kycDatetime = null) {
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
            if (maxEnd) {
                const endDateObj = new Date(maxEnd.split(' ')[0]);
                const startDateObj = new Date(endDateObj);
                startDateObj.setMonth(startDateObj.getMonth() - 3);
                const calculatedStart = startDateObj.toISOString().split('T')[0];
                
                // 더 이른 날짜 사용
                if (!minStart || calculatedStart < minStart.split(' ')[0]) {
                    minStart = calculatedStart + ' 00:00:00.000000000';
                }
            }
            
            return { 
                start: minStart, 
                end: maxEnd
            };
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

        setLoading(isLoading) {
            if (this.searchBtn) {
                this.searchBtn.disabled = isLoading;
                this.searchBtn.textContent = isLoading ? '조회 중...' : '조회';
            }
        }

        showCompleteMessage(alertId) {
            const container = document.getElementById('query-result-container');
            const text = document.getElementById('query-complete-text');
            
            if (container && text) {
                text.textContent = `ALERT ID ${alertId} 데이터 조회가 완료되었습니다.`;
                container.style.display = 'block';
            }
        }
    }

    // ==================== 초기화 ====================
    document.addEventListener('DOMContentLoaded', function() {
        window.alertManager = new AlertSearchManager();
        window.tomlExporter = new TomlExportManager();
        
        console.log('%c📌 STR Dashboard Menu1_1 초기화 완료', 'color: #4caf50; font-size: 14px; font-weight: bold;');
        console.log('%c💡 조회 후 window.COLLECTED_DATA로 전체 데이터에 접근 가능합니다.', 'color: #29b6f6; font-style: italic;');
    });

})();