/**
 * Alert 검색 관리 컴포넌트
 */
import { apiClient } from '../core/api_client.js';
import { uiManager } from '../core/ui_manager.js';
import { stateManager } from '../core/state_manager.js';
import { DataProcessor } from '../utils/data_processor.js';

export class AlertSearchManager {
    constructor() {
        this.urls = window.URLS || {};
        this.searchBtn = document.getElementById('alert_id_search_btn');
        this.inputField = document.getElementById('alert_id_input');
        this.init();
    }

    init() {
        this.setupEventListeners();
    }

    setupEventListeners() {
        if (this.searchBtn) {
            this.searchBtn.addEventListener('click', () => this.search());
        }
        
        if (this.inputField) {
            this.inputField.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') this.search();
            });
        }
    }

    async search() {
        // 이미 검색 중이면 중단
        if (stateManager.getState('isSearching')) {
            console.log('Already searching');
            return;
        }

        // DB 연결 확인
        const dbConnections = stateManager.getState('dbConnections');
        if (!dbConnections.oracle) {
            uiManager.showError('먼저 Oracle DB 연결을 완료해 주세요.');
            document.getElementById('btn-open-db-modal')?.click();
            return;
        }
        
        const alertId = this.inputField?.value?.trim();
        if (!alertId) {
            uiManager.showError('ALERT ID를 입력하세요.');
            return;
        }

        // 이전 검색과 동일한 경우 무시
        if (stateManager.getState('currentAlertId') === alertId && 
            stateManager.getState('alertData')) {
            console.log('Same alert ID, skipping search');
            return;
        }

        // 상태 초기화 및 UI 초기화
        stateManager.reset();
        stateManager.setState('currentAlertId', alertId);
        stateManager.setState('isSearching', true);
        uiManager.hideAllSections();
        uiManager.showLoading('#alert_id_search_btn', true);

        try {
            // 1. ALERT 정보 조회
            const alertData = await apiClient.post(this.urls.query_alert, { alert_id: alertId });
            
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
            const fullAlertData = { 
                cols, 
                rows, 
                currentAlertId: alertId,
                ...processedData 
            };
            
            stateManager.setAlertData(fullAlertData);
            
            // 세션에 저장
            await this.saveToSession('current_alert_data', fullAlertData);
            await this.saveToSession('current_alert_id', alertId);

            // 2. 모든 섹션 렌더링
            await this.renderAllSections();
            
            // TOML 저장 버튼 표시
            const tomlBtn = document.getElementById('toml_save_btn');
            if (tomlBtn) {
                tomlBtn.style.display = 'inline-flex';
            }
            
        } catch (error) {
            console.error('Alert search error:', error);
            uiManager.showError(error.message || '조회 중 오류가 발생했습니다.');
            uiManager.hideAllSections();
        } finally {
            stateManager.setState('isSearching', false);
            uiManager.showLoading('#alert_id_search_btn', false);
        }
    }

    async renderAllSections() {
        const alertData = stateManager.getState('alertData');
        if (!alertData) return;
        
        const { cols, rows, repRuleId, custIdForPerson, canonicalIds } = alertData;
        
        // Promise 배열로 병렬 처리
        const promises = [];

        // 1. 고객 정보 조회
        if (custIdForPerson) {
            promises.push(this.fetchCustomerInfo(custIdForPerson));
        }

        // 2. Rule 히스토리 조회
        if (canonicalIds && canonicalIds.length > 0) {
            promises.push(this.fetchRuleHistory(canonicalIds));
        }

        // 병렬 실행
        await Promise.allSettled(promises);

        // 3. 동기 렌더링 (Alert 데이터 기반)
        this.renderSyncSections(cols, rows, repRuleId, canonicalIds);
    }

    async fetchCustomerInfo(custId) {
        try {
            const data = await apiClient.post(this.urls.query_customer_unified, { 
                cust_id: String(custId) 
            });
            
            if (data.success) {
                // 세션에 저장
                await this.saveToSession('current_customer_data', {
                    columns: data.columns || [],
                    rows: data.rows || [],
                    customer_type: data.customer_type || null
                });
                
                // 고객 정보 렌더링
                window.TableRenderer.renderCustomerUnified(data.columns || [], data.rows || []);
                
                // 고객 유형별 추가 조회
                await this.fetchAdditionalCustomerData(custId, data);
            }
        } catch (error) {
            console.error('Customer info fetch failed:', error);
            window.TableRenderer.renderCustomerUnified([], []);
        }
    }

    async fetchAdditionalCustomerData(custId, customerData) {
        const customerType = customerData.customer_type;
        const subPromises = [];
        
        if (customerType === '법인') {
            subPromises.push(this.fetchCorpRelated(custId));
        } else if (customerType === '개인') {
            const alertData = stateManager.getState('alertData');
            const tranPeriod = DataProcessor.extractTransactionPeriod(
                alertData.cols, 
                alertData.rows
            );
            if (tranPeriod.start && tranPeriod.end) {
                subPromises.push(this.fetchPersonRelated(custId, tranPeriod));
            }
        }
        
        // 중복 회원 검색
        if (customerData.rows && customerData.rows.length > 0) {
            subPromises.push(this.fetchDuplicatePersons(custId, customerData.columns, customerData.rows[0], customerType));
        }
        
        // IP 접속 이력 및 Orderbook
        const memId = this.extractMID(customerData.columns, customerData.rows);
        if (memId) {
            const alertData = stateManager.getState('alertData');
            const tranPeriod = DataProcessor.extractTransactionPeriod(
                alertData.cols, 
                alertData.rows
            );
            if (tranPeriod.start && tranPeriod.end) {
                subPromises.push(this.fetchIPHistory(memId, tranPeriod));
                
                const dbConnections = stateManager.getState('dbConnections');
                if (dbConnections.redshift) {
                    subPromises.push(this.fetchOrderbook(memId, tranPeriod));
                }
            }
        }
        
        await Promise.allSettled(subPromises);
    }

    async fetchRuleHistory(canonicalIds) {
        try {
            const ruleKey = canonicalIds.slice().sort().join(',');
            const data = await apiClient.post(this.urls.rule_history, { rule_key: ruleKey });
            
            if (data.success) {
                await this.saveToSession('current_rule_history_data', {
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
            const data = await apiClient.post(this.urls.query_corp_related_persons, { 
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
            const data = await apiClient.post(this.urls.query_person_related_summary, {
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
            const params = DataProcessor.extractDuplicateParams(columns, row);
            params.current_cust_id = String(custId);
            
            const data = await apiClient.post(this.urls.query_duplicate_unified, params);
            if (data.success) {
                const matchCriteria = DataProcessor.buildMatchCriteria(params, custType);
                window.TableRenderer.renderDuplicatePersons(data.columns, data.rows, matchCriteria);
            }
        } catch (error) {
            console.error('Duplicate persons fetch failed:', error);
        }
    }

    async fetchIPHistory(memId, tranPeriod) {
        try {
            const data = await apiClient.post(this.urls.query_ip_access_history, {
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
            const response = await apiClient.post(this.urls.query_redshift_orderbook, {
                user_id: String(memId),
                tran_start: tranPeriod.start.split(' ')[0],
                tran_end: tranPeriod.end.split(' ')[0]
            });
            
            if (response.success && response.rows_count > 0) {
                // 2. 분석 실행
                const analysis = await apiClient.post(this.urls.analyze_cached_orderbook, {
                    cache_key: response.cache_key
                });
                
                if (analysis.success) {
                    analysis.monthsBack = tranPeriod.monthsBack;
                    
                    await this.saveToSession('current_orderbook_analysis', {
                        patterns: analysis.patterns,
                        period_info: analysis.period_info,
                        text_summary: analysis.text_summary,
                        cache_key: response.cache_key
                    });
                    
                    const alertData = stateManager.getState('alertData');
                    window.TableRenderer.renderOrderbookAnalysis(analysis, alertData);
                }
            }
        } catch (error) {
            console.error('Orderbook fetch/analysis failed:', error);
        }
    }

    renderSyncSections(cols, rows, repRuleId, canonicalIds) {
        const ruleObjMap = window.RULE_OBJ_MAP || {};
        const alertId = stateManager.getState('currentAlertId');
        
        // Alert 히스토리
        window.TableRenderer.renderAlertHistory(cols, rows, alertId);
        
        // 의심거래 객관식
        window.TableRenderer.renderObjectives(cols, rows, ruleObjMap, canonicalIds, repRuleId);
        
        // Rule 설명
        window.TableRenderer.renderRuleDesc(cols, rows, canonicalIds, repRuleId);
    }

    async saveToSession(key, data) {
        try {
            await apiClient.post('/api/save_to_session/', {
                key: key,
                data: JSON.stringify(data)
            });
        } catch (error) {
            console.error('Session save error:', error);
        }
    }

    extractMID(columns, rows) {
        if (!rows || rows.length === 0) return null;
        const midIdx = columns.indexOf('MID');
        return midIdx >= 0 ? rows[0][midIdx] : null;
    }
}