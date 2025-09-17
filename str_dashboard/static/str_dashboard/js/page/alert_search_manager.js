// str_dashboard/static/str_dashboard/js/page/alert_search_manager.js
/**
 * ALERT ID 조회 및 관련 데이터 렌더링을 총괄하는 메인 컨트롤러
 */
(function(window) {
    'use strict';

    // 의존성 확인
    const dependencies = ['APIClient', 'SearchState', 'UIManager', 'DataProcessor', 'TableRenderer', 'AppHelpers'];
    for (const dep of dependencies) {
        if (!window[dep]) {
            console.error(`AlertSearchManager requires ${dep} to be loaded first.`);
            return;
        }
    }

    const { $, saveToSession } = window.AppHelpers;

    class AlertSearchManager {
        constructor() {
            this.api = new window.APIClient();
            this.state = new window.SearchState();
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
            if (this.state.isSearching) return;

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

            if (this.state.currentAlertId === alertId && this.state.alertData) return;

            this._prepareNewSearch(alertId);

            try {
                await this._fetchAndProcessInitialData(alertId);
                await this._fetchAllRelatedData();
                this._renderSyncSections();
                
                const tomlBtn = $('#toml_save_btn');
                if (tomlBtn) tomlBtn.style.display = 'inline-flex';

            } catch (error) {
                console.error('Alert search process failed:', error);
                UIManager.showError(error.message || '조회 중 오류가 발생했습니다.');
                UIManager.hideAllSections();
            } finally {
                this.state.setSearching(false);
                UIManager.showLoading(false);
            }
        }
        
        _prepareNewSearch(alertId) {
            this.state.reset();
            this.state.currentAlertId = alertId;
            this.state.setSearching(true);
            UIManager.hideAllSections();
            UIManager.showLoading(true);
        }

        async _fetchAndProcessInitialData(alertId) {
            const alertData = await this.api.post(window.URLS.query_alert, { alert_id: alertId });
            if (!alertData.success) throw new Error(alertData.message);
            if (alertData.rows.length === 0) throw new Error('해당 ALERT ID에 대한 데이터가 없습니다.');

            const { cols, rows } = alertData;
            const processedData = DataProcessor.processAlertData(cols, rows, alertId);
            
            this.state.setAlertData({ cols, rows, ...processedData });
            saveToSession('current_alert_data', { alert_id: alertId, cols, rows, ...processedData });
            saveToSession('current_alert_id', alertId);
        }

        async _fetchAllRelatedData() {
            const { custIdForPerson, canonicalIds } = this.state.alertData;
            
            const promises = [];
            if (custIdForPerson) promises.push(this._fetchAndRenderCustomerData(custIdForPerson));
            if (canonicalIds.length > 0) promises.push(this._fetchAndRenderRuleHistory(canonicalIds));
            
            await Promise.allSettled(promises);
        }

        async _fetchAndRenderCustomerData(custId) {
            const data = await this.api.post(window.URLS.query_customer_unified, { cust_id: String(custId) });
            if (!data.success) return;

            const { columns, rows, customer_type } = data;
            const kycDatetime = rows.length > 0 ? (columns.indexOf('KYC완료일시') >= 0 ? rows[0][columns.indexOf('KYC완료일시')] : null) : null;
            
            saveToSession('current_customer_data', { columns, rows, customer_type, kyc_datetime: kycDatetime });
            TableRenderer.renderCustomerUnified(columns, rows);
            
            const tranPeriod = DataProcessor.extractTransactionPeriod(this.state.alertData.cols, this.state.alertData.rows, kycDatetime);
            
            const subPromises = [];
            if (customer_type === '법인') subPromises.push(this._fetchCorpRelated(custId));
            if (customer_type === '개인' && tranPeriod.start) subPromises.push(this._fetchPersonRelated(custId, tranPeriod));
            if (rows.length > 0) subPromises.push(this._fetchDuplicatePersons(custId, columns, rows[0], customer_type));
            
            const memId = rows.length > 0 ? (columns.indexOf('MID') >= 0 ? rows[0][columns.indexOf('MID')] : null) : null;
            if (memId && tranPeriod.start) {
                subPromises.push(this._fetchIPHistory(memId, tranPeriod));
                if (window.dualDBManager?.isRedshiftConnected()) {
                    subPromises.push(this._fetchOrderbook(memId, tranPeriod));
                }
            }
            
            await Promise.allSettled(subPromises);
        }

        _renderSyncSections() {
            const { cols, rows, repRuleId, canonicalIds } = this.state.alertData;
            const alertId = this.state.currentAlertId;
            const ruleObjMap = window.RULE_OBJ_MAP || {};

            TableRenderer.renderAlertHistory(cols, rows, alertId);
            TableRenderer.renderObjectives(cols, rows, ruleObjMap, canonicalIds, repRuleId);
            TableRenderer.renderRuleDesc(cols, rows, canonicalIds, repRuleId);
        }

        // --- 세부 데이터 조회 헬퍼 메서드들 ---
        
        async _fetchAndRenderRuleHistory(canonicalIds) {
            const ruleKey = canonicalIds.slice().sort().join(',');
            const data = await this.api.post(window.URLS.rule_history, { rule_key: ruleKey });
            if (data.success) {
                saveToSession('current_rule_history_data', { columns: data.columns, rows: data.rows });
                TableRenderer.renderRuleHistory(data.columns, data.rows, data.searched_rule || ruleKey, data.similar_list);
            }
        }

        async _fetchCorpRelated(custId) {
            const data = await this.api.post(window.URLS.query_corp_related_persons, { cust_id: String(custId) });
            if (data.success) TableRenderer.renderCorpRelated(data.columns, data.rows);
        }

        async _fetchPersonRelated(custId, tranPeriod) {
            const data = await this.api.post(window.URLS.query_person_related_summary, {
                cust_id: String(custId), start_date: tranPeriod.start, end_date: tranPeriod.end
            });
            if (data.success) TableRenderer.renderPersonRelated(data.related_persons);
        }
        
        async _fetchDuplicatePersons(custId, columns, row, custType) {
             const getVal = (name) => {
                const idx = columns.indexOf(name);
                return idx >= 0 ? (row[idx] || '') : '';
            };
            const phone = getVal('연락처');
            const params = {
                current_cust_id: String(custId),
                full_email: getVal('이메일'),
                phone_suffix: phone.length >= 4 ? phone.slice(-4) : '',
                address: getVal('거주지주소'),
                detail_address: getVal('거주지상세주소'),
                workplace_name: getVal('직장명'),
                workplace_address: getVal('직장주소'),
                workplace_detail_address: getVal('직장상세주소')
            };

            const data = await this.api.post(window.URLS.query_duplicate_unified, params);
            if (data.success) {
                params.email_prefix = params.full_email ? params.full_email.split('@')[0] : null;
                TableRenderer.renderDuplicatePersons(data.columns, data.rows, {...params, customer_type: custType});
            }
        }
        
        async _fetchIPHistory(memId, tranPeriod) {
            const data = await this.api.post(window.URLS.query_ip_access_history, {
                mem_id: String(memId), start_date: tranPeriod.start.split(' ')[0], end_date: tranPeriod.end.split(' ')[0]
            });
            if (data.success) TableRenderer.renderIPHistory(data.columns, data.rows);
        }

        async _fetchOrderbook(memId, tranPeriod) {
            const orderbookData = await this.api.post(window.URLS.query_redshift_orderbook, {
                user_id: String(memId), tran_start: tranPeriod.start.split(' ')[0], tran_end: tranPeriod.end.split(' ')[0]
            });
            if (!orderbookData.success || orderbookData.rows_count === 0) return;

            const analysis = await this.api.post(window.URLS.analyze_cached_orderbook, { cache_key: orderbookData.cache_key });
            if (analysis.success) {
                analysis.monthsBack = tranPeriod.monthsBack;
                saveToSession('current_orderbook_analysis', {
                    patterns: analysis.patterns, period_info: analysis.period_info,
                    text_summary: analysis.text_summary, cache_key: orderbookData.cache_key
                });
                
                const alertDataWithPeriod = { ...this.state.alertData, tranPeriod };
                TableRenderer.renderOrderbookAnalysis(analysis, alertDataWithPeriod);
            }
        }
    }

    window.AlertSearchManager = AlertSearchManager;

})(window);