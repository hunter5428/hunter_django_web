// str_dashboard/static/str_dashboard/js/menu1_1.js

/**
 * ALERT ID 조회 페이지 메인 JavaScript
 * - 듀얼 DB (Oracle + Redshift) 지원
 * - 통합 고객 정보 조회 적용
 * - 테이블 컴포넌트 활용
 */
(function() {
    'use strict';

    // ==================== 유틸리티 ====================
    const $ = (sel) => document.querySelector(sel);
    
    const getCookie = (name) => {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        return parts.length === 2 ? decodeURIComponent(parts.pop().split(';').shift()) : undefined;
    };

    // ==================== ALERT 조회 관리 ====================
    class AlertSearchManager {
        constructor() {
            this.searchBtn = $('#alert_id_search_btn');
            this.inputField = $('#alert_id_input');
            this.alertData = null;  // Alert 데이터 저장
            this.init();
        }

        init() {
            this.searchBtn?.addEventListener('click', () => this.search());
            this.inputField?.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') this.search();
            });
        }

        async search() {
            // 듀얼 DB 매니저를 통해 Oracle 연결 상태 확인
            if (!window.dualDBManager || !window.dualDBManager.isOracleConnected()) {
                alert('먼저 Oracle DB 연결을 완료해 주세요.');
                $('#btn-open-db-modal')?.click();
                return;
            }
            
            const alertId = this.inputField?.value?.trim();
            
            if (!alertId) {
                alert('ALERT ID를 입력하세요.');
                return;
            }

            console.log('Searching for ALERT ID:', alertId);
            console.log('Oracle connected:', window.dualDBManager.isOracleConnected());
            console.log('Redshift connected:', window.dualDBManager.isRedshiftConnected());

            // 초기화: 모든 섹션 숨기기
            document.querySelectorAll('.section').forEach(section => {
                section.style.display = 'none';
            });

            try {
                // ALERT 정보 조회 (Oracle)
                const alertResponse = await fetch(window.URLS.query_alert, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({ alert_id: alertId })
                });

                const alertData = await alertResponse.json();
                console.log('Alert query response:', alertData);
                
                if (!alertData.success) {
                    alert(alertData.message || '조회 실패');
                    return;
                }

                const cols = (alertData.columns || []).map(c => String(c || '').toUpperCase());
                const rows = alertData.rows || [];
                console.log('Alert data - columns:', cols.length, 'rows:', rows.length);
                
                const processedData = this.processAlertData(cols, rows, alertId);
                console.log('Processed data:', processedData);
                
                // Alert 데이터 저장 (나중에 사용)
                this.alertData = { cols, rows, ...processedData };
                
                // 모든 섹션 데이터 조회 및 렌더링
                await this.fetchAndRenderAllSections(processedData);
                
                // Redshift 연결되어 있으면 추가 데이터 조회 가능
                if (window.dualDBManager && window.dualDBManager.isRedshiftConnected()) {
                    console.log('Redshift is connected, additional queries can be performed');
                    // TODO: Redshift에서 추가 데이터 조회
                    // await this.fetchRedshiftData(alertId);
                }
                
            } catch (error) {
                console.error('Alert search error:', error);
                alert('조회 중 오류가 발생했습니다. 일부 데이터만 표시될 수 있습니다.');
            }
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
                for (const row of rows) {
                    const ruleId = row[idxRule];
                    if (ruleId != null) {
                        const strId = String(ruleId).trim();
                        if (!seen.has(strId)) {
                            seen.add(strId);
                            canonicalIds.push(strId);
                        }
                    }
                }
            }

            return { cols, rows, alertId, repRuleId, custIdForPerson, canonicalIds };
        }

        async fetchCustomerInfo(custId) {
            try {
                // 통합 API 호출 - 한 번의 요청으로 모든 고객 정보 조회
                const response = await fetch(window.URLS.query_customer_unified, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({ cust_id: String(custId) })
                });
                
                const customerData = await response.json();
                
                if (customerData.success) {
                    const customerType = customerData.customer_type;
                    console.log(`Customer unified info loaded - type: ${customerType}, rows: ${customerData.rows?.length}`);
                    
                    // 통합 테이블 렌더링
                    window.renderCustomerUnifiedSection(
                        customerData.columns || [], 
                        customerData.rows || []
                    );
                    
                    // 고객 유형에 따른 추가 조회
                    if (customerType === '법인') {
                        console.log('Fetching corp related persons...');
                        await this.fetchCorpRelatedPersons(custId);
                    } else if (customerType === '개인') {
                        console.log('Fetching person related summary...');
                        await this.fetchPersonRelatedSummary(custId);
                    }
                    
                    // 중복 회원 검색 (데이터가 있는 경우만)
                    if (customerData.rows && customerData.rows.length > 0) {
                        console.log(`Fetching duplicate persons for ${customerType}...`);
                        await this.fetchDuplicatePersons(
                            custId, 
                            customerData.columns, 
                            customerData.rows[0], 
                            customerType
                        );
                    }
                    
                    // MID 추출 (통합 정보에서)
                    let memId = null;
                    if (customerData.rows && customerData.rows.length > 0) {
                        const midIdx = customerData.columns.indexOf('MID');
                        if (midIdx >= 0) {
                            memId = customerData.rows[0][midIdx];
                        }
                    }
                    
                    // MID가 있고 거래 기간이 있으면 IP 접속 이력 조회
                    if (memId) {
                        const tranPeriod = this.extractTransactionPeriod();
                        if (tranPeriod.start && tranPeriod.end) {
                            console.log(`Fetching IP access history for MID: ${memId}`);
                            await this.fetchIPAccessHistory(memId, tranPeriod);
                        }
                    }

                    if (memId) {
                        const tranPeriod = this.extractTransactionPeriod();
                        if (tranPeriod.start && tranPeriod.end) {
                            console.log(`Fetching IP access history for MID: ${memId}`);
                            await this.fetchIPAccessHistory(memId, tranPeriod);
                            
                            // Redshift Orderbook 조회 추가
                            if (window.dualDBManager && window.dualDBManager.isRedshiftConnected()) {
                                console.log(`Fetching Redshift orderbook for MID: ${memId}`);
                                await this.fetchRedshiftOrderbook(memId, tranPeriod);
                            }
                        }
                    }



                } else {
                    console.error('Customer info query failed:', customerData.message);
                    window.renderCustomerUnifiedSection([], []);
                }
            } catch (error) {
                console.error('Customer info fetch failed:', error);
                window.renderCustomerUnifiedSection([], []);
            }
        }

        async fetchIPAccessHistory(memId, tranPeriod) {
            try {
                // 날짜 형식 변환 (YYYY-MM-DD 형식으로)
                const startDate = tranPeriod.start.split(' ')[0];
                const endDate = tranPeriod.end.split(' ')[0];
                
                const response = await fetch(window.URLS.query_ip_access_history, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({
                        mem_id: String(memId),
                        start_date: startDate,
                        end_date: endDate
                    })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    console.log(`IP access history loaded - rows: ${result.rows?.length}`);
                    window.renderIPAccessHistorySection(result.columns || [], result.rows || []);
                } else {
                    console.error('IP access history query failed:', result.message);
                    window.renderIPAccessHistorySection([], []);
                }
            } catch (error) {
                console.error('IP access history fetch failed:', error);
                window.renderIPAccessHistorySection([], []);
            }
        }

        async fetchCorpRelatedPersons(custId) {
            try {
                const response = await fetch(window.URLS.query_corp_related_persons, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({ 
                        cust_id: String(custId)
                    })
                });
                
                const relatedData = await response.json();
                if (relatedData.success) {
                    window.renderCorpRelatedSection(relatedData.columns || [], relatedData.rows || []);
                } else {
                    console.error('Corp related persons query failed:', relatedData.message);
                    window.renderCorpRelatedSection([], []);
                }
            } catch (error) {
                console.error('Corp related persons fetch failed:', error);
                window.renderCorpRelatedSection([], []);
            }
        }

        async fetchPersonRelatedSummary(custId) {
            try {
                // ALERT 데이터에서 거래 기간 추출
                const tranPeriod = this.extractTransactionPeriod();
                
                if (!tranPeriod.start || !tranPeriod.end) {
                    console.log('No transaction period found, skipping person related summary');
                    window.renderPersonRelatedSection(null);
                    return;
                }
                
                console.log(`Fetching person related summary for period: ${tranPeriod.start} ~ ${tranPeriod.end}`);
                
                const response = await fetch(window.URLS.query_person_related_summary, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({ 
                        cust_id: String(custId),
                        start_date: tranPeriod.start,
                        end_date: tranPeriod.end
                    })
                });
                
                const relatedData = await response.json();
                if (relatedData.success) {
                    window.renderPersonRelatedSection(relatedData.related_persons);
                } else {
                    console.error('Person related summary query failed:', relatedData.message);
                    window.renderPersonRelatedSection(null);
                }
            } catch (error) {
                console.error('Person related summary fetch failed:', error);
                window.renderPersonRelatedSection(null);
            }
        }

        extractTransactionPeriod() {
            // Alert 데이터에서 TRAN_STRT와 TRAN_END 컬럼 찾기
            if (!this.alertData) {
                return { start: null, end: null };
            }
            
            const { cols, rows } = this.alertData;
            const idxTranStart = cols.indexOf('TRAN_STRT');
            const idxTranEnd = cols.indexOf('TRAN_END');
            
            if (idxTranStart < 0 || idxTranEnd < 0) {
                console.log('TRAN_STRT or TRAN_END columns not found');
                return { start: null, end: null };
            }
            
            let minStart = null;
            let maxEnd = null;
            
            // 모든 행에서 최소 시작일과 최대 종료일 찾기
            rows.forEach(row => {
                const startDate = row[idxTranStart];
                const endDate = row[idxTranEnd];
                
                // 날짜 유효성 검증
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
            
            // 날짜 형식 변환 (타임스탬프 형식 확인)
            if (minStart) {
                if (minStart.includes(' ')) {
                    minStart = minStart;
                } else {
                    minStart = minStart + ' 00:00:00.000000000';
                }
            }
            
            if (maxEnd) {
                if (maxEnd.includes(' ')) {
                    maxEnd = maxEnd;
                } else {
                    maxEnd = maxEnd + ' 23:59:59.999999999';
                }
            }
            
            console.log(`Extracted transaction period: ${minStart} ~ ${maxEnd}`);
            
            return { start: minStart, end: maxEnd };
        }

        async fetchDuplicatePersons(custId, columns, row, custType) {
            console.log(`Starting duplicate search for ${custType} customer...`);
            
            // 컬럼 인덱스 찾기
            let emailIdx, phoneIdx, addressIdx, detailAddressIdx;
            let workplaceNameIdx, workplaceAddressIdx, workplaceDetailAddressIdx;
            
            // 통합 고객 정보의 컬럼명 매핑
            emailIdx = columns.indexOf('이메일');
            phoneIdx = columns.indexOf('연락처');
            addressIdx = columns.indexOf('거주지주소');
            detailAddressIdx = columns.indexOf('거주지상세주소');
            workplaceNameIdx = columns.indexOf('직장명');
            workplaceAddressIdx = columns.indexOf('직장주소');
            workplaceDetailAddressIdx = columns.indexOf('직장상세주소');
            
            // 값 추출
            let fullEmail = '';
            let phoneSuffix = '';
            let address = '';
            let detailAddress = '';
            let workplaceName = '';
            let workplaceAddress = '';
            let workplaceDetailAddress = '';
            
            if (emailIdx >= 0 && row[emailIdx]) {
                fullEmail = row[emailIdx];
                console.log('Email found');
            }
            
            if (phoneIdx >= 0 && row[phoneIdx]) {
                const phone = String(row[phoneIdx]);
                if (phone.length >= 4) {
                    phoneSuffix = phone.slice(-4);
                    console.log('Phone suffix:', phoneSuffix);
                }
            }
            
            if (addressIdx >= 0) {
                address = row[addressIdx] || '';
            }
            
            if (detailAddressIdx >= 0) {
                detailAddress = row[detailAddressIdx] || '';
            }
            
            if (workplaceNameIdx >= 0) {
                workplaceName = row[workplaceNameIdx] || '';
            }
            
            if (workplaceAddressIdx >= 0) {
                workplaceAddress = row[workplaceAddressIdx] || '';
            }
            
            if (workplaceDetailAddressIdx >= 0) {
                workplaceDetailAddress = row[workplaceDetailAddressIdx] || '';
            }
            
            // 조회할 조건이 하나도 없으면 빈 결과 반환
            if (!fullEmail && !address && !workplaceName && !workplaceAddress) {
                console.log('No search criteria found, skipping duplicate search');
                window.renderDuplicatePersonsSection([], [], {});
                return;
            }
            
            try {
                const response = await fetch(window.URLS.query_duplicate_unified, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({
                        current_cust_id: String(custId),
                        full_email: fullEmail,
                        phone_suffix: phoneSuffix,
                        address: address,
                        detail_address: detailAddress,
                        workplace_name: workplaceName,
                        workplace_address: workplaceAddress,
                        workplace_detail_address: workplaceDetailAddress
                    })
                });
                
                const result = await response.json();
                console.log(`Found ${result.rows ? result.rows.length : 0} duplicate records`);
                
                if (result.success) {
                    let emailPrefix = '';
                    if (fullEmail) {
                        const atIndex = fullEmail.indexOf('@');
                        if (atIndex > 0) {
                            emailPrefix = fullEmail.substring(0, atIndex);
                        }
                    }
                    
                    const matchCriteria = {
                        email_prefix: emailPrefix || null,
                        full_email: fullEmail || null,
                        phone_suffix: phoneSuffix || null,
                        address: address || null,
                        detail_address: detailAddress || null,
                        workplace_name: workplaceName || null,
                        workplace_address: workplaceAddress || null,
                        workplace_detail_address: workplaceDetailAddress || null,
                        customer_type: custType
                    };
                    
                    window.renderDuplicatePersonsSection(result.columns, result.rows, matchCriteria);
                } else {
                    console.error('Duplicate query failed:', result.message);
                    window.renderDuplicatePersonsSection([], [], {});
                }
                
            } catch (error) {
                console.error('Duplicate persons fetch failed:', error);
                window.renderDuplicatePersonsSection([], [], {});
            }
        }

        async fetchAndRenderAllSections(data) {
            const { cols, rows, alertId, repRuleId, custIdForPerson, canonicalIds } = data;

            // Promise.allSettled를 사용하여 병렬 처리 (에러가 있어도 계속 진행)
            const promises = [];

            // 통합 고객 정보 조회
            if (custIdForPerson) {
                promises.push(
                    this.fetchCustomerInfo(custIdForPerson).catch(e => {
                        console.error('Customer info failed:', e);
                        window.renderCustomerUnifiedSection([], []);
                    })
                );
            } else {
                window.renderCustomerUnifiedSection([], []);
            }

            // RULE 히스토리 조회
            if (canonicalIds.length > 0) {
                const ruleKey = canonicalIds.slice().sort().join(',');
                promises.push(
                    fetch(window.URLS.rule_history, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'X-CSRFToken': getCookie('csrftoken')
                        },
                        body: new URLSearchParams({ rule_key: ruleKey })
                    })
                    .then(response => response.json())
                    .then(historyData => {
                        if (historyData.success) {
                            window.renderRuleHistorySection(
                                historyData.columns || [], 
                                historyData.rows || [],
                                historyData.searched_rule || ruleKey,
                                historyData.similar_list || null
                            );
                        } else {
                            window.renderRuleHistorySection([], [], ruleKey, null);
                        }
                    })
                    .catch(error => {
                        console.error('Rule history fetch failed:', error);
                        window.renderRuleHistorySection([], [], ruleKey, null);
                    })
                );
            }

            // 모든 비동기 작업 대기
            await Promise.allSettled(promises);

            // 동기 렌더링 섹션들 (Alert 데이터 기반)
            const ruleObjMap = window.RULE_OBJ_MAP || {};
            
            // Alert 히스토리 렌더링
            window.renderAlertHistSection(cols, rows, alertId);
            
            // 의심거래 객관식 렌더링
            window.renderObjectivesSection(cols, rows, ruleObjMap, canonicalIds, repRuleId);
            
            // Rule 설명 렌더링
            window.renderRuleDescSection(cols, rows, canonicalIds, repRuleId);
        }

        async fetchRedshiftOrderbook(memId, tranPeriod) {

            // Redshift 연결 확인
            if (!window.dualDBManager || !window.dualDBManager.isRedshiftConnected()) {
                console.log('Redshift not connected, skipping orderbook query');
                return null;
            }
            
            if (!memId || !tranPeriod.start || !tranPeriod.end) {
                console.log('Missing parameters for orderbook query');
                return null;
            }
            
            try {
                // 날짜 형식 변환 (YYYY-MM-DD 형식으로)
                const startDate = tranPeriod.start.split(' ')[0];
                const endDate = tranPeriod.end.split(' ')[0];
                
                console.log(`Fetching Redshift orderbook - MID: ${memId}, period: ${startDate} ~ ${endDate}`);
                
                const response = await fetch(window.URLS.query_redshift_orderbook, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({
                        user_id: String(memId),
                        tran_start: startDate,
                        tran_end: endDate
                    })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    console.log(`Orderbook data cached successfully:`);
                    console.log(`  Cache key: ${result.cache_key}`);
                    console.log(`  Rows: ${result.rows_count}`);
                    console.log(`  Period queried: ${result.period.queried}`);
                    
                    // 성공 메시지 표시 (선택사항)
                    if (result.rows_count > 0) {
                        this.showOrderbookStatus(
                            `Orderbook 데이터 ${result.rows_count}건이 메모리에 저장되었습니다.`,
                            'success'
                        );
                    } else {
                        this.showOrderbookStatus(
                            'Orderbook 데이터가 없습니다.',
                            'info'
                        );
                    }
                    
                    return result;
                } else {
                    console.error('Orderbook query failed:', result.message);
                    this.showOrderbookStatus(
                        `Orderbook 조회 실패: ${result.message}`,
                        'error'
                    );
                    return null;
                }
                
            } catch (error) {
                console.error('Orderbook fetch error:', error);
                this.showOrderbookStatus(
                    'Orderbook 조회 중 오류가 발생했습니다.',
                    'error'
                );
                return null;
            }
        }

        showOrderbookStatus(message, type = 'info') {
            // 상태 표시 영역이 없으면 생성
            let statusDiv = document.getElementById('orderbook-status');
            if (!statusDiv) {
                statusDiv = document.createElement('div');
                statusDiv.id = 'orderbook-status';
                statusDiv.style.cssText = `
                    position: fixed;
                    bottom: 20px;
                    right: 20px;
                    padding: 12px 20px;
                    border-radius: 8px;
                    font-size: 13px;
                    z-index: 9999;
                    transition: all 0.3s ease;
                    max-width: 400px;
                `;
                document.body.appendChild(statusDiv);
            }
            
            // 타입별 스타일 설정
            const styles = {
                'success': 'background: #0f2314; color: #4caf50; border: 1px solid #214a2c;',
                'error': 'background: #2a1414; color: #ff6b6b; border: 1px solid #4a2424;',
                'info': 'background: #141a2a; color: #4fc3f7; border: 1px solid #243a4a;'
            };
            
            statusDiv.style.cssText += styles[type] || styles['info'];
            statusDiv.textContent = message;
            statusDiv.style.display = 'block';
            
            // 5초 후 자동 숨김
            setTimeout(() => {
                statusDiv.style.opacity = '0';
                setTimeout(() => {
                    statusDiv.style.display = 'none';
                    statusDiv.style.opacity = '1';
                }, 300);
            }, 5000);
        }

    }

    // ==================== 초기화 ====================
    document.addEventListener('DOMContentLoaded', function() {
        // Alert 검색 매니저만 생성 (DB 연결은 dual_db_manager.js에서 처리)
        window.alertManager = new AlertSearchManager();

        // 초기 상태: 섹션 숨김
        document.querySelectorAll('.section').forEach(section => {
            section.style.display = 'none';
        });

        console.log('Menu1_1 page initialized with dual DB support');
    });

})();