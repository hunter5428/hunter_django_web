// str_dashboard/static/str_dashboard/js/menu1_1.js

/**
 * ALERT ID 조회 페이지 메인 JavaScript
 * - 테이블 컴포넌트 활용
 * - 기존 UX 유지
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

    // ==================== 모달 관리 ====================
    class ModalManager {
        constructor(modalId) {
            this.modal = $(modalId);
            this.init();
        }

        init() {
            this.modal?.addEventListener('click', (e) => {
                if (e.target === this.modal) this.close();
            });
        }

        open() {
            if (this.modal) this.modal.style.display = 'flex';
        }

        close() {
            if (this.modal) this.modal.style.display = 'none';
        }
    }

    // ==================== DB 연결 관리 ====================
    class DBConnectionManager {
        constructor() {
            this.modal = new ModalManager('#db-modal');
            this.statusBadge = $('#db-status');
            this.init();
        }

        init() {
            $('#btn-open-db-modal')?.addEventListener('click', () => this.modal.open());
            $('#btn-close-db-modal')?.addEventListener('click', () => this.modal.close());
            $('#btn-test-conn')?.addEventListener('click', () => this.testConnection());
        }

        async testConnection() {
            const fields = ['host', 'port', 'service_name', 'username', 'password'];
            const data = {};

            for (const field of fields) {
                const value = $(`#${field}`)?.value?.trim();
                if (!value) {
                    alert(`${field}를 입력해주세요.`);
                    return;
                }
                data[field] = value;
            }

            try {
                const response = await fetch(window.URLS.test_connection, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams(data)
                });

                const result = await response.json();
                
                if (result.success) {
                    alert('연결에 성공했습니다');
                    this.updateStatus(true);
                    this.modal.close();
                } else {
                    alert(result.message || '연결에 실패했습니다');
                }
            } catch (error) {
                alert('연결에 실패했습니다');
                console.error(error);
            }
        }

        updateStatus(isConnected) {
            if (this.statusBadge) {
                if (isConnected) {
                    this.statusBadge.textContent = '연결 완료';
                    this.statusBadge.classList.add('ok');
                } else {
                    this.statusBadge.textContent = '연결 필요';
                    this.statusBadge.classList.remove('ok');
                }
            }
        }
    }

    // ==================== ALERT 조회 관리 ====================
    class AlertSearchManager {
        constructor() {
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
            const alertId = this.inputField?.value?.trim();
            
            if (!alertId) {
                alert('ALERT ID를 입력하세요.');
                return;
            }

            try {
                // ALERT 정보 조회
                const alertResponse = await fetch(window.URLS.query_alert, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({ alert_id: alertId })
                });

                const alertData = await alertResponse.json();
                
                if (!alertData.success) {
                    alert(alertData.message || '조회 실패');
                    return;
                }

                const cols = (alertData.columns || []).map(c => String(c || '').toUpperCase());
                const rows = alertData.rows || [];
                const processedData = this.processAlertData(cols, rows, alertId);
                
                await this.fetchAndRenderAllSections(processedData);
                
            } catch (error) {
                alert('조회 실패');
                console.error(error);
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

        async fetchPersonInfo(custId) {
            try {
                const response = await fetch(window.URLS.query_person, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({ cust_id: String(custId) })
                });
                
                const personData = await response.json();
                if (personData.success) {
                    window.renderPersonInfoSection(personData.columns || [], personData.rows || []);
                    
                    // 고객 구분 추출 (첫 번째 컬럼이 '고객구분'인 경우)
                    if (personData.columns && personData.rows && personData.rows.length > 0) {
                        const custTypeIdx = personData.columns.indexOf('고객구분');
                        if (custTypeIdx >= 0) {
                            const custType = personData.rows[0][custTypeIdx];
                            // 고객 상세 정보도 조회
                            await this.fetchPersonDetailInfo(custId, custType);
                        } else {
                            // 고객구분이 없으면 기본값으로 조회
                            await this.fetchPersonDetailInfo(custId, '개인');
                        }
                    }
                } else {
                    window.renderPersonInfoSection([], []);
                    window.renderPersonDetailSection([], []);
                }
            } catch (error) {
                console.error('Person info fetch failed:', error);
                window.renderPersonInfoSection([], []);
                window.renderPersonDetailSection([], []);
            }
        }

        async fetchPersonDetailInfo(custId, custType) {
            try {
                const response = await fetch('/api/query_person_detail_info/', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({ 
                        cust_id: String(custId),
                        cust_type: custType || '개인'
                    })
                });
                
                const detailData = await response.json();
                if (detailData.success) {
                    window.renderPersonDetailSection(detailData.columns || [], detailData.rows || []);
                    
                    // 법인인 경우 관련인 정보도 조회
                    if (custType === '법인') {
                        await this.fetchCorpRelatedPersons(custId);
                    }
                    // 개인인 경우에만 중복 회원 검색
                    else if (custType === '개인' && detailData.rows && detailData.rows.length > 0) {
                        await this.fetchDuplicatePersons(custId, detailData.columns, detailData.rows[0]);
                    }
                } else {
                    console.error('Person detail query failed:', detailData.message);
                    window.renderPersonDetailSection([], []);
                }
            } catch (error) {
                console.error('Person detail info fetch failed:', error);
                window.renderPersonDetailSection([], []);
            }
        }

        async fetchCorpRelatedPersons(custId) {
            try {
                const response = await fetch('/api/query_corp_related_persons/', {
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

        async fetchDuplicatePersons(custId, columns, row) {
            // 컬럼 인덱스 찾기
            const emailIdx = columns.indexOf('E-mail');
            const phoneIdx = columns.indexOf('휴대폰 번호');
            const addressIdx = columns.indexOf('거주주소');
            const detailAddressIdx = columns.indexOf('거주상세주소');
            
            // 값 추출
            let emailPrefix = '';
            let phoneSuffix = '';
            let address = '';
            let detailAddress = '';
            
            if (emailIdx >= 0 && row[emailIdx]) {
                const email = String(row[emailIdx]);
                const atIndex = email.indexOf('@');
                if (atIndex > 0) {
                    emailPrefix = email.substring(0, atIndex);
                }
            }
            
            if (phoneIdx >= 0 && row[phoneIdx]) {
                const phone = String(row[phoneIdx]);
                if (phone.length >= 4) {
                    phoneSuffix = phone.slice(-4);
                }
            }
            
            if (addressIdx >= 0) {
                address = row[addressIdx] || '';
            }
            
            if (detailAddressIdx >= 0) {
                detailAddress = row[detailAddressIdx] || '';
            }
            
            // 병렬로 두 쿼리 실행
            const promises = [];
            
            // 이메일 기준 조회
            if (emailPrefix) {
                promises.push(
                    fetch(window.URLS.query_duplicate_by_email, {  // URL 수정
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'X-CSRFToken': getCookie('csrftoken')
                        },
                        body: new URLSearchParams({
                            email_prefix: emailPrefix,
                            phone_suffix: phoneSuffix,
                            current_cust_id: String(custId)
                        })
                    }).then(res => res.json())
                );
            }
            
            // 주소 기준 조회
            if (address) {
                promises.push(
                    fetch(window.URLS.query_duplicate_by_address, {  // URL 수정
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'X-CSRFToken': getCookie('csrftoken')
                        },
                        body: new URLSearchParams({
                            address: address,
                            detail_address: detailAddress,
                            phone_suffix: phoneSuffix,
                            current_cust_id: String(custId)
                        })
                    }).then(res => res.json())
                );
            }
            
            if (promises.length === 0) {
                window.renderDuplicatePersonsSection([], [], {});
                return;
            }
            
            try {
                const results = await Promise.all(promises);
                
                // 결과 병합 (중복 제거)
                const allRows = [];
                const seenCustIds = new Set();
                const columns = results[0]?.columns || [];
                
                for (const result of results) {
                    if (result.success && result.rows) {
                        const custIdIdx = result.columns?.indexOf('고객ID') || 0;
                        
                        for (const row of result.rows) {
                            const custId = row[custIdIdx];
                            if (!seenCustIds.has(custId)) {
                                seenCustIds.add(custId);
                                allRows.push(row);
                            }
                        }
                    }
                }
                
                // 매칭 정보
                const matchCriteria = {
                    email_prefix: emailPrefix || null,
                    phone_suffix: phoneSuffix || null,
                    address: address || null,
                    detail_address: detailAddress || null
                };
                
                window.renderDuplicatePersonsSection(columns, allRows, matchCriteria);
                
            } catch (error) {
                console.error('Duplicate persons fetch failed:', error);
                window.renderDuplicatePersonsSection([], [], {});
            }
        }

        async fetchAndRenderAllSections(data) {
            const { cols, rows, alertId, repRuleId, custIdForPerson, canonicalIds } = data;

            // 고객 정보 조회 (기본 정보 + 상세 정보)
            if (custIdForPerson) {
                await this.fetchPersonInfo(custIdForPerson);
            } else {
                window.renderPersonInfoSection([], []);
                window.renderPersonDetailSection([], []);
            }

            // RULE 히스토리 조회
            if (canonicalIds.length > 0) {
                const ruleKey = canonicalIds.slice().sort().join(',');
                try {
                    const response = await fetch(window.URLS.rule_history, {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'X-CSRFToken': getCookie('csrftoken')
                        },
                        body: new URLSearchParams({ rule_key: ruleKey })
                    });
                    
                    const historyData = await response.json();
                    if (historyData.success) {
                        window.renderRuleHistorySection(
                            historyData.columns || [], 
                            historyData.rows || [],
                            historyData.searched_rule || ruleKey,
                            historyData.similar_list || null
                        );
                    }
                } catch (error) {
                    console.error('Rule history fetch failed:', error);
                    window.renderRuleHistorySection([], [], ruleKey, null);
                }
            }

            // 나머지 섹션 렌더링 (테이블 컴포넌트 활용)
            const ruleObjMap = window.RULE_OBJ_MAP || {};
            window.renderObjectivesSection(cols, rows, ruleObjMap, canonicalIds, repRuleId);
            window.renderAlertHistSection(cols, rows, alertId);
            window.renderRuleDescSection(cols, rows, canonicalIds, repRuleId);
        }
    }

    // ==================== 초기화 ====================
    document.addEventListener('DOMContentLoaded', function() {
        // 매니저 인스턴스 생성
        window.dbManager = new DBConnectionManager();
        window.alertManager = new AlertSearchManager();

        // 초기 상태: 섹션 숨김
        document.querySelectorAll('.section').forEach(section => {
            section.style.display = 'none';
        });

        console.log('Menu1_1 page initialized');
    });

})();