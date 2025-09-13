// str_dashboard/static/str_dashboard/js/menu1_1.js

/**
 * ALERT ID 조회 페이지 메인 JavaScript
 * - 테이블 컴포넌트 활용
 * - 기존 UX 유지
 * - 부분 실패 시에도 성공한 데이터는 표시
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

        isConnected() {
            return this.statusBadge && this.statusBadge.classList.contains('ok');
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
            // DB 연결 상태 먼저 확인
            if (!window.dbManager || !window.dbManager.isConnected()) {
                alert('먼저 DB Connection에서 연결을 완료해 주세요.');
                $('#btn-open-db-modal')?.click();
                return;
            }
            
            const alertId = this.inputField?.value?.trim();
            
            if (!alertId) {
                alert('ALERT ID를 입력하세요.');
                return;
            }

            console.log('Searching for ALERT ID:', alertId);

            // 초기화: 모든 섹션 숨기기
            document.querySelectorAll('.section').forEach(section => {
                section.style.display = 'none';
            });

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
                console.log('Alert query response:', alertData);
                
                if (!alertData.success) {
                    alert(alertData.message || '조회 실패');
                    // Alert 조회가 실패해도 다른 데이터는 조회 시도하지 않음
                    return;
                }

                const cols = (alertData.columns || []).map(c => String(c || '').toUpperCase());
                const rows = alertData.rows || [];
                console.log('Alert data - columns:', cols.length, 'rows:', rows.length);
                
                const processedData = this.processAlertData(cols, rows, alertId);
                console.log('Processed data:', processedData);
                
                // 모든 섹션 데이터 조회 및 렌더링 (에러가 있어도 계속 진행)
                await this.fetchAndRenderAllSections(processedData);
                
            } catch (error) {
                console.error('Alert search error:', error);
                alert('조회 중 오류가 발생했습니다. 일부 데이터만 표시될 수 있습니다.');
                // 에러가 있어도 가능한 데이터는 표시
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
                            console.log(`Customer type detected: ${custType}`);
                            // 고객 상세 정보도 조회
                            await this.fetchPersonDetailInfo(custId, custType);
                        } else {
                            console.log('Customer type not found, using default: 개인');
                            // 고객구분이 없으면 기본값으로 조회
                            await this.fetchPersonDetailInfo(custId, '개인');
                        }
                    }
                } else {
                    console.error('Person info query failed:', personData.message);
                    window.renderPersonInfoSection([], []);
                }
            } catch (error) {
                console.error('Person info fetch failed:', error);
                window.renderPersonInfoSection([], []);
            }
        }

        async fetchPersonDetailInfo(custId, custType) {
            try {
                // 고객 유형 확인 및 정확한 값 전달
                const actualCustType = (custType === '법인') ? '법인' : '개인';
                console.log(`Fetching detail for custId: ${custId}, type: ${actualCustType}`);
                
                const response = await fetch(window.URLS.query_person_detail, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({ 
                        cust_id: String(custId),
                        cust_type: actualCustType
                    })
                });
                
                const detailData = await response.json();
                console.log(`Detail query response - success: ${detailData.success}, rows: ${detailData.rows ? detailData.rows.length : 0}`);
                
                if (detailData.success) {
                    window.renderPersonDetailSection(detailData.columns || [], detailData.rows || []);
                    
                    // 법인인 경우 관련인 정보 조회
                    if (actualCustType === '법인') {
                        console.log('Fetching corp related persons...');
                        await this.fetchCorpRelatedPersons(custId);
                    }
                    
                    // 개인/법인 구분 없이 중복 회원 검색 (데이터가 있는 경우만)
                    if (detailData.rows && detailData.rows.length > 0) {
                        console.log(`Fetching duplicate persons for ${actualCustType}...`);
                        await this.fetchDuplicatePersons(custId, detailData.columns, detailData.rows[0], actualCustType);
                    }
                } else {
                    console.error('Person detail query failed:', detailData.message);
                    window.renderPersonDetailSection([], []);
                    // 실패해도 법인인 경우 관련인 정보는 시도
                    if (actualCustType === '법인') {
                        await this.fetchCorpRelatedPersons(custId);
                    }
                }
            } catch (error) {
                console.error('Person detail info fetch failed:', error);
                window.renderPersonDetailSection([], []);
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
                    // 실패해도 빈 테이블 표시
                    window.renderCorpRelatedSection([], []);
                }
            } catch (error) {
                console.error('Corp related persons fetch failed:', error);
                // 에러가 있어도 빈 테이블 표시
                window.renderCorpRelatedSection([], []);
            }
        }

        async fetchDuplicatePersons(custId, columns, row, custType) {
            console.log(`Starting duplicate search for ${custType} customer...`);
            
            // 컬럼 인덱스 찾기
            let emailIdx, phoneIdx, addressIdx, detailAddressIdx;
            let workplaceNameIdx, workplaceAddressIdx, workplaceDetailAddressIdx;
            
            if (custType === '법인') {
                // 법인의 경우 매핑
                emailIdx = columns.indexOf('E-mail');  // 대표자 이메일
                phoneIdx = columns.indexOf('대표번호');  // 대표번호
                
                // 법인의 본점소재지 주소 사용
                addressIdx = columns.indexOf('본점소재지주소');
                detailAddressIdx = columns.indexOf('본점소재지상세주소');
                
                // 직장명은 법인명, 직장주소는 본점소재지
                workplaceNameIdx = columns.indexOf('법인명');
                workplaceAddressIdx = columns.indexOf('본점소재지주소');
                workplaceDetailAddressIdx = columns.indexOf('본점소재지상세주소');
                
                console.log('Corp column indices:', {
                    email: emailIdx,
                    phone: phoneIdx,
                    address: addressIdx,
                    corpName: workplaceNameIdx
                });
            } else {
                // 개인의 경우 기존 매핑
                emailIdx = columns.indexOf('E-mail');
                phoneIdx = columns.indexOf('휴대폰 번호');
                addressIdx = columns.indexOf('거주주소');
                detailAddressIdx = columns.indexOf('거주상세주소');
                workplaceNameIdx = columns.indexOf('직장명');
                workplaceAddressIdx = columns.indexOf('직장주소');
                workplaceDetailAddressIdx = columns.indexOf('직장상세주소');
                
                console.log('Personal column indices:', {
                    email: emailIdx,
                    phone: phoneIdx,
                    address: addressIdx,
                    workplace: workplaceNameIdx
                });
            }
            
            // 값 추출
            let fullEmail = '';
            let phoneSuffix = '';
            let address = '';
            let detailAddress = '';
            let workplaceName = '';
            let workplaceAddress = '';
            let workplaceDetailAddress = '';
            
            // 이메일
            if (emailIdx >= 0 && row[emailIdx]) {
                fullEmail = row[emailIdx];
                console.log('Email found:', fullEmail.substring(0, fullEmail.indexOf('@')) + '@...');
            }
            
            // 전화번호 뒷자리
            if (phoneIdx >= 0 && row[phoneIdx]) {
                const phone = String(row[phoneIdx]);
                if (phone.length >= 4) {
                    phoneSuffix = phone.slice(-4);
                    console.log('Phone suffix:', phoneSuffix);
                }
            }
            
            // 주소
            if (addressIdx >= 0) {
                address = row[addressIdx] || '';
                if (address) console.log('Address found');
            }
            
            if (detailAddressIdx >= 0) {
                detailAddress = row[detailAddressIdx] || '';
            }
            
            // 직장명
            if (workplaceNameIdx >= 0) {
                workplaceName = row[workplaceNameIdx] || '';
                if (workplaceName) console.log('Workplace name found:', workplaceName);
            }
            
            // 직장주소
            if (workplaceAddressIdx >= 0) {
                workplaceAddress = row[workplaceAddressIdx] || '';
                if (workplaceAddress) console.log('Workplace address found');
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
                // 통합 API 호출 (단일 쿼리)
                const startTime = performance.now();
                console.log('Starting unified duplicate search...');
                console.log('Search criteria:', {
                    email: fullEmail ? 'yes' : 'no',
                    address: address ? 'yes' : 'no',
                    workplace: workplaceName || workplaceAddress ? 'yes' : 'no',
                    phone_suffix: phoneSuffix || 'none'
                });
                
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
                const endTime = performance.now();
                console.log(`Duplicate search completed in ${(endTime - startTime).toFixed(2)}ms`);
                console.log(`Found ${result.rows ? result.rows.length : 0} duplicate records`);
                
                if (result.success) {
                    // MATCH_TYPES 컬럼 처리
                    const columns = result.columns || [];
                    const rows = result.rows || [];
                    
                    // 매칭 정보 (표시용으로 이메일 앞부분만 추출)
                    let emailPrefix = '';
                    if (fullEmail) {
                        const atIndex = fullEmail.indexOf('@');
                        if (atIndex > 0) {
                            emailPrefix = fullEmail.substring(0, atIndex);
                        }
                    }
                    
                    const matchCriteria = {
                        email_prefix: emailPrefix || null,  // UI 표시용
                        full_email: fullEmail || null,      // 실제 비교용
                        phone_suffix: phoneSuffix || null,
                        address: address || null,
                        detail_address: detailAddress || null,
                        workplace_name: workplaceName || null,
                        workplace_address: workplaceAddress || null,
                        workplace_detail_address: workplaceDetailAddress || null,
                        customer_type: custType  // 고객 유형 추가
                    };
                    
                    window.renderDuplicatePersonsSection(columns, rows, matchCriteria);
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
                    } else {
                        // 실패해도 빈 테이블 표시
                        window.renderRuleHistorySection([], [], ruleKey, null);
                    }
                } catch (error) {
                    console.error('Rule history fetch failed:', error);
                    // 에러가 있어도 빈 테이블 표시
                    window.renderRuleHistorySection([], [], ruleKey, null);
                }
            }

            // 나머지 섹션 렌더링 (테이블 컴포넌트 활용)
            // 이 섹션들은 Alert 데이터에서 직접 렌더링하므로 항상 표시
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