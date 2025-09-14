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

        // menu1_1.js의 AlertSearchManager 클래스에 추가할 메서드들
        // fetchRedshiftOrderbook 메서드 - 분석까지 자동 수행
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
                
                // 1. Orderbook 데이터 조회 및 캐싱
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
                
                if (!result.success) {
                    console.error('Orderbook query failed:', result.message);
                    this.showOrderbookStatus(
                        `Orderbook 조회 실패: ${result.message}`,
                        'error'
                    );
                    return null;
                }
                
                console.log(`Orderbook data cached successfully:`);
                console.log(`  Cache key: ${result.cache_key}`);
                console.log(`  Rows: ${result.rows_count}`);
                
                // 데이터가 없으면 여기서 종료
                if (result.rows_count === 0) {
                    this.showOrderbookStatus('Orderbook 데이터가 없습니다.', 'info');
                    return result;
                }
                
                // 2. 자동으로 분석 수행
                console.log('Analyzing orderbook data...');
                const analysisResponse = await fetch(window.URLS.analyze_cached_orderbook, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({
                        cache_key: result.cache_key
                    })
                });
                
                const analysisResult = await analysisResponse.json();
                
                if (analysisResult.success) {
                    console.log(`Orderbook analysis completed: ${analysisResult.segments_count} segments`);
                    
                    // 3. 분석 결과를 화면에 표시
                    this.renderOrderbookAnalysis(analysisResult);
                    
                    // 성공 메시지
                    this.showOrderbookStatus(
                        `Orderbook 분석 완료: ${analysisResult.segments_count}개 구간으로 요약되었습니다.`,
                        'success'
                    );
                    
                    return analysisResult;
                } else {
                    console.error('Orderbook analysis failed:', analysisResult.message);
                    this.showOrderbookStatus(
                        `분석 실패: ${analysisResult.message}`,
                        'error'
                    );
                    return result;
                }
                
            } catch (error) {
                console.error('Orderbook fetch/analysis error:', error);
                this.showOrderbookStatus(
                    'Orderbook 처리 중 오류가 발생했습니다.',
                    'error'
                );
                return null;
            }
        }

        // Orderbook 분석 결과를 화면에 렌더링
        renderOrderbookAnalysis(analysisResult) {
            // 섹션 찾기 또는 생성
            let section = document.getElementById('section_orderbook_analysis');
            if (!section) {
                // 섹션이 없으면 생성
                section = document.createElement('div');
                section.id = 'section_orderbook_analysis';
                section.className = 'section';
                section.innerHTML = `
                    <h3>거래원장(Orderbook) 분석</h3>
                    <div class="table-wrap" id="result_orderbook_analysis"></div>
                `;
                
                // IP 접속 이력 섹션 다음에 추가
                const ipSection = document.getElementById('section_ip_access_history');
                if (ipSection && ipSection.parentNode) {
                    ipSection.parentNode.insertBefore(section, ipSection.nextSibling);
                } else {
                    // 적절한 위치에 추가
                    document.querySelector('.app-main').appendChild(section);
                }
            }
            
            // 섹션 표시
            section.style.display = 'block';
            
            // 결과 컨테이너
            const container = document.getElementById('result_orderbook_analysis');
            if (!container) return;
            
            // HTML 구성
            let html = '';
            
            // 1. 패턴 분석 요약 (클릭 가능한 카드 형태)
            if (analysisResult.patterns) {
                const patterns = analysisResult.patterns;
                html += `
                <div class="orderbook-patterns-summary card" style="margin-bottom: 15px;">
                    <h4 style="margin: 0 0 10px 0; font-size: 14px;">거래 패턴 분석</h4>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 10px;">`;
                
                // 매수
                if (patterns.total_buy_amount >= 0) {
                    html += `
                        <div class="pattern-stat clickable" data-action="buy">
                            <span style="color: #9a9a9a; font-size: 12px;">총 매수</span>
                            <div style="font-size: 16px; font-weight: 700;">
                                ${Number(patterns.total_buy_amount).toLocaleString('ko-KR')}원
                            </div>
                            <div style="font-size: 13px; color: #9a9a9a;">
                                ${Number(patterns.total_buy_count || 0).toLocaleString('ko-KR')}건
                            </div>
                            <div class="pattern-detail" id="detail-buy" style="display: none;"></div>
                        </div>`;
                }
                
                // 매도
                if (patterns.total_sell_amount >= 0) {
                    html += `
                        <div class="pattern-stat clickable" data-action="sell">
                            <span style="color: #9a9a9a; font-size: 12px;">총 매도</span>
                            <div style="font-size: 16px; font-weight: 700;">
                                ${Number(patterns.total_sell_amount).toLocaleString('ko-KR')}원
                            </div>
                            <div style="font-size: 13px; color: #9a9a9a;">
                                ${Number(patterns.total_sell_count || 0).toLocaleString('ko-KR')}건
                            </div>
                            <div class="pattern-detail" id="detail-sell" style="display: none;"></div>
                        </div>`;
                }
                
                // 원화 입금
                if (patterns.total_deposit_krw >= 0) {
                    html += `
                        <div class="pattern-stat clickable" data-action="deposit_krw">
                            <span style="color: #9a9a9a; font-size: 12px;">원화 입금</span>
                            <div style="font-size: 16px; font-weight: 700;">
                                ${Number(patterns.total_deposit_krw).toLocaleString('ko-KR')}원
                            </div>
                            <div style="font-size: 13px; color: #9a9a9a;">
                                ${Number(patterns.total_deposit_krw_count || 0).toLocaleString('ko-KR')}건
                            </div>
                            <div class="pattern-detail" id="detail-deposit_krw" style="display: none;"></div>
                        </div>`;
                }
                
                // 원화 출금
                if (patterns.total_withdraw_krw >= 0) {
                    html += `
                        <div class="pattern-stat clickable" data-action="withdraw_krw">
                            <span style="color: #9a9a9a; font-size: 12px;">원화 출금</span>
                            <div style="font-size: 16px; font-weight: 700;">
                                ${Number(patterns.total_withdraw_krw).toLocaleString('ko-KR')}원
                            </div>
                            <div style="font-size: 13px; color: #9a9a9a;">
                                ${Number(patterns.total_withdraw_krw_count || 0).toLocaleString('ko-KR')}건
                            </div>
                            <div class="pattern-detail" id="detail-withdraw_krw" style="display: none;"></div>
                        </div>`;
                }
                
                // 가상자산 입금
                if (patterns.total_deposit_crypto >= 0) {
                    html += `
                        <div class="pattern-stat clickable" data-action="deposit_crypto">
                            <span style="color: #9a9a9a; font-size: 12px;">가상자산 입금</span>
                            <div style="font-size: 16px; font-weight: 700;">
                                ${Number(patterns.total_deposit_crypto).toLocaleString('ko-KR')}원
                            </div>
                            <div style="font-size: 13px; color: #9a9a9a;">
                                ${Number(patterns.total_deposit_crypto_count || 0).toLocaleString('ko-KR')}건
                            </div>
                            <div class="pattern-detail" id="detail-deposit_crypto" style="display: none;"></div>
                        </div>`;
                }
                
                // 가상자산 출금
                if (patterns.total_withdraw_crypto >= 0) {
                    html += `
                        <div class="pattern-stat clickable" data-action="withdraw_crypto">
                            <span style="color: #9a9a9a; font-size: 12px;">가상자산 출금</span>
                            <div style="font-size: 16px; font-weight: 700;">
                                ${Number(patterns.total_withdraw_crypto).toLocaleString('ko-KR')}원
                            </div>
                            <div style="font-size: 13px; color: #9a9a9a;">
                                ${Number(patterns.total_withdraw_crypto_count || 0).toLocaleString('ko-KR')}건
                            </div>
                            <div class="pattern-detail" id="detail-withdraw_crypto" style="display: none;"></div>
                        </div>`;
                }
                
                html += `
                    </div>
                </div>`;
                
                // 패턴 상세 데이터 저장
                window.orderbookPatternDetails = patterns;
            }
            
            // 2. 구간별 상세 테이블
            if (analysisResult.summary_data && analysisResult.summary_data.length > 0) {
                html += `
                <div class="orderbook-segments-table">
                    <h4 style="margin: 0 0 10px 0; font-size: 14px;">구간별 상세 내역</h4>
                    <table class="table orderbook-summary-table">
                        <thead>
                            <tr>
                                <th>구간</th>
                                <th>Cat</th>
                                <th>행동</th>
                                <th>시작시간</th>
                                <th>종료시간</th>
                                <th>소요시간</th>
                                <th>건수</th>
                                <th>종목수</th>
                                <th>주요종목</th>
                                <th>총금액(원)</th>
                            </tr>
                        </thead>
                        <tbody>`;
                
                analysisResult.summary_data.forEach((segment, idx) => {
                    const amount = segment['총금액(KRW)'] || 0;
                    const amountFormatted = amount > 0 ? Number(amount).toLocaleString('ko-KR') : '-';
                    const countFormatted = Number(segment['건수'] || 0).toLocaleString('ko-KR');
                    const transCat = segment['trans_cat'] || '-';
                    
                    html += `
                            <tr class="segment-row" data-segment="${idx}">
                                <td>${segment['구간']}</td>
                                <td>${transCat}</td>
                                <td style="font-weight: 600;">${segment['행동']}</td>
                                <td style="font-size: 11px;">${segment['시작시간']}</td>
                                <td style="font-size: 11px;">${segment['종료시간']}</td>
                                <td>${segment['소요시간']}</td>
                                <td>${countFormatted}</td>
                                <td>${segment['종목수']}</td>
                                <td style="font-size: 11px;">${segment['주요종목'] || '-'}</td>
                                <td style="text-align: right; font-weight: 600;">${amountFormatted}</td>
                            </tr>`;
                    
                    // 상세내역이 있으면 숨겨진 행으로 추가
                    if (segment['상세내역']) {
                        html += `
                            <tr class="segment-detail-row" id="segment-detail-${idx}" style="display: none;">
                                <td colspan="10" style="padding: 10px 20px; background: #0a0a0a;">
                                    <pre style="margin: 0; font-size: 11px; color: #bdbdbd;">${segment['상세내역']}</pre>
                                </td>
                            </tr>`;
                    }
                });
                
                html += `
                        </tbody>
                    </table>
                </div>`;
            }
            
            // HTML 삽입
            container.innerHTML = html;
            
            // 이벤트 리스너 추가
            this.attachOrderbookEventListeners();
            
            // 스타일 추가 (한 번만)
            if (!document.getElementById('orderbook-styles')) {
                const style = document.createElement('style');
                style.id = 'orderbook-styles';
                style.textContent = `
                    .orderbook-patterns-summary .pattern-stat {
                        padding: 10px;
                        background: #0a0a0a;
                        border: 1px solid #2a2a2a;
                        border-radius: 8px;
                        text-align: center;
                        position: relative;
                    }
                    
                    .orderbook-patterns-summary .pattern-stat.clickable {
                        cursor: pointer;
                        transition: background 0.2s ease;
                    }
                    
                    .orderbook-patterns-summary .pattern-stat.clickable:hover {
                        background: #151515;
                    }
                    
                    .orderbook-patterns-summary .pattern-stat.expanded {
                        background: #151515;
                    }
                    
                    .pattern-detail {
                        text-align: left;
                        margin-top: 10px;
                        padding-top: 10px;
                        border-top: 1px solid #2a2a2a;
                        font-size: 11px;
                        color: #9a9a9a;
                        max-height: 200px;
                        overflow-y: auto;
                    }
                    
                    .orderbook-summary-table {
                        font-size: 12px;
                    }
                    
                    .orderbook-summary-table th {
                        white-space: nowrap;
                        font-size: 11px;
                    }
                    
                    .orderbook-summary-table .segment-row {
                        cursor: pointer;
                    }
                    
                    .orderbook-summary-table .segment-row:hover {
                        background: #1a1a1a;
                    }
                    
                    .orderbook-summary-table .segment-detail-row {
                        border-top: none;
                    }
                `;
                document.head.appendChild(style);
            }
        }

        // 이벤트 리스너 연결
        attachOrderbookEventListeners() {
            // 구간 행 클릭시 상세내역 토글
            document.querySelectorAll('.segment-row').forEach(row => {
                row.addEventListener('click', function() {
                    const segmentIdx = this.dataset.segment;
                    const detailRow = document.getElementById(`segment-detail-${segmentIdx}`);
                    if (detailRow) {
                        detailRow.style.display = detailRow.style.display === 'none' ? 'table-row' : 'none';
                        this.classList.toggle('expanded');
                    }
                });
            });
            
            // 패턴 카드 클릭시 상세내역 토글
            document.querySelectorAll('.pattern-stat.clickable').forEach(card => {
                card.addEventListener('click', function() {
                    const action = this.dataset.action;
                    const detailDiv = this.querySelector('.pattern-detail');
                    
                    if (!detailDiv) return;
                    
                    if (detailDiv.style.display === 'none') {
                        // 상세 내역 생성
                        let detailHtml = '';
                        const patterns = window.orderbookPatternDetails;
                        
                        if (action === 'buy' && patterns.buy_details) {
                            detailHtml = formatActionDetails(patterns.buy_details);
                        } else if (action === 'sell' && patterns.sell_details) {
                            detailHtml = formatActionDetails(patterns.sell_details);
                        } else if (action === 'deposit_krw' && patterns.deposit_krw_details) {
                            detailHtml = formatActionDetails(patterns.deposit_krw_details);
                        } else if (action === 'withdraw_krw' && patterns.withdraw_krw_details) {
                            detailHtml = formatActionDetails(patterns.withdraw_krw_details);
                        } else if (action === 'deposit_crypto' && patterns.deposit_crypto_details) {
                            detailHtml = formatActionDetails(patterns.deposit_crypto_details);
                        } else if (action === 'withdraw_crypto' && patterns.withdraw_crypto_details) {
                            detailHtml = formatActionDetails(patterns.withdraw_crypto_details);
                        }
                        
                        detailDiv.innerHTML = detailHtml || '<div style="color: #666;">상세 내역이 없습니다.</div>';
                        detailDiv.style.display = 'block';
                        this.classList.add('expanded');
                    } else {
                        detailDiv.style.display = 'none';
                        this.classList.remove('expanded');
                    }
                });
            });
            
            // 종목별 상세 포맷팅 함수
            function formatActionDetails(details) {
                if (!details || details.length === 0) {
                    return '<div style="color: #666;">상세 내역이 없습니다.</div>';
                }
                
                let html = '<div style="line-height: 1.5;">';
                details.forEach(([ticker, data]) => {
                    const quantity = Number(data.quantity || 0).toLocaleString('ko-KR', {maximumFractionDigits: 4});
                    const amount = Number(data.amount_krw || 0).toLocaleString('ko-KR');
                    const count = Number(data.count || 0).toLocaleString('ko-KR');
                    
                    html += `<div style="margin-bottom: 5px;">
                        <strong>${ticker}</strong>: 
                        수량 ${quantity}개, 
                        금액 ${amount}원, 
                        횟수 ${count}건
                    </div>`;
                });
                html += '</div>';
                
                return html;
            }
        }

        // HTML 이스케이프 헬퍼
        escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
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