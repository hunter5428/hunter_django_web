// str_dashboard/static/str_dashboard/js/menu1_1_simple.js

/**
 * ALERT ID 조회 페이지 JavaScript (단순화 버전)
 * - 테이블 컴포넌트 활용
 * - 기존 UX 유지
 * - 불필요한 기능 제거
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
            this.isOpen = false;
            this.init();
        }

        init() {
            // 배경 클릭 시 닫기
            this.modal?.addEventListener('click', (e) => {
                if (e.target === this.modal) {
                    this.close();
                }
            });
        }

        open() {
            if (this.modal) {
                this.modal.style.display = 'flex';
                this.isOpen = true;
            }
        }

        close() {
            if (this.modal) {
                this.modal.style.display = 'none';
                this.isOpen = false;
            }
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
            $('#btn-open-db-modal')?.addEventListener('click', () => {
                this.modal.open();
            });

            $('#btn-close-db-modal')?.addEventListener('click', () => {
                this.modal.close();
            });

            $('#btn-test-conn')?.addEventListener('click', () => {
                this.testConnection();
            });
        }

        async testConnection() {
            const fields = ['host', 'port', 'service_name', 'username', 'password'];
            const data = {};

            // 입력값 검증
            for (const field of fields) {
                const value = $(`#${field}`)?.value?.trim();
                if (!value) {
                    alert(`${field}를 입력해주세요.`);
                    return;
                }
                data[field] = value;
            }

            try {
                const response = await fetch("{% url 'test_oracle_connection' %}", {
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
            this.searchBtn?.addEventListener('click', () => {
                this.search();
            });

            // Enter 키로 검색
            this.inputField?.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.search();
                }
            });
        }

        async search() {
            const alertId = this.inputField?.value?.trim();
            
            if (!alertId) {
                alert('ALERT ID를 입력하세요.');
                return;
            }

            try {
                // 1. ALERT 정보 조회
                const alertResponse = await fetch("{% url 'query_alert_info' %}", {
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

                // 데이터 처리
                const processedData = this.processAlertData(cols, rows, alertId);
                
                // 추가 데이터 조회 및 렌더링
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

            // 대표 RULE 찾기
            if (idxAlert >= 0 && idxRule >= 0) {
                const repRow = rows.find(r => String(r[idxAlert]) === alertId);
                repRuleId = repRow ? String(repRow[idxRule]) : null;
                if (repRow && idxCust >= 0) {
                    custIdForPerson = repRow[idxCust];
                }
            }

            // 고객 ID 확보
            if (!custIdForPerson && rows.length && idxCust >= 0) {
                custIdForPerson = rows[0][idxCust];
            }

            // DISTINCT RULE_ID 추출 (순서 유지)
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

            return {
                cols,
                rows,
                alertId,
                repRuleId,
                custIdForPerson,
                canonicalIds
            };
        }

        async fetchAndRenderAllSections(data) {
            const { cols, rows, alertId, repRuleId, custIdForPerson, canonicalIds } = data;

            // 1. 고객 정보 조회
            if (custIdForPerson) {
                try {
                    const response = await fetch("{% url 'query_person_info' %}", {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'X-CSRFToken': getCookie('csrftoken')
                        },
                        body: new URLSearchParams({ cust_id: String(custIdForPerson) })
                    });
                    
                    const personData = await response.json();
                    if (personData.success) {
                        window.renderPersonInfoSection(personData.columns || [], personData.rows || []);
                    }
                } catch (error) {
                    console.error('Person info fetch failed:', error);
                    window.renderPersonInfoSection([], []);
                }
            } else {
                window.renderPersonInfoSection([], []);
            }

            // 2. RULE 히스토리 조회
            if (canonicalIds.length > 0) {
                const ruleKey = canonicalIds.slice().sort().join(',');
                try {
                    const response = await fetch("{% url 'rule_history_search' %}", {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/x-www-form-urlencoded',
                            'X-CSRFToken': getCookie('csrftoken')
                        },
                        body: new URLSearchParams({ rule_key: ruleKey })
                    });
                    
                    const historyData = await response.json();
                    if (historyData.success) {
                        window.renderRuleHistorySection(historyData.columns || [], historyData.rows || []);
                    }
                } catch (error) {
                    console.error('Rule history fetch failed:', error);
                    window.renderRuleHistorySection([], []);
                }
            }

            // 3. 나머지 섹션 렌더링 (테이블 컴포넌트 활용)
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
        const sections = document.querySelectorAll('.section');
        sections.forEach(section => {
            section.style.display = 'none';
        });

        console.log('Menu1_1 page initialized');
    });

})();