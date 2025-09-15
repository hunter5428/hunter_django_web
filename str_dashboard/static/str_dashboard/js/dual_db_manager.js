// str_dashboard/static/str_dashboard/js/dual_db_manager.js

/**
 * 듀얼 데이터베이스 연결 관리 (Oracle + Redshift)
 */
(function() {
    'use strict';

    console.log('dual_db_manager.js loading...'); // 디버깅용

    // 유틸리티
    const $ = (sel) => document.querySelector(sel);
    
    const getCookie = (name) => {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        return parts.length === 2 ? decodeURIComponent(parts.pop().split(';').shift()) : undefined;
    };

    /**
     * 듀얼 DB 연결 관리자
     */
    class DualDBConnectionManager {
        constructor() {
            console.log('DualDBConnectionManager constructor called'); // 디버깅용
            this.oracleStatus = false;
            this.redshiftStatus = false;
            this.modal = null;
            this.init();
        }

        init() {
            console.log('DualDBConnectionManager init called'); // 디버깅용
            
            // 요소 확인
            const openBtn = $('#btn-open-db-modal');
            const closeBtn = $('#btn-close-db-modal');
            const modal = $('#db-modal');
            
            console.log('Elements found:', {
                openBtn: !!openBtn,
                closeBtn: !!closeBtn,
                modal: !!modal
            });
            
            // 모달 관련 이벤트
            if (openBtn) {
                openBtn.addEventListener('click', (e) => {
                    console.log('Open modal button clicked');
                    e.preventDefault();
                    this.openModal();
                });
            } else {
                console.error('btn-open-db-modal not found!');
            }
            
            if (closeBtn) {
                closeBtn.addEventListener('click', (e) => {
                    console.log('Close modal button clicked');
                    e.preventDefault();
                    this.closeModal();
                });
            }
            
            // 개별 테스트 버튼
            const oracleTestBtn = $('#btn-test-oracle');
            const redshiftTestBtn = $('#btn-test-redshift');
            
            if (oracleTestBtn) {
                oracleTestBtn.addEventListener('click', (e) => {
                    console.log('Oracle test button clicked');
                    e.preventDefault();
                    this.testOracleConnection();
                });
            }
            
            if (redshiftTestBtn) {
                redshiftTestBtn.addEventListener('click', (e) => {
                    console.log('Redshift test button clicked');
                    e.preventDefault();
                    this.testRedshiftConnection();
                });
            }
            
            // 모두 연결 버튼
            const connectAllBtn = $('#btn-connect-all');
            if (connectAllBtn) {
                connectAllBtn.addEventListener('click', (e) => {
                    console.log('Connect all button clicked');
                    e.preventDefault();
                    this.connectAllDatabases();
                });
            }
            
            // 모달 배경 클릭시 닫기
            this.modal = modal;
            if (this.modal) {
                this.modal.addEventListener('click', (e) => {
                    if (e.target === this.modal) {
                        console.log('Modal backdrop clicked');
                        this.closeModal();
                    }
                });
            }
            
            // 초기 상태 확인
            this.checkInitialStatus();
        }

        checkInitialStatus() {
            // 페이지 로드시 상태 배지 확인
            this.oracleStatus = $('#oracle-status')?.classList.contains('ok') || false;
            this.redshiftStatus = $('#redshift-status')?.classList.contains('ok') || false;
            console.log('Initial status:', {
                oracle: this.oracleStatus,
                redshift: this.redshiftStatus
            });
        }

        openModal() {
            console.log('Opening modal...');
            if (this.modal) {
                this.modal.style.display = 'flex';
                this.modal.classList.add('show');
                // 기존 연결 상태 결과 초기화
                this.clearTestResults();
            } else {
                console.error('Modal element not found!');
            }
        }

        closeModal() {
            console.log('Closing modal...');
            if (this.modal) {
                this.modal.style.display = 'none';
                this.modal.classList.remove('show');
            }
        }

        clearTestResults() {
            const oracleResult = $('#oracle-test-result');
            const redshiftResult = $('#redshift-test-result');
            
            if (oracleResult) {
                oracleResult.style.display = 'none';
                oracleResult.classList.remove('success', 'fail');
            }
            
            if (redshiftResult) {
                redshiftResult.style.display = 'none';
                redshiftResult.classList.remove('success', 'fail');
            }
        }

        /**
         * Oracle 연결 테스트
         */
        async testOracleConnection() {
            console.log('Testing Oracle connection...');
            
            const fields = ['oracle_host', 'oracle_port', 'oracle_service', 'oracle_username', 'oracle_password'];
            const data = {};
            
            // 입력값 검증
            for (const field of fields) {
                const inputId = field;
                const value = $(`#${inputId}`)?.value?.trim();
                
                if (!value && field !== 'oracle_password') {
                    alert(`Oracle ${field.replace('oracle_', '')}를 입력해주세요.`);
                    return;
                }
                
                // 백엔드 파라미터명으로 매핑
                const paramName = field.replace('oracle_', '').replace('service', 'service_name');
                data[paramName] = value || '';
            }
            
            console.log('Oracle connection data:', data);
            
            const button = $('#btn-test-oracle');
            const resultSpan = $('#oracle-test-result');
            
            try {
                button.classList.add('loading');
                button.disabled = true;
                
                const response = await fetch(window.URLS.test_oracle_connection, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams(data)
                });
                
                const result = await response.json();
                console.log('Oracle test result:', result);
                
                // 결과 표시
                resultSpan.style.display = 'inline-block';
                if (result.success) {
                    resultSpan.textContent = '✓ 연결 성공';
                    resultSpan.classList.remove('fail');
                    resultSpan.classList.add('success');
                    this.oracleStatus = true;
                    this.updateOracleStatusBadge(true);
                } else {
                    resultSpan.textContent = '✗ ' + (result.message || '연결 실패');
                    resultSpan.classList.remove('success');
                    resultSpan.classList.add('fail');
                    this.oracleStatus = false;
                }
                
            } catch (error) {
                console.error('Oracle connection test failed:', error);
                resultSpan.style.display = 'inline-block';
                resultSpan.textContent = '✗ 연결 테스트 실패';
                resultSpan.classList.remove('success');
                resultSpan.classList.add('fail');
                this.oracleStatus = false;
            } finally {
                button.classList.remove('loading');
                button.disabled = false;
            }
        }

        // ... 나머지 메서드들은 동일 ...

        /**
         * Oracle 상태 배지 업데이트
         */
        updateOracleStatusBadge(isConnected) {
            const badge = $('#oracle-status');
            if (badge) {
                if (isConnected) {
                    badge.textContent = 'Oracle 연결';
                    badge.classList.add('ok');
                } else {
                    badge.textContent = 'Oracle 미연결';
                    badge.classList.remove('ok');
                }
            }
        }

        /**
         * Redshift 상태 배지 업데이트
         */
        updateRedshiftStatusBadge(isConnected) {
            const badge = $('#redshift-status');
            if (badge) {
                if (isConnected) {
                    badge.textContent = 'Redshift 연결';
                    badge.classList.add('ok');
                } else {
                    badge.textContent = 'Redshift 미연결';
                    badge.classList.remove('ok');
                }
            }
        }
    }

    // DOMContentLoaded 이벤트 대기
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', function() {
            console.log('DOM loaded, initializing DualDBConnectionManager...');
            window.dualDBManager = new DualDBConnectionManager();
            console.log('Dual DB Connection Manager initialized');
        });
    } else {
        // 이미 DOM이 로드된 경우
        console.log('DOM already loaded, initializing DualDBConnectionManager...');
        window.dualDBManager = new DualDBConnectionManager();
        console.log('Dual DB Connection Manager initialized');
    }

})();