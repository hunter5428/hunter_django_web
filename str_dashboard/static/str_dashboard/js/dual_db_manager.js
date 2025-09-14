// str_dashboard/static/str_dashboard/js/dual_db_manager.js

/**
 * 듀얼 데이터베이스 연결 관리 (Oracle + Redshift)
 */
(function() {
    'use strict';

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
            this.oracleStatus = false;
            this.redshiftStatus = false;
            this.modal = null;
            this.init();
        }

        init() {
            // 모달 관련 이벤트
            $('#btn-open-db-modal')?.addEventListener('click', () => this.openModal());
            $('#btn-close-db-modal')?.addEventListener('click', () => this.closeModal());
            
            // 개별 테스트 버튼
            $('#btn-test-oracle')?.addEventListener('click', () => this.testOracleConnection());
            $('#btn-test-redshift')?.addEventListener('click', () => this.testRedshiftConnection());
            
            // 모두 연결 버튼
            $('#btn-connect-all')?.addEventListener('click', () => this.connectAllDatabases());
            
            // 모달 배경 클릭시 닫기
            this.modal = $('#db-modal');
            this.modal?.addEventListener('click', (e) => {
                if (e.target === this.modal) this.closeModal();
            });
            
            // 초기 상태 확인
            this.checkInitialStatus();
        }

        checkInitialStatus() {
            // 페이지 로드시 상태 배지 확인
            this.oracleStatus = $('#oracle-status')?.classList.contains('ok') || false;
            this.redshiftStatus = $('#redshift-status')?.classList.contains('ok') || false;
        }

        openModal() {
            if (this.modal) {
                this.modal.style.display = 'flex';
                // 기존 연결 상태 결과 초기화
                this.clearTestResults();
            }
        }

        closeModal() {
            if (this.modal) {
                this.modal.style.display = 'none';
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

        /**
         * Redshift 연결 테스트
         */
        async testRedshiftConnection() {
            const fields = ['redshift_host', 'redshift_port', 'redshift_dbname', 'redshift_username', 'redshift_password'];
            const data = {};
            
            // 입력값 검증
            for (const field of fields) {
                const inputId = field;
                const value = $(`#${inputId}`)?.value?.trim();
                
                if (!value && field !== 'redshift_password') {
                    alert(`Redshift ${field.replace('redshift_', '')}를 입력해주세요.`);
                    return;
                }
                
                // 백엔드 파라미터명으로 매핑
                const paramName = field.replace('redshift_', '');
                data[paramName] = value || '';
            }
            
            const button = $('#btn-test-redshift');
            const resultSpan = $('#redshift-test-result');
            
            try {
                button.classList.add('loading');
                button.disabled = true;
                
                const response = await fetch(window.URLS.test_redshift_connection, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams(data)
                });
                
                const result = await response.json();
                
                // 결과 표시
                resultSpan.style.display = 'inline-block';
                if (result.success) {
                    resultSpan.textContent = '✓ 연결 성공';
                    resultSpan.classList.remove('fail');
                    resultSpan.classList.add('success');
                    this.redshiftStatus = true;
                    this.updateRedshiftStatusBadge(true);
                } else {
                    resultSpan.textContent = '✗ ' + (result.message || '연결 실패');
                    resultSpan.classList.remove('success');
                    resultSpan.classList.add('fail');
                    this.redshiftStatus = false;
                }
                
            } catch (error) {
                console.error('Redshift connection test failed:', error);
                resultSpan.style.display = 'inline-block';
                resultSpan.textContent = '✗ 연결 테스트 실패';
                resultSpan.classList.remove('success');
                resultSpan.classList.add('fail');
                this.redshiftStatus = false;
            } finally {
                button.classList.remove('loading');
                button.disabled = false;
            }
        }

        /**
         * 모든 데이터베이스 연결
         */
        async connectAllDatabases() {
            // Oracle 정보
            const oracleData = {
                host: $('#oracle_host')?.value?.trim(),
                port: $('#oracle_port')?.value?.trim(),
                service_name: $('#oracle_service')?.value?.trim(),
                username: $('#oracle_username')?.value?.trim(),
                password: $('#oracle_password')?.value || ''
            };
            
            // Redshift 정보
            const redshiftData = {
                host: $('#redshift_host')?.value?.trim(),
                port: $('#redshift_port')?.value?.trim(),
                dbname: $('#redshift_dbname')?.value?.trim(),
                username: $('#redshift_username')?.value?.trim(),
                password: $('#redshift_password')?.value || ''
            };
            
            // 필수 필드 검증
            const oracleRequired = ['host', 'port', 'service_name', 'username', 'password'];
            const redshiftRequired = ['host', 'port', 'dbname', 'username', 'password'];
            
            for (const field of oracleRequired) {
                if (!oracleData[field]) {
                    alert(`Oracle ${field}를 입력해주세요.`);
                    return;
                }
            }
            
            for (const field of redshiftRequired) {
                if (!redshiftData[field]) {
                    alert(`Redshift ${field}를 입력해주세요.`);
                    return;
                }
            }
            
            const button = $('#btn-connect-all');
            
            try {
                button.classList.add('loading');
                button.disabled = true;
                button.textContent = '연결 중...';
                
                const response = await fetch(window.URLS.connect_all_databases, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({
                        ...Object.entries(oracleData).reduce((acc, [key, val]) => {
                            acc[`oracle_${key}`] = val;
                            return acc;
                        }, {}),
                        ...Object.entries(redshiftData).reduce((acc, [key, val]) => {
                            acc[`redshift_${key}`] = val;
                            return acc;
                        }, {})
                    })
                });
                
                const result = await response.json();
                
                if (result.success) {
                    // 개별 상태 업데이트
                    if (result.oracle_status === 'ok') {
                        this.updateOracleStatusBadge(true);
                        $('#oracle-test-result').textContent = '✓ 연결됨';
                        $('#oracle-test-result').className = 'connection-status success';
                        $('#oracle-test-result').style.display = 'inline-block';
                    }
                    
                    if (result.redshift_status === 'ok') {
                        this.updateRedshiftStatusBadge(true);
                        $('#redshift-test-result').textContent = '✓ 연결됨';
                        $('#redshift-test-result').className = 'connection-status success';
                        $('#redshift-test-result').style.display = 'inline-block';
                    }
                    
                    alert('데이터베이스 연결이 완료되었습니다.');
                    
                    // 3초 후 모달 닫기
                    setTimeout(() => {
                        this.closeModal();
                    }, 2000);
                    
                } else {
                    alert(result.message || '연결에 실패했습니다.');
                    
                    // 실패한 연결 표시
                    if (result.oracle_error) {
                        $('#oracle-test-result').textContent = '✗ ' + result.oracle_error;
                        $('#oracle-test-result').className = 'connection-status fail';
                        $('#oracle-test-result').style.display = 'inline-block';
                    }
                    
                    if (result.redshift_error) {
                        $('#redshift-test-result').textContent = '✗ ' + result.redshift_error;
                        $('#redshift-test-result').className = 'connection-status fail';
                        $('#redshift-test-result').style.display = 'inline-block';
                    }
                }
                
            } catch (error) {
                console.error('Database connection failed:', error);
                alert('연결 중 오류가 발생했습니다.');
            } finally {
                button.classList.remove('loading');
                button.disabled = false;
                button.textContent = '모두 연결';
            }
        }

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

        /**
         * 연결 상태 확인
         */
        isOracleConnected() {
            return $('#oracle-status')?.classList.contains('ok') || false;
        }

        isRedshiftConnected() {
            return $('#redshift-status')?.classList.contains('ok') || false;
        }

        isBothConnected() {
            return this.isOracleConnected() && this.isRedshiftConnected();
        }
    }

    // 전역 인스턴스 생성
    document.addEventListener('DOMContentLoaded', function() {
        window.dualDBManager = new DualDBConnectionManager();
        console.log('Dual DB Connection Manager initialized');
    });

})();