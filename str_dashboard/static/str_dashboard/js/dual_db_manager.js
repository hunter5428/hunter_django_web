// str_dashboard/static/str_dashboard/js/dual_db_manager.js
// 듀얼 데이터베이스 연결 관리 - 에러 수정 버전

(function() {
    'use strict';

    const $ = (sel) => document.querySelector(sel);
    const $$ = (sel) => document.querySelectorAll(sel);  // querySelectorAll 추가
    
    const getCookie = (name) => {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        return parts.length === 2 ? decodeURIComponent(parts.pop().split(';').shift()) : undefined;
    };

    class DualDBConnectionManager {
        constructor() {
            this.oracleStatus = false;
            this.redshiftStatus = false;
            this.modal = $('#db-modal');
            this.init();
        }

        init() {
            // 이벤트 바인딩
            $('#btn-open-db-modal')?.addEventListener('click', e => {
                e.preventDefault();
                this.openModal();
            });

            $('#btn-close-db-modal')?.addEventListener('click', e => {
                e.preventDefault();
                this.closeModal();
            });

            $('#btn-test-oracle')?.addEventListener('click', e => {
                e.preventDefault();
                this.testConnection('oracle');
            });

            $('#btn-test-redshift')?.addEventListener('click', e => {
                e.preventDefault();
                this.testConnection('redshift');
            });

            $('#btn-connect-all')?.addEventListener('click', e => {
                e.preventDefault();
                this.connectAll();
            });

            // 모달 배경 클릭
            this.modal?.addEventListener('click', e => {
                if (e.target === this.modal) this.closeModal();
            });

            // 초기 상태
            this.oracleStatus = $('#oracle-status')?.classList.contains('ok') || false;
            this.redshiftStatus = $('#redshift-status')?.classList.contains('ok') || false;
        }

        openModal() {
            if (this.modal) {
                this.modal.style.display = 'flex';
                this.modal.classList.add('show');
                // querySelectorAll을 사용하여 모든 .connection-status 요소 선택
                const statusElements = $$('.connection-status');
                statusElements.forEach(el => el.style.display = 'none');
            }
        }

        closeModal() {
            if (this.modal) {
                this.modal.style.display = 'none';
                this.modal.classList.remove('show');
            }
        }

        async testConnection(type) {
            const config = type === 'oracle' ? 
                {
                    fields: ['host', 'port', 'service', 'username', 'password'],
                    url: window.URLS.test_oracle_connection,
                    mapField: (f) => f === 'service' ? 'service_name' : f
                } : 
                {
                    fields: ['host', 'port', 'dbname', 'username', 'password'],
                    url: window.URLS.test_redshift_connection,
                    mapField: (f) => f
                };

            const data = {};
            const prefix = type;

            for (const field of config.fields) {
                const value = $(`#${prefix}_${field}`)?.value?.trim();
                if (!value && field !== 'password') {
                    alert(`${type} ${field}를 입력해주세요.`);
                    return;
                }
                data[config.mapField(field)] = value || '';
            }

            const button = $(`#btn-test-${type}`);
            const resultSpan = $(`#${type}-test-result`);

            try {
                button.disabled = true;
                const response = await fetch(config.url, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams(data)
                });

                const result = await response.json();
                this.showResult(resultSpan, result.success, result.message);
                this[`${type}Status`] = result.success;
                this.updateBadge(type, result.success);

            } catch (error) {
                this.showResult(resultSpan, false, '연결 테스트 실패');
            } finally {
                button.disabled = false;
            }
        }

        async connectAll() {
            const button = $('#btn-connect-all');
            button.disabled = true;

            const params = {};
            ['oracle', 'redshift'].forEach(db => {
                const fields = db === 'oracle' ? 
                    ['host', 'port', 'service_name', 'username', 'password'] :
                    ['host', 'port', 'dbname', 'username', 'password'];
                
                fields.forEach(field => {
                    const inputField = field === 'service_name' ? 'service' : field;
                    params[`${db}_${field}`] = $(`#${db}_${inputField}`)?.value?.trim() || '';
                });
            });

            try {
                const response = await fetch(window.URLS.connect_all_databases, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams(params)
                });

                const result = await response.json();
                
                ['oracle', 'redshift'].forEach(db => {
                    const status = result[`${db}_status`] === 'ok';
                    this.showResult($(`#${db}-test-result`), status, result[`${db}_error`]);
                    this[`${db}Status`] = status;
                    this.updateBadge(db, status);
                });

                if (result.success) {
                    alert('모든 데이터베이스 연결에 성공했습니다.');
                    setTimeout(() => this.closeModal(), 1000);
                } else {
                    alert('일부 데이터베이스 연결에 실패했습니다.');
                }

            } catch (error) {
                alert('연결 중 오류가 발생했습니다.');
            } finally {
                button.disabled = false;
            }
        }

        showResult(element, success, message) {
            if (!element) return;
            element.style.display = 'inline-block';
            element.textContent = success ? '✓ 연결 성공' : `✗ ${message || '연결 실패'}`;
            element.className = `connection-status ${success ? 'success' : 'fail'}`;
        }

        updateBadge(type, connected) {
            const badge = $(`#${type}-status`);
            if (badge) {
                badge.textContent = `${type === 'oracle' ? 'Oracle' : 'Redshift'} ${connected ? '연결' : '미연결'}`;
                badge.classList.toggle('ok', connected);
            }
        }

        isOracleConnected() { return this.oracleStatus; }
        isRedshiftConnected() { return this.redshiftStatus; }
    }

    // 초기화
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            window.dualDBManager = new DualDBConnectionManager();
        });
    } else {
        window.dualDBManager = new DualDBConnectionManager();
    }

})();