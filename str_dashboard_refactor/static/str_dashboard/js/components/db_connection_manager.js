/**
 * 데이터베이스 연결 관리 컴포넌트
 */
import { apiClient } from '../core/api-client.js';
import { uiManager } from '../core/ui-manager.js';
import { stateManager } from '../core/state-manager.js';

export class DBConnectionManager {
    constructor() {
        this.urls = window.URLS || {};
        this.init();
    }

    init() {
        this.setupEventListeners();
        this.checkInitialStatus();
    }

    setupEventListeners() {
        // 모달 열기/닫기 - 기존 이벤트 유지
        const openBtn = document.getElementById('btn-open-db-modal');
        const closeBtn = document.getElementById('btn-close-db-modal');
        
        if (openBtn) {
            openBtn.addEventListener('click', (e) => {
                e.preventDefault();
                this.openModal();
            });
        }
        
        if (closeBtn) {
            closeBtn.addEventListener('click', (e) => {
                e.preventDefault();
                this.closeModal();
            });
        }
        
        // 개별 테스트 버튼
        const oracleTestBtn = document.getElementById('btn-test-oracle');
        const redshiftTestBtn = document.getElementById('btn-test-redshift');
        
        if (oracleTestBtn) {
            oracleTestBtn.addEventListener('click', (e) => {
                e.preventDefault();
                this.testOracleConnection();
            });
        }
        
        if (redshiftTestBtn) {
            redshiftTestBtn.addEventListener('click', (e) => {
                e.preventDefault();
                this.testRedshiftConnection();
            });
        }
        
        // 모두 연결 버튼
        const connectAllBtn = document.getElementById('btn-connect-all');
        if (connectAllBtn) {
            connectAllBtn.addEventListener('click', (e) => {
                e.preventDefault();
                this.connectAllDatabases();
            });
        }
        
        // 모달 배경 클릭시 닫기
        const modal = document.getElementById('db-modal');
        if (modal) {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) {
                    this.closeModal();
                }
            });
        }
    }

    checkInitialStatus() {
        // 페이지 로드시 상태 확인
        const oracleStatus = document.getElementById('oracle-status')?.classList.contains('ok') || false;
        const redshiftStatus = document.getElementById('redshift-status')?.classList.contains('ok') || false;
        
        stateManager.setDbConnection('oracle', oracleStatus);
        stateManager.setDbConnection('redshift', redshiftStatus);
    }

    openModal() {
        uiManager.openModal('#db-modal');
        this.clearTestResults();
    }

    closeModal() {
        uiManager.closeModal('#db-modal');
    }

    clearTestResults() {
        uiManager.showTestResult('#oracle-test-result', false, '');
        uiManager.showTestResult('#redshift-test-result', false, '');
        document.getElementById('oracle-test-result').style.display = 'none';
        document.getElementById('redshift-test-result').style.display = 'none';
    }

    async testOracleConnection() {
        const fields = ['oracle_host', 'oracle_port', 'oracle_service', 'oracle_username', 'oracle_password'];
        const data = {};
        
        for (const field of fields) {
            const value = uiManager.getInputValue(`#${field}`);
            if (!value && field !== 'oracle_password') {
                uiManager.showError(`Oracle ${field.replace('oracle_', '')}를 입력해주세요.`);
                return;
            }
            const paramName = field.replace('oracle_', '').replace('service', 'service_name');
            data[paramName] = value || '';
        }
        
        const button = document.getElementById('btn-test-oracle');
        
        try {
            button.classList.add('loading');
            button.disabled = true;
            
            const result = await apiClient.post(this.urls.test_oracle_connection, data);
            
            if (result.success) {
                uiManager.showTestResult('#oracle-test-result', true, '연결 성공');
                uiManager.updateStatusBadge('#oracle-status', true, 'Oracle 연결', 'Oracle 미연결');
                stateManager.setDbConnection('oracle', true);
            } else {
                uiManager.showTestResult('#oracle-test-result', false, result.message || '연결 실패');
                stateManager.setDbConnection('oracle', false);
            }
        } catch (error) {
            console.error('Oracle connection test failed:', error);
            uiManager.showTestResult('#oracle-test-result', false, '연결 테스트 실패');
            stateManager.setDbConnection('oracle', false);
        } finally {
            button.classList.remove('loading');
            button.disabled = false;
        }
    }

    async testRedshiftConnection() {
        const fields = ['redshift_host', 'redshift_port', 'redshift_dbname', 'redshift_username', 'redshift_password'];
        const data = {};
        
        for (const field of fields) {
            const value = uiManager.getInputValue(`#${field}`);
            if (!value && field !== 'redshift_password') {
                uiManager.showError(`Redshift ${field.replace('redshift_', '')}를 입력해주세요.`);
                return;
            }
            const paramName = field.replace('redshift_', '');
            data[paramName] = value || '';
        }
        
        const button = document.getElementById('btn-test-redshift');
        
        try {
            button.classList.add('loading');
            button.disabled = true;
            
            const result = await apiClient.post(this.urls.test_redshift_connection, data);
            
            if (result.success) {
                uiManager.showTestResult('#redshift-test-result', true, '연결 성공');
                uiManager.updateStatusBadge('#redshift-status', true, 'Redshift 연결', 'Redshift 미연결');
                stateManager.setDbConnection('redshift', true);
            } else {
                uiManager.showTestResult('#redshift-test-result', false, result.message || '연결 실패');
                stateManager.setDbConnection('redshift', false);
            }
        } catch (error) {
            console.error('Redshift connection test failed:', error);
            uiManager.showTestResult('#redshift-test-result', false, '연결 테스트 실패');
            stateManager.setDbConnection('redshift', false);
        } finally {
            button.classList.remove('loading');
            button.disabled = false;
        }
    }

    async connectAllDatabases() {
        const button = document.getElementById('btn-connect-all');
        button.classList.add('loading');
        button.disabled = true;
        
        // 파라미터 수집
        const allData = {};
        
        // Oracle 파라미터
        ['oracle_host', 'oracle_port', 'oracle_service', 'oracle_username', 'oracle_password'].forEach(field => {
            const paramName = field.replace('service', 'service_name');
            allData[paramName] = uiManager.getInputValue(`#${field}`) || '';
        });
        
        // Redshift 파라미터
        ['redshift_host', 'redshift_port', 'redshift_dbname', 'redshift_username', 'redshift_password'].forEach(field => {
            allData[field] = uiManager.getInputValue(`#${field}`) || '';
        });
        
        try {
            const result = await apiClient.post(this.urls.connect_all_databases, allData);
            
            // 상태 업데이트
            if (result.oracle_status === 'ok') {
                uiManager.showTestResult('#oracle-test-result', true, '연결 성공');
                uiManager.updateStatusBadge('#oracle-status', true, 'Oracle 연결', 'Oracle 미연결');
                stateManager.setDbConnection('oracle', true);
            } else {
                uiManager.showTestResult('#oracle-test-result', false, result.oracle_error || '연결 실패');
            }
            
            if (result.redshift_status === 'ok') {
                uiManager.showTestResult('#redshift-test-result', true, '연결 성공');
                uiManager.updateStatusBadge('#redshift-status', true, 'Redshift 연결', 'Redshift 미연결');
                stateManager.setDbConnection('redshift', true);
            } else {
                uiManager.showTestResult('#redshift-test-result', false, result.redshift_error || '연결 실패');
            }
            
            if (result.success) {
                alert('모든 데이터베이스 연결에 성공했습니다.');
                setTimeout(() => this.closeModal(), 1000);
            } else {
                alert('일부 데이터베이스 연결에 실패했습니다.');
            }
        } catch (error) {
            console.error('Connect all failed:', error);
            alert('연결 중 오류가 발생했습니다.');
        } finally {
            button.classList.remove('loading');
            button.disabled = false;
        }
    }

    isOracleConnected() {
        return stateManager.getState('dbConnections').oracle;
    }

    isRedshiftConnected() {
        return stateManager.getState('dbConnections').redshift;
    }
}