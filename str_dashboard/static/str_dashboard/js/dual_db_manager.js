// str_dashboard/static/str_dashboard/js/dual_db_manager.js

class DualDBManager {
    constructor() {
        this.modal = document.getElementById('db-modal');
        this.oracleStatus = document.getElementById('oracle-status');
        this.redshiftStatus = document.getElementById('redshift-status');
        this.init();
    }
    
    init() {
        // 모달 열기
        document.getElementById('btn-open-db-modal').addEventListener('click', () => {
            this.openModal();
        });
        
        // 모달 닫기
        document.getElementById('btn-close-db-modal').addEventListener('click', () => {
            this.closeModal();
        });
        
        // Oracle 연결 테스트
        document.getElementById('btn-test-oracle').addEventListener('click', () => {
            this.testOracleConnection();
        });
        
        // Redshift 연결 테스트
        document.getElementById('btn-test-redshift').addEventListener('click', () => {
            this.testRedshiftConnection();
        });
        
        // 모두 연결
        document.getElementById('btn-connect-all').addEventListener('click', () => {
            this.connectAll();
        });
        
        // ESC 키로 모달 닫기
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape' && this.modal.style.display === 'flex') {
                this.closeModal();
            }
        });
    }
    
    openModal() {
        this.modal.style.display = 'flex';
        // 기존 연결 상태 표시 초기화
        document.getElementById('oracle-test-result').style.display = 'none';
        document.getElementById('redshift-test-result').style.display = 'none';
    }
    
    closeModal() {
        this.modal.style.display = 'none';
    }
    
    async testOracleConnection() {
        const btn = document.getElementById('btn-test-oracle');
        const result = document.getElementById('oracle-test-result');
        
        btn.disabled = true;
        btn.textContent = '연결 테스트 중...';
        
        const formData = new FormData();
        formData.append('host', document.getElementById('oracle_host').value);
        formData.append('port', document.getElementById('oracle_port').value);
        formData.append('service_name', document.getElementById('oracle_service').value);
        formData.append('username', document.getElementById('oracle_username').value);
        formData.append('password', document.getElementById('oracle_password').value);
        formData.append('csrfmiddlewaretoken', window.APP_CONFIG.csrfToken);
        
        try {
            const response = await fetch(window.URLS.test_oracle_connection, {
                method: 'POST',
                body: formData
            });
            
            const data = await response.json();
            
            result.style.display = 'inline-block';
            if (data.success) {
                result.className = 'connection-status success';
                result.textContent = '연결 성공';
                this.updateOracleStatus(true);
            } else {
                result.className = 'connection-status fail';
                result.textContent = data.message || '연결 실패';
                this.updateOracleStatus(false);
            }
        } catch (error) {
            result.style.display = 'inline-block';
            result.className = 'connection-status fail';
            result.textContent = '연결 테스트 오류';
            console.error('Oracle connection test error:', error);
        } finally {
            btn.disabled = false;
            btn.textContent = 'Oracle 연결 테스트';
        }
    }
    
    async testRedshiftConnection() {
        const btn = document.getElementById('btn-test-redshift');
        const result = document.getElementById('redshift-test-result');
        
        btn.disabled = true;
        btn.textContent = '연결 테스트 중...';
        
        const formData = new FormData();
        formData.append('host', document.getElementById('redshift_host').value);
        formData.append('port', document.getElementById('redshift_port').value);
        formData.append('dbname', document.getElementById('redshift_dbname').value);
        formData.append('username', document.getElementById('redshift_username').value);
        formData.append('password', document.getElementById('redshift_password').value);
        formData.append('csrfmiddlewaretoken', window.APP_CONFIG.csrfToken);
        
        try {
            const response = await fetch(window.URLS.test_redshift_connection, {
                method: 'POST',
                body: formData
            });
            
            const data = await response.json();
            
            result.style.display = 'inline-block';
            if (data.success) {
                result.className = 'connection-status success';
                result.textContent = '연결 성공';
                this.updateRedshiftStatus(true);
            } else {
                result.className = 'connection-status fail';
                result.textContent = data.message || '연결 실패';
                this.updateRedshiftStatus(false);
            }
        } catch (error) {
            result.style.display = 'inline-block';
            result.className = 'connection-status fail';
            result.textContent = '연결 테스트 오류';
            console.error('Redshift connection test error:', error);
        } finally {
            btn.disabled = false;
            btn.textContent = 'Redshift 연결 테스트';
        }
    }
    
    async connectAll() {
        const btn = document.getElementById('btn-connect-all');
        btn.disabled = true;
        btn.textContent = '연결 중...';
        
        const formData = new FormData();
        // Oracle 파라미터
        formData.append('oracle_host', document.getElementById('oracle_host').value);
        formData.append('oracle_port', document.getElementById('oracle_port').value);
        formData.append('oracle_service_name', document.getElementById('oracle_service').value);
        formData.append('oracle_username', document.getElementById('oracle_username').value);
        formData.append('oracle_password', document.getElementById('oracle_password').value);
        
        // Redshift 파라미터
        formData.append('redshift_host', document.getElementById('redshift_host').value);
        formData.append('redshift_port', document.getElementById('redshift_port').value);
        formData.append('redshift_dbname', document.getElementById('redshift_dbname').value);
        formData.append('redshift_username', document.getElementById('redshift_username').value);
        formData.append('redshift_password', document.getElementById('redshift_password').value);
        
        formData.append('csrfmiddlewaretoken', window.APP_CONFIG.csrfToken);
        
        try {
            const response = await fetch(window.URLS.connect_all_databases, {
                method: 'POST',
                body: formData
            });
            
            const data = await response.json();
            
            // Oracle 상태 업데이트
            const oracleResult = document.getElementById('oracle-test-result');
            oracleResult.style.display = 'inline-block';
            if (data.oracle_status === 'ok') {
                oracleResult.className = 'connection-status success';
                oracleResult.textContent = '연결 성공';
                this.updateOracleStatus(true);
            } else {
                oracleResult.className = 'connection-status fail';
                oracleResult.textContent = data.oracle_error || '연결 실패';
                this.updateOracleStatus(false);
            }
            
            // Redshift 상태 업데이트
            const redshiftResult = document.getElementById('redshift-test-result');
            redshiftResult.style.display = 'inline-block';
            if (data.redshift_status === 'ok') {
                redshiftResult.className = 'connection-status success';
                redshiftResult.textContent = '연결 성공';
                this.updateRedshiftStatus(true);
            } else {
                redshiftResult.className = 'connection-status fail';
                redshiftResult.textContent = data.redshift_error || '연결 실패';
                this.updateRedshiftStatus(false);
            }
            
            // 모두 성공시 알림 및 모달 닫기
            if (data.success) {
                setTimeout(() => {
                    alert('Oracle과 Redshift 모두 연결되었습니다.');
                    this.closeModal();
                }, 500);
            }
            
        } catch (error) {
            console.error('Connect all error:', error);
            alert('연결 중 오류가 발생했습니다.');
        } finally {
            btn.disabled = false;
            btn.textContent = '모두 연결';
        }
    }
    
    updateOracleStatus(connected) {
        if (this.oracleStatus) {
            if (connected) {
                this.oracleStatus.classList.add('ok');
                this.oracleStatus.textContent = 'Oracle 연결';
                window.DB_STATUS.oracle = 'ok';
            } else {
                this.oracleStatus.classList.remove('ok');
                this.oracleStatus.textContent = 'Oracle 미연결';
                window.DB_STATUS.oracle = 'need';
            }
        }
    }
    
    updateRedshiftStatus(connected) {
        if (this.redshiftStatus) {
            if (connected) {
                this.redshiftStatus.classList.add('ok');
                this.redshiftStatus.textContent = 'Redshift 연결';
                window.DB_STATUS.redshift = 'ok';
            } else {
                this.redshiftStatus.classList.remove('ok');
                this.redshiftStatus.textContent = 'Redshift 미연결';
                window.DB_STATUS.redshift = 'need';
            }
        }
    }
}

// 페이지 로드 시 초기화
document.addEventListener('DOMContentLoaded', () => {
    window.dbManager = new DualDBManager();
});