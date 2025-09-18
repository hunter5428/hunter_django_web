// str_dashboard/static/str_dashboard/js/menu1_1.js
// ALERT ID 조회 페이지 - DB 연결 상태 확인 기능 추가

(function() {
    'use strict';

    // ==================== 유틸리티 ====================
    const $ = (sel) => document.querySelector(sel);
    
    const getCookie = (name) => {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        return parts.length === 2 ? decodeURIComponent(parts.pop().split(';').shift()) : undefined;
    };

    // ==================== TOML 저장 관리 ====================
    class TomlExportManager {
        constructor() {
            this.init();
        }

        init() {
            const tomlBtn = $('#toml_save_btn');
            if (tomlBtn) {
                tomlBtn.addEventListener('click', () => this.showConfigModal());
            }
            
            this.setupModalEvents();
        }

        setupModalEvents() {
            const modal = $('#toml-config-modal');
            if (!modal) return;
            
            const cancelBtn = modal.querySelector('.toml-cancel-btn');
            if (cancelBtn) {
                cancelBtn.addEventListener('click', () => this.closeModal());
            }
            
            const downloadBtn = modal.querySelector('.toml-download-btn');
            if (downloadBtn) {
                downloadBtn.addEventListener('click', () => this.downloadToml());
            }
            
            // 모달 외부 클릭시 닫기
            modal.addEventListener('click', (e) => {
                if (e.target === modal) this.closeModal();
            });
            
            // ESC 키로 닫기
            document.addEventListener('keydown', (e) => {
                if (e.key === 'Escape' && modal.classList.contains('show')) {
                    this.closeModal();
                }
            });
        }

        showConfigModal() {
            const modal = $('#toml-config-modal');
            if (modal) {
                modal.style.display = 'flex';
                modal.classList.add('show');
            }
        }

        closeModal() {
            const modal = $('#toml-config-modal');
            if (modal) {
                modal.style.display = 'none';
                modal.classList.remove('show');
            }
        }

        async downloadToml() {
            const downloadBtn = $('.toml-download-btn');
            const originalText = downloadBtn ? downloadBtn.textContent : '';
            
            if (downloadBtn) {
                downloadBtn.disabled = true;
                downloadBtn.textContent = '처리 중...';
            }
            
            try {
                // TOML 데이터 준비
                const response = await fetch(window.URLS.prepare_toml_data, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    }
                });
                
                const result = await response.json();
                
                if (result.success) {
                    // 파일 다운로드
                    window.location.href = window.URLS.download_toml;
                    
                    setTimeout(() => {
                        this.closeModal();
                        console.log('✅ TOML 파일이 다운로드되었습니다.');
                    }, 1000);
                } else {
                    alert('TOML 데이터 준비 실패: ' + (result.message || '알 수 없는 오류'));
                }
            } catch (error) {
                console.error('TOML export error:', error);
                alert('TOML 저장 중 오류가 발생했습니다.');
            } finally {
                if (downloadBtn) {
                    downloadBtn.disabled = false;
                    downloadBtn.textContent = originalText;
                }
            }
        }
    }

    // ==================== ALERT 검색 매니저 (통합 API 사용) ====================
    class AlertSearchManager {
        constructor() {
            this.searchBtn = $('#alert_id_search_btn');
            this.inputField = $('#alert_id_input');
            this.isSearching = false;
            this.currentTimerId = null;  // 현재 타이머 ID 저장
            this.init();
        }

        init() {
            this.searchBtn?.addEventListener('click', () => this.search());
            this.inputField?.addEventListener('keypress', (e) => {
                if (e.key === 'Enter' && !this.isSearching) {
                    this.search();
                }
            });
        }

        checkDatabaseConnection() {
            // 전역 DB 상태 확인
            const oracleStatus = window.DB_STATUS?.oracle || 'need';
            const redshiftStatus = window.DB_STATUS?.redshift || 'need';
            
            // Oracle 상태 요소 확인
            const oracleElement = $('#oracle-status');
            const redshiftElement = $('#redshift-status');
            
            // DOM 요소의 상태도 확인
            const isOracleConnected = oracleStatus === 'ok' || 
                                     (oracleElement && oracleElement.classList.contains('ok'));
            const isRedshiftConnected = redshiftStatus === 'ok' || 
                                       (redshiftElement && redshiftElement.classList.contains('ok'));
            
            return {
                oracle: isOracleConnected,
                redshift: isRedshiftConnected
            };
        }

        async search() {
            if (this.isSearching) {
                console.log('⚠️ 이미 조회가 진행 중입니다.');
                return;
            }

            const alertId = this.inputField?.value?.trim();
            if (!alertId) {
                alert('ALERT ID를 입력하세요.');
                this.inputField?.focus();
                return;
            }

            // DB 연결 상태 확인
            const dbStatus = this.checkDatabaseConnection();
            
            if (!dbStatus.oracle) {
                alert('Oracle 데이터베이스 연결이 필요합니다.\nDB Connection 버튼을 클릭하여 연결하세요.');
                $('#btn-open-db-modal')?.click();
                return;
            }
            
            if (!dbStatus.redshift) {
                const proceed = confirm(
                    'Redshift가 연결되지 않았습니다.\n' +
                    'Orderbook 데이터 없이 계속하시겠습니까?\n\n' +
                    '취소를 누르면 DB 연결 창이 열립니다.'
                );
                if (!proceed) {
                    $('#btn-open-db-modal')?.click();
                    return;
                }
            }

            this.isSearching = true;
            this.setLoading(true);

            // 이전 타이머가 있으면 종료
            if (this.currentTimerId) {
                try {
                    console.timeEnd(this.currentTimerId);
                } catch(e) {
                    // 타이머가 이미 종료된 경우 무시
                }
            }

            // 고유한 타이머 ID 생성
            this.currentTimerId = `query_${alertId}_${Date.now()}`;
            
            console.group(`%c🔍 ALERT ID: ${alertId} 통합 조회 시작`, 'color: #4fc3f7; font-size: 16px; font-weight: bold;');
            console.time(this.currentTimerId);

            try {
                // 이전 결과 숨기기
                this.hideResults();
                
                // 통합 API 호출 - 모든 데이터를 한번에 조회
                const response = await fetch(window.URLS.query_all_integrated, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken'),
                        'X-Requested-With': 'XMLHttpRequest'
                    },
                    body: new URLSearchParams({ alert_id: alertId })
                });
                
                if (!response.ok) {
                    throw new Error(`HTTP error! status: ${response.status}`);
                }
                
                const result = await response.json();
                
                if (!result.success) {
                    throw new Error(result.message || '조회 실패');
                }
                
                // DataFrame Manager 상태 조회
                const statusResponse = await fetch(window.URLS.df_manager_status, {
                    headers: { 
                        'X-CSRFToken': getCookie('csrftoken'),
                        'X-Requested-With': 'XMLHttpRequest'
                    }
                });
                const statusData = await statusResponse.json();
                
                // 조회 완료 메시지 표시
                this.showCompleteMessage(alertId, result);
                
                // TOML 저장 버튼 표시
                const tomlBtn = $('#toml_save_btn');
                if (tomlBtn) {
                    tomlBtn.style.display = 'inline-flex';
                    // 애니메이션 효과
                    tomlBtn.classList.add('fade-in');
                }
                
                console.timeEnd(this.currentTimerId);
                console.log('%c✅ 통합 데이터 조회 완료', 'color: #4caf50; font-size: 14px; font-weight: bold;');
                
                // 요약 정보 출력
                if (result.summary) {
                    console.log('%c📊 조회 요약:', 'color: #ffa726; font-size: 14px; font-weight: bold;');
                    console.table({
                        'ALERT ID': result.alert_id || alertId,
                        '데이터셋 수': result.dataset_count || 0,
                        '고객 ID': result.summary.metadata?.cust_id || 'N/A',
                        'Rule IDs': result.summary.metadata?.canonical_ids?.join(', ') || 'N/A',
                        '거래 기간': `${result.summary.metadata?.tran_start || 'N/A'} ~ ${result.summary.metadata?.tran_end || 'N/A'}`
                    });
                }
                
                console.log('%c📂 DataFrame Manager 상태:', 'color: #29b6f6; font-size: 14px; font-weight: bold;');
                console.log(statusData);
                
                // 전역 변수로 노출 (개발자 콘솔 접근용)
                window.DF_MANAGER_STATUS = statusData;
                window.LAST_QUERY_RESULT = result;
                
                // 사용 가능한 데이터셋 목록 표시
                if (statusData.datasets_list && statusData.datasets_list.length > 0) {
                    console.log('%c📁 사용 가능한 데이터셋:', 'color: #29b6f6; font-weight: bold;');
                    statusData.datasets_list.forEach(name => {
                        const datasetInfo = statusData.summary?.datasets?.[name];
                        if (datasetInfo) {
                            console.log(`  - ${name}: ${datasetInfo.shape?.[0] || 0}행 × ${datasetInfo.shape?.[1] || 0}열`);
                        } else {
                            console.log(`  - ${name}`);
                        }
                    });
                }
                
                console.log('%c💡 콘솔 명령어:', 'color: #29b6f6; font-style: italic;');
                console.log('  window.DF_MANAGER_STATUS - 전체 상태 조회');
                console.log('  window.LAST_QUERY_RESULT - 마지막 조회 결과');
                console.log('  window.downloadDataset("dataset_name") - CSV 다운로드');
                console.log('  window.refreshStatus() - 상태 새로고침');
                
            } catch (error) {
                console.error('❌ Alert search error:', error);
                alert(error.message || '조회 중 오류가 발생했습니다.');
            } finally {
                console.groupEnd();
                this.isSearching = false;
                this.setLoading(false);
                this.currentTimerId = null;  // 타이머 ID 초기화
            }
        }

        setLoading(isLoading) {
            if (this.searchBtn) {
                this.searchBtn.disabled = isLoading;
                this.searchBtn.textContent = isLoading ? '조회 중...' : '조회';
                
                if (isLoading) {
                    this.searchBtn.classList.add('loading');
                } else {
                    this.searchBtn.classList.remove('loading');
                }
            }
        }

        hideResults() {
            const container = $('#query-result-container');
            if (container) {
                container.style.display = 'none';
            }
            
            const tomlBtn = $('#toml_save_btn');
            if (tomlBtn) {
                tomlBtn.style.display = 'none';
                tomlBtn.classList.remove('fade-in');
            }
        }

        showCompleteMessage(alertId, result) {
            const container = $('#query-result-container');
            const text = $('#query-complete-text');
            
            if (container && text) {
                const datasetCount = result.dataset_count || 0;
                const message = result.message || `데이터 조회가 완료되었습니다.`;
                
                text.innerHTML = `
                    <strong>ALERT ID: ${alertId}</strong><br>
                    <span style="font-size: 13px;">${message}</span><br>
                    <span style="font-size: 12px; color: #888;">총 ${datasetCount}개 데이터셋 조회됨</span>
                `;
                
                container.style.display = 'block';
                container.classList.add('fade-in');
            }
        }
    }

    // ==================== 개발자 콘솔 헬퍼 함수 ====================
    window.downloadDataset = function(datasetName) {
        if (!datasetName) {
            console.error('Dataset name is required');
            console.log('Available datasets:', window.DF_MANAGER_STATUS?.datasets_list || []);
            return;
        }
        
        console.log(`📥 Downloading ${datasetName}...`);
        
        fetch(`${window.URLS.export_dataframe_csv}?dataset=${datasetName}`, {
            headers: { 
                'X-CSRFToken': getCookie('csrftoken'),
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`Download failed: ${response.status}`);
            }
            return response.blob();
        })
        .then(blob => {
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `${datasetName}_${new Date().toISOString().slice(0,10)}.csv`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
            console.log(`✅ ${datasetName} downloaded successfully`);
        })
        .catch(error => {
            console.error('❌ Download failed:', error);
        });
    };
    
    window.refreshStatus = function() {
        console.log('🔄 Refreshing DataFrame Manager status...');
        
        fetch(window.URLS.df_manager_status, {
            headers: { 
                'X-CSRFToken': getCookie('csrftoken'),
                'X-Requested-With': 'XMLHttpRequest'
            }
        })
        .then(response => {
            if (!response.ok) {
                throw new Error(`Status refresh failed: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            window.DF_MANAGER_STATUS = data;
            console.log('✅ Status refreshed successfully');
            console.log('📊 Current status:', data);
            
            if (data.datasets_list && data.datasets_list.length > 0) {
                console.log('📁 Available datasets:', data.datasets_list);
            }
        })
        .catch(error => {
            console.error('❌ Failed to refresh status:', error);
        });
    };

    // ==================== 초기화 ====================
    document.addEventListener('DOMContentLoaded', function() {
        // 매니저 인스턴스 생성
        window.alertManager = new AlertSearchManager();
        window.tomlExporter = new TomlExportManager();
        
        // CSS 애니메이션 클래스 추가
        const style = document.createElement('style');
        style.textContent = `
            .fade-in {
                animation: fadeIn 0.3s ease-in;
            }
            
            @keyframes fadeIn {
                from { opacity: 0; transform: translateY(-10px); }
                to { opacity: 1; transform: translateY(0); }
            }
            
            .loading {
                position: relative;
                color: transparent !important;
            }
            
            .loading::after {
                content: '';
                position: absolute;
                top: 50%;
                left: 50%;
                width: 16px;
                height: 16px;
                margin: -8px 0 0 -8px;
                border: 2px solid #4fc3f7;
                border-right-color: transparent;
                border-radius: 50%;
                animation: spin 0.8s linear infinite;
            }
            
            @keyframes spin {
                to { transform: rotate(360deg); }
            }
        `;
        document.head.appendChild(style);
        
        console.log('%c🚀 STR Dashboard 초기화 완료', 'color: #4caf50; font-size: 14px; font-weight: bold;');
        console.log('%c💡 사용법: ALERT ID 입력 후 조회 버튼 클릭', 'color: #29b6f6; font-style: italic;');
        console.log('%c📌 DB 연결 상태를 먼저 확인하세요', 'color: #ffa726; font-style: italic;');
    });

})();