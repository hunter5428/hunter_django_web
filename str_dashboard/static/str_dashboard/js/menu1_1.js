// str_dashboard/static/str_dashboard/js/menu1_1.js
// ALERT ID 조회 페이지 - 간소화 버전 (백엔드 통합 API 사용)

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
            
            modal.addEventListener('click', (e) => {
                if (e.target === modal) this.closeModal();
            });
        }

        showConfigModal() {
            const modal = $('#toml-config-modal');
            if (modal) {
                modal.classList.add('show');
            }
        }

        closeModal() {
            const modal = $('#toml-config-modal');
            if (modal) {
                modal.classList.remove('show');
            }
        }

        async downloadToml() {
            const tomlBtn = $('#toml_save_btn');
            if (tomlBtn) {
                tomlBtn.disabled = true;
                tomlBtn.textContent = '처리 중...';
            }
            
            try {
                const response = await fetch(window.URLS.prepare_toml_data, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    }
                });
                
                const result = await response.json();
                
                if (result.success) {
                    window.location.href = window.URLS.download_toml;
                    
                    setTimeout(() => {
                        this.closeModal();
                        alert('TOML 파일이 다운로드되었습니다.');
                    }, 1000);
                } else {
                    alert('TOML 데이터 준비 실패: ' + result.message);
                }
            } catch (error) {
                console.error('TOML export error:', error);
                alert('TOML 저장 중 오류가 발생했습니다.');
            } finally {
                if (tomlBtn) {
                    tomlBtn.disabled = false;
                    tomlBtn.textContent = 'TOML 저장';
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
            this.init();
        }

        init() {
            this.searchBtn?.addEventListener('click', () => this.search());
            this.inputField?.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') this.search();
            });
        }

        async search() {
            if (this.isSearching) return;

            // DB 연결 확인
            if (!window.dualDBManager?.isOracleConnected()) {
                alert('먼저 Oracle DB 연결을 완료해 주세요.');
                $('#btn-open-db-modal')?.click();
                return;
            }
            
            const alertId = this.inputField?.value?.trim();
            if (!alertId) {
                alert('ALERT ID를 입력하세요.');
                return;
            }

            this.isSearching = true;
            this.setLoading(true);

            console.group(`%c🔍 ALERT ID: ${alertId} 통합 조회 시작`, 'color: #4fc3f7; font-size: 16px; font-weight: bold;');
            console.time('통합 조회 시간');

            try {
                // 통합 API 호출 - 모든 데이터를 한번에 조회
                const response = await fetch(window.URLS.query_all_integrated, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    },
                    body: new URLSearchParams({ alert_id: alertId })
                });
                
                const result = await response.json();
                
                if (!result.success) {
                    throw new Error(result.message || '조회 실패');
                }
                
                // DataFrame Manager 상태 조회
                const statusResponse = await fetch(window.URLS.df_manager_status, {
                    headers: { 'X-CSRFToken': getCookie('csrftoken') }
                });
                const statusData = await statusResponse.json();
                
                // 조회 완료 메시지 표시
                this.showCompleteMessage(alertId);
                
                // TOML 저장 버튼 표시
                const tomlBtn = $('#toml_save_btn');
                if (tomlBtn) {
                    tomlBtn.style.display = 'inline-flex';
                }
                
                console.timeEnd('통합 조회 시간');
                console.log('%c✅ 통합 데이터 조회 완료', 'color: #4caf50; font-size: 14px; font-weight: bold;');
                console.log('%c📊 DataFrame Manager 상태:', 'color: #ffa726; font-size: 14px; font-weight: bold;');
                console.log(statusData);
                
                // 전역 변수로 노출 (개발자 콘솔 접근용)
                window.DF_MANAGER_STATUS = statusData;
                
                // 사용 가능한 데이터셋 목록 표시
                if (statusData.datasets_list && statusData.datasets_list.length > 0) {
                    console.log('%c📂 사용 가능한 데이터셋:', 'color: #29b6f6; font-weight: bold;');
                    statusData.datasets_list.forEach(name => {
                        console.log(`  - ${name}`);
                    });
                }
                
                console.log('%c💡 콘솔 명령어:', 'color: #29b6f6; font-style: italic;');
                console.log('  window.DF_MANAGER_STATUS - 전체 상태 조회');
                console.log('  window.downloadDataset("dataset_name") - CSV 다운로드');
                console.log('  window.refreshStatus() - 상태 새로고침');
                
            } catch (error) {
                console.error('❌ Alert search error:', error);
                alert(error.message || '조회 중 오류가 발생했습니다.');
            } finally {
                console.groupEnd();
                this.isSearching = false;
                this.setLoading(false);
            }
        }

        setLoading(isLoading) {
            if (this.searchBtn) {
                this.searchBtn.disabled = isLoading;
                this.searchBtn.textContent = isLoading ? '조회 중...' : '조회';
            }
        }

        showCompleteMessage(alertId) {
            const container = $('#query-result-container');
            const text = $('#query-complete-text');
            
            if (container && text) {
                text.textContent = `ALERT ID ${alertId} 데이터 조회가 완료되었습니다.`;
                container.style.display = 'block';
            }
        }
    }

    // ==================== 개발자 콘솔 헬퍼 함수 ====================
    window.downloadDataset = function(datasetName) {
        if (!datasetName) {
            console.error('Dataset name is required');
            return;
        }
        
        fetch(`${window.URLS.export_dataframe_csv}?dataset=${datasetName}`, {
            headers: { 'X-CSRFToken': getCookie('csrftoken') }
        })
        .then(response => {
            if (!response.ok) throw new Error('Download failed');
            return response.blob();
        })
        .then(blob => {
            const a = document.createElement('a');
            a.href = URL.createObjectURL(blob);
            a.download = `${datasetName}_${new Date().toISOString().slice(0,10)}.csv`;
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            console.log(`✅ ${datasetName} downloaded successfully`);
        })
        .catch(error => {
            console.error('Download failed:', error);
        });
    };
    
    window.refreshStatus = function() {
        fetch(window.URLS.df_manager_status, {
            headers: { 'X-CSRFToken': getCookie('csrftoken') }
        })
        .then(response => response.json())
        .then(data => {
            window.DF_MANAGER_STATUS = data;
            console.log('✅ Status refreshed:', data);
        })
        .catch(error => {
            console.error('Failed to refresh status:', error);
        });
    };

    // ==================== 초기화 ====================
    document.addEventListener('DOMContentLoaded', function() {
        window.alertManager = new AlertSearchManager();
        window.tomlExporter = new TomlExportManager();
        
        console.log('%c📌 STR Dashboard 초기화 완료', 'color: #4caf50; font-size: 14px; font-weight: bold;');
        console.log('%c💡 사용법: ALERT ID 입력 후 조회 버튼 클릭', 'color: #29b6f6; font-style: italic;');
    });

})();