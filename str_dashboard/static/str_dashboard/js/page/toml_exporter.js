// str_dashboard/static/str_dashboard/js/page/toml_exporter.js
/**
 * TOML 저장 및 다운로드 기능을 관리하는 모듈
 */
(function(window) {
    'use strict';

    if (!window.AppHelpers) {
        console.error('TomlExportManager requires AppHelpers to be loaded first.');
        return;
    }

    const { $, getCookie } = window.AppHelpers;

    class TomlExportManager {
        constructor() {
            this.modal = $('#toml-config-modal');
            this.tomlBtn = $('#toml_save_btn');
            this.downloadBtn = this.modal?.querySelector('.toml-download-btn');
            this.cancelBtn = this.modal?.querySelector('.toml-cancel-btn');
            this.init();
        }

        init() {
            if (!this.modal || !this.tomlBtn) return;
            
            this.tomlBtn.addEventListener('click', () => this.showConfigModal());
            this.downloadBtn?.addEventListener('click', () => this.downloadToml());
            this.cancelBtn?.addEventListener('click', () => this.closeModal());
            this.modal.addEventListener('click', (e) => {
                if (e.target === this.modal) this.closeModal();
            });
        }

        showConfigModal() {
            this.modal?.classList.add('show');
        }

        closeModal() {
            this.modal?.classList.remove('show');
        }

        async downloadToml() {
            if (!this.tomlBtn) return;

            this.tomlBtn.disabled = true;
            this.tomlBtn.textContent = '처리 중...';

            try {
                // 1. 서버에 TOML 데이터 준비 요청
                const response = await fetch(window.URLS.prepare_toml_data, {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'X-CSRFToken': getCookie('csrftoken')
                    }
                });
                
                const result = await response.json();
                
                if (result.success) {
                    // 2. 준비 완료 후 다운로드 URL로 이동
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
                this.tomlBtn.disabled = false;
                this.tomlBtn.textContent = 'TOML 저장';
            }
        }
    }
    
    window.TomlExportManager = TomlExportManager;

})(window);