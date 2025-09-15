/**
 * TOML 내보내기 관리 컴포넌트
 */
import { apiClient } from '../core/api-client.js';
import { uiManager } from '../core/ui-manager.js';

export class TomlExportManager {
    constructor() {
        this.urls = window.URLS || {};
        this.init();
    }

    init() {
        this.setupEventListeners();
    }

    setupEventListeners() {
        // TOML 저장 버튼 - 기존 스타일 유지
        const tomlBtn = document.getElementById('toml_save_btn');
        if (tomlBtn) {
            tomlBtn.addEventListener('click', () => this.showConfigModal());
        }
        
        // 모달 이벤트
        this.setupModalEvents();
    }

    setupModalEvents() {
        const modal = document.getElementById('toml-config-modal');
        if (!modal) return;
        
        // 취소 버튼
        const cancelBtn = modal.querySelector('.toml-cancel-btn');
        if (cancelBtn) {
            cancelBtn.addEventListener('click', () => this.closeModal());
        }
        
        // 다운로드 버튼
        const downloadBtn = modal.querySelector('.toml-download-btn');
        if (downloadBtn) {
            downloadBtn.addEventListener('click', () => this.downloadToml());
        }
        
        // 모달 외부 클릭
        modal.addEventListener('click', (e) => {
            if (e.target === modal) this.closeModal();
        });
    }

    showConfigModal() {
        const modal = document.getElementById('toml-config-modal');
        if (modal) {
            modal.classList.add('show');
        }
    }

    closeModal() {
        const modal = document.getElementById('toml-config-modal');
        if (modal) {
            modal.classList.remove('show');
        }
    }

    async downloadToml() {
        const tomlBtn = document.getElementById('toml_save_btn');
        if (tomlBtn) {
            tomlBtn.disabled = true;
            tomlBtn.textContent = '처리 중...';
        }
        
        try {
            // 데이터 준비 요청
            const response = await apiClient.post(this.urls.prepare_toml_data);
            
            if (response.success) {
                // 다운로드 실행
                window.location.href = this.urls.download_toml;
                
                setTimeout(() => {
                    this.closeModal();
                    alert('TOML 파일이 다운로드되었습니다.');
                }, 1000);
            } else {
                alert('TOML 데이터 준비 실패: ' + response.message);
            }
        } catch (error) {
            console.error('TOML export error:', error);
            alert('TOML 저장 중 오류가 발생했습니다.');
        } finally {
            if (tomlBtn) {
                tomlBtn.disabled = false;
                tomlBtn.innerHTML = `
                    <svg class="icon" viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>
                        <polyline points="14 2 14 8 20 8"/>
                        <line x1="16" y1="13" x2="8" y2="13"/>
                        <line x1="16" y1="17" x2="8" y2="17"/>
                        <polyline points="10 9 9 9 8 9"/>
                    </svg>
                    TOML 저장
                `;
            }
        }
    }

    // 선택된 데이터 옵션 가져오기
    getSelectedOptions() {
        const options = {
            maskSensitive: document.getElementById('toml-mask-sensitive')?.checked || false
        };
        return options;
    }

    // TOML 저장 버튼 표시/숨기기
    showSaveButton(show = true) {
        const tomlBtn = document.getElementById('toml_save_btn');
        if (tomlBtn) {
            tomlBtn.style.display = show ? 'inline-flex' : 'none';
        }
    }
}