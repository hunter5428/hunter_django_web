/**
 * UI 관리 모듈 - 기존 디자인 유지
 */
export class UIManager {
    constructor() {
        this.$ = (sel) => document.querySelector(sel);
        this.$$ = (sel) => document.querySelectorAll(sel);
    }

    // 모든 섹션 숨기기
    hideAllSections() {
        this.$$('.section').forEach(section => {
            section.style.display = 'none';
            const container = section.querySelector('.table-wrap');
            if (container) {
                container.innerHTML = '';
            }
        });
    }

    // 특정 섹션 표시
    showSection(sectionId) {
        const section = document.getElementById(sectionId);
        if (section) {
            section.style.display = 'block';
        }
    }

    // 로딩 표시 - 기존 스타일 유지
    showLoading(buttonId, show = true) {
        const btn = this.$(buttonId);
        if (btn) {
            btn.disabled = show;
            btn.textContent = show ? '조회 중...' : '조회';
            if (show) {
                btn.classList.add('loading');
            } else {
                btn.classList.remove('loading');
            }
        }
    }

    // 에러 메시지 - 기존 alert 사용
    showError(message) {
        alert(message || '조회 중 오류가 발생했습니다.');
    }

    // 성공 메시지
    showSuccess(message) {
        console.log('Success:', message);
    }

    // 상태 배지 업데이트 - 기존 디자인 유지
    updateStatusBadge(badgeId, isConnected, connectedText, disconnectedText) {
        const badge = this.$(badgeId);
        if (badge) {
            if (isConnected) {
                badge.textContent = connectedText;
                badge.classList.add('ok');
            } else {
                badge.textContent = disconnectedText;
                badge.classList.remove('ok');
            }
        }
    }

    // 모달 관리 - 기존 스타일 유지
    openModal(modalId) {
        const modal = this.$(modalId);
        if (modal) {
            modal.style.display = 'flex';
            modal.classList.add('show');
        }
    }

    closeModal(modalId) {
        const modal = this.$(modalId);
        if (modal) {
            modal.style.display = 'none';
            modal.classList.remove('show');
        }
    }

    // 입력 필드 값 가져오기
    getInputValue(inputId) {
        const input = this.$(inputId);
        return input ? input.value.trim() : '';
    }

    // 입력 필드 값 설정
    setInputValue(inputId, value) {
        const input = this.$(inputId);
        if (input) {
            input.value = value;
        }
    }

    // 테스트 결과 표시 - 기존 스타일 유지
    showTestResult(resultId, success, message) {
        const resultSpan = this.$(resultId);
        if (resultSpan) {
            resultSpan.style.display = 'inline-block';
            resultSpan.textContent = success ? '✓ ' + message : '✗ ' + message;
            resultSpan.classList.remove(success ? 'fail' : 'success');
            resultSpan.classList.add(success ? 'success' : 'fail');
        }
    }

    // 섹션 토글 - 기존 collapse 기능 유지
    toggleSection(sectionId) {
        const section = this.$(sectionId);
        if (section) {
            section.classList.toggle('collapsed');
            
            // localStorage에 상태 저장
            const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
            states[sectionId] = section.classList.contains('collapsed');
            localStorage.setItem('sectionStates', JSON.stringify(states));
        }
    }

    // 저장된 섹션 상태 복원
    restoreSectionStates() {
        const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
        Object.entries(states).forEach(([sectionId, isCollapsed]) => {
            const section = this.$(sectionId);
            if (section) {
                section.classList.toggle('collapsed', isCollapsed);
            }
        });
    }
}

// 싱글톤 인스턴스
export const uiManager = new UIManager();