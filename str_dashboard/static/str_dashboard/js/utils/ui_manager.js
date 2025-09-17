// str_dashboard/static/str_dashboard/js/utils/ui_manager.js
/**
 * 페이지 UI 상태를 관리하는 모듈 (보이기/숨기기, 로딩 등)
 */
(function(window) {
    'use strict';

    if (!window.AppHelpers) {
        console.error('UIManager requires AppHelpers to be loaded first.');
        return;
    }

    const { $, $$ } = window.AppHelpers;

    class UIManager {
        /**
         * 모든 결과 섹션을 숨기고 내용을 초기화합니다.
         */
        static hideAllSections() {
            $$('.section').forEach(section => {
                section.style.display = 'none';
                const container = section.querySelector('.table-wrap');
                if (container) {
                    container.innerHTML = '';
                }
            });
        }

        /**
         * 특정 ID를 가진 섹션을 보여줍니다.
         * @param {string} sectionId - 보여줄 섹션의 ID
         */
        static showSection(sectionId) {
            const section = document.getElementById(sectionId);
            if (section) {
                section.style.display = 'block';
            }
        }

        /**
         * 조회 버튼의 로딩 상태를 제어합니다.
         * @param {boolean} show - 로딩 상태를 표시할지 여부
         */
        static showLoading(show = true) {
            const btn = $('#alert_id_search_btn');
            if (btn) {
                btn.disabled = show;
                btn.textContent = show ? '조회 중...' : '조회';
            }
        }

        /**
         * 사용자에게 에러 메시지를 alert으로 표시합니다.
         * @param {string} message - 표시할 에러 메시지
         */
        static showError(message) {
            alert(message || '조회 중 오류가 발생했습니다.');
        }
    }

    // 전역 window 객체에 노출
    window.UIManager = UIManager;

})(window);