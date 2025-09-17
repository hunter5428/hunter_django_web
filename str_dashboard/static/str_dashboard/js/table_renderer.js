// str_dashboard/static/str_dashboard/js/table_renderer.js
/**
 * 메인 테이블 렌더러 (Facade)
 * 분리된 렌더러 모듈들을 조합하여 단일 인터페이스를 제공합니다.
 */
(function(window) {
    'use strict';

    // 모든 렌더러 모듈이 로드되었는지 확인
    const dependencies = ['CustomerRenderer', 'RuleRenderer', 'OrderbookRenderer'];
    for (const dep of dependencies) {
        if (!window[dep]) {
            console.error(`TableRenderer facade requires ${dep} to be loaded first.`);
            return;
        }
    }

    // 각 모듈의 메서드를 하나의 TableRenderer 객체로 통합
    const TableRenderer = {
        ...window.CustomerRenderer,
        ...window.RuleRenderer,
        ...window.OrderbookRenderer,
    };
    
    // 섹션 토글 기능 초기화
    function initSectionToggle() {
        document.addEventListener('click', function(e) {
            const h3 = e.target.closest('.section h3');
            if (h3) {
                const section = h3.parentElement;
                section.classList.toggle('collapsed');
                // 로컬 스토리지에 상태 저장 (선택적)
                try {
                    const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
                    states[section.id] = section.classList.contains('collapsed');
                    localStorage.setItem('sectionStates', JSON.stringify(states));
                } catch (err) {
                    console.warn('Could not save section state to localStorage.', err);
                }
            }
        });

        // 페이지 로드 시 저장된 상태 복원
        try {
            const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
            Object.entries(states).forEach(([sectionId, isCollapsed]) => {
                const section = document.getElementById(sectionId);
                if (section) {
                    section.classList.toggle('collapsed', isCollapsed);
                }
            });
        } catch (err) {
            console.warn('Could not load section state from localStorage.', err);
        }
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initSectionToggle);
    } else {
        initSectionToggle();
    }

    // 최종적으로 window.TableRenderer에 할당
    window.TableRenderer = TableRenderer;
    console.log('TableRenderer facade initialized.');

})(window);