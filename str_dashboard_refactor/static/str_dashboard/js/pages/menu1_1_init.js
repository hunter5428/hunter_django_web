/**
 * Menu1_1 페이지 초기화 스크립트 (non-module)
 * HTML에서 직접 로드되는 진입점
 */
(function() {
    'use strict';

    // 모듈 로드 함수
    async function loadModules() {
        try {
            // 동적 모듈 임포트
            const modules = await Promise.all([
                import('./menu1_1_app.js'),
                import('../components/table_renderer_wrapper.js')
            ]);
            
            console.log('All modules loaded successfully');
            
            // 초기화 완료 이벤트 발생
            window.dispatchEvent(new CustomEvent('menu1_1:initialized', {
                detail: { 
                    app: window.menu1_1App,
                    renderer: window.TableRenderer
                }
            }));
            
        } catch (error) {
            console.error('Failed to load modules:', error);
            alert('페이지 초기화 실패. 새로고침 해주세요.');
        }
    }

    // DOM 준비 확인
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', loadModules);
    } else {
        loadModules();
    }

    // 전역 에러 핸들러
    window.addEventListener('error', (event) => {
        console.error('Global error:', event.error);
    });

    window.addEventListener('unhandledrejection', (event) => {
        console.error('Unhandled promise rejection:', event.reason);
    });
})();