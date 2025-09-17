menu1_1.js// str_dashboard/static/str_dashboard/js/menu1_1.js
/**
 * ALERT ID 조회 페이지 초기화 스크립트
 * 각 모듈을 로드하고 메인 애플리케이션 컨트롤러를 생성합니다.
 */
(function(window) {
    'use strict';

    function initializePage() {
        // 의존성 모듈들이 모두 로드되었는지 확인
        const requiredModules = [
            'AlertSearchManager', 
            'TomlExportManager', 
            'TableRenderer', 
            'UIManager'
        ];
        
        for (const mod of requiredModules) {
            if (!window[mod]) {
                console.error(`Initialization failed: ${mod} is not loaded.`);
                return;
            }
        }

        // 메인 컨트롤러 및 기타 관리자 인스턴스 생성
        window.alertManager = new window.AlertSearchManager();
        window.tomlExporter = new window.TomlExportManager();

        // 초기 UI 상태 설정: 모든 결과 섹션 숨기기
        window.UIManager.hideAllSections();
        
        console.log('Menu1_1 page initialized successfully with refactored architecture.');
    }

    // DOM 콘텐츠가 로드된 후 초기화 실행
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', initializePage);
    } else {
        // DOM이 이미 로드된 경우
        initializePage();
    }

})(window);