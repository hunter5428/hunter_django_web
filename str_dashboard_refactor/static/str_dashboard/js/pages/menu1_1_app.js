/**
 * Menu1_1 페이지 메인 애플리케이션
 */
import { DBConnectionManager } from '../components/db_connection_manager.js';
import { AlertSearchManager } from '../components/alert_search_manager.js';
import { TomlExportManager } from '../components/toml_export_manager.js';
import { uiManager } from '../core/ui_manager.js';
import { stateManager } from '../core/state_manager.js';
import { eventBus, Events } from '../utils/event_bus.js';

class Menu1_1App {
    constructor() {
        this.dbManager = null;
        this.alertManager = null;
        this.tomlManager = null;
        this.init();
    }

    init() {
        // DOM이 준비되었는지 확인
        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', () => this.setup());
        } else {
            this.setup();
        }
    }

    setup() {
        console.log('Initializing Menu1_1 Application...');
        
        // 컴포넌트 초기화
        this.initializeComponents();
        
        // 이벤트 리스너 설정
        this.setupEventListeners();
        
        // UI 초기 상태 설정
        this.setupInitialUI();
        
        // 섹션 상태 복원
        uiManager.restoreSectionStates();
        
        console.log('Menu1_1 Application initialized successfully');
    }

    initializeComponents() {
        // DB 연결 관리자
        this.dbManager = new DBConnectionManager();
        
        // Alert 검색 관리자
        this.alertManager = new AlertSearchManager();
        
        // TOML 내보내기 관리자
        this.tomlManager = new TomlExportManager();
        
        // TableRenderer가 로드될 때까지 대기
        this.waitForTableRenderer();
    }

    waitForTableRenderer() {
        const checkInterval = setInterval(() => {
            if (window.TableRenderer) {
                clearInterval(checkInterval);
                console.log('TableRenderer loaded');
                eventBus.emit(Events.DATA_LOADED, { component: 'TableRenderer' });
            }
        }, 100);
    }

    setupEventListeners() {
        // 섹션 토글 이벤트
        document.addEventListener('click', (e) => {
            if (e.target.matches('.section h3')) {
                const section = e.target.parentElement;
                uiManager.toggleSection(`#${section.id}`);
                eventBus.emit(Events.SECTION_TOGGLE, { sectionId: section.id });
            }
        });
        
        // DB 연결 상태 변경 이벤트
        stateManager.subscribe('dbConnections', (newValue, oldValue) => {
            this.handleDBConnectionChange(newValue, oldValue);
        });
        
        // Alert 데이터 변경 이벤트
        stateManager.subscribe('alertData', (newValue) => {
            if (newValue) {
                this.handleAlertDataChange(newValue);
            }
        });
        
        // 전역 이벤트 리스너
        eventBus.on(Events.ALERT_SEARCH_SUCCESS, (data) => {
            console.log('Alert search successful:', data);
        });
        
        eventBus.on(Events.DB_ERROR, (error) => {
            console.error('Database error:', error);
            uiManager.showError(error.message);
        });
    }

    setupInitialUI() {
        // 초기 상태: 모든 섹션 숨김
        uiManager.hideAllSections();
        
        // TOML 저장 버튼 초기 숨김
        const tomlBtn = document.getElementById('toml_save_btn');
        if (tomlBtn) {
            tomlBtn.style.display = 'none';
        }
    }

    handleDBConnectionChange(newValue, oldValue) {
        // Oracle 연결 상태 변경
        if (newValue.oracle !== oldValue?.oracle) {
            if (newValue.oracle) {
                eventBus.emit(Events.DB_CONNECTED, { type: 'oracle' });
            } else {
                eventBus.emit(Events.DB_DISCONNECTED, { type: 'oracle' });
            }
        }
        
        // Redshift 연결 상태 변경
        if (newValue.redshift !== oldValue?.redshift) {
            if (newValue.redshift) {
                eventBus.emit(Events.DB_CONNECTED, { type: 'redshift' });
            } else {
                eventBus.emit(Events.DB_DISCONNECTED, { type: 'redshift' });
            }
        }
    }

    handleAlertDataChange(alertData) {
        // Alert 데이터가 로드되면 TOML 저장 버튼 표시
        if (alertData.currentAlertId) {
            this.tomlManager.showSaveButton(true);
        }
    }

    // 외부에서 접근 가능한 메서드들
    getDBManager() {
        return this.dbManager;
    }

    getAlertManager() {
        return this.alertManager;
    }

    getTomlManager() {
        return this.tomlManager;
    }

    // 상태 초기화
    reset() {
        stateManager.reset();
        uiManager.hideAllSections();
        this.tomlManager.showSaveButton(false);
    }
}

// 전역 앱 인스턴스 생성
window.menu1_1App = new Menu1_1App();

// 전역 접근을 위한 익스포트
export default Menu1_1App;