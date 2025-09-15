/**
 * 애플리케이션 초기화 관리
 */
import { storageManager } from '../utils/storage_manager.js';
import { eventBus, Events } from '../utils/event_bus.js';

export class AppInitializer {
    constructor() {
        this.initialized = false;
        this.components = new Map();
    }

    /**
     * 애플리케이션 초기화
     */
    async initialize() {
        if (this.initialized) {
            console.warn('App already initialized');
            return;
        }

        try {
            console.log('Starting application initialization...');
            
            // 1. 브라우저 호환성 체크
            this.checkBrowserCompatibility();
            
            // 2. 환경 설정 로드
            await this.loadConfiguration();
            
            // 3. 스토리지 초기화
            this.initializeStorage();
            
            // 4. 전역 이벤트 리스너 설정
            this.setupGlobalListeners();
            
            // 5. 사용자 세션 체크
            await this.checkUserSession();
            
            this.initialized = true;
            eventBus.emit(Events.APP_INITIALIZED);
            
            console.log('Application initialization complete');
            
        } catch (error) {
            console.error('Application initialization failed:', error);
            this.handleInitializationError(error);
        }
    }

    /**
     * 브라우저 호환성 체크
     */
    checkBrowserCompatibility() {
        const required = {
            'Promise': typeof Promise !== 'undefined',
            'fetch': typeof fetch !== 'undefined',
            'localStorage': typeof localStorage !== 'undefined',
            'sessionStorage': typeof sessionStorage !== 'undefined',
            'JSON': typeof JSON !== 'undefined'
        };

        const unsupported = Object.entries(required)
            .filter(([, supported]) => !supported)
            .map(([feature]) => feature);

        if (unsupported.length > 0) {
            throw new Error(`브라우저가 다음 기능을 지원하지 않습니다: ${unsupported.join(', ')}`);
        }
    }

    /**
     * 환경 설정 로드
     */
    async loadConfiguration() {
        // window.URLS와 window.RULE_OBJ_MAP 등이 있는지 확인
        if (!window.URLS) {
            console.warn('URLs configuration not found');
        }
        
        if (!window.RULE_OBJ_MAP) {
            console.warn('Rule object map not found');
        }

        // 필요한 경우 서버에서 추가 설정 로드
        // const config = await apiClient.get('/api/config/');
    }

    /**
     * 스토리지 초기화
     */
    initializeStorage() {
        // 만료된 데이터 정리
        this.cleanupExpiredData();
        
        // 섹션 상태 복원
        const sectionStates = storageManager.getSectionStates();
        if (sectionStates) {
            console.log('Restored section states:', Object.keys(sectionStates).length);
        }
    }

    /**
     * 만료된 데이터 정리
     */
    cleanupExpiredData() {
        // 임시 폼 데이터 정리
        const tempDataKeys = ['form_', 'temp_', 'cache_'];
        const allKeys = Object.keys(sessionStorage);
        
        allKeys.forEach(key => {
            if (tempDataKeys.some(prefix => key.includes(prefix))) {
                try {
                    const data = JSON.parse(sessionStorage.getItem(key));
                    if (data.expires && new Date(data.expires) < new Date()) {
                        sessionStorage.removeItem(key);
                        console.log(`Removed expired data: ${key}`);
                    }
                } catch {
                    // 파싱 실패한 경우 무시
                }
            }
        });
    }

    /**
     * 전역 이벤트 리스너 설정
     */
    setupGlobalListeners() {
        // 페이지 종료 시 정리
        window.addEventListener('beforeunload', (e) => {
            this.handleBeforeUnload(e);
        });

        // 네트워크 상태 변경 감지
        window.addEventListener('online', () => {
            console.log('Network: Online');
            eventBus.emit(Events.NETWORK_ONLINE);
        });

        window.addEventListener('offline', () => {
            console.log('Network: Offline');
            eventBus.emit(Events.NETWORK_OFFLINE);
        });

        // 키보드 단축키
        document.addEventListener('keydown', (e) => {
            this.handleKeyboardShortcuts(e);
        });
    }

    /**
     * 키보드 단축키 처리
     */
    handleKeyboardShortcuts(e) {
        // Ctrl/Cmd + S: TOML 저장
        if ((e.ctrlKey || e.metaKey) && e.key === 's') {
            e.preventDefault();
            const tomlBtn = document.getElementById('toml_save_btn');
            if (tomlBtn && tomlBtn.style.display !== 'none') {
                tomlBtn.click();
            }
        }

        // Ctrl/Cmd + Enter: Alert 검색
        if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
            const searchBtn = document.getElementById('alert_id_search_btn');
            if (searchBtn && !searchBtn.disabled) {
                searchBtn.click();
            }
        }

        // ESC: 모달 닫기
        if (e.key === 'Escape') {
            const modals = document.querySelectorAll('.modal-backdrop.show');
            modals.forEach(modal => {
                modal.style.display = 'none';
                modal.classList.remove('show');
            });
        }
    }

    /**
     * 사용자 세션 체크
     */
    async checkUserSession() {
        // 세션 유효성 확인
        const sessionValid = await this.validateSession();
        
        if (!sessionValid) {
            console.warn('Session invalid or expired');
            // 필요한 경우 로그인 페이지로 리다이렉트
        }
    }

    /**
     * 세션 유효성 확인
     */
    async validateSession() {
        // Django 세션 쿠키 확인
        const sessionId = this.getCookie('sessionid');
        return !!sessionId;
    }

    /**
     * 쿠키 가져오기
     */
    getCookie(name) {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        return parts.length === 2 ? decodeURIComponent(parts.pop().split(';').shift()) : null;
    }

    /**
     * 페이지 종료 시 처리
     */
    handleBeforeUnload(e) {
        // 저장되지 않은 데이터가 있는지 확인
        const hasUnsavedData = this.checkUnsavedData();
        
        if (hasUnsavedData) {
            const message = '저장되지 않은 데이터가 있습니다. 정말 페이지를 떠나시겠습니까?';
            e.preventDefault();
            e.returnValue = message;
            return message;
        }
    }

    /**
     * 저장되지 않은 데이터 확인
     */
    checkUnsavedData() {
        // 구현 필요: 폼 데이터나 임시 데이터 확인
        return false;
    }

    /**
     * 초기화 에러 처리
     */
    handleInitializationError(error) {
        console.error('Initialization error:', error);
        
        // 사용자에게 에러 표시
        const errorMessage = '애플리케이션 초기화에 실패했습니다. 페이지를 새로고침 해주세요.';
        
        // 에러 메시지 표시
        const errorDiv = document.createElement('div');
        errorDiv.className = 'initialization-error';
        errorDiv.style.cssText = `
            position: fixed;
            top: 20px;
            left: 50%;
            transform: translateX(-50%);
            background: #ff6b6b;
            color: white;
            padding: 15px 20px;
            border-radius: 8px;
            z-index: 9999;
            box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        `;
        errorDiv.textContent = errorMessage;
        document.body.appendChild(errorDiv);
    }

    /**
     * 컴포넌트 등록
     */
    registerComponent(name, component) {
        this.components.set(name, component);
        console.log(`Component registered: ${name}`);
    }

    /**
     * 컴포넌트 가져오기
     */
    getComponent(name) {
        return this.components.get(name);
    }

    /**
     * 모든 컴포넌트 가져오기
     */
    getAllComponents() {
        return Array.from(this.components.entries());
    }
}

// 싱글톤 인스턴스
export const appInitializer = new AppInitializer();