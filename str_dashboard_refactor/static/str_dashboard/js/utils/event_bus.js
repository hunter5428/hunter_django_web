/**
 * 이벤트 버스 - 컴포넌트 간 통신
 */
export class EventBus {
    constructor() {
        this.events = new Map();
    }

    /**
     * 이벤트 구독
     * @param {string} eventName - 이벤트 이름
     * @param {Function} callback - 콜백 함수
     * @returns {Function} - 구독 해제 함수
     */
    on(eventName, callback) {
        if (!this.events.has(eventName)) {
            this.events.set(eventName, new Set());
        }
        
        this.events.get(eventName).add(callback);
        
        // 구독 해제 함수 반환
        return () => this.off(eventName, callback);
    }

    /**
     * 이벤트 구독 해제
     * @param {string} eventName - 이벤트 이름
     * @param {Function} callback - 콜백 함수
     */
    off(eventName, callback) {
        const callbacks = this.events.get(eventName);
        if (callbacks) {
            callbacks.delete(callback);
            if (callbacks.size === 0) {
                this.events.delete(eventName);
            }
        }
    }

    /**
     * 이벤트 한 번만 구독
     * @param {string} eventName - 이벤트 이름
     * @param {Function} callback - 콜백 함수
     */
    once(eventName, callback) {
        const onceCallback = (...args) => {
            callback(...args);
            this.off(eventName, onceCallback);
        };
        this.on(eventName, onceCallback);
    }

    /**
     * 이벤트 발행
     * @param {string} eventName - 이벤트 이름
     * @param {...any} args - 이벤트 데이터
     */
    emit(eventName, ...args) {
        const callbacks = this.events.get(eventName);
        if (callbacks) {
            callbacks.forEach(callback => {
                try {
                    callback(...args);
                } catch (error) {
                    console.error(`Error in event handler for ${eventName}:`, error);
                }
            });
        }
    }

    /**
     * 모든 이벤트 리스너 제거
     * @param {string} eventName - 이벤트 이름 (선택적)
     */
    clear(eventName) {
        if (eventName) {
            this.events.delete(eventName);
        } else {
            this.events.clear();
        }
    }

    /**
     * 이벤트 리스너 개수 확인
     * @param {string} eventName - 이벤트 이름
     * @returns {number} - 리스너 개수
     */
    listenerCount(eventName) {
        const callbacks = this.events.get(eventName);
        return callbacks ? callbacks.size : 0;
    }
}

// 싱글톤 인스턴스
export const eventBus = new EventBus();

// 이벤트 이름 상수 (타입 안정성)
export const Events = {
    // DB 연결 이벤트
    DB_CONNECTED: 'db:connected',
    DB_DISCONNECTED: 'db:disconnected',
    DB_ERROR: 'db:error',
    
    // Alert 검색 이벤트
    ALERT_SEARCH_START: 'alert:search:start',
    ALERT_SEARCH_SUCCESS: 'alert:search:success',
    ALERT_SEARCH_ERROR: 'alert:search:error',
    
    // 데이터 로드 이벤트
    DATA_LOADING: 'data:loading',
    DATA_LOADED: 'data:loaded',
    DATA_ERROR: 'data:error',
    
    // UI 이벤트
    SECTION_TOGGLE: 'ui:section:toggle',
    MODAL_OPEN: 'ui:modal:open',
    MODAL_CLOSE: 'ui:modal:close',
    
    // TOML 내보내기 이벤트
    TOML_EXPORT_START: 'toml:export:start',
    TOML_EXPORT_SUCCESS: 'toml:export:success',
    TOML_EXPORT_ERROR: 'toml:export:error',
    
    // Orderbook 이벤트
    ORDERBOOK_CACHE_UPDATED: 'orderbook:cache:updated',
    ORDERBOOK_ANALYSIS_COMPLETE: 'orderbook:analysis:complete'
};