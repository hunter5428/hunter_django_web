/**
 * 전역 상태 관리 모듈
 */
export class StateManager {
    constructor() {
        this.state = {
            currentAlertId: null,
            alertData: null,
            customerData: null,
            orderbookData: null,
            isSearching: false,
            dbConnections: {
                oracle: false,
                redshift: false
            }
        };
        
        this.subscribers = new Map();
    }

    // 상태 업데이트
    setState(key, value) {
        const oldValue = this.state[key];
        this.state[key] = value;
        
        // 구독자에게 알림
        this.notify(key, value, oldValue);
    }

    // 상태 가져오기
    getState(key) {
        return this.state[key];
    }

    // 전체 상태 가져오기
    getAllState() {
        return { ...this.state };
    }

    // 상태 초기화
    reset() {
        const initialState = {
            currentAlertId: null,
            alertData: null,
            customerData: null,
            orderbookData: null,
            isSearching: false,
            dbConnections: this.state.dbConnections
        };
        
        Object.keys(initialState).forEach(key => {
            this.setState(key, initialState[key]);
        });
    }

    // 구독 관리
    subscribe(key, callback) {
        if (!this.subscribers.has(key)) {
            this.subscribers.set(key, new Set());
        }
        this.subscribers.get(key).add(callback);
        
        // 구독 해제 함수 반환
        return () => {
            this.subscribers.get(key)?.delete(callback);
        };
    }

    // 구독자에게 알림
    notify(key, newValue, oldValue) {
        const callbacks = this.subscribers.get(key);
        if (callbacks) {
            callbacks.forEach(callback => {
                try {
                    callback(newValue, oldValue);
                } catch (error) {
                    console.error('State change callback error:', error);
                }
            });
        }
    }

    // DB 연결 상태 업데이트
    setDbConnection(dbType, isConnected) {
        this.state.dbConnections[dbType] = isConnected;
        this.notify('dbConnections', this.state.dbConnections);
    }

    // Alert 데이터 저장
    setAlertData(data) {
        this.state.alertData = data;
        if (data?.alert_id) {
            this.state.currentAlertId = data.alert_id;
        }
        this.notify('alertData', data);
    }
}

// 싱글톤 인스턴스
export const stateManager = new StateManager();