/**
 * 브라우저 스토리지 관리 유틸리티
 */
export class StorageManager {
    constructor() {
        this.prefix = 'str_dashboard_';
    }

    // LocalStorage 메서드
    setLocal(key, value) {
        try {
            const fullKey = this.prefix + key;
            const data = JSON.stringify(value);
            localStorage.setItem(fullKey, data);
            return true;
        } catch (error) {
            console.error('LocalStorage save error:', error);
            return false;
        }
    }

    getLocal(key, defaultValue = null) {
        try {
            const fullKey = this.prefix + key;
            const data = localStorage.getItem(fullKey);
            return data ? JSON.parse(data) : defaultValue;
        } catch (error) {
            console.error('LocalStorage read error:', error);
            return defaultValue;
        }
    }

    removeLocal(key) {
        const fullKey = this.prefix + key;
        localStorage.removeItem(fullKey);
    }

    clearLocal() {
        const keys = Object.keys(localStorage);
        keys.forEach(key => {
            if (key.startsWith(this.prefix)) {
                localStorage.removeItem(key);
            }
        });
    }

    // SessionStorage 메서드
    setSession(key, value) {
        try {
            const fullKey = this.prefix + key;
            const data = JSON.stringify(value);
            sessionStorage.setItem(fullKey, data);
            return true;
        } catch (error) {
            console.error('SessionStorage save error:', error);
            return false;
        }
    }

    getSession(key, defaultValue = null) {
        try {
            const fullKey = this.prefix + key;
            const data = sessionStorage.getItem(fullKey);
            return data ? JSON.parse(data) : defaultValue;
        } catch (error) {
            console.error('SessionStorage read error:', error);
            return defaultValue;
        }
    }

    removeSession(key) {
        const fullKey = this.prefix + key;
        sessionStorage.removeItem(fullKey);
    }

    clearSession() {
        const keys = Object.keys(sessionStorage);
        keys.forEach(key => {
            if (key.startsWith(this.prefix)) {
                sessionStorage.removeItem(key);
            }
        });
    }

    // 쿠키 관리
    setCookie(name, value, days = 365) {
        const date = new Date();
        date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
        const expires = `expires=${date.toUTCString()}`;
        document.cookie = `${this.prefix}${name}=${encodeURIComponent(value)};${expires};path=/`;
    }

    getCookie(name) {
        const fullName = this.prefix + name;
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${fullName}=`);
        if (parts.length === 2) {
            return decodeURIComponent(parts.pop().split(';').shift());
        }
        return null;
    }

    deleteCookie(name) {
        document.cookie = `${this.prefix}${name}=;expires=Thu, 01 Jan 1970 00:00:00 UTC;path=/`;
    }

    // 섹션 상태 저장/복원 (기존 UI 유지)
    saveSectionStates(states) {
        this.setLocal('sectionStates', states);
    }

    getSectionStates() {
        return this.getLocal('sectionStates', {});
    }

    // 폼 데이터 임시 저장
    saveFormData(formId, data) {
        this.setSession(`form_${formId}`, data);
    }

    getFormData(formId) {
        return this.getSession(`form_${formId}`, {});
    }

    clearFormData(formId) {
        this.removeSession(`form_${formId}`);
    }

    // 전체 초기화
    clearAll() {
        this.clearLocal();
        this.clearSession();
        console.log('All storage cleared');
    }
}

// 싱글톤 인스턴스
export const storageManager = new StorageManager();