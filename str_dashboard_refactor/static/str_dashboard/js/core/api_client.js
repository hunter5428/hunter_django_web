/**
 * API 통신 모듈
 */
export class APIClient {
    constructor() {
        this.baseHeaders = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-CSRFToken': this.getCookie('csrftoken'),
            'X-Requested-With': 'XMLHttpRequest'
        };
    }

    getCookie(name) {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        return parts.length === 2 ? decodeURIComponent(parts.pop().split(';').shift()) : undefined;
    }

    async post(url, data = {}) {
        try {
            const response = await fetch(url, {
                method: 'POST',
                headers: this.baseHeaders,
                body: new URLSearchParams(data)
            });
            
            const contentType = response.headers.get('content-type');
            if (!contentType || !contentType.includes('application/json')) {
                throw new Error('서버 응답이 JSON 형식이 아닙니다. 세션이 만료되었을 수 있습니다.');
            }
            
            return await response.json();
        } catch (error) {
            console.error('API call failed:', error);
            throw error;
        }
    }

    async get(url, params = {}) {
        try {
            const queryString = new URLSearchParams(params).toString();
            const fullUrl = queryString ? `${url}?${queryString}` : url;
            
            const response = await fetch(fullUrl, {
                method: 'GET',
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                }
            });
            
            return await response.json();
        } catch (error) {
            console.error('API GET failed:', error);
            throw error;
        }
    }
}

// 싱글톤 인스턴스
export const apiClient = new APIClient();