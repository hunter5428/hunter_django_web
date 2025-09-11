// str_dashboard/static/str_dashboard/js/api.service.js

/**
 * API 서비스 레이어
 * 모든 API 호출을 중앙에서 관리
 */
(function(window) {
    'use strict';

    /**
     * API 에러 클래스
     */
    class APIError extends Error {
        constructor(message, status, data) {
            super(message);
            this.name = 'APIError';
            this.status = status;
            this.data = data;
            this.timestamp = new Date();
        }
    }

    /**
     * API 요청 옵션 빌더
     */
    class RequestBuilder {
        constructor() {
            this.options = {
                method: 'GET',
                headers: {
                    'X-Requested-With': 'XMLHttpRequest'
                }
            };
        }

        setMethod(method) {
            this.options.method = method.toUpperCase();
            return this;
        }

        setHeaders(headers) {
            this.options.headers = { ...this.options.headers, ...headers };
            return this;
        }

        setBody(data) {
            if (data instanceof FormData) {
                this.options.body = data;
            } else if (typeof data === 'object') {
                this.options.headers['Content-Type'] = 'application/x-www-form-urlencoded';
                this.options.body = new URLSearchParams(data);
            } else {
                this.options.body = data;
            }
            return this;
        }

        setCsrfToken(token) {
            this.options.headers['X-CSRFToken'] = token;
            return this;
        }

        build() {
            return this.options;
        }
    }

    /**
     * API 서비스 메인 클래스
     */
    class APIService {
        constructor() {
            this.baseURL = window.location.origin;
            this.timeout = 30000; // 30초
            this.retryCount = 3;
            this.retryDelay = 1000; // 1초
            
            // 진행 중인 요청 추적
            this.activeRequests = new Map();
            
            // 인터셉터
            this.requestInterceptors = [];
            this.responseInterceptors = [];
            
            // 에러 핸들러
            this.errorHandlers = new Map();
            
            this.init();
        }

        init() {
            // 기본 에러 핸들러 등록
            this.registerErrorHandler(400, this.handleBadRequest);
            this.registerErrorHandler(401, this.handleUnauthorized);
            this.registerErrorHandler(403, this.handleForbidden);
            this.registerErrorHandler(404, this.handleNotFound);
            this.registerErrorHandler(500, this.handleServerError);
            this.registerErrorHandler('network', this.handleNetworkError);
            this.registerErrorHandler('timeout', this.handleTimeout);
        }

        /**
         * CSRF 토큰 가져오기
         */
        getCsrfToken() {
            // Django CSRF 토큰 가져오기
            const cookieValue = document.cookie
                .split('; ')
                .find(row => row.startsWith('csrftoken='))
                ?.split('=')[1];
            
            return cookieValue || document.querySelector('[name=csrfmiddlewaretoken]')?.value || '';
        }

        /**
         * 요청 인터셉터 추가
         */
        addRequestInterceptor(interceptor) {
            this.requestInterceptors.push(interceptor);
            return () => {
                const index = this.requestInterceptors.indexOf(interceptor);
                if (index > -1) {
                    this.requestInterceptors.splice(index, 1);
                }
            };
        }

        /**
         * 응답 인터셉터 추가
         */
        addResponseInterceptor(interceptor) {
            this.responseInterceptors.push(interceptor);
            return () => {
                const index = this.responseInterceptors.indexOf(interceptor);
                if (index > -1) {
                    this.responseInterceptors.splice(index, 1);
                }
            };
        }

        /**
         * 에러 핸들러 등록
         */
        registerErrorHandler(status, handler) {
            this.errorHandlers.set(status, handler);
        }

        /**
         * 타임아웃 처리를 위한 fetch 래퍼
         */
        async fetchWithTimeout(url, options, timeout = this.timeout) {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), timeout);
            
            try {
                const response = await fetch(url, {
                    ...options,
                    signal: controller.signal
                });
                clearTimeout(timeoutId);
                return response;
            } catch (error) {
                clearTimeout(timeoutId);
                if (error.name === 'AbortError') {
                    throw new APIError('Request timeout', 'timeout', { url, timeout });
                }
                throw error;
            }
        }

        /**
         * 재시도 로직
         */
        async retryRequest(fn, retries = this.retryCount, delay = this.retryDelay) {
            try {
                return await fn();
            } catch (error) {
                if (retries <= 0) {
                    throw error;
                }
                
                // 재시도 가능한 에러인지 확인
                if (this.isRetryableError(error)) {
                    console.log(`Retrying request... (${this.retryCount - retries + 1}/${this.retryCount})`);
                    await this.sleep(delay);
                    return this.retryRequest(fn, retries - 1, delay * 2); // 지수 백오프
                }
                
                throw error;
            }
        }

        /**
         * 재시도 가능한 에러 판단
         */
        isRetryableError(error) {
            // 네트워크 에러나 5xx 서버 에러는 재시도
            return error.status === 'network' || 
                   error.status === 'timeout' ||
                   (error.status >= 500 && error.status < 600);
        }

        /**
         * 대기 함수
         */
        sleep(ms) {
            return new Promise(resolve => setTimeout(resolve, ms));
        }

        /**
         * 메인 요청 함수
         */
        async request(url, options = {}) {
            const requestId = this.generateRequestId();
            const fullUrl = url.startsWith('http') ? url : `${this.baseURL}${url}`;
            
            // Request Builder 사용
            const builder = new RequestBuilder()
                .setMethod(options.method || 'GET')
                .setHeaders(options.headers || {});
            
            // POST/PUT/PATCH 요청의 경우 CSRF 토큰 추가
            if (['POST', 'PUT', 'PATCH', 'DELETE'].includes(builder.options.method)) {
                builder.setCsrfToken(this.getCsrfToken());
            }
            
            if (options.data) {
                builder.setBody(options.data);
            }
            
            let requestOptions = builder.build();
            
            // 요청 인터셉터 실행
            for (const interceptor of this.requestInterceptors) {
                requestOptions = await interceptor(requestOptions);
            }
            
            // 활성 요청 추적
            this.activeRequests.set(requestId, { url: fullUrl, startTime: Date.now() });
            
            try {
                // 재시도 로직과 함께 요청 실행
                const response = await this.retryRequest(async () => {
                    const res = await this.fetchWithTimeout(fullUrl, requestOptions);
                    
                    // 응답 상태 확인
                    if (!res.ok) {
                        const errorData = await this.parseErrorResponse(res);
                        throw new APIError(
                            errorData.message || `HTTP ${res.status} error`,
                            res.status,
                            errorData
                        );
                    }
                    
                    return res;
                });
                
                // 응답 파싱
                let data = await this.parseResponse(response);
                
                // 응답 인터셉터 실행
                for (const interceptor of this.responseInterceptors) {
                    data = await interceptor(data, response);
                }
                
                // 활성 요청에서 제거
                this.activeRequests.delete(requestId);
                
                return data;
                
            } catch (error) {
                // 활성 요청에서 제거
                this.activeRequests.delete(requestId);
                
                // 에러 처리
                await this.handleError(error);
                throw error;
            }
        }

        /**
         * 응답 파싱
         */
        async parseResponse(response) {
            const contentType = response.headers.get('content-type');
            
            if (contentType && contentType.includes('application/json')) {
                return response.json();
            } else if (contentType && contentType.includes('text/html')) {
                return response.text();
            } else {
                return response.blob();
            }
        }

        /**
         * 에러 응답 파싱
         */
        async parseErrorResponse(response) {
            try {
                const contentType = response.headers.get('content-type');
                if (contentType && contentType.includes('application/json')) {
                    return await response.json();
                } else {
                    const text = await response.text();
                    return { message: text };
                }
            } catch {
                return { message: 'Unknown error' };
            }
        }

        /**
         * 에러 처리
         */
        async handleError(error) {
            let handler;
            
            if (error instanceof APIError) {
                handler = this.errorHandlers.get(error.status);
            } else if (error.name === 'NetworkError' || !navigator.onLine) {
                handler = this.errorHandlers.get('network');
            } else {
                handler = this.errorHandlers.get('default');
            }
            
            if (handler) {
                await handler.call(this, error);
            }
            
            // 전역 에러 이벤트 발생
            window.dispatchEvent(new CustomEvent('api:error', {
                detail: { error }
            }));
        }

        /**
         * 요청 ID 생성
         */
        generateRequestId() {
            return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        }

        /**
         * 진행 중인 요청 취소
         */
        cancelRequest(requestId) {
            if (this.activeRequests.has(requestId)) {
                // AbortController를 사용한 취소 로직 구현 필요
                this.activeRequests.delete(requestId);
            }
        }

        /**
         * 모든 진행 중인 요청 취소
         */
        cancelAllRequests() {
            this.activeRequests.clear();
        }

        // ==================== 기본 에러 핸들러 ====================
        
        handleBadRequest(error) {
            console.error('Bad Request:', error);
            window.APP?.notify?.error('잘못된 요청입니다. 입력값을 확인해주세요.');
        }

        handleUnauthorized(error) {
            console.error('Unauthorized:', error);
            window.APP?.notify?.error('인증이 필요합니다. 다시 로그인해주세요.');
            // 로그인 페이지로 리다이렉트
            setTimeout(() => {
                window.location.href = '/';
            }, 2000);
        }

        handleForbidden(error) {
            console.error('Forbidden:', error);
            window.APP?.notify?.error('접근 권한이 없습니다.');
        }

        handleNotFound(error) {
            console.error('Not Found:', error);
            window.APP?.notify?.error('요청한 리소스를 찾을 수 없습니다.');
        }

        handleServerError(error) {
            console.error('Server Error:', error);
            window.APP?.notify?.error('서버 오류가 발생했습니다. 잠시 후 다시 시도해주세요.');
        }

        handleNetworkError(error) {
            console.error('Network Error:', error);
            window.APP?.notify?.error('네트워크 연결을 확인해주세요.');
        }

        handleTimeout(error) {
            console.error('Timeout:', error);
            window.APP?.notify?.error('요청 시간이 초과되었습니다. 다시 시도해주세요.');
        }

        // ==================== 편의 메서드 ====================
        
        get(url, options = {}) {
            return this.request(url, { ...options, method: 'GET' });
        }

        post(url, data, options = {}) {
            return this.request(url, { ...options, method: 'POST', data });
        }

        put(url, data, options = {}) {
            return this.request(url, { ...options, method: 'PUT', data });
        }

        patch(url, data, options = {}) {
            return this.request(url, { ...options, method: 'PATCH', data });
        }

        delete(url, options = {}) {
            return this.request(url, { ...options, method: 'DELETE' });
        }
    }

    // ==================== STR Dashboard 전용 API ====================
    
    class STRDashboardAPI extends APIService {
        constructor() {
            super();
            this.endpoints = {
                testConnection: '/api/test_oracle_connection/',
                queryAlert: '/api/query_alert_info/',
                queryPerson: '/api/query_person_info/',
                ruleHistory: '/api/rule_history_search/'
            };
        }

        /**
         * Oracle DB 연결 테스트
         */
        async testOracleConnection(connectionData) {
            return this.post(this.endpoints.testConnection, connectionData);
        }

        /**
         * Alert 정보 조회
         */
        async queryAlertInfo(alertId) {
            return this.post(this.endpoints.queryAlert, { alert_id: alertId });
        }

        /**
         * 고객 정보 조회
         */
        async queryPersonInfo(custId) {
            return this.post(this.endpoints.queryPerson, { cust_id: custId });
        }

        /**
         * Rule 히스토리 검색
         */
        async searchRuleHistory(ruleKey) {
            return this.post(this.endpoints.ruleHistory, { rule_key: ruleKey });
        }

        /**
         * 복합 조회 (Alert + Person + History)
         */
        async fetchCompleteAlertData(alertId) {
            try {
                // 1. Alert 정보 조회
                const alertData = await this.queryAlertInfo(alertId);
                
                if (!alertData.success) {
                    throw new APIError(alertData.message || '조회 실패', 400, alertData);
                }

                const result = {
                    alert: alertData,
                    person: null,
                    history: null
                };

                // 2. 추가 데이터 처리
                const cols = (alertData.columns || []).map(c => String(c || '').toUpperCase());
                const rows = alertData.rows || [];
                
                // 고객 ID 추출
                const custIdIndex = cols.indexOf('CUST_ID');
                const custId = rows[0]?.[custIdIndex];
                
                // Rule ID 추출
                const ruleIdIndex = cols.indexOf('STR_RULE_ID');
                const ruleIds = [...new Set(rows.map(r => r[ruleIdIndex]).filter(Boolean))];
                
                // 병렬 조회
                const promises = [];
                
                if (custId) {
                    promises.push(
                        this.queryPersonInfo(custId)
                            .then(data => { result.person = data; })
                            .catch(err => console.error('Person info fetch failed:', err))
                    );
                }
                
                if (ruleIds.length > 0) {
                    const ruleKey = ruleIds.sort().join(',');
                    promises.push(
                        this.searchRuleHistory(ruleKey)
                            .then(data => { result.history = data; })
                            .catch(err => console.error('Rule history fetch failed:', err))
                    );
                }
                
                await Promise.allSettled(promises);
                
                return result;
                
            } catch (error) {
                console.error('Complete alert data fetch failed:', error);
                throw error;
            }
        }
    }

    // ==================== 전역 등록 ====================
    
    // 싱글톤 인스턴스 생성
    const apiService = new STRDashboardAPI();
    
    // 전역 등록
    window.API = apiService;
    
    // jQuery 스타일 별칭 (선택사항)
    window.$api = {
        get: apiService.get.bind(apiService),
        post: apiService.post.bind(apiService),
        put: apiService.put.bind(apiService),
        patch: apiService.patch.bind(apiService),
        delete: apiService.delete.bind(apiService),
        
        // STR Dashboard 전용
        testConnection: apiService.testOracleConnection.bind(apiService),
        queryAlert: apiService.queryAlertInfo.bind(apiService),
        queryPerson: apiService.queryPersonInfo.bind(apiService),
        searchHistory: apiService.searchRuleHistory.bind(apiService),
        fetchComplete: apiService.fetchCompleteAlertData.bind(apiService)
    };

})(window);