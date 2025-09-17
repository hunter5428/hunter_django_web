// str_dashboard/static/str_dashboard/js/utils/api_client.js
/**
 * 서버 API 호출을 위한 클라이언트 모듈
 */
(function(window) {
    'use strict';

    // AppHelpers가 먼저 로드되어야 합니다.
    if (!window.AppHelpers) {
        console.error('APIClient requires AppHelpers to be loaded first.');
        return;
    }

    const { getCookie } = window.AppHelpers;

    class APIClient {
        constructor(baseHeaders = {}) {
            this.headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-CSRFToken': getCookie('csrftoken'),
                ...baseHeaders
            };
        }

        /**
         * POST 요청을 보냅니다.
         * @param {string} url - 요청할 URL
         * @param {object} data - 전송할 데이터
         * @returns {Promise<object>} - 서버의 JSON 응답
         */
        async post(url, data) {
            try {
                const response = await fetch(url, {
                    method: 'POST',
                    headers: this.headers,
                    body: new URLSearchParams(data)
                });

                // HTML 응답 체크 (로그인 페이지 리다이렉트 등 세션 만료 감지)
                const contentType = response.headers.get('content-type');
                if (!contentType || !contentType.includes('application/json')) {
                    throw new Error('서버 응답이 JSON 형식이 아닙니다. 세션이 만료되었을 수 있습니다.');
                }

                return await response.json();
            } catch (error) {
                console.error(`API call to ${url} failed:`, error);
                // 에러를 다시 던져 호출한 쪽에서 처리하도록 함
                throw error;
            }
        }
    }

    // 전역 window 객체에 노출
    window.APIClient = APIClient;

})(window);