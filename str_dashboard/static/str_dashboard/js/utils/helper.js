// str_dashboard/static/str_dashboard/js/utils/helpers.js
/**
 * 공통 유틸리티 함수 모음
 */
(function(window) {
    'use strict';

    /**
     * DOM 요소를 선택합니다.
     * @param {string} selector - CSS 선택자
     * @returns {Element|null}
     */
    const $ = (selector) => document.querySelector(selector);

    /**
     * 여러 DOM 요소를 선택합니다.
     * @param {string} selector - CSS 선택자
     * @returns {NodeListOf<Element>}
     */
    const $$ = (selector) => document.querySelectorAll(selector);

    /**
     * 쿠키 값을 가져옵니다.
     * @param {string} name - 쿠키 이름
     * @returns {string|undefined}
     */
    const getCookie = (name) => {
        const value = `; ${document.cookie}`;
        const parts = value.split(`; ${name}=`);
        return parts.length === 2 ? decodeURIComponent(parts.pop().split(';').shift()) : undefined;
    };
    
    /**
     * 서버 세션에 데이터를 저장합니다. (비동기, 오류 무시)
     * @param {string} key - 세션에 저장할 키
     * @param {object} data - 저장할 데이터 (JSON 직렬화 가능해야 함)
     */
    const saveToSession = (key, data) => {
        fetch('/api/save_to_session/', { // 이 URL은 실제 프로젝트의 엔드포인트에 맞게 수정해야 합니다.
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                'X-CSRFToken': getCookie('csrftoken')
            },
            body: new URLSearchParams({
                key: key,
                data: JSON.stringify(data)
            })
        }).catch(error => {
            console.error(`Session save error for key "${key}":`, error);
        });
    };

    // 전역 window 객체에 노출
    window.AppHelpers = {
        $,
        $$,
        getCookie,
        saveToSession
    };

})(window);