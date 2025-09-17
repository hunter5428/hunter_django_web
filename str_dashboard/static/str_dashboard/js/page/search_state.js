// str_dashboard/static/str_dashboard/js/page/search_state.js
/**
 * ALERT ID 조회 페이지의 상태를 관리하는 클래스
 */
(function(window) {
    'use strict';

    class SearchState {
        constructor() {
            this.reset();
        }

        /**
         * 모든 상태를 초기화합니다.
         */
        reset() {
            this.currentAlertId = null;
            this.alertData = null; // 초기 ALERT 조회 결과 및 처리된 데이터
            this.customerData = null; // 고객 정보
            this.isSearching = false;
            this.abortController = null; // (현재 미사용, 추후 요청 취소 기능 추가시 사용)
        }

        /**
         * 검색 진행 상태를 설정합니다.
         * @param {boolean} value - 검색 중 여부
         */
        setSearching(value) {
            this.isSearching = value;
        }

        /**
         * ALERT 조회 데이터를 상태에 저장합니다.
         * @param {object} data - 저장할 데이터
         */
        setAlertData(data) {
            this.alertData = data;
        }

        /**
         * 현재 진행 중인 fetch 요청을 중단합니다. (미사용)
         */
        abort() {
            if (this.abortController) {
                this.abortController.abort();
                this.abortController = null;
            }
        }
    }

    window.SearchState = SearchState;

})(window);