// str_dashboard/static/str_dashboard/js/renderers/orderbook_renderer.js
/**
 * Orderbook(거래원장) 분석 결과 렌더링 모듈
 */
(function(window) {
    'use strict';
    
    // AppHelpers.getCookie 와 같은 공용 유틸리티가 필요하다면 여기서 로드 확인
    if (!window.AppHelpers) {
        console.error('OrderbookRenderer requires AppHelpers to be loaded first.');
        return;
    }
    const { getCookie } = window.AppHelpers;


    const OrderbookRenderer = {
        renderOrderbookAnalysis(analysisResult, alertData) {
            this._createOrderbookSections();
            const periodInfo = this._extractPeriodInfo(analysisResult, alertData);

            this._renderPatternSection('result_orderbook_patterns', `의심거래기간_${periodInfo.dayCountLabel}+(${periodInfo.startStr}~${periodInfo.endStr})`, analysisResult.patterns);
            this._renderPatternSection('result_orderbook_minmax', `의심거래기간_MIN_MAX기간(${periodInfo.displayStartDate}~${periodInfo.end_date})`, analysisResult.patterns);
            
            this._renderStdsDtmSummary(alertData, analysisResult, periodInfo);
            this._renderAlertOrderbook(analysisResult, alertData);
            this._renderOrderbookDaily(analysisResult.daily_summary);
        },
        
        // ... (기존 _createOrderbookSections, _renderOrderbookPatterns 등 모든 private 헬퍼 메서드들을 여기에 포함)
        _createOrderbookSections() { /* ... */ },
        _extractPeriodInfo(analysisResult, alertData) { /* ... */ return periodInfo; },
        _renderPatternSection(containerId, title, patterns) { /* ... */ },
        _renderStdsDtmSummary(alertData, analysisResult, periodInfo) { /* ... */ },
        _renderAlertOrderbook(analysisResult, alertData) { /* ... */ },
        _renderOrderbookDaily(dailySummary) { /* ... */ },
        _formatAmount(amount) { /* ... */ },

        _getCookie // _loadAlertDetail 등에서 사용하기 위함
    };

    window.OrderbookRenderer = OrderbookRenderer;

})(window);