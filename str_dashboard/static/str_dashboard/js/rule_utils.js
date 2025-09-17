/**
 * 규칙별 일수 조회 유틸리티
 * 특정 RULE ID에 따라 다른 조회 기간을 적용하기 위한 유틸리티 함수
 */

(function() {
    'use strict';

    // 규칙 데이터를 저장할 전역 객체
    let ruleData = null;
    
    /**
     * 규칙 데이터 로드 함수 (비동기)
     * @returns {Promise} 로드 완료 Promise
     */
    async function loadRuleData() {
        if (ruleData !== null) {
            return Promise.resolve(ruleData);
        }
        
        try {
            const response = await fetch('/static/str_dashboard/js/rule_daycount.json');
            if (!response.ok) {
                throw new Error('규칙 데이터 로드 실패');
            }
            
            ruleData = await response.json();
            console.log('규칙 데이터 로드 완료:', ruleData);
            return ruleData;
        } catch (error) {
            console.error('규칙 데이터 로드 오류:', error);
            // 오류 발생시 기본값 사용
            ruleData = {
                special_rules: [
                    { rule_id: "IO000", days: 365, label: "365일" },
                    { rule_id: "IO111", days: 365, label: "365일" }
                ],
                default: { days: 90, label: "90일" }
            };
            return ruleData;
        }
    }
    
    /**
     * 특정 RULE ID에 대한 일수 설정 조회
     * @param {string} ruleId - RULE ID
     * @returns {Object} { days, label, isSpecial } 일수 정보
     */
    function getDayCountForRule(ruleId) {
        if (!ruleData) {
            // 아직 로드되지 않은 경우 기본값 사용
            const isSpecial = ruleId === 'IO000' || ruleId === 'IO111';
            return {
                days: isSpecial ? 365 : 90,
                label: isSpecial ? '365일' : '90일',
                isSpecial: isSpecial
            };
        }
        
        // 특별 규칙 찾기
        const specialRule = ruleData.special_rules.find(rule => rule.rule_id === ruleId);
        
        if (specialRule) {
            return {
                days: specialRule.days,
                label: specialRule.label,
                isSpecial: true
            };
        }
        
        // 기본값 반환
        return {
            days: ruleData.default.days,
            label: ruleData.default.label,
            isSpecial: false
        };
    }
    
    // 글로벌 객체에 노출
    window.RuleUtils = {
        loadRuleData: loadRuleData,
        getDayCountForRule: getDayCountForRule
    };
    
    // 페이지 로드 시 규칙 데이터 자동 로드
    document.addEventListener('DOMContentLoaded', function() {
        loadRuleData().catch(console.error);
    });
    
})();
