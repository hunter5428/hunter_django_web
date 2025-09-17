// str_dashboard/static/str_dashboard/js/page/data_processor.js
/**
 * API 응답 데이터를 가공하고 필요한 정보를 추출하는 데이터 처리 모듈
 */
(function(window) {
    'use strict';

    class DataProcessor {
        /**
         * ALERT 데이터에서 대표 RULE ID, 고객 ID, 중복 제거된 RULE ID 목록을 추출합니다.
         */
        static processAlertData(cols, rows, alertId) {
            const idxAlert = cols.indexOf('STR_ALERT_ID');
            const idxRule = cols.indexOf('STR_RULE_ID');
            const idxCust = cols.indexOf('CUST_ID');

            let repRuleId = null;
            let custIdForPerson = null;

            // 1. 대표 ALERT ID와 동일한 행을 찾아 대표 RULE ID와 고객 ID를 설정
            if (idxAlert >= 0 && idxRule >= 0) {
                const repRow = rows.find(r => String(r[idxAlert]) === alertId);
                if (repRow) {
                    repRuleId = String(repRow[idxRule]);
                    if (idxCust >= 0) {
                        custIdForPerson = repRow[idxCust];
                    }
                }
            }

            // 2. 대표 고객 ID가 없으면 첫 번째 행의 고객 ID를 사용
            if (!custIdForPerson && rows.length > 0 && idxCust >= 0) {
                custIdForPerson = rows[0][idxCust];
            }

            // 3. 전체 RULE ID 목록에서 중복을 제거하여 canonicalIds 생성
            const canonicalIds = [];
            if (idxRule >= 0) {
                const seen = new Set();
                rows.forEach(row => {
                    const ruleId = row[idxRule];
                    if (ruleId != null) {
                        const strId = String(ruleId).trim();
                        if (!seen.has(strId)) {
                            seen.add(strId);
                            canonicalIds.push(strId);
                        }
                    }
                });
            }

            return { repRuleId, custIdForPerson, canonicalIds };
        }

        /**
         * 거래 기간(TRAN_STRT, TRAN_END)과 KYC 완료일시를 기반으로 최종 조회 기간을 계산합니다.
         */
        static extractTransactionPeriod(cols, rows, kycDatetime = null) {
            const idxTranStart = cols.indexOf('TRAN_STRT');
            const idxTranEnd = cols.indexOf('TRAN_END');
            const idxRuleId = cols.indexOf('STR_RULE_ID');
            
            if (idxTranStart < 0 || idxTranEnd < 0) {
                return { start: null, end: null };
            }

            // 1. 특정 RULE ID 포함 여부 확인 (e.g., IO000, IO111)
            let hasSpecialRule = false;
            if (idxRuleId >= 0 && window.RuleUtils) {
                hasSpecialRule = rows.some(row => {
                    const ruleId = row[idxRuleId];
                    return ruleId && window.RuleUtils.getDayCountForRule(ruleId).isSpecial;
                });
            }
            
            // 2. 원본 데이터의 MIN(TRAN_STRT)과 MAX(TRAN_END) 추출
            let originalMinStart = null;
            let originalMaxEnd = null;
            rows.forEach(row => {
                const startDate = row[idxTranStart];
                const endDate = row[idxTranEnd];
                if (startDate && (!originalMinStart || startDate < originalMinStart)) originalMinStart = startDate;
                if (endDate && (!originalMaxEnd || endDate > originalMaxEnd)) originalMaxEnd = endDate;
            });

            // 3. 조회 기간 계산 (특정 RULE 12개월, 그 외 3개월)
            const monthsBack = hasSpecialRule ? 12 : 3;
            let calculatedStart = null;
            if (originalMaxEnd) {
                const endDateObj = new Date(originalMaxEnd.split(' ')[0]);
                endDateObj.setMonth(endDateObj.getMonth() - monthsBack);
                calculatedStart = endDateObj.toISOString().split('T')[0];
            }
            
            // 4. 최종 시작일 결정: MIN(MIN(TRAN_STRT), 계산된 시작일)
            let finalStartDate = originalMinStart ? originalMinStart.split(' ')[0] : null;
            if (finalStartDate && calculatedStart && calculatedStart < finalStartDate) {
                finalStartDate = calculatedStart;
            } else if (!finalStartDate && calculatedStart) {
                finalStartDate = calculatedStart;
            }

            // 5. KYC 완료일시 적용: KYC 날짜가 최종 시작일보다 나중이면 KYC 날짜를 시작일로 사용
            const kycDate = kycDatetime ? kycDatetime.split(' ')[0] : null;
            let useKycDate = false;
            if (kycDate && finalStartDate && kycDate > finalStartDate) {
                finalStartDate = kycDate;
                useKycDate = true;
            }

            const finalEndDate = originalMaxEnd ? originalMaxEnd.split(' ')[0] : null;

            return { 
                start: finalStartDate ? `${finalStartDate} 00:00:00.000000000` : null, 
                end: finalEndDate ? `${finalEndDate} 23:59:59.999999999` : null,
                monthsBack,
                original_min_start: originalMinStart ? originalMinStart.split(' ')[0] : null,
                original_max_end: finalEndDate,
                kyc_date: kycDate,
                used_kyc_date: useKycDate,
                has_special_rule: hasSpecialRule
            };
        }
    }

    window.DataProcessor = DataProcessor;

})(window);