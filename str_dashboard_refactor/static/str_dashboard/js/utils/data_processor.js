/**
 * 데이터 처리 유틸리티
 */
export class DataProcessor {
    static processAlertData(cols, rows, alertId) {
        const idxAlert = cols.indexOf('STR_ALERT_ID');
        const idxRule = cols.indexOf('STR_RULE_ID');
        const idxCust = cols.indexOf('CUST_ID');

        let repRuleId = null;
        let custIdForPerson = null;
        const canonicalIds = [];

        if (idxAlert >= 0 && idxRule >= 0) {
            const repRow = rows.find(r => String(r[idxAlert]) === alertId);
            repRuleId = repRow ? String(repRow[idxRule]) : null;
            if (repRow && idxCust >= 0) {
                custIdForPerson = repRow[idxCust];
            }
        }

        if (!custIdForPerson && rows.length && idxCust >= 0) {
            custIdForPerson = rows[0][idxCust];
        }

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

    static extractTransactionPeriod(cols, rows) {
        const idxTranStart = cols.indexOf('TRAN_STRT');
        const idxTranEnd = cols.indexOf('TRAN_END');
        const idxRuleId = cols.indexOf('STR_RULE_ID');
        
        if (idxTranStart < 0 || idxTranEnd < 0) {
            return { start: null, end: null, monthsBack: 3 };
        }
        
        let minStart = null;
        let maxEnd = null;
        let hasSpecialRule = false;
        
        // 특정 RULE ID 체크
        if (idxRuleId >= 0) {
            rows.forEach(row => {
                const ruleId = row[idxRuleId];
                if (ruleId === 'IO000' || ruleId === 'IO111') {
                    hasSpecialRule = true;
                }
            });
        }
        
        rows.forEach(row => {
            const startDate = row[idxTranStart];
            const endDate = row[idxTranEnd];
            
            if (startDate && /^\d{4}-\d{2}-\d{2}/.test(startDate)) {
                if (!minStart || startDate < minStart) {
                    minStart = startDate;
                }
            }
            
            if (endDate && /^\d{4}-\d{2}-\d{2}/.test(endDate)) {
                if (!maxEnd || endDate > maxEnd) {
                    maxEnd = endDate;
                }
            }
        });
        
        // 특정 RULE ID가 있으면 12개월, 없으면 3개월 이전
        const monthsBack = hasSpecialRule ? 12 : 3;
        
        if (minStart) {
            const startDateObj = new Date(minStart);
            startDateObj.setMonth(startDateObj.getMonth() - monthsBack);
            minStart = startDateObj.toISOString().split('T')[0] + ' 00:00:00.000000000';
        }
        
        if (maxEnd) {
            maxEnd = maxEnd.includes(' ') ? maxEnd : maxEnd + ' 23:59:59.999999999';
        }
        
        return { start: minStart, end: maxEnd, monthsBack };
    }

    static extractDuplicateParams(columns, row) {
        const getColumnValue = (colName) => {
            const idx = columns.indexOf(colName);
            return idx >= 0 ? (row[idx] || '') : '';
        };
        
        const phone = getColumnValue('연락처');
        const phoneSuffix = phone.length >= 4 ? phone.slice(-4) : '';
        
        return {
            full_email: getColumnValue('이메일'),
            phone_suffix: phoneSuffix,
            address: getColumnValue('거주지주소'),
            detail_address: getColumnValue('거주지상세주소'),
            workplace_name: getColumnValue('직장명'),
            workplace_address: getColumnValue('직장주소'),
            workplace_detail_address: getColumnValue('직장상세주소') || ''
        };
    }

    static buildMatchCriteria(params, custType) {
        return {
            email_prefix: params.full_email ? params.full_email.split('@')[0] : null,
            full_email: params.full_email || null,
            phone_suffix: params.phone_suffix || null,
            address: params.address || null,
            detail_address: params.detail_address || null,
            workplace_name: params.workplace_name || null,
            workplace_address: params.workplace_address || null,
            workplace_detail_address: params.workplace_detail_address || null,
            customer_type: custType
        };
    }

    static extractColumnValue(columns, row, columnName) {
        try {
            const idx = columns.indexOf(columnName);
            if (idx >= 0 && idx < row.length) {
                return row[idx];
            }
        } catch (error) {
            console.error('Error extracting column value:', error);
        }
        return null;
    }

    static formatCurrency(amount, unit = '원') {
        if (amount == 0) return `0${unit}`;
        
        const absAmount = Math.abs(amount);
        
        if (absAmount >= 100000000) {  // 1억 이상
            const eok = Math.floor(absAmount / 100000000);
            const man = Math.floor((absAmount % 100000000) / 10000);
            if (man > 0) {
                return `${eok.toLocaleString('ko-KR')}억 ${man.toLocaleString('ko-KR')}만${unit}`;
            }
            return `${eok.toLocaleString('ko-KR')}억${unit}`;
        } else if (absAmount >= 10000) {  // 1만 이상
            const man = Math.floor(absAmount / 10000);
            return `${man.toLocaleString('ko-KR')}만${unit}`;
        } else {
            return `${Math.floor(absAmount).toLocaleString('ko-KR')}${unit}`;
        }
    }

    static cleanSqlValue(value) {
        if (value === null || value === undefined) return null;
        
        if (typeof value === 'string') {
            value = value.trim();
            if (value === '') return null;
        }
        
        return value;
    }

    static calculateAge(birthDate) {
        if (!birthDate) return null;
        
        try {
            const birthClean = String(birthDate).replace(/[^0-9]/g, '');
            
            if (birthClean.length >= 8) {
                const year = parseInt(birthClean.substr(0, 4));
                const month = parseInt(birthClean.substr(4, 2));
                const day = parseInt(birthClean.substr(6, 2));
                
                const birth = new Date(year, month - 1, day);
                const today = new Date();
                
                let age = today.getFullYear() - birth.getFullYear();
                const monthDiff = today.getMonth() - birth.getMonth();
                
                if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < birth.getDate())) {
                    age--;
                }
                
                return age;
            }
        } catch (error) {
            console.error('Error calculating age:', error);
        }
        
        return null;
    }
}