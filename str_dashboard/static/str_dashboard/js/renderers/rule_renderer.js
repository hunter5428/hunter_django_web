// str_dashboard/static/str_dashboard/js/renderers/rule_renderer.js
/**
 * Rule 및 Alert 정보 관련 섹션 렌더링 모듈
 */
(function(window) {
    'use strict';

    if (!window.RendererBases) {
        console.error('RuleRenderer requires RendererBases to be loaded first.');
        return;
    }
    const { DataTable } = window.RendererBases;

    const RuleRenderer = {
        renderAlertHistory(cols, rows, alertId) {
            document.getElementById('section_alert_rule').style.display = 'block';
            new DataTable('result_table_alert_rule', {
                className: 'table alert-history-table',
                highlightRow: (row) => String(row[cols.indexOf('STR_ALERT_ID')]) === String(alertId),
                highlightClass: 'rep-row'
            }).setData(cols, rows).render();
        },

        renderRuleHistory(columns, rows, searchedRule, similarList) {
            document.getElementById('section_rule_hist').style.display = 'block';
            const container = document.getElementById('result_table_rule_hist');
            if (rows.length === 0) {
                container.innerHTML = this._buildRuleHistoryEmptyHTML(searchedRule, similarList);
            } else {
                new DataTable('result_table_rule_hist', {
                    className: 'table rule-history-table'
                }).setData(columns, rows).render();
            }
        },

        _buildRuleHistoryEmptyHTML(searchedRule, similarList) {
            // ... (기존 _renderRuleHistoryEmpty 로직과 동일)
            return `<div>... complex HTML for empty rule history ...</div>`; // 간단하게 표현
        },

        renderObjectives(cols, rows, ruleObjMap, canonicalIds, repRuleId) {
            document.getElementById('section_objectives').style.display = 'block';
            // ... (데이터 가공 로직)
            const tableRows = canonicalIds.map(ruleId => [ /* ... */ ]);
            new DataTable('result_table_objectives', {
                className: 'table objectives-table',
                highlightRow: (row) => String(row[0]) === String(repRuleId),
                highlightClass: 'rep-row',
                formatters: { '객관식정보': (value) => value.replace(/\n/g, '<br>') }
            }).setData(['STR_RULE_ID', 'STR_RULE_NM', '객관식정보'], tableRows).render();
        },

        renderRuleDesc(cols, rows, canonicalIds, repRuleId) {
            document.getElementById('section_rule_distinct').style.display = 'block';
             // ... (데이터 가공 및 DISTINCT 처리 로직)
            const uniqueRules = new Map();
            rows.forEach(row => { /* ... */ });
            const tableRows = canonicalIds.map(id => uniqueRules.get(id)).filter(Boolean);
            new DataTable('result_table_rule_distinct', {
                className: 'table rule-desc-table',
                highlightRow: (row) => String(row[0]) === String(repRuleId),
                highlightClass: 'rep-row'
            }).setData(['STR_RULE_ID', /* ... */], tableRows).render();
        }
    };

    window.RuleRenderer = RuleRenderer;

})(window);