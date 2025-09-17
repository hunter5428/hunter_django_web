// str_dashboard/static/str_dashboard/js/renderers/customer_renderer.js
/**
 * 고객 정보 관련 섹션 렌더링 모듈
 */
(function(window) {
    'use strict';

    if (!window.RendererBases) {
        console.error('CustomerRenderer requires RendererBases to be loaded first.');
        return;
    }
    const { KeyValueTable, DataTable } = window.RendererBases;

    const CustomerRenderer = {
        renderCustomerUnified(columns, rows) {
            document.getElementById('section_customer_unified').style.display = 'block';
            new KeyValueTable('result_table_customer_unified', {
                className: 'customer-unified-table',
                emptyMessage: '고객 정보가 없습니다.'
            }).setData(columns, rows).render();
        },

        renderCorpRelated(columns, rows) {
            const section = document.getElementById('section_corp_related');
            section.style.display = 'block';
            new DataTable('result_table_corp_related', {
                className: 'table corp-related-table',
                emptyMessage: '등록된 관련인 정보가 없습니다.'
            }).setData(columns, rows).render();
        },
        
        renderPersonRelated(relatedPersonsData) {
            const section = document.getElementById('section_person_related');
            const container = document.getElementById('result_table_person_related');
            section.style.display = 'block';

            if (!relatedPersonsData || Object.keys(relatedPersonsData).length === 0) {
                container.innerHTML = '<div class="card empty-row">내부입출금 거래 관련인 정보가 없습니다.</div>';
                return;
            }

            let html = Object.entries(relatedPersonsData).map(([custId, data], index) => {
                return this._buildPersonRelatedHTML(index + 1, custId, data.info, data.transactions || []);
            }).join('');
            
            container.innerHTML = html || '<div class="card empty-row">내부입출금 거래 관련인 정보가 없습니다.</div>';
        },
        
        _buildPersonRelatedHTML(index, custId, info, transactions) {
            // ... (기존 _renderPersonRelatedHTML 로직과 동일)
            return `<div>... complex HTML for person ${custId} ...</div>`; // 간단하게 표현
        },

        renderDuplicatePersons(columns, rows, matchCriteria) {
             const section = document.getElementById('section_duplicate_persons');
             section.style.display = 'block';
             const container = document.getElementById('result_table_duplicate_persons');

             if (!rows || rows.length === 0) {
                 container.innerHTML = '<div class="card empty-row">동일한 정보를 가진 회원이 조회되지 않습니다.</div>';
                 return;
             }

             container.innerHTML = '<div class="duplicate-persons-wrapper">' +
                 rows.map(row => this._buildDuplicatePersonHTML(columns, row, matchCriteria)).join('') +
                 '</div>';
        },

        _buildDuplicatePersonHTML(columns, row, matchCriteria) {
            // ... (기존 _renderDuplicatePersonHTML 로직과 동일)
             return `<div>... complex HTML for duplicate person ...</div>`; // 간단하게 표현
        },

        renderIPHistory(columns, rows) {
            const section = document.getElementById('section_ip_access_history');
            section.style.display = 'block';
            section.classList.add('collapsed');

            new DataTable('result_table_ip_access_history', {
                className: 'table ip-history-table',
                emptyMessage: 'IP 접속 이력이 없습니다.',
                highlightRow: (row) => {
                    const countryIdx = columns.indexOf('국가한글명');
                    const country = countryIdx >= 0 ? String(row[countryIdx] || '').trim() : '';
                    return country && country !== '대한민국' && country !== '한국';
                },
                highlightClass: 'rep-row'
            }).setData(columns, rows).render();
        }
    };

    // 전역 네임스페이스에 추가
    window.CustomerRenderer = CustomerRenderer;

})(window);