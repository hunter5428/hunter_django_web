// str_dashboard/static/str_dashboard/js/renderers/base_table.js
/**
 * 테이블 렌더링을 위한 기본 클래스 모음
 */
(function(window) {
    'use strict';

    class BaseTable {
        constructor(containerId, options = {}) {
            this.container = document.getElementById(containerId);
            this.options = {
                emptyMessage: '데이터가 없습니다.',
                showHeader: true,
                className: 'table',
                ...options
            };
        }

        setData(columns, rows) {
            this.columns = columns || [];
            this.rows = rows || [];
            return this;
        }

        render() {
            if (!this.container) return;
            if (!this.rows || this.rows.length === 0) {
                this.renderEmpty();
                return;
            }
            this.renderTable();
        }

        renderEmpty() {
            this.container.innerHTML = `<div class="card empty-row">${this.escapeHtml(this.options.emptyMessage)}</div>`;
        }

        renderTable() {
            // 서브클래스에서 구현
            throw new Error('renderTable() must be implemented by subclasses.');
        }

        escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text || '';
            return div.innerHTML;
        }

        formatNumber(value) {
            if (typeof value !== 'number') return String(value || '');
            return value.toLocaleString('ko-KR');
        }
    }

    class KeyValueTable extends BaseTable {
        renderTable() {
            const table = document.createElement('table');
            table.className = this.options.className;
            const tbody = document.createElement('tbody');
            const row = this.rows[0];
            const validFields = this.columns.map((col, idx) => ({ col, value: row[idx] }))
                                            .filter(f => f.value != null && f.value !== '');

            for (let i = 0; i < validFields.length; i += 2) {
                const tr = document.createElement('tr');
                this.addKeyValuePair(tr, validFields[i]);
                if (i + 1 < validFields.length) {
                    this.addKeyValuePair(tr, validFields[i + 1]);
                } else {
                    tr.innerHTML += '<th>&nbsp;</th><td>&nbsp;</td>';
                }
                tbody.appendChild(tr);
            }
            table.appendChild(tbody);
            this.container.innerHTML = '';
            this.container.appendChild(table);
        }

        addKeyValuePair(tr, field) {
            const th = document.createElement('th');
            th.textContent = field.col;
            tr.appendChild(th);
            const td = document.createElement('td');
            td.innerHTML = this.escapeHtml(field.value);
            tr.appendChild(td);
        }
    }

    class DataTable extends BaseTable {
        renderTable() {
            const table = document.createElement('table');
            table.className = this.options.className;

            if (this.options.showHeader && this.columns.length > 0) {
                const thead = document.createElement('thead');
                const tr = document.createElement('tr');
                this.columns.forEach(col => {
                    tr.innerHTML += `<th>${this.escapeHtml(col)}</th>`;
                });
                thead.appendChild(tr);
                table.appendChild(thead);
            }

            const tbody = document.createElement('tbody');
            this.rows.forEach((row, rowIndex) => {
                const tr = document.createElement('tr');
                if (this.options.highlightRow && this.options.highlightRow(row, rowIndex)) {
                    tr.className = this.options.highlightClass || 'highlighted';
                }
                this.columns.forEach((col, colIndex) => {
                    const value = row[colIndex];
                    tr.innerHTML += `<td>${this.formatCellValue(value, col)}</td>`;
                });
                tbody.appendChild(tr);
            });
            table.appendChild(tbody);
            this.container.innerHTML = '';
            this.container.appendChild(table);
        }

        formatCellValue(value, columnName) {
            if (value == null) return '';
            if (this.options.formatters && this.options.formatters[columnName]) {
                return this.options.formatters[columnName](value);
            }
            if (typeof value === 'number') return this.formatNumber(value);
            return this.escapeHtml(String(value));
        }
    }

    // 전역 네임스페이스에 노출
    window.RendererBases = { BaseTable, KeyValueTable, DataTable };

})(window);