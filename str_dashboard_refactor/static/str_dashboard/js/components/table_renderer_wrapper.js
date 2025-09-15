// str_dashboard/static/str_dashboard/js/modules/table_renderer_wrapper.js

/**
 * 기존 table_renderer.js를 ES6 모듈로 래핑
 * 기존 코드는 IIFE로 작성되어 있어서 window.TableRenderer로 접근
 */

export class TableRendererWrapper {
    constructor() {
        // 기존 TableRenderer가 로드될 때까지 대기
        this.renderer = null;
        this.initialized = false;
        this.initPromise = this._initialize();
    }

    async _initialize() {
        // TableRenderer가 로드될 때까지 대기
        let attempts = 0;
        const maxAttempts = 50;
        
        while (!window.TableRenderer && attempts < maxAttempts) {
            await new Promise(resolve => setTimeout(resolve, 100));
            attempts++;
        }
        
        if (window.TableRenderer) {
            this.renderer = window.TableRenderer;
            this.initialized = true;
            console.log('TableRendererWrapper initialized successfully');
            return true;
        } else {
            console.error('TableRenderer not found after waiting');
            throw new Error('TableRenderer initialization failed');
        }
    }

    async ensureInitialized() {
        if (!this.initialized) {
            await this.initPromise;
        }
    }

    // ==================== 프록시 메서드들 ====================
    
    async renderCustomerUnified(columns, rows) {
        await this.ensureInitialized();
        return this.renderer.renderCustomerUnified(columns, rows);
    }

    async renderCorpRelated(columns, rows) {
        await this.ensureInitialized();
        return this.renderer.renderCorpRelated(columns, rows);
    }

    async renderPersonRelated(relatedPersonsData) {
        await this.ensureInitialized();
        return this.renderer.renderPersonRelated(relatedPersonsData);
    }

    async renderRuleHistory(columns, rows, searchedRule, similarList) {
        await this.ensureInitialized();
        return this.renderer.renderRuleHistory(columns, rows, searchedRule, similarList);
    }

    async renderAlertHistory(cols, rows, alertId) {
        await this.ensureInitialized();
        return this.renderer.renderAlertHistory(cols, rows, alertId);
    }

    async renderObjectives(cols, rows, ruleObjMap, canonicalIds, repRuleId) {
        await this.ensureInitialized();
        return this.renderer.renderObjectives(cols, rows, ruleObjMap, canonicalIds, repRuleId);
    }

    async renderRuleDesc(cols, rows, canonicalIds, repRuleId) {
        await this.ensureInitialized();
        return this.renderer.renderRuleDesc(cols, rows, canonicalIds, repRuleId);
    }

    async renderDuplicatePersons(columns, rows, matchCriteria) {
        await this.ensureInitialized();
        return this.renderer.renderDuplicatePersons(columns, rows, matchCriteria);
    }

    async renderIPHistory(columns, rows) {
        await this.ensureInitialized();
        return this.renderer.renderIPHistory(columns, rows);
    }

    async renderOrderbookAnalysis(analysisResult, alertData) {
        await this.ensureInitialized();
        return this.renderer.renderOrderbookAnalysis(analysisResult, alertData);
    }

    // ==================== 헬퍼 메서드들 ====================
    
    /**
     * 섹션 토글 상태 관리
     */
    toggleSection(sectionId) {
        const section = document.getElementById(sectionId);
        if (section) {
            section.classList.toggle('collapsed');
            
            // 상태 저장
            const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
            states[sectionId] = section.classList.contains('collapsed');
            localStorage.setItem('sectionStates', JSON.stringify(states));
        }
    }

    /**
     * 저장된 섹션 상태 복원
     */
    restoreSectionStates() {
        const states = JSON.parse(localStorage.getItem('sectionStates') || '{}');
        Object.entries(states).forEach(([sectionId, isCollapsed]) => {
            const section = document.getElementById(sectionId);
            if (section) {
                section.classList.toggle('collapsed', isCollapsed);
            }
        });
    }

    /**
     * 모든 섹션 숨기기
     */
    hideAllSections() {
        const sections = document.querySelectorAll('.section');
        sections.forEach(section => {
            section.style.display = 'none';
            const container = section.querySelector('.table-wrap');
            if (container) {
                container.innerHTML = '';
            }
        });
    }

    /**
     * 특정 섹션 표시
     */
    showSection(sectionId) {
        const section = document.getElementById(sectionId);
        if (section) {
            section.style.display = 'block';
        }
    }

    /**
     * 에러 메시지 렌더링
     */
    renderError(containerId, message) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = `
                <div class="card empty-row">
                    ${this.escapeHtml(message || '데이터를 불러올 수 없습니다.')}
                </div>
            `;
        }
    }

    /**
     * 로딩 상태 표시
     */
    renderLoading(containerId) {
        const container = document.getElementById(containerId);
        if (container) {
            container.innerHTML = `
                <div class="alert-loading">데이터 로딩 중...</div>
            `;
        }
    }

    /**
     * HTML 이스케이프
     */
    escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text || '';
        return div.innerHTML;
    }

    /**
     * 숫자 포맷팅
     */
    formatNumber(value) {
        if (typeof value !== 'number') return String(value || '');
        return value.toLocaleString('ko-KR');
    }

    /**
     * 금액 포맷팅 (억원, 만원 단위)
     */
    formatAmount(amount) {
        const absAmount = Math.abs(amount);
        if (absAmount === 0) return '0원';
        
        const units = [
            { value: 1000000000000, name: '조' },
            { value: 100000000, name: '억' },
            { value: 10000, name: '만' }
        ];
        
        let result = [];
        let remaining = absAmount;
        
        for (const unit of units) {
            if (remaining >= unit.value) {
                const unitAmount = Math.floor(remaining / unit.value);
                result.push(`${unitAmount.toLocaleString('ko-KR')}${unit.name}`);
                remaining = remaining % unit.value;
            }
        }
        
        if (result.length === 0) {
            return `${absAmount.toLocaleString('ko-KR')}원`;
        }
        
        // 남은 금액이 있으면 추가
        if (remaining >= 10000) {
            const manAmount = Math.floor(remaining / 10000);
            result.push(`${manAmount.toLocaleString('ko-KR')}만`);
        }
        
        return result.join(' ') + '원';
    }

    /**
     * 날짜 포맷팅
     */
    formatDate(dateStr, format = 'YYYY-MM-DD') {
        if (!dateStr) return '';
        
        try {
            const date = new Date(dateStr);
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            const hours = String(date.getHours()).padStart(2, '0');
            const minutes = String(date.getMinutes()).padStart(2, '0');
            const seconds = String(date.getSeconds()).padStart(2, '0');
            
            return format
                .replace('YYYY', year)
                .replace('MM', month)
                .replace('DD', day)
                .replace('HH', hours)
                .replace('mm', minutes)
                .replace('ss', seconds);
        } catch (e) {
            return dateStr;
        }
    }

    /**
     * 테이블 정렬 기능 추가
     */
    enableTableSort(tableId) {
        const table = document.getElementById(tableId);
        if (!table) return;
        
        const headers = table.querySelectorAll('th.sortable');
        headers.forEach((header, index) => {
            header.addEventListener('click', () => {
                this.sortTable(table, index, header);
            });
        });
    }

    /**
     * 테이블 정렬 수행
     */
    sortTable(table, columnIndex, header) {
        const tbody = table.querySelector('tbody');
        if (!tbody) return;
        
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const isAscending = header.classList.contains('sort-asc');
        
        // 정렬 상태 초기화
        table.querySelectorAll('th').forEach(th => {
            th.classList.remove('sort-asc', 'sort-desc');
        });
        
        // 정렬
        rows.sort((a, b) => {
            const aValue = a.cells[columnIndex]?.textContent || '';
            const bValue = b.cells[columnIndex]?.textContent || '';
            
            // 숫자 정렬 시도
            const aNum = parseFloat(aValue.replace(/[^0-9.-]/g, ''));
            const bNum = parseFloat(bValue.replace(/[^0-9.-]/g, ''));
            
            if (!isNaN(aNum) && !isNaN(bNum)) {
                return isAscending ? bNum - aNum : aNum - bNum;
            }
            
            // 문자열 정렬
            return isAscending ? 
                bValue.localeCompare(aValue, 'ko-KR') : 
                aValue.localeCompare(bValue, 'ko-KR');
        });
        
        // DOM 업데이트
        rows.forEach(row => tbody.appendChild(row));
        
        // 정렬 상태 업데이트
        header.classList.add(isAscending ? 'sort-desc' : 'sort-asc');
    }

    /**
     * 테이블 필터링 기능
     */
    enableTableFilter(tableId, filterInputId) {
        const table = document.getElementById(tableId);
        const filterInput = document.getElementById(filterInputId);
        
        if (!table || !filterInput) return;
        
        filterInput.addEventListener('input', (e) => {
            const filter = e.target.value.toLowerCase();
            const tbody = table.querySelector('tbody');
            if (!tbody) return;
            
            const rows = tbody.querySelectorAll('tr');
            rows.forEach(row => {
                const text = row.textContent.toLowerCase();
                row.style.display = text.includes(filter) ? '' : 'none';
            });
        });
    }

    /**
     * CSV 내보내기
     */
    exportTableToCSV(tableId, filename = 'export.csv') {
        const table = document.getElementById(tableId);
        if (!table) return;
        
        const rows = [];
        
        // 헤더 추출
        const headers = Array.from(table.querySelectorAll('thead th'))
            .map(th => th.textContent.trim());
        if (headers.length > 0) {
            rows.push(headers);
        }
        
        // 데이터 추출
        const tbody = table.querySelector('tbody');
        if (tbody) {
            tbody.querySelectorAll('tr').forEach(tr => {
                const row = Array.from(tr.querySelectorAll('td'))
                    .map(td => td.textContent.trim());
                rows.push(row);
            });
        }
        
        // CSV 생성
        const csv = rows.map(row => 
            row.map(cell => `"${cell.replace(/"/g, '""')}"`).join(',')
        ).join('\n');
        
        // BOM 추가 (Excel에서 한글 깨짐 방지)
        const bom = '\uFEFF';
        const blob = new Blob([bom + csv], { type: 'text/csv;charset=utf-8;' });
        
        // 다운로드
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = filename;
        link.click();
        
        // 메모리 정리
        URL.revokeObjectURL(link.href);
    }

    /**
     * 테이블 인쇄
     */
    printTable(tableId) {
        const table = document.getElementById(tableId);
        if (!table) return;
        
        const printWindow = window.open('', '_blank');
        printWindow.document.write(`
            <html>
                <head>
                    <title>인쇄</title>
                    <style>
                        table { border-collapse: collapse; width: 100%; }
                        th, td { border: 1px solid #000; padding: 8px; text-align: left; }
                        th { background-color: #f0f0f0; font-weight: bold; }
                        @media print {
                            .no-print { display: none; }
                        }
                    </style>
                </head>
                <body>
                    ${table.outerHTML}
                </body>
            </html>
        `);
        
        printWindow.document.close();
        printWindow.print();
    }
}

// 싱글톤 인스턴스 생성 및 내보내기
const tableRendererWrapper = new TableRendererWrapper();
export default tableRendererWrapper;