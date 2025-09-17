// str_dashboard/static/str_dashboard/js/base.js
// 기본 레이아웃 JavaScript - 간소화 버전

(function() {
    'use strict';

    // 유틸리티
    const $ = (selector) => document.querySelector(selector);
    const $$ = (selectors) => document.querySelectorAll(selectors);

    // 쿠키 관리
    const CookieManager = {
        get(name) {
            const value = `; ${document.cookie}`;
            const parts = value.split(`; ${name}=`);
            return parts.length === 2 ? 
                decodeURIComponent(parts.pop().split(';').shift()) : 
                undefined;
        },
        set(name, value, days = 365) {
            const date = new Date();
            date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
            document.cookie = `${name}=${encodeURIComponent(value)};expires=${date.toUTCString()};path=/`;
        }
    };

    // 사이드바 관리
    class SidebarManager {
        constructor() {
            this.body = document.body;
            this.logoBtn = $('#logo-toggle');
            this.init();
        }

        init() {
            if (this.logoBtn) {
                this.logoBtn.addEventListener('click', (e) => {
                    e.preventDefault();
                    this.toggle();
                });
            }
        }

        toggle() {
            const isCollapsed = this.body.classList.contains('sidebar-collapsed');
            if (isCollapsed) {
                this.body.classList.remove('sidebar-collapsed');
                CookieManager.set('sidebar', 'open');
            } else {
                this.body.classList.add('sidebar-collapsed');
                CookieManager.set('sidebar', 'collapsed');
            }
        }
    }

    // 메뉴 활성화 상태 설정
    class MenuManager {
        constructor() {
            this.init();
        }

        init() {
            const currentTop = document.body.dataset.topMenu;
            const currentSub = document.body.dataset.subMenu;

            if (currentTop) {
                const topEl = $(`[data-topmenu="${currentTop}"]`);
                if (topEl) topEl.classList.add('active');
            }

            if (currentSub) {
                const subEl = $(`[data-submenu="${currentSub}"]`);
                if (subEl) subEl.classList.add('active');
            }

            // 로그아웃 확인
            const logoutBtn = $('#logout-btn');
            if (logoutBtn) {
                logoutBtn.addEventListener('click', (e) => {
                    if (!confirm('로그아웃 하시겠습니까?')) {
                        e.preventDefault();
                    }
                });
            }
        }
    }

    // 초기화
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => {
            new SidebarManager();
            new MenuManager();
        });
    } else {
        new SidebarManager();
        new MenuManager();
    }

})();