// str_dashboard/static/str_dashboard/js/base.js

/**
 * 기본 레이아웃 JavaScript 모듈
 */
(function() {
    'use strict';

    // ==================== 유틸리티 ==================== 
    const $ = (selector) => document.querySelector(selector);
    const $$ = (selectors) => document.querySelectorAll(selectors);

    /**
     * 쿠키 관리 유틸리티
     */
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
            const expires = `expires=${date.toUTCString()}`;
            document.cookie = `${name}=${encodeURIComponent(value)};${expires};path=/`;
        },
        
        delete(name) {
            document.cookie = `${name}=;expires=Thu, 01 Jan 1970 00:00:00 UTC;path=/`;
        }
    };

    /**
     * 세션/로컬 스토리지 관리
     */
    const StorageManager = {
        clearAll() {
            try {
                sessionStorage.clear();
                localStorage.removeItem('app_temp');
                console.log('Storage cleared');
            } catch(e) {
                console.error('Failed to clear storage:', e);
            }
        }
    };

    // ==================== 사이드바 관리 ==================== 
    class SidebarManager {
        constructor() {
            this.body = document.body;
            this.logoBtn = $('#logo-toggle');
            this.logoImg = $('#logo-img');
            this.init();
        }

        init() {
            if (this.logoBtn) {
                this.logoBtn.addEventListener('click', (e) => {
                    e.preventDefault();
                    e.stopPropagation();
                    this.toggle();
                });
            }

            // 모바일에서 외부 클릭 시 사이드바 닫기
            if (window.innerWidth <= 768) {
                document.addEventListener('click', (e) => {
                    const sidebar = $('.app-sidebar');
                    if (sidebar && !sidebar.contains(e.target) && 
                        !this.logoBtn.contains(e.target) &&
                        this.body.classList.contains('sidebar-open')) {
                        this.close();
                    }
                });
            }
        }

        toggle() {
            const isCollapsed = this.body.classList.contains('sidebar-collapsed');
            
            if (isCollapsed) {
                this.open();
            } else {
                this.close();
            }

            // 로고 회전 애니메이션
            this.animateLogo();
        }

        open() {
            this.body.classList.remove('sidebar-collapsed');
            this.body.classList.add('sidebar-open');
            CookieManager.set('sidebar', 'open');
        }

        close() {
            this.body.classList.add('sidebar-collapsed');
            this.body.classList.remove('sidebar-open');
            CookieManager.set('sidebar', 'collapsed');
        }

        animateLogo() {
            if (this.logoImg) {
                this.logoImg.classList.remove('spin');
                void this.logoImg.offsetWidth; // 리플로우 강제
                this.logoImg.classList.add('spin');
                
                this.logoImg.addEventListener('animationend', () => {
                    this.logoImg.classList.remove('spin');
                }, { once: true });
            }
        }
    }

    // ==================== 네비게이션 관리 ==================== 
    class NavigationManager {
        constructor() {
            this.currentPath = window.location.pathname;
            this.currentTop = this.getCurrentTopMenu();
            this.currentSub = this.getCurrentSubMenu();
            this.init();
        }

        init() {
            this.setupMenuHandlers();
            this.setActiveStates();
            this.setupNavigationWarnings();
        }

        getCurrentTopMenu() {
            // body의 data 속성이나 전역 변수에서 가져오기
            return window.APP_CONFIG?.currentTopMenu || 
                   document.body.dataset.topMenu || '';
        }

        getCurrentSubMenu() {
            return window.APP_CONFIG?.currentSubMenu || 
                   document.body.dataset.subMenu || '';
        }

        setupMenuHandlers() {
            // 상위 메뉴 클릭 핸들러
            $$('[data-topmenu]').forEach(menuEl => {
                menuEl.addEventListener('click', (e) => {
                    const menuName = menuEl.dataset.topmenu;
                    this.handleTopMenuClick(menuName, e);
                });
            });

            // 하위 메뉴는 기본 링크 동작 사용
        }

        handleTopMenuClick(menuName, event) {
            const firstSub = $(`[data-submenu^="${menuName}_"]`);
            if (!firstSub) return;

            const targetHref = firstSub.getAttribute('href');
            
            // 같은 상위메뉴 내 이동이거나 홈에서 이동하는 경우
            if (this.currentTop === menuName || this.currentTop === '') {
                window.location.href = targetHref;
            } else {
                // 다른 상위메뉴 간 이동
                if (this.confirmNavigation()) {
                    StorageManager.clearAll();
                    window.location.href = targetHref;
                }
            }
        }

        setupNavigationWarnings() {
            // 홈 링크
            const homeLink = $('#app-title-link');
            if (homeLink) {
                homeLink.addEventListener('click', (e) => {
                    if (this.currentPath === window.APP_CONFIG.urls.home) {
                        return; // 이미 홈이면 확인 없이 이동
                    }
                    
                    if (!this.confirmNavigation()) {
                        e.preventDefault();
                        e.stopPropagation();
                    } else {
                        StorageManager.clearAll();
                    }
                });
            }

            // 로그아웃 버튼
            const logoutBtn = $('#logout-btn');
            if (logoutBtn) {
                logoutBtn.addEventListener('click', (e) => {
                    if (!confirm('로그아웃 하시겠습니까?')) {
                        e.preventDefault();
                        e.stopPropagation();
                    }
                });
            }
        }

        confirmNavigation() {
            return confirm('이동하시겠습니까? 메모리 초기화됩니다.');
        }

        setActiveStates() {
            // 현재 활성 메뉴 표시
            if (this.currentTop) {
                const topEl = $(`[data-topmenu="${this.currentTop}"]`);
                if (topEl) {
                    topEl.classList.add('active');
                }
            }

            if (this.currentSub) {
                const subEl = $(`[data-submenu="${this.currentSub}"]`);
                if (subEl) {
                    subEl.classList.add('active');
                }
            }
        }
    }

    // ==================== 페이지 초기화 ==================== 
    class AppInitializer {
        constructor() {
            this.sidebar = null;
            this.navigation = null;
        }

        init() {
            // DOM 준비 확인
            if (document.readyState === 'loading') {
                document.addEventListener('DOMContentLoaded', () => this.setup());
            } else {
                this.setup();
            }
        }

        setup() {
            console.log('Initializing STR TF APP...');
            
            // 매니저 인스턴스 생성
            this.sidebar = new SidebarManager();
            this.navigation = new NavigationManager();
            
            // 전역 접근 가능하도록 설정
            window.APP = {
                sidebar: this.sidebar,
                navigation: this.navigation,
                cookie: CookieManager,
                storage: StorageManager
            };

            // 초기화 완료 이벤트 발생
            window.dispatchEvent(new CustomEvent('app:initialized', {
                detail: { 
                    managers: {
                        sidebar: this.sidebar,
                        navigation: this.navigation
                    }
                }
            }));

            console.log('STR TF APP initialized successfully');
        }
    }

    // ==================== 추가 유틸리티 함수 ==================== 
    
    /**
     * Debounce 함수
     */
    window.debounce = function(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    };

    /**
     * Throttle 함수
     */
    window.throttle = function(func, limit) {
        let inThrottle;
        return function(...args) {
            if (!inThrottle) {
                func.apply(this, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    };

    // ==================== 앱 시작 ==================== 
    const app = new AppInitializer();
    app.init();

})();