// str_dashboard/static/str_dashboard/js/base.js
// 기본 레이아웃 JavaScript - 사이드바 메뉴 토글 포함

(function() {
    'use strict';

    // 유틸리티
    const $ = (selector) => document.querySelector(selector);
    const $$ = (selector) => document.querySelectorAll(selector);

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

    // 메뉴 관리자 클래스
    class MenuManager {
        constructor() {
            this.menuTops = $$('.menu-top');
            this.init();
        }

        init() {
            // 현재 페이지에 맞는 메뉴 활성화
            this.setActiveMenu();
            
            // 메뉴 토글 이벤트 추가
            this.addMenuToggleEvents();
            
            // 로그아웃 확인
            this.setupLogoutButton();
        }

        setActiveMenu() {
            const currentTop = document.body.dataset.topMenu;
            const currentSub = document.body.dataset.subMenu;

            if (currentTop) {
                const topEl = $(`[data-topmenu="${currentTop}"]`);
                if (topEl) {
                    topEl.classList.add('active');
                    // 현재 페이지의 상위 메뉴를 자동으로 펼침
                    const submenu = topEl.nextElementSibling;
                    if (submenu && submenu.classList.contains('submenu')) {
                        submenu.style.display = 'block';
                    }
                }
            }

            if (currentSub) {
                const subEl = $(`[data-submenu="${currentSub}"]`);
                if (subEl) {
                    subEl.classList.add('active');
                }
            }
        }

        addMenuToggleEvents() {
            this.menuTops.forEach(menuTop => {
                menuTop.addEventListener('click', (e) => {
                    e.preventDefault();
                    this.toggleMenu(menuTop);
                });

                // 키보드 접근성
                menuTop.addEventListener('keypress', (e) => {
                    if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        this.toggleMenu(menuTop);
                    }
                });
            });
        }

        toggleMenu(menuTop) {
            const isActive = menuTop.classList.contains('active');
            const submenu = menuTop.nextElementSibling;
            
            if (!submenu || !submenu.classList.contains('submenu')) {
                return;
            }

            if (isActive) {
                // 메뉴 접기
                menuTop.classList.remove('active');
                this.slideUp(submenu);
            } else {
                // 다른 메뉴들 모두 접기 (아코디언 효과)
                this.menuTops.forEach(otherMenu => {
                    if (otherMenu !== menuTop) {
                        otherMenu.classList.remove('active');
                        const otherSubmenu = otherMenu.nextElementSibling;
                        if (otherSubmenu && otherSubmenu.classList.contains('submenu')) {
                            this.slideUp(otherSubmenu);
                        }
                    }
                });

                // 현재 메뉴 펼치기
                menuTop.classList.add('active');
                this.slideDown(submenu);
            }

            // 상태 저장
            const menuName = menuTop.dataset.topmenu;
            if (menuName) {
                CookieManager.set(`menu_${menuName}`, isActive ? 'closed' : 'open');
            }
        }

        slideDown(element) {
            element.style.display = 'block';
            element.style.height = '0';
            element.style.overflow = 'hidden';
            element.style.transition = 'height 0.3s ease';
            
            const height = element.scrollHeight;
            
            requestAnimationFrame(() => {
                element.style.height = height + 'px';
                
                setTimeout(() => {
                    element.style.height = '';
                    element.style.overflow = '';
                    element.style.transition = '';
                }, 300);
            });
        }

        slideUp(element) {
            element.style.height = element.scrollHeight + 'px';
            element.style.overflow = 'hidden';
            element.style.transition = 'height 0.3s ease';
            
            requestAnimationFrame(() => {
                element.style.height = '0';
                
                setTimeout(() => {
                    element.style.display = 'none';
                    element.style.height = '';
                    element.style.overflow = '';
                    element.style.transition = '';
                }, 300);
            });
        }

        setupLogoutButton() {
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
    function init() {
        new SidebarManager();
        new MenuManager();
        
        // 로고 회전 애니메이션 (옵션)
        const logoImg = $('#logo-img');
        if (logoImg) {
            $('#logo-toggle')?.addEventListener('click', () => {
                logoImg.classList.add('spin');
                setTimeout(() => {
                    logoImg.classList.remove('spin');
                }, 600);
            });
        }
    }

    // DOM 준비 완료 시 초기화
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }

})();