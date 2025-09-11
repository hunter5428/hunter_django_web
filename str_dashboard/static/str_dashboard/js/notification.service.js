// str_dashboard/static/str_dashboard/js/notification.service.js

/**
 * 통합 알림 및 에러 처리 서비스
 */
(function(window) {
    'use strict';

    /**
     * 알림 타입 정의
     */
    const NotificationType = {
        SUCCESS: 'success',
        ERROR: 'error',
        WARNING: 'warning',
        INFO: 'info',
        LOADING: 'loading'
    };

    /**
     * 알림 아이콘 정의
     */
    const NotificationIcons = {
        [NotificationType.SUCCESS]: `
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M20 6L9 17l-5-5"/>
            </svg>
        `,
        [NotificationType.ERROR]: `
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="10"/>
                <line x1="15" y1="9" x2="9" y2="15"/>
                <line x1="9" y1="9" x2="15" y2="15"/>
            </svg>
        `,
        [NotificationType.WARNING]: `
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>
                <line x1="12" y1="9" x2="12" y2="13"/>
                <line x1="12" y1="17" x2="12.01" y2="17"/>
            </svg>
        `,
        [NotificationType.INFO]: `
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <circle cx="12" cy="12" r="10"/>
                <line x1="12" y1="16" x2="12" y2="12"/>
                <line x1="12" y1="8" x2="12.01" y2="8"/>
            </svg>
        `,
        [NotificationType.LOADING]: `
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" class="notification-spinner">
                <path d="M21 12a9 9 0 11-6.219-8.56"/>
            </svg>
        `
    };

    /**
     * 알림 클래스
     */
    class Notification {
        constructor(options) {
            this.id = this.generateId();
            this.type = options.type || NotificationType.INFO;
            this.title = options.title || '';
            this.message = options.message || '';
            this.duration = options.duration ?? 5000; // 0이면 자동 닫힘 없음
            this.closable = options.closable ?? true;
            this.position = options.position || 'top-right';
            this.actions = options.actions || [];
            this.onClose = options.onClose || null;
            this.element = null;
            this.timeoutId = null;
            
            this.create();
            this.show();
        }

        generateId() {
            return `notification-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
        }

        create() {
            // 알림 요소 생성
            this.element = document.createElement('div');
            this.element.id = this.id;
            this.element.className = `notification notification-${this.type} notification-${this.position}`;
            
            // HTML 구성
            const icon = NotificationIcons[this.type];
            const actionsHtml = this.createActionsHtml();
            
            this.element.innerHTML = `
                <div class="notification-content">
                    <div class="notification-icon">${icon}</div>
                    <div class="notification-body">
                        ${this.title ? `<div class="notification-title">${this.escapeHtml(this.title)}</div>` : ''}
                        ${this.message ? `<div class="notification-message">${this.escapeHtml(this.message)}</div>` : ''}
                        ${actionsHtml ? `<div class="notification-actions">${actionsHtml}</div>` : ''}
                    </div>
                    ${this.closable ? `
                        <button class="notification-close" aria-label="닫기">
                            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <line x1="18" y1="6" x2="6" y2="18"/>
                                <line x1="6" y1="6" x2="18" y2="18"/>
                            </svg>
                        </button>
                    ` : ''}
                </div>
                ${this.type === NotificationType.LOADING ? '<div class="notification-progress"></div>' : ''}
            `;
            
            // 이벤트 바인딩
            this.bindEvents();
        }

        createActionsHtml() {
            if (!this.actions.length) return '';
            
            return this.actions.map((action, index) => `
                <button class="notification-action" data-action-index="${index}">
                    ${this.escapeHtml(action.label)}
                </button>
            `).join('');
        }

        bindEvents() {
            // 닫기 버튼
            const closeBtn = this.element.querySelector('.notification-close');
            if (closeBtn) {
                closeBtn.addEventListener('click', () => this.close());
            }
            
            // 액션 버튼들
            this.element.querySelectorAll('.notification-action').forEach(btn => {
                const index = parseInt(btn.dataset.actionIndex);
                btn.addEventListener('click', () => {
                    const action = this.actions[index];
                    if (action && action.handler) {
                        action.handler();
                    }
                    if (action && action.closeOnClick !== false) {
                        this.close();
                    }
                });
            });
        }

        show() {
            // 컨테이너 확인 또는 생성
            let container = document.querySelector('.notification-container');
            if (!container) {
                container = document.createElement('div');
                container.className = 'notification-container';
                document.body.appendChild(container);
            }
            
            // 알림 추가
            container.appendChild(this.element);
            
            // 애니메이션을 위한 지연
            requestAnimationFrame(() => {
                this.element.classList.add('notification-show');
            });
            
            // 자동 닫힘 설정
            if (this.duration > 0) {
                this.timeoutId = setTimeout(() => this.close(), this.duration);
            }
        }

        close() {
            if (this.timeoutId) {
                clearTimeout(this.timeoutId);
            }
            
            // 페이드 아웃 애니메이션
            this.element.classList.remove('notification-show');
            this.element.classList.add('notification-hide');
            
            // 애니메이션 완료 후 제거
            setTimeout(() => {
                if (this.element && this.element.parentNode) {
                    this.element.parentNode.removeChild(this.element);
                }
                
                // 콜백 실행
                if (this.onClose) {
                    this.onClose();
                }
                
                // 컨테이너가 비었으면 제거
                const container = document.querySelector('.notification-container');
                if (container && container.children.length === 0) {
                    container.remove();
                }
            }, 300);
        }

        update(options) {
            // 타입 변경
            if (options.type && options.type !== this.type) {
                this.element.classList.remove(`notification-${this.type}`);
                this.type = options.type;
                this.element.classList.add(`notification-${this.type}`);
                
                // 아이콘 업데이트
                const iconEl = this.element.querySelector('.notification-icon');
                if (iconEl) {
                    iconEl.innerHTML = NotificationIcons[this.type];
                }
            }
            
            // 제목 업데이트
            if (options.title !== undefined) {
                const titleEl = this.element.querySelector('.notification-title');
                if (titleEl) {
                    titleEl.textContent = options.title;
                } else if (options.title) {
                    const bodyEl = this.element.querySelector('.notification-body');
                    const titleDiv = document.createElement('div');
                    titleDiv.className = 'notification-title';
                    titleDiv.textContent = options.title;
                    bodyEl.insertBefore(titleDiv, bodyEl.firstChild);
                }
            }
            
            // 메시지 업데이트
            if (options.message !== undefined) {
                const messageEl = this.element.querySelector('.notification-message');
                if (messageEl) {
                    messageEl.textContent = options.message;
                } else if (options.message) {
                    const bodyEl = this.element.querySelector('.notification-body');
                    const messageDiv = document.createElement('div');
                    messageDiv.className = 'notification-message';
                    messageDiv.textContent = options.message;
                    bodyEl.appendChild(messageDiv);
                }
            }
        }

        escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
    }

    /**
     * 알림 서비스 클래스
     */
    class NotificationService {
        constructor() {
            this.notifications = new Map();
            this.defaultOptions = {
                position: 'top-right',
                duration: 5000,
                closable: true
            };
            
            this.init();
        }

        init() {
            // 스타일 주입
            this.injectStyles();
            
            // 전역 에러 리스너
            window.addEventListener('error', (event) => {
                this.error(`JavaScript 오류: ${event.message}`);
            });
            
            // Promise rejection 리스너
            window.addEventListener('unhandledrejection', (event) => {
                this.error(`처리되지 않은 오류: ${event.reason}`);
            });
            
            // API 에러 리스너
            window.addEventListener('api:error', (event) => {
                const error = event.detail.error;
                this.handleAPIError(error);
            });
        }

        injectStyles() {
            if (document.getElementById('notification-styles')) return;
            
            const style = document.createElement('style');
            style.id = 'notification-styles';
            style.textContent = `
                .notification-container {
                    position: fixed;
                    top: 20px;
                    right: 20px;
                    z-index: 9999;
                    pointer-events: none;
                }
                
                .notification {
                    background: #1a1a1a;
                    border: 1px solid #2a2a2a;
                    border-radius: 8px;
                    padding: 16px;
                    margin-bottom: 12px;
                    min-width: 320px;
                    max-width: 480px;
                    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
                    display: flex;
                    opacity: 0;
                    transform: translateX(100%);
                    transition: all 0.3s ease;
                    pointer-events: all;
                }
                
                .notification-show {
                    opacity: 1;
                    transform: translateX(0);
                }
                
                .notification-hide {
                    opacity: 0;
                    transform: translateX(100%);
                }
                
                .notification-content {
                    display: flex;
                    align-items: flex-start;
                    gap: 12px;
                    width: 100%;
                }
                
                .notification-icon {
                    flex-shrink: 0;
                    width: 24px;
                    height: 24px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                }
                
                .notification-body {
                    flex: 1;
                }
                
                .notification-title {
                    font-weight: 600;
                    font-size: 14px;
                    margin-bottom: 4px;
                    color: #eaeaea;
                }
                
                .notification-message {
                    font-size: 13px;
                    color: #bdbdbd;
                    line-height: 1.4;
                }
                
                .notification-actions {
                    margin-top: 8px;
                    display: flex;
                    gap: 8px;
                }
                
                .notification-action {
                    padding: 4px 12px;
                    font-size: 12px;
                    border-radius: 4px;
                    border: 1px solid #2a2a2a;
                    background: transparent;
                    color: #4fc3f7;
                    cursor: pointer;
                    transition: all 0.2s ease;
                }
                
                .notification-action:hover {
                    background: #2a2a2a;
                }
                
                .notification-close {
                    flex-shrink: 0;
                    width: 24px;
                    height: 24px;
                    display: flex;
                    align-items: center;
                    justify-content: center;
                    background: transparent;
                    border: none;
                    color: #bdbdbd;
                    cursor: pointer;
                    padding: 0;
                    transition: color 0.2s ease;
                }
                
                .notification-close:hover {
                    color: #eaeaea;
                }
                
                /* 타입별 스타일 */
                .notification-success {
                    border-color: #2e7d32;
                    background: linear-gradient(135deg, #1a1a1a 0%, #0f2314 100%);
                }
                
                .notification-success .notification-icon {
                    color: #4caf50;
                }
                
                .notification-error {
                    border-color: #c62828;
                    background: linear-gradient(135deg, #1a1a1a 0%, #2a1414 100%);
                }
                
                .notification-error .notification-icon {
                    color: #ff6b6b;
                }
                
                .notification-warning {
                    border-color: #f57c00;
                    background: linear-gradient(135deg, #1a1a1a 0%, #2a1f14 100%);
                }
                
                .notification-warning .notification-icon {
                    color: #ffa726;
                }
                
                .notification-info {
                    border-color: #0288d1;
                    background: linear-gradient(135deg, #1a1a1a 0%, #14202a 100%);
                }
                
                .notification-info .notification-icon {
                    color: #4fc3f7;
                }
                
                .notification-loading .notification-icon {
                    color: #4fc3f7;
                }
                
                .notification-spinner {
                    animation: spin 1s linear infinite;
                }
                
                @keyframes spin {
                    to { transform: rotate(360deg); }
                }
                
                .notification-progress {
                    position: absolute;
                    bottom: 0;
                    left: 0;
                    right: 0;
                    height: 2px;
                    background: #4fc3f7;
                    border-radius: 0 0 8px 8px;
                    animation: progress 2s linear infinite;
                }
                
                @keyframes progress {
                    0% { transform: translateX(-100%); }
                    100% { transform: translateX(100%); }
                }
                
                /* 반응형 */
                @media (max-width: 480px) {
                    .notification-container {
                        left: 10px;
                        right: 10px;
                        top: 10px;
                    }
                    
                    .notification {
                        min-width: 0;
                        max-width: 100%;
                    }
                }
            `;
            
            document.head.appendChild(style);
        }

        create(options) {
            const notification = new Notification({
                ...this.defaultOptions,
                ...options
            });
            
            this.notifications.set(notification.id, notification);
            
            return notification;
        }

        success(message, title = '성공', options = {}) {
            return this.create({
                ...options,
                type: NotificationType.SUCCESS,
                title,
                message
            });
        }

        error(message, title = '오류', options = {}) {
            return this.create({
                ...options,
                type: NotificationType.ERROR,
                title,
                message,
                duration: 8000 // 에러는 더 오래 표시
            });
        }

        warning(message, title = '경고', options = {}) {
            return this.create({
                ...options,
                type: NotificationType.WARNING,
                title,
                message
            });
        }

        info(message, title = '알림', options = {}) {
            return this.create({
                ...options,
                type: NotificationType.INFO,
                title,
                message
            });
        }

        loading(message = '처리 중...', title = '', options = {}) {
            return this.create({
                ...options,
                type: NotificationType.LOADING,
                title,
                message,
                duration: 0, // 자동 닫힘 없음
                closable: false
            });
        }

        confirm(message, title = '확인', options = {}) {
            return new Promise((resolve) => {
                this.create({
                    ...options,
                    type: NotificationType.WARNING,
                    title,
                    message,
                    duration: 0,
                    closable: false,
                    actions: [
                        {
                            label: '취소',
                            handler: () => resolve(false)
                        },
                        {
                            label: '확인',
                            handler: () => resolve(true)
                        }
                    ]
                });
            });
        }

        handleAPIError(error) {
            // API 에러 타입에 따른 처리
            if (error.status === 401) {
                this.error('인증이 만료되었습니다. 다시 로그인해주세요.', '인증 오류');
            } else if (error.status === 403) {
                this.error('접근 권한이 없습니다.', '권한 오류');
            } else if (error.status === 404) {
                this.warning('요청한 데이터를 찾을 수 없습니다.', '데이터 없음');
            } else if (error.status >= 500) {
                this.error('서버 오류가 발생했습니다. 잠시 후 다시 시도해주세요.', '서버 오류');
            } else if (error.status === 'network') {
                this.error('네트워크 연결을 확인해주세요.', '연결 오류');
            } else if (error.status === 'timeout') {
                this.warning('요청 시간이 초과되었습니다.', '시간 초과');
            } else {
                this.error(error.message || '알 수 없는 오류가 발생했습니다.', '오류');
            }
        }

        closeAll() {
            this.notifications.forEach(notification => notification.close());
            this.notifications.clear();
        }

        close(id) {
            const notification = this.notifications.get(id);
            if (notification) {
                notification.close();
                this.notifications.delete(id);
            }
        }
    }

    // ==================== 전역 등록 ====================
    
    const notificationService = new NotificationService();
    
    // 전역 등록
    window.Notify = notificationService;
    
    // APP 객체에 추가
    if (window.APP) {
        window.APP.notify = notificationService;
    } else {
        // APP 객체가 없으면 이벤트 리스너로 대기
        window.addEventListener('app:initialized', (event) => {
            window.APP.notify = notificationService;
        });
    }

})(window);