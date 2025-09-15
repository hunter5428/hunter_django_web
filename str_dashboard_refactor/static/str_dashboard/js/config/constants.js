/**
 * 애플리케이션 상수 정의
 */

// API 엔드포인트
export const API_ENDPOINTS = {
    // 인증
    LOGIN: '/login/',
    LOGOUT: '/logout/',
    HOME: '/home/',
    
    // DB 연결
    TEST_ORACLE: '/api/test_oracle_connection/',
    TEST_REDSHIFT: '/api/test_redshift_connection/',
    CONNECT_ALL_DB: '/api/connect_all_databases/',
    CHECK_DB_STATUS: '/api/check_db_status/',
    
    // Alert 관련
    QUERY_ALERT: '/api/query_alert_info/',
    RULE_HISTORY: '/api/rule_history_search/',
    
    // 고객 정보
    QUERY_CUSTOMER: '/api/query_customer_unified/',
    QUERY_DUPLICATE: '/api/query_duplicate_unified/',
    QUERY_CORP_RELATED: '/api/query_corp_related_persons/',
    QUERY_PERSON_RELATED: '/api/query_person_related_summary/',
    QUERY_IP_HISTORY: '/api/query_ip_access_history/',
    
    // Orderbook
    QUERY_ORDERBOOK: '/api/query_redshift_orderbook/',
    GET_CACHED_ORDERBOOK: '/api/get_cached_orderbook_info/',
    CLEAR_ORDERBOOK_CACHE: '/api/clear_orderbook_cache/',
    ANALYZE_ORDERBOOK: '/api/analyze_cached_orderbook/',
    GET_ORDERBOOK_SUMMARY: '/api/get_orderbook_summary/',
    ANALYZE_ALERT_ORDERBOOK: '/api/analyze_alert_orderbook/',
    ANALYZE_STDS_DTM: '/api/analyze_stds_dtm_orderbook/',
    
    // Export
    PREPARE_TOML: '/api/prepare_toml_data/',
    DOWNLOAD_TOML: '/api/download_toml/',
    SAVE_TO_SESSION: '/api/save_to_session/'
};

// 세션 키
export const SESSION_KEYS = {
    DB_CONN: 'db_conn',
    DB_CONN_STATUS: 'db_conn_status',
    RS_CONN: 'rs_conn',
    RS_CONN_STATUS: 'rs_conn_status',
    CURRENT_ALERT_ID: 'current_alert_id',
    CURRENT_ALERT_DATA: 'current_alert_data',
    CURRENT_CUSTOMER_DATA: 'current_customer_data',
    CURRENT_CORP_RELATED_DATA: 'current_corp_related_data',
    CURRENT_PERSON_RELATED_DATA: 'current_person_related_data',
    CURRENT_RULE_HISTORY_DATA: 'current_rule_history_data',
    DUPLICATE_PERSONS_DATA: 'duplicate_persons_data',
    IP_HISTORY_DATA: 'ip_history_data',
    CURRENT_ORDERBOOK_ANALYSIS: 'current_orderbook_analysis',
    CURRENT_STDS_DTM_SUMMARY: 'current_stds_dtm_summary',
    TOML_TEMP_PATH: 'toml_temp_path'
};

// UI 상태
export const UI_STATES = {
    LOADING: 'loading',
    SUCCESS: 'success',
    ERROR: 'error',
    IDLE: 'idle'
};

// 타임아웃 설정 (밀리초)
export const TIMEOUTS = {
    API_REQUEST: 30000,  // 30초
    DB_CONNECTION: 20000, // 20초
    ORDERBOOK_ANALYSIS: 60000, // 60초
    TOML_EXPORT: 30000 // 30초
};

// 특수 Rule ID
export const SPECIAL_RULE_IDS = ['IO000', 'IO111'];

// 날짜 형식
export const DATE_FORMATS = {
    DEFAULT: 'YYYY-MM-DD',
    DATETIME: 'YYYY-MM-DD HH:mm:ss',
    DATETIME_WITH_MS: 'YYYY-MM-DD HH:mm:ss.SSS',
    DATETIME_WITH_NS: 'YYYY-MM-DD HH:mm:ss.SSSSSSSSS'
};

// 에러 메시지
export const ERROR_MESSAGES = {
    DB_NOT_CONNECTED: '먼저 데이터베이스 연결을 완료해 주세요.',
    ORACLE_NOT_CONNECTED: '먼저 Oracle DB 연결을 완료해 주세요.',
    REDSHIFT_NOT_CONNECTED: '먼저 Redshift 연결을 완료해 주세요.',
    INVALID_ALERT_ID: 'ALERT ID를 입력하세요.',
    NO_DATA_FOUND: '해당 ALERT ID에 대한 데이터가 없습니다.',
    QUERY_FAILED: '조회 중 오류가 발생했습니다.',
    SESSION_EXPIRED: '세션이 만료되었습니다. 다시 로그인해 주세요.',
    NETWORK_ERROR: '네트워크 오류가 발생했습니다.',
    UNKNOWN_ERROR: '알 수 없는 오류가 발생했습니다.'
};

// 기본값
export const DEFAULTS = {
    ORACLE_HOST: '127.0.0.1',
    ORACLE_PORT: '40112',
    ORACLE_SERVICE: 'PRDAMLKR.OCIAMLPRODDBA.OCIAMLPROD.ORACLEVCN.COM',
    REDSHIFT_HOST: '127.0.0.1',
    REDSHIFT_PORT: '40127',
    REDSHIFT_DBNAME: 'prod',
    MONTHS_BACK_DEFAULT: 3,
    MONTHS_BACK_SPECIAL: 12,
    ORDERBOOK_BATCH_SIZE: 10000,
    MAX_DUPLICATE_RESULTS: 50
};

// 정규식 패턴
export const PATTERNS = {
    EMAIL: /^[^\s@]+@[^\s@]+\.[^\s@]+$/,
    PHONE: /^0\d{9,10}$/,
    IP_V4: /^(\d{1,3}\.){3}\d{1,3}$/,
    IP_V6: /^([0-9a-fA-F]{0,4}:){7}[0-9a-fA-F]{0,4}$/,
    DATE: /^\d{4}-\d{2}-\d{2}$/,
    DATETIME: /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/,
    ALERT_ID: /^[A-Za-z0-9]+$/,
    CUSTOMER_ID: /^[A-Za-z0-9]+$/
};

// 테이블 설정
export const TABLE_CONFIG = {
    DEFAULT_EMPTY_MESSAGE: '데이터가 없습니다.',
    DEFAULT_PAGE_SIZE: 20,
    MAX_ROWS_DISPLAY: 1000,
    BATCH_RENDER_SIZE: 100
};

// 캐시 설정
export const CACHE_CONFIG = {
    ORDERBOOK_TTL: 3600000, // 1시간
    SEARCH_RESULT_TTL: 1800000, // 30분
    SESSION_DATA_TTL: 7200000 // 2시간
};