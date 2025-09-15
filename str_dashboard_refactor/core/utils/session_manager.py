"""
Session management utilities
"""
import json
import logging
from typing import Any, Dict, List, Optional
from django.http import HttpRequest

logger = logging.getLogger(__name__)


class SessionManager:
    """세션 데이터 관리를 위한 헬퍼 클래스"""
    
    # 세션 키 상수
    class Keys:
        """세션 키 상수 정의"""
        # 데이터베이스 연결
        DB_CONN = 'db_conn'
        DB_CONN_STATUS = 'db_conn_status'
        RS_CONN = 'rs_conn'
        RS_CONN_STATUS = 'rs_conn_status'
        
        # Alert 관련
        CURRENT_ALERT_ID = 'current_alert_id'
        CURRENT_ALERT_DATA = 'current_alert_data'
        
        # 고객 정보
        CURRENT_CUSTOMER_DATA = 'current_customer_data'
        CURRENT_CORP_RELATED_DATA = 'current_corp_related_data'
        CURRENT_PERSON_RELATED_DATA = 'current_person_related_data'
        
        # Rule 관련
        CURRENT_RULE_HISTORY_DATA = 'current_rule_history_data'
        
        # 중복/IP 관련
        DUPLICATE_PERSONS_DATA = 'duplicate_persons_data'
        IP_HISTORY_DATA = 'ip_history_data'
        
        # Orderbook 관련
        CURRENT_ORDERBOOK_ANALYSIS = 'current_orderbook_analysis'
        CURRENT_STDS_DTM_SUMMARY = 'current_stds_dtm_summary'
        
        # TOML Export
        TOML_TEMP_PATH = 'toml_temp_path'
    
    @staticmethod
    def save_data(request: HttpRequest, key: str, data: Any, force_update: bool = True) -> None:
        """
        세션에 데이터 저장
        
        Args:
            request: Django request 객체
            key: 세션 키
            data: 저장할 데이터
            force_update: 세션 수정 플래그 강제 설정 여부
        """
        try:
            # 대용량 데이터 체크
            if data:
                data_size = len(json.dumps(data, default=str)) if isinstance(data, (dict, list)) else len(str(data))
                if data_size > 1024 * 1024:  # 1MB 이상
                    logger.warning(f"Large session data for key '{key}': {data_size:,} bytes")
            
            request.session[key] = data
            if force_update:
                request.session.modified = True
                
            logger.debug(f"Session saved: {key} (size: {data_size if data else 0} bytes)")
            
        except Exception as e:
            logger.error(f"Failed to save session data for key '{key}': {e}")
            raise
    
    @staticmethod
    def save_multiple(request: HttpRequest, data_dict: Dict[str, Any]) -> None:
        """
        여러 데이터를 한번에 세션에 저장
        
        Args:
            request: Django request 객체
            data_dict: {key: value} 형태의 딕셔너리
        """
        try:
            for key, value in data_dict.items():
                request.session[key] = value
            request.session.modified = True
            logger.debug(f"Session saved: {len(data_dict)} items")
        except Exception as e:
            logger.error(f"Failed to save multiple session data: {e}")
            raise
    
    @staticmethod
    def get_data(request: HttpRequest, key: str, default: Any = None) -> Any:
        """
        세션에서 데이터 가져오기
        
        Args:
            request: Django request 객체
            key: 세션 키
            default: 기본값
            
        Returns:
            세션 데이터 또는 기본값
        """
        return request.session.get(key, default)
    
    @staticmethod
    def get_multiple(request: HttpRequest, keys: List[str]) -> Dict[str, Any]:
        """
        여러 키의 데이터를 한번에 가져오기
        
        Args:
            request: Django request 객체
            keys: 세션 키 리스트
            
        Returns:
            {key: value} 딕셔너리
        """
        return {key: request.session.get(key) for key in keys}
    
    @staticmethod
    def exists(request: HttpRequest, key: str) -> bool:
        """
        세션 키 존재 여부 확인
        
        Args:
            request: Django request 객체
            key: 세션 키
            
        Returns:
            존재 여부
        """
        return key in request.session
    
    @staticmethod
    def clear_keys(request: HttpRequest, keys: List[str]) -> None:
        """
        특정 키들 삭제
        
        Args:
            request: Django request 객체
            keys: 삭제할 키 리스트
        """
        removed_count = 0
        for key in keys:
            if key in request.session:
                del request.session[key]
                removed_count += 1
        
        if removed_count > 0:
            request.session.modified = True
            logger.debug(f"Cleared {removed_count} session keys")
    
    @staticmethod
    def clear_pattern(request: HttpRequest, pattern: str) -> None:
        """
        특정 패턴으로 시작하는 모든 키 삭제
        
        Args:
            request: Django request 객체
            pattern: 키 패턴 (prefix)
        """
        keys_to_remove = [k for k in request.session.keys() if k.startswith(pattern)]
        
        for key in keys_to_remove:
            del request.session[key]
            
        if keys_to_remove:
            request.session.modified = True
            logger.debug(f"Cleared {len(keys_to_remove)} session keys with pattern: {pattern}")
    
    @staticmethod
    def clear_all_custom(request: HttpRequest) -> None:
        """
        Django 기본 세션 키를 제외한 모든 커스텀 키 삭제
        """
        # Django 기본 세션 키들
        django_keys = {'_auth_user_id', '_auth_user_backend', '_auth_user_hash'}
        
        keys_to_remove = [k for k in request.session.keys() if k not in django_keys]
        
        for key in keys_to_remove:
            del request.session[key]
            
        if keys_to_remove:
            request.session.modified = True
            logger.info(f"Cleared {len(keys_to_remove)} custom session keys")
    
    @staticmethod
    def get_db_connection_info(request: HttpRequest) -> Dict[str, Any]:
        """
        데이터베이스 연결 정보 가져오기
        
        Returns:
            {
                'oracle': {...},
                'redshift': {...}
            }
        """
        return {
            'oracle': {
                'connected': request.session.get(SessionManager.Keys.DB_CONN_STATUS) == 'ok',
                'info': request.session.get(SessionManager.Keys.DB_CONN),
                'status': request.session.get(SessionManager.Keys.DB_CONN_STATUS, 'need')
            },
            'redshift': {
                'connected': request.session.get(SessionManager.Keys.RS_CONN_STATUS) == 'ok',
                'info': request.session.get(SessionManager.Keys.RS_CONN),
                'status': request.session.get(SessionManager.Keys.RS_CONN_STATUS, 'need')
            }
        }
    
    @staticmethod
    def save_oracle_connection(request: HttpRequest, connection_info: Dict[str, Any]) -> None:
        """Oracle 연결 정보 저장"""
        SessionManager.save_multiple(request, {
            SessionManager.Keys.DB_CONN: connection_info,
            SessionManager.Keys.DB_CONN_STATUS: 'ok'
        })
        logger.info("Oracle connection info saved to session")
    
    @staticmethod
    def save_redshift_connection(request: HttpRequest, connection_info: Dict[str, Any]) -> None:
        """Redshift 연결 정보 저장"""
        SessionManager.save_multiple(request, {
            SessionManager.Keys.RS_CONN: connection_info,
            SessionManager.Keys.RS_CONN_STATUS: 'ok'
        })
        logger.info("Redshift connection info saved to session")
    
    @staticmethod
    def clear_db_connections(request: HttpRequest) -> None:
        """모든 데이터베이스 연결 정보 삭제"""
        SessionManager.clear_keys(request, [
            SessionManager.Keys.DB_CONN,
            SessionManager.Keys.DB_CONN_STATUS,
            SessionManager.Keys.RS_CONN,
            SessionManager.Keys.RS_CONN_STATUS
        ])
        logger.info("All database connection info cleared from session")