"""
Decorator utilities for views
"""
import logging
from functools import wraps
from typing import Callable, Optional

from django.http import JsonResponse, HttpRequest
from django.conf import settings

from ..database import OracleConnection, RedshiftConnection, ConnectionConfig
from .session_manager import SessionManager

logger = logging.getLogger(__name__)


def require_db_connection(func: Callable) -> Callable:
    """
    Oracle 데이터베이스 연결이 필요한 뷰 데코레이터
    세션에서 연결 정보를 확인하고 OracleConnection 객체를 생성하여 전달
    
    Usage:
        @require_db_connection
        def my_view(request, oracle_conn=None):
            # oracle_conn 사용
            pass
    """
    @wraps(func)
    def wrapper(request: HttpRequest, *args, **kwargs):
        # 세션에서 연결 정보 확인
        db_info = SessionManager.get_data(request, SessionManager.Keys.DB_CONN)
        db_status = SessionManager.get_data(request, SessionManager.Keys.DB_CONN_STATUS)
        
        if not db_info or db_status != 'ok':
            logger.warning(f"Oracle DB not connected for view: {func.__name__}")
            return JsonResponse({
                'success': False,
                'message': '먼저 Oracle DB Connection에서 연결을 완료해 주세요.'
            })
        
        try:
            # ConnectionConfig 생성
            if 'jdbc_url' in db_info:
                # 기존 JDBC URL 방식
                oracle_conn = OracleConnection.from_jdbc_url(
                    jdbc_url=db_info['jdbc_url'],
                    username=db_info['username'],
                    password=db_info['password'],
                    driver_path=db_info.get('driver_path'),
                    driver_class=db_info.get('driver_class')
                )
            else:
                # 새로운 ConnectionConfig 방식
                config = ConnectionConfig.from_dict(db_info)
                oracle_conn = OracleConnection(config)
            
            # 함수에 oracle_conn 파라미터 추가
            kwargs['oracle_conn'] = oracle_conn
            
            return func(request, *args, **kwargs)
            
        except Exception as e:
            logger.exception(f"Oracle connection setup failed in decorator: {e}")
            return JsonResponse({
                'success': False,
                'message': f'데이터베이스 연결 설정 실패: {e}'
            })
    
    return wrapper


def require_redshift_connection(func: Callable) -> Callable:
    """
    Redshift 데이터베이스 연결이 필요한 뷰 데코레이터
    세션에서 연결 정보를 확인하고 RedshiftConnection 객체를 생성하여 전달
    
    Usage:
        @require_redshift_connection
        def my_view(request, redshift_conn=None):
            # redshift_conn 사용
            pass
    """
    @wraps(func)
    def wrapper(request: HttpRequest, *args, **kwargs):
        # 세션에서 연결 정보 확인
        rs_info = SessionManager.get_data(request, SessionManager.Keys.RS_CONN)
        rs_status = SessionManager.get_data(request, SessionManager.Keys.RS_CONN_STATUS)
        
        if not rs_info or rs_status != 'ok':
            logger.warning(f"Redshift not connected for view: {func.__name__}")
            return JsonResponse({
                'success': False,
                'message': '먼저 Redshift 연결을 완료해 주세요.'
            })
        
        try:
            # ConnectionConfig 생성
            config = ConnectionConfig(
                host=rs_info['host'],
                port=rs_info['port'],
                username=rs_info['username'],
                password=rs_info['password'],
                database=rs_info.get('dbname', rs_info.get('database'))  # 호환성
            )
            
            redshift_conn = RedshiftConnection(config)
            
            # 함수에 redshift_conn 파라미터 추가
            kwargs['redshift_conn'] = redshift_conn
            
            return func(request, *args, **kwargs)
            
        except Exception as e:
            logger.exception(f"Redshift connection setup failed in decorator: {e}")
            return JsonResponse({
                'success': False,
                'message': f'Redshift 연결 설정 실패: {e}'
            })
    
    return wrapper


def require_both_connections(func: Callable) -> Callable:
    """
    Oracle과 Redshift 모두 연결이 필요한 뷰 데코레이터
    
    Usage:
        @require_both_connections
        def my_view(request, oracle_conn=None, redshift_conn=None):
            # 두 연결 모두 사용
            pass
    """
    @wraps(func)
    @require_db_connection
    @require_redshift_connection
    def wrapper(request: HttpRequest, *args, **kwargs):
        return func(request, *args, **kwargs)
    
    return wrapper


def ajax_required(func: Callable) -> Callable:
    """
    AJAX 요청만 허용하는 데코레이터
    """
    @wraps(func)
    def wrapper(request: HttpRequest, *args, **kwargs):
        if not request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            logger.warning(f"Non-AJAX request to {func.__name__}")
            return JsonResponse({
                'success': False,
                'message': 'AJAX 요청만 허용됩니다.'
            }, status=400)
        return func(request, *args, **kwargs)
    
    return wrapper


def log_execution_time(func: Callable) -> Callable:
    """
    함수 실행 시간을 로깅하는 데코레이터
    """
    import time
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        
        logger.info(f"{func.__name__} executed in {execution_time:.2f} seconds")
        
        return result
    
    return wrapper