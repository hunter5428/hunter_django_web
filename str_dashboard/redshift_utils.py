# str_dashboard/redshift_utils.py
"""
Redshift 데이터베이스 연결 및 쿼리 실행 유틸리티
"""
import os
import logging
from functools import wraps
from typing import Tuple, List, Optional, Dict, Any
from contextlib import contextmanager
from pathlib import Path

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from django.conf import settings
from django.http import JsonResponse

logger = logging.getLogger(__name__)


class RedshiftConnectionError(Exception):
    """Redshift 연결 관련 커스텀 예외"""
    pass


class RedshiftQueryError(Exception):
    """Redshift 쿼리 실행 관련 커스텀 예외"""
    pass


class RedshiftConnection:
    """Redshift 데이터베이스 연결 관리 클래스"""
    
    def __init__(self, host: str, port: str, dbname: str, username: str, password: str):
        """
        Redshift 연결을 위한 클래스
        
        Args:
            host: 데이터베이스 호스트
            port: 포트 번호
            dbname: 데이터베이스 이름
            username: 사용자명
            password: 비밀번호
        """
        self.host = host
        self.port = port
        self.dbname = dbname
        self.username = username
        self.password = password
        self._connection = None
    
    @classmethod
    def from_session(cls, session_data: Dict[str, Any]) -> 'RedshiftConnection':
        """세션 데이터에서 연결 객체 생성"""
        return cls(
            host=session_data['host'],
            port=session_data['port'],
            dbname=session_data['dbname'],
            username=session_data['username'],
            password=session_data['password']
        )
    
    def get_connection_string(self) -> str:
        """PostgreSQL 연결 문자열 생성"""
        return (
            f"host={self.host} "
            f"port={self.port} "
            f"dbname={self.dbname} "
            f"user={self.username} "
            f"password={self.password} "
            f"sslmode=require"  # Redshift는 SSL 연결 권장
        )
    
    def connect(self):
        """데이터베이스 연결"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.username,
                password=self.password,
                sslmode='require',
                connect_timeout=10
            )
            logger.debug("Redshift connected successfully")
            return self._connection
        except psycopg2.OperationalError as e:
            error_msg = self._parse_redshift_error(str(e))
            logger.exception(f"Redshift connection failed: {error_msg}")
            raise RedshiftConnectionError(error_msg)
        except Exception as e:
            logger.exception(f"Unexpected Redshift connection error: {e}")
            raise RedshiftConnectionError(f"연결 실패: {e}")
    
    def close(self):
        """연결 종료"""
        if self._connection:
            try:
                self._connection.close()
                logger.debug("Redshift connection closed")
            except Exception as e:
                logger.warning(f"Error closing Redshift connection: {e}")
            finally:
                self._connection = None
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """커서 컨텍스트 매니저"""
        conn = None
        cursor = None
        try:
            conn = self.connect()
            cursor = conn.cursor(cursor_factory=cursor_factory)
            yield cursor
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            self.close()
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> Tuple[List[str], List[List]]:
        """
        쿼리 실행 및 결과 반환
        
        Returns:
            (columns, rows) 튜플
        """
        params = params or []
        
        with self.get_cursor() as cursor:
            try:
                cursor.execute(sql, params)
                
                # SELECT 쿼리인 경우 결과 반환
                if cursor.description:
                    cols = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return cols, rows
                else:
                    # INSERT/UPDATE/DELETE 등의 경우
                    return [], []
                    
            except psycopg2.Error as e:
                logger.exception(f"Redshift query execution failed: {e}")
                raise RedshiftQueryError(f"쿼리 실행 실패: {e}")
    
    def execute_query_dict(self, sql: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """
        쿼리 실행 및 딕셔너리 형태로 결과 반환
        
        Returns:
            딕셔너리 리스트
        """
        params = params or []
        
        with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
            try:
                cursor.execute(sql, params)
                
                if cursor.description:
                    return cursor.fetchall()
                else:
                    return []
                    
            except psycopg2.Error as e:
                logger.exception(f"Redshift query execution failed: {e}")
                raise RedshiftQueryError(f"쿼리 실행 실패: {e}")
    
    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Redshift connection test failed: {e}")
            return False
    
    @staticmethod
    def _parse_redshift_error(error_text: str) -> str:
        """Redshift 에러 메시지 파싱 및 사용자 친화적 메시지 변환"""
        error_lower = error_text.lower()
        
        if 'could not connect to server' in error_lower:
            return '연결 실패: 서버에 연결할 수 없습니다. 호스트와 포트를 확인하세요.'
        elif 'authentication failed' in error_lower or 'password authentication failed' in error_lower:
            return '연결 실패: 사용자명 또는 비밀번호가 올바르지 않습니다.'
        elif 'database' in error_lower and 'does not exist' in error_lower:
            return '연결 실패: 데이터베이스가 존재하지 않습니다.'
        elif 'timeout' in error_lower:
            return '연결 실패: 연결 시간이 초과되었습니다.'
        elif 'permission denied' in error_lower:
            return '연결 실패: 권한이 없습니다.'
        elif 'ssl' in error_lower:
            return '연결 실패: SSL 연결 오류가 발생했습니다.'
        else:
            return f'연결 실패: {error_text}'


def require_redshift_connection(func):
    """
    Redshift 연결이 필요한 뷰 데코레이터
    세션에서 연결 정보를 확인하고 RedshiftConnection 객체를 생성하여 전달
    """
    @wraps(func)
    def wrapper(request, *args, **kwargs):
        rs_info = request.session.get('rs_conn')
        if not rs_info or request.session.get('rs_conn_status') != 'ok':
            return JsonResponse({
                'success': False,
                'message': '먼저 Redshift 연결을 완료해 주세요.'
            })
        
        try:
            redshift_conn = RedshiftConnection.from_session(rs_info)
            # 함수에 redshift_conn 파라미터 추가
            return func(request, redshift_conn=redshift_conn, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Redshift connection setup failed: {e}")
            return JsonResponse({
                'success': False,
                'message': f'Redshift 연결 설정 실패: {e}'
            })
    
    return wrapper


def execute_redshift_query_with_error_handling(
    redshift_conn: RedshiftConnection,
    sql: str,
    params: Optional[List] = None
) -> Dict[str, Any]:
    """
    Redshift 쿼리 실행 및 에러 처리를 포함한 헬퍼 함수
    
    Returns:
        {'success': True/False, 'columns': [...], 'rows': [...], 'message': '...'}
    """
    try:
        cols, rows = redshift_conn.execute_query(sql, params)
        
        return {
            'success': True,
            'columns': cols,
            'rows': rows
        }
    
    except RedshiftQueryError as e:
        return {'success': False, 'message': str(e)}
    except Exception as e:
        logger.exception(f"Unexpected error in Redshift query execution: {e}")
        return {'success': False, 'message': f'예상치 못한 오류: {e}'}