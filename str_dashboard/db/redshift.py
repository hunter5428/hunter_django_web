# str_dashboard/db/redshift.py
"""
Redshift 데이터베이스 연결 및 쿼리 실행 유틸리티 (읽기 전용)
"""
import os
import logging
from functools import wraps
from typing import Tuple, List, Optional, Dict, Any
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor
from django.http import JsonResponse

logger = logging.getLogger(__name__)


class RedshiftConnectionError(Exception):
    """Redshift 연결 관련 커스텀 예외"""
    pass


class RedshiftQueryError(Exception):
    """Redshift 쿼리 실행 관련 커스텀 예외"""
    pass


class RedshiftConnection:
    """Redshift 데이터베이스 연결 관리 클래스 (읽기 전용)"""
    
    def __init__(self, host: str, port: str, dbname: str, username: str, password: str):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.username = username
        self.password = password
        self._connection = None
    
    @classmethod
    def from_session(cls, session_data: Dict[str, Any]) -> 'RedshiftConnection':
        """세션 데이터에서 연결 객체 생성"""
        return cls(**session_data)
    
    def connect(self):
        """데이터베이스 연결 (읽기 전용)"""
        try:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.dbname,
                user=self.username,
                password=self.password,
                sslmode='require',
                connect_timeout=10,
                options='-c default_transaction_read_only=on'
            )
            self._connection.autocommit = True
            logger.debug("Redshift connected successfully (read-only mode)")
            return self._connection
        except psycopg2.OperationalError as e:
            error_msg = self._parse_redshift_error(str(e))
            logger.exception(f"Redshift connection failed: {error_msg}")
            raise RedshiftConnectionError(error_msg)
    
    def close(self):
        """연결 종료"""
        if self._connection:
            self._connection.close()
            self._connection = None
            logger.debug("Redshift connection closed")
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> Tuple[List[str], List[List]]:
        """쿼리 실행 및 결과 반환 (SELECT만 허용)"""
        if not sql.strip().upper().startswith(('SELECT', 'WITH')):
            raise RedshiftQueryError("읽기 전용 연결에서는 SELECT 쿼리만 실행할 수 있습니다.")
        
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute(sql, params or [])
                if cursor.description:
                    cols = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return cols, rows
                return [], []
        except psycopg2.Error as e:
            logger.exception(f"Redshift query execution failed: {e}")
            raise RedshiftQueryError(f"쿼리 실행 실패: {e}")
        finally:
            self.close()

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            conn = self.connect()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return cursor.fetchone()[0] == 1
        except Exception as e:
            logger.error(f"Redshift connection test failed: {e}")
            return False
        finally:
            self.close()

    @staticmethod
    def _parse_redshift_error(error_text: str) -> str:
        """Redshift 에러 메시지 파싱"""
        if 'authentication failed' in error_text.lower():
            return '연결 실패: 사용자명 또는 비밀번호가 올바르지 않습니다.'
        elif 'timeout' in error_text.lower():
            return '연결 실패: 연결 시간이 초과되었습니다.'
        return f'연결 실패: {error_text}'


def require_redshift_connection(func):
    """Redshift 연결 확인 데코레이터"""
    @wraps(func)
    def wrapper(request, *args, **kwargs):
        if request.session.get('rs_conn_status') != 'ok':
            return JsonResponse({
                'success': False,
                'message': '먼저 Redshift DB 연결을 완료해 주세요.'
            })
        try:
            rs_info = request.session.get('rs_conn')
            redshift_conn = RedshiftConnection.from_session(rs_info)
            return func(request, redshift_conn=redshift_conn, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Redshift connection setup failed: {e}")
            return JsonResponse({
                'success': False,
                'message': f'Redshift 연결 설정 실패: {e}'
            })
    return wrapper