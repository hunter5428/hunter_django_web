# str_dashboard/db/oracle.py
"""
Oracle 데이터베이스 연결 및 쿼리 실행 유틸리티
"""
import os
import re
import logging
from functools import wraps
from pathlib import Path
from typing import Tuple, List, Optional, Dict, Any
from contextlib import contextmanager

import jaydebeapi
from django.conf import settings
from django.http import JsonResponse

logger = logging.getLogger(__name__)


class OracleConnectionError(Exception):
    """Oracle 연결 관련 커스텀 예외"""
    pass


class OracleQueryError(Exception):
    """Oracle 쿼리 실행 관련 커스텀 예외"""
    pass


class OracleConnection:
    """Oracle 데이터베이스 연결 관리 클래스"""
    
    def __init__(self, jdbc_url: str, username: str, password: str,
                 driver_path: Optional[str] = None, driver_class: Optional[str] = None):
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password
        self.driver_path = driver_path or os.getenv('ORACLE_JAR', r'C:\ojdbc11-21.5.0.0.jar')
        self.driver_class = driver_class or os.getenv('ORACLE_DRIVER', 'oracle.jdbc.driver.OracleDriver')
        self._connection = None
    
    @classmethod
    def from_session(cls, session_data: Dict[str, Any]) -> 'OracleConnection':
        """세션 데이터에서 연결 객체 생성"""
        return cls(
            jdbc_url=session_data['jdbc_url'],
            username=session_data['username'],
            password=session_data['password'],
            driver_path=session_data.get('driver_path'),
            driver_class=session_data.get('driver_class')
        )
    
    @classmethod
    def build_jdbc_url(cls, host: str, port: str, service_name: str) -> str:
        """JDBC URL 생성"""
        return f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
    
    def connect(self):
        """데이터베이스 연결"""
        try:
            self._connection = jaydebeapi.connect(
                self.driver_class,
                self.jdbc_url,
                [self.username, self.password],
                self.driver_path
            )
            logger.debug("Oracle JDBC connected successfully")
            return self._connection
        except Exception as e:
            error_msg = self._parse_oracle_error(str(e))
            logger.exception(f"Oracle JDBC connection failed: {error_msg}")
            raise OracleConnectionError(error_msg)
    
    def close(self):
        """연결 종료"""
        if self._connection:
            try:
                self._connection.close()
                logger.debug("Oracle connection closed")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None
    
    @contextmanager
    def get_cursor(self, prefetch: int = 1000):
        """커서 컨텍스트 매니저"""
        conn = None
        cursor = None
        try:
            conn = self.connect()
            try:
                conn.jconn.setDefaultRowPrefetch(prefetch)
            except Exception:
                pass 
            
            cursor = conn.cursor()
            yield cursor
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            self.close()
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> Tuple[List[str], List[List]]:
        """쿼리 실행 및 결과 반환"""
        params = params or []
        
        with self.get_cursor() as cursor:
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                cols = [d[0] for d in cursor.description] if curs.description else []
                return cols, rows
            except Exception as e:
                logger.exception(f"Query execution failed: {e}")
                raise OracleQueryError(f"쿼리 실행 실패: {e}")
    
    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            self.connect().close()
            return True
        except Exception:
            return False
    
    @staticmethod
    def _parse_oracle_error(error_text: str) -> str:
        """Oracle 에러 메시지 파싱"""
        if 'ORA-12514' in error_text:
            return '연결 실패: SERVICE_NAME을 확인하세요. (ORA-12514)'
        elif 'ORA-12154' in error_text:
            return '연결 실패: 호스트/포트/서비스명을 확인하세요. (ORA-12154)'
        elif 'ORA-01017' in error_text:
            return '연결 실패: 사용자명 또는 비밀번호가 올바르지 않습니다. (ORA-01017)'
        return f'연결 실패: {error_text}'


class SQLQueryManager:
    """SQL 쿼리 파일 관리 클래스"""
    
    QUERIES_DIR = settings.BASE_DIR / 'str_dashboard' / 'queries'
    
    @classmethod
    def load_sql(cls, filename: str) -> str:
        """SQL 파일 로드"""
        sql_path = cls.QUERIES_DIR / filename
        try:
            return sql_path.read_text(encoding='utf-8')
        except FileNotFoundError:
            raise FileNotFoundError(f"SQL 파일을 찾을 수 없습니다: {filename}")
    
    @staticmethod
    def prepare_sql(sql: str, params_dict: Optional[Dict[str, Any]] = None) -> Tuple[str, List[Any]]:
        """
        SQL 준비 (주석 제거, 바인드 변수 처리)
        - :key 형태의 명명된 파라미터를 '?' 플레이스홀더로 변환하고, 순서에 맞는 파라미터 리스트를 반환
        """
        # 주석 제거
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
        sql = re.sub(r"--.*?$", "", sql, flags=re.M)
        sql = sql.strip()

        if not params_dict:
            return sql, []

        # SQL에 나타나는 순서대로 바인드 변수 찾기
        bind_vars = re.findall(r":(\w+)", sql)
        
        # 순서에 맞게 파라미터 리스트 생성
        ordered_params = [params_dict[f":{var}"] for var in bind_vars if f":{var}" in params_dict]

        # SQL의 바인드 변수를 '?'로 치환
        prepared_sql = re.sub(r":(\w+)", "?", sql)

        if prepared_sql.rstrip().endswith(";"):
            prepared_sql = prepared_sql.rstrip()[:-1]

        return prepared_sql, ordered_params

    @classmethod
    def load_and_prepare(cls, filename: str, params_dict: Optional[Dict[str, Any]] = None) -> Tuple[str, List[Any]]:
        """SQL 파일 로드 및 준비"""
        raw_sql = cls.load_sql(filename)
        return cls.prepare_sql(raw_sql, params_dict)


def require_db_connection(func):
    """DB 연결 확인 데코레이터"""
    @wraps(func)
    def wrapper(request, *args, **kwargs):
        if request.session.get('db_conn_status') != 'ok':
            return JsonResponse({
                'success': False,
                'message': '먼저 Oracle DB 연결을 완료해 주세요.'
            })
        try:
            db_info = request.session.get('db_conn')
            oracle_conn = OracleConnection.from_session(db_info)
            return func(request, oracle_conn=oracle_conn, *args, **kwargs)
        except Exception as e:
            logger.exception(f"DB connection setup failed: {e}")
            return JsonResponse({
                'success': False,
                'message': f'DB 연결 설정 실패: {e}'
            })
    return wrapper