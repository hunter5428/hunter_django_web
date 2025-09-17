# str_dashboard/database.py
"""
데이터베이스 연결 및 쿼리 실행 통합 모듈
Oracle (jaydebeapi) + Redshift (psycopg2)
"""

import os
import re
import logging
from functools import wraps
from pathlib import Path
from typing import Tuple, List, Optional, Dict, Any
from contextlib import contextmanager

import jaydebeapi
import psycopg2
from django.conf import settings
from django.http import JsonResponse

logger = logging.getLogger(__name__)


# ==================== 예외 클래스 ====================
class OracleConnectionError(Exception):
    """Oracle 연결 관련 커스텀 예외"""
    pass


class OracleQueryError(Exception):
    """Oracle 쿼리 실행 관련 커스텀 예외"""
    pass


class RedshiftConnectionError(Exception):
    """Redshift 연결 관련 커스텀 예외"""
    pass


class RedshiftQueryError(Exception):
    """Redshift 쿼리 실행 관련 커스텀 예외"""
    pass


# ==================== Oracle 연결 클래스 ====================
class OracleConnection:
    """Oracle 데이터베이스 연결 관리 클래스"""
    
    def __init__(self, jdbc_url: str, username: str, password: str,
                 driver_path: Optional[str] = None, driver_class: Optional[str] = None):
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password
        self.driver_path = driver_path or os.getenv('ORACLE_JAR', r'C:\ojdbc11-21.5.0.0.jar')
        self.driver_class = driver_class or os.getenv('ORACLE_DRIVER', 'oracle.jdbc.driver.OracleDriver')
    
    @classmethod
    def from_session(cls, session_data: Dict[str, Any]) -> 'OracleConnection':
        """세션 데이터에서 연결 객체 생성"""
        return cls(**session_data)
    
    @staticmethod
    def build_jdbc_url(host: str, port: str, service_name: str) -> str:
        """JDBC URL 생성"""
        return f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"
    
    @contextmanager
    def transaction(self, prefetch: int = 1000):
        """하나의 요청 내에서 커넥션을 유지하고 재사용하기 위한 컨텍스트 매니저"""
        conn = None
        try:
            conn = jaydebeapi.connect(
                self.driver_class,
                self.jdbc_url,
                [self.username, self.password],
                self.driver_path
            )
            logger.debug("Oracle connection opened for a transaction.")
            try:
                conn.jconn.setDefaultRowPrefetch(prefetch)
            except Exception as e:
                 logger.debug(f"Could not set row prefetch: {e}")
            
            yield conn
        except Exception as e:
            error_msg = self._parse_oracle_error(str(e))
            logger.exception(f"Oracle JDBC connection failed: {error_msg}")
            raise OracleConnectionError(error_msg) from e
        finally:
            if conn:
                try:
                    conn.close()
                    logger.debug("Oracle connection closed after a transaction.")
                except Exception as e:
                    logger.warning(f"Error closing Oracle connection: {e}")
    
    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            with self.transaction():
                pass
            return True
        except OracleConnectionError:
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
        elif 'ORA-28000' in error_text:
            return '연결 실패: 계정이 잠겼습니다. (ORA-28000)'
        return f'연결 실패: {error_text.splitlines()[0]}'


# ==================== Redshift 연결 클래스 ====================
class RedshiftConnection:
    """Redshift 데이터베이스 연결 관리 클래스 (읽기 전용)"""
    
    def __init__(self, **kwargs):
        self.conn_params = kwargs
    
    @classmethod
    def from_session(cls, session_data: Dict[str, Any]) -> 'RedshiftConnection':
        """세션 데이터에서 연결 객체 생성"""
        return cls(**session_data)
    
    @contextmanager
    def transaction(self):
        """하나의 요청 내에서 커넥션을 유지하고 재사용하기 위한 컨텍스트 매니저"""
        conn = None
        try:
            conn = psycopg2.connect(
                **self.conn_params,
                sslmode='require',
                connect_timeout=10
            )
            conn.set_session(readonly=True, autocommit=True)
            logger.debug("Redshift connection opened for a transaction.")
            yield conn
        except psycopg2.OperationalError as e:
            error_msg = self._parse_redshift_error(str(e))
            logger.exception(f"Redshift connection failed: {error_msg}")
            raise RedshiftConnectionError(error_msg) from e
        except Exception as e:
            logger.exception(f"Unexpected Redshift connection error: {e}")
            raise RedshiftConnectionError(f"연결 실패: {e}") from e
        finally:
            if conn:
                try:
                    conn.close()
                    logger.debug("Redshift connection closed after a transaction.")
                except Exception as e:
                    logger.warning(f"Error closing Redshift connection: {e}")

    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            with self.transaction() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    result = cursor.fetchone()
                    return result[0] == 1
        except RedshiftConnectionError:
            return False
    
    @staticmethod
    def _parse_redshift_error(error_text: str) -> str:
        """Redshift 에러 메시지 파싱"""
        error_lower = error_text.lower()
        if 'could not connect to server' in error_lower:
            return '연결 실패: 서버에 연결할 수 없습니다. 호스트와 포트를 확인하세요.'
        elif 'authentication failed' in error_lower:
            return '연결 실패: 사용자명 또는 비밀번호가 올바르지 않습니다.'
        elif 'database' in error_lower and 'does not exist' in error_lower:
            return '연결 실패: 데이터베이스가 존재하지 않습니다.'
        return f'연결 실패: {error_text.splitlines()[0]}'


# ==================== SQL 쿼리 관리 ====================
class SQLQueryManager:
    """SQL 쿼리 파일 관리 클래스"""
    
    QUERIES_DIR = settings.BASE_DIR / 'str_dashboard' / 'queries'
    
    @classmethod
    def load_sql(cls, filename: str) -> str:
        """SQL 파일 로드"""
        sql_path = cls.QUERIES_DIR / filename
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL 파일을 찾을 수 없습니다: {sql_path}")
        return sql_path.read_text(encoding='utf-8')
    
    @staticmethod
    def prepare_sql(sql: str, bind_params: Optional[Dict[str, str]] = None) -> Tuple[str, int]:
        """SQL 준비 (바인드 변수 처리)"""
        sql = re.sub(r"--.*?$", "", sql, flags=re.M).strip()
        
        param_count = 0
        if bind_params:
            for bind_var, placeholder in bind_params.items():
                count = len(re.findall(bind_var, sql))
                sql = re.sub(bind_var, placeholder, sql)
                param_count += count
        else:
            param_count = sql.count("?")
        
        return sql.removesuffix(";"), param_count

# ==================== 헬퍼 함수 ====================
def execute_oracle_query(
    db_conn: jaydebeapi.Connection,
    sql_filename: str,
    bind_params: Dict[str, str],
    query_params: List
) -> Dict[str, Any]:
    """Oracle 쿼리 실행 및 에러 처리 (수정된 시그니처)"""
    try:
        raw_sql = SQLQueryManager.load_sql(sql_filename)
        prepared_sql, param_count = SQLQueryManager.prepare_sql(raw_sql, bind_params)

        if param_count != len(query_params):
            raise OracleQueryError(
                f"파라미터 개수 불일치 ({sql_filename}): 필요 {param_count}개, 제공 {len(query_params)}개"
            )

        with db_conn.cursor() as cursor:
            cursor.execute(prepared_sql, query_params)
            rows = cursor.fetchall()
            cols = [d[0] for d in cursor.description] if cursor.description else []
        
        return {'success': True, 'columns': cols, 'rows': rows}

    except (FileNotFoundError, OracleQueryError) as e:
        logger.error(str(e))
        return {'success': False, 'message': str(e)}
    except Exception as e:
        logger.exception(f"Oracle 쿼리 실행 중 예외 발생 ({sql_filename}): {e}")
        raise OracleQueryError(f"{sql_filename} 쿼리 실행 실패") from e