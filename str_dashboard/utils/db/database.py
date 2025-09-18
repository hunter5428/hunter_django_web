# str_dashboard/utils/db/database.py
"""
데이터베이스 연결 관리 모듈
Oracle (jaydebeapi) + Redshift (psycopg2)
"""

import logging
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
from pathlib import Path

import jaydebeapi
import psycopg2

logger = logging.getLogger(__name__)


# ==================== 예외 클래스 정의 ====================
class OracleConnectionError(Exception):
    """Oracle 연결 관련 예외"""
    pass


class OracleQueryError(Exception):
    """Oracle 쿼리 실행 관련 예외"""
    pass


class RedshiftConnectionError(Exception):
    """Redshift 연결 관련 예외"""
    pass


class RedshiftQueryError(Exception):
    """Redshift 쿼리 실행 관련 예외"""
    pass


# ==================== 기본 연결 설정 ====================
DEFAULT_CONFIG = {
    'ORACLE': {
        'HOST': '127.0.0.1',
        'PORT': '40112',
        'SERVICE': 'PRDAMLKR.OCIAMLPRODDBA.OCIAMLPROD.ORACLEVCN.COM',
        'USERNAME': 'BTCAMLDB_OWN_READ',
        'PASSWORD': ''
    },
    'REDSHIFT': {
        'HOST': '127.0.0.1',
        'PORT': '40127',
        'DBNAME': 'prod',
        'USERNAME': 'aml_user',
        'PASSWORD': ''
    }
}


# ==================== Oracle 연결 클래스 ====================
class OracleConnection:
    """Oracle 데이터베이스 연결 관리 클래스"""
    
    def __init__(self, jdbc_url: str, username: str, password: str,
                 driver_path: Optional[str] = None, driver_class: Optional[str] = None):
        self.jdbc_url = jdbc_url
        self.username = username
        self.password = password
        self.driver_path = driver_path or r'C:\ojdbc11-21.5.0.0.jar'
        self.driver_class = driver_class or 'oracle.jdbc.driver.OracleDriver'
    
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
        """트랜잭션 컨텍스트 매니저"""
        conn = None
        try:
            conn = jaydebeapi.connect(
                self.driver_class,
                self.jdbc_url,
                [self.username, self.password],
                self.driver_path
            )
            logger.debug("Oracle connection opened")
            
            try:
                conn.jconn.setDefaultRowPrefetch(prefetch)
            except Exception as e:
                logger.debug(f"Could not set row prefetch: {e}")
            
            yield conn
            
        except Exception as e:
            logger.exception(f"Oracle connection failed: {e}")
            raise OracleConnectionError(f"Oracle 연결 실패: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                    logger.debug("Oracle connection closed")
                except Exception as e:
                    logger.warning(f"Error closing Oracle connection: {e}")
    
    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            with self.transaction():
                return True
        except Exception:
            return False


# ==================== Redshift 연결 클래스 ====================
class RedshiftConnection:
    """Redshift 데이터베이스 연결 관리 클래스"""
    
    def __init__(self, **kwargs):
        self.conn_params = {
            'host': kwargs.get('host', DEFAULT_CONFIG['REDSHIFT']['HOST']),
            'port': kwargs.get('port', DEFAULT_CONFIG['REDSHIFT']['PORT']),
            'dbname': kwargs.get('dbname', DEFAULT_CONFIG['REDSHIFT']['DBNAME']),
            'user': kwargs.get('username', kwargs.get('user')),
            'password': kwargs.get('password', '')
        }
    
    @classmethod
    def from_session(cls, session_data: Dict[str, Any]) -> 'RedshiftConnection':
        """세션 데이터에서 연결 객체 생성"""
        return cls(**session_data)
    
    @contextmanager
    def transaction(self):
        """트랜잭션 컨텍스트 매니저"""
        conn = None
        try:
            sslmode = 'prefer' if self.conn_params['host'] in ['127.0.0.1', 'localhost'] else 'require'
            
            conn = psycopg2.connect(
                **self.conn_params,
                sslmode=sslmode,
                connect_timeout=10
            )
            conn.set_session(readonly=True, autocommit=True)
            logger.debug("Redshift connection opened")
            
            yield conn
            
        except Exception as e:
            logger.exception(f"Redshift connection failed: {e}")
            raise RedshiftConnectionError(f"Redshift 연결 실패: {e}")
        finally:
            if conn:
                try:
                    conn.close()
                    logger.debug("Redshift connection closed")
                except Exception as e:
                    logger.warning(f"Error closing Redshift connection: {e}")
    
    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            with self.transaction() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    return cursor.fetchone()[0] == 1
        except Exception:
            return False


# ==================== SQL 쿼리 관리 ====================
class SQLQueryManager:
    """SQL 쿼리 파일 관리 클래스"""
    
    def __init__(self, base_path: Optional[Path] = None):
        """
        Args:
            base_path: SQL 파일이 저장된 기본 경로
        """
        if base_path is None:
            # 기본적으로 str_dashboard/queries 경로 사용
            from django.conf import settings
            self.base_path = Path(settings.BASE_DIR) / 'str_dashboard' / 'queries'
        else:
            self.base_path = Path(base_path)
    
    def load_query(self, filename: str) -> str:
        """
        SQL 쿼리 파일을 로드
        
        Args:
            filename: SQL 파일명 (확장자 포함)
            
        Returns:
            SQL 쿼리 문자열
            
        Raises:
            FileNotFoundError: 파일이 없는 경우
        """
        file_path = self.base_path / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"SQL file not found: {file_path}")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def load_query_with_params(self, filename: str, **params) -> str:
        """
        SQL 쿼리 파일을 로드하고 파라미터를 치환
        
        Args:
            filename: SQL 파일명
            **params: 치환할 파라미터들
            
        Returns:
            파라미터가 치환된 SQL 쿼리
        """
        query = self.load_query(filename)
        
        # 파라미터 치환 (예: {param_name} -> 실제 값)
        for key, value in params.items():
            query = query.replace(f'{{{key}}}', str(value))
        
        return query


# ==================== 헬퍼 함수 ====================
def execute_oracle_query(connection: OracleConnection, query: str, 
                        params: Optional[List] = None) -> Dict[str, Any]:
    """
    Oracle 쿼리 실행 헬퍼 함수
    
    Args:
        connection: OracleConnection 인스턴스
        query: 실행할 SQL 쿼리
        params: 바인드 파라미터 리스트
        
    Returns:
        {'success': bool, 'columns': [...], 'rows': [...], 'message': ...}
    """
    try:
        with connection.transaction() as conn:
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # 결과 가져오기
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                return {
                    'success': True,
                    'columns': columns,
                    'rows': rows
                }
                
    except Exception as e:
        logger.error(f"Oracle query execution failed: {e}")
        raise OracleQueryError(f"쿼리 실행 실패: {e}")


def execute_redshift_query(connection: RedshiftConnection, query: str,
                          params: Optional[List] = None) -> Dict[str, Any]:
    """
    Redshift 쿼리 실행 헬퍼 함수
    
    Args:
        connection: RedshiftConnection 인스턴스
        query: 실행할 SQL 쿼리
        params: 바인드 파라미터 리스트
        
    Returns:
        {'success': bool, 'columns': [...], 'rows': [...], 'message': ...}
    """
    try:
        with connection.transaction() as conn:
            with conn.cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                # 결과 가져오기
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                
                return {
                    'success': True,
                    'columns': columns,
                    'rows': rows
                }
                
    except Exception as e:
        logger.error(f"Redshift query execution failed: {e}")
        raise RedshiftQueryError(f"쿼리 실행 실패: {e}")


def get_default_config():
    """기본 설정값 반환"""
    return DEFAULT_CONFIG