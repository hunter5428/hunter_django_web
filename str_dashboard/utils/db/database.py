# str_dashboard/utils/db/database.py
"""
데이터베이스 연결 관리 모듈
Oracle (jaydebeapi) + Redshift (psycopg2)
"""

import logging
from typing import Dict, Any, Optional
from contextlib import contextmanager

import jaydebeapi
import psycopg2

logger = logging.getLogger(__name__)


# 기본 연결 설정
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
            raise
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
            raise
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


def get_default_config():
    """기본 설정값 반환"""
    return DEFAULT_CONFIG