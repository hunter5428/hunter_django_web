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
from psycopg2.extras import RealDictCursor
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
                cols = [d[0] for d in cursor.description] if cursor.description else []
                return cols, rows
            except Exception as e:
                logger.exception(f"Query execution failed: {e}")
                raise OracleQueryError(f"쿼리 실행 실패: {e}")
    
    def test_connection(self) -> bool:
        """연결 테스트"""
        try:
            conn = self.connect()
            self.close()
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
        elif 'ORA-28000' in error_text:
            return '연결 실패: 계정이 잠겼습니다. (ORA-28000)'
        return f'연결 실패: {error_text}'


# ==================== Redshift 연결 클래스 ====================
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
        return cls(
            host=session_data['host'],
            port=session_data['port'],
            dbname=session_data['dbname'],
            username=session_data['username'],
            password=session_data['password']
        )
    
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
        """커서 컨텍스트 매니저 (읽기 전용)"""
        conn = None
        cursor = None
        try:
            conn = self.connect()
            conn.set_session(readonly=True, autocommit=True)
            cursor = conn.cursor(cursor_factory=cursor_factory)
            yield cursor
        except Exception as e:
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
            self.close()
    
    def execute_query(self, sql: str, params: Optional[List] = None) -> Tuple[List[str], List[List]]:
        """쿼리 실행 및 결과 반환 (읽기 전용 - SELECT만 허용)"""
        params = params or []
        
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
            raise RedshiftQueryError("읽기 전용 연결입니다. SELECT 쿼리만 실행 가능합니다.")
        
        conn = None
        cursor = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            logger.debug(f"Executing Redshift query with {len(params)} parameters")
            cursor.execute(sql, params)
            
            if cursor.description:
                cols = [desc[0] for desc in cursor.description]
                
                rows = []
                batch_size = 10000
                
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    rows.extend(batch)
                    
                    if len(rows) > 1000000:
                        logger.warning(f"Large result set: {len(rows)} rows fetched")
                
                logger.debug(f"Query returned {len(rows)} rows")
                return cols, rows
            else:
                return [], []
                
        except psycopg2.Error as e:
            logger.error(f"Redshift query error: {e}")
            raise RedshiftQueryError(f"쿼리 실행 실패: {str(e)}")
        except Exception as e:
            logger.exception(f"Unexpected error in Redshift query execution: {e}")
            raise RedshiftQueryError(f"쿼리 실행 실패: {str(e)}")
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception as e:
                    logger.warning(f"Error closing cursor: {e}")
            
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logger.warning(f"Error closing connection: {e}")
    
    def test_connection(self) -> bool:
        """연결 테스트 (읽기 전용)"""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            self.close()
            return result[0] == 1
        except Exception as e:
            logger.error(f"Redshift connection test failed: {e}")
            return False
    
    @staticmethod
    def _parse_redshift_error(error_text: str) -> str:
        """Redshift 에러 메시지 파싱"""
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
            return '연결 실패: 권한이 없습니다. 읽기 권한을 확인하세요.'
        else:
            return f'연결 실패: {error_text}'


# ==================== SQL 쿼리 관리 ====================
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
        except Exception as e:
            raise Exception(f"SQL 파일 로드 실패: {e}")
    
    @staticmethod
    def strip_comments(sql: str) -> str:
        """SQL 주석 제거"""
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
        sql = re.sub(r"--.*?$", "", sql, flags=re.M)
        return sql.strip()
    
    @staticmethod
    def prepare_sql(sql: str, bind_params: Optional[Dict[str, str]] = None) -> Tuple[str, int]:
        """SQL 준비 (주석 제거, 바인드 변수 처리)"""
        sql = SQLQueryManager.strip_comments(sql)
        
        param_count = 0
        if bind_params:
            for bind_var, placeholder in bind_params.items():
                count = sql.count(bind_var)
                sql = sql.replace(bind_var, placeholder)
                param_count += count
                logger.debug(f"Bind variable {bind_var}: replaced {count} times")
        else:
            param_count = sql.count("?")
        
        if sql.rstrip().endswith(";"):
            sql = sql.rstrip()[:-1]
        
        logger.debug(f"Prepared SQL - total parameter count: {param_count}")
        
        return sql, param_count
    
    @classmethod
    def load_and_prepare(cls, filename: str, bind_params: Optional[Dict[str, str]] = None) -> Tuple[str, int]:
        """SQL 파일 로드 및 준비"""
        raw_sql = cls.load_sql(filename)
        return cls.prepare_sql(raw_sql, bind_params)


# ==================== 데코레이터 ====================
def require_oracle_connection(func):
    """Oracle 연결이 필요한 뷰 데코레이터"""
    @wraps(func)
    def wrapper(request, *args, **kwargs):
        db_info = request.session.get('db_conn')
        if not db_info or request.session.get('db_conn_status') != 'ok':
            return JsonResponse({
                'success': False,
                'message': '먼저 Oracle 연결을 완료해 주세요.'
            })
        
        try:
            oracle_conn = OracleConnection.from_session(db_info)
            return func(request, oracle_conn=oracle_conn, *args, **kwargs)
        except Exception as e:
            logger.exception(f"DB connection setup failed: {e}")
            return JsonResponse({
                'success': False,
                'message': f'데이터베이스 연결 설정 실패: {e}'
            })
    
    return wrapper


def require_redshift_connection(func):
    """Redshift 연결이 필요한 뷰 데코레이터"""
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
            return func(request, redshift_conn=redshift_conn, *args, **kwargs)
        except Exception as e:
            logger.exception(f"Redshift connection setup failed: {e}")
            return JsonResponse({
                'success': False,
                'message': f'Redshift 연결 설정 실패: {e}'
            })
    
    return wrapper


# ==================== 헬퍼 함수 ====================
def execute_oracle_query_with_error_handling(
    oracle_conn: OracleConnection,
    sql_filename: str,
    bind_params: Optional[Dict[str, str]] = None,
    query_params: Optional[List] = None
) -> Dict[str, Any]:
    """Oracle 쿼리 실행 및 에러 처리"""
    try:
        prepared_sql, param_count = SQLQueryManager.load_and_prepare(sql_filename, bind_params)
        
        logger.debug(f"SQL File: {sql_filename}")
        logger.debug(f"Expected params: {param_count}, Provided params: {len(query_params) if query_params else 0}")
        
        if param_count > 0 and not query_params:
            return {
                'success': False,
                'message': f'쿼리에 {param_count}개의 파라미터가 필요하지만 제공되지 않았습니다.'
            }
        
        if param_count != (len(query_params) if query_params else 0):
            return {
                'success': False,
                'message': f'파라미터 개수 불일치: 필요 {param_count}개, 제공 {len(query_params) if query_params else 0}개'
            }
        
        cols, rows = oracle_conn.execute_query(prepared_sql, query_params)
        
        return {
            'success': True,
            'columns': cols,
            'rows': rows
        }
    
    except FileNotFoundError as e:
        return {'success': False, 'message': str(e)}
    except OracleQueryError as e:
        return {'success': False, 'message': str(e)}
    except Exception as e:
        logger.exception(f"Unexpected error in query execution: {e}")
        return {'success': False, 'message': f'예상치 못한 오류: {e}'}


def execute_redshift_query_with_error_handling(
    redshift_conn: RedshiftConnection,
    sql: str,
    params: Optional[List] = None
) -> Dict[str, Any]:
    """Redshift 쿼리 실행 및 에러 처리"""
    try:
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
            return {
                'success': False,
                'message': '읽기 전용 연결입니다. SELECT 쿼리만 실행 가능합니다.'
            }
        
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