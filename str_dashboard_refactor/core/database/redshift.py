"""
Amazon Redshift database connection module
"""
import logging
from typing import Tuple, List, Optional, Dict, Any
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from .base import (
    DatabaseConnectionBase,
    ConnectionConfig,
    DatabaseConnectionError,
    DatabaseQueryError
)

logger = logging.getLogger(__name__)


class RedshiftConnectionError(DatabaseConnectionError):
    """Redshift 연결 관련 커스텀 예외"""
    pass


class RedshiftQueryError(DatabaseQueryError):
    """Redshift 쿼리 실행 관련 커스텀 예외"""
    pass


class RedshiftConnection(DatabaseConnectionBase):
    """Redshift 데이터베이스 연결 관리 클래스 (읽기 전용)"""
    
    def __init__(self, config: ConnectionConfig):
        """
        Redshift 연결 초기화
        
        Args:
            config: 연결 설정 객체
        """
        super().__init__(config)
        
        # Redshift는 database 필드 사용 (dbname)
        if not config.database:
            raise RedshiftConnectionError("Redshift 연결에는 database 이름이 필요합니다.")
    
    def connect(self):
        """데이터베이스 연결 (읽기 전용)"""
        try:
            self._connection = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.database,
                user=self.config.username,
                password=self.config.password,
                sslmode='require',
                connect_timeout=10,
                options='-c default_transaction_read_only=on'  # 읽기 전용 설정
            )
            # autocommit 활성화 (읽기 전용이므로 트랜잭션 불필요)
            self._connection.autocommit = True
            self.logger.debug("Redshift connected successfully (read-only mode)")
            return self._connection
            
        except psycopg2.OperationalError as e:
            error_msg = self._parse_redshift_error(str(e))
            self.logger.exception(f"Redshift connection failed: {error_msg}")
            raise RedshiftConnectionError(error_msg)
        except Exception as e:
            self.logger.exception(f"Unexpected Redshift connection error: {e}")
            raise RedshiftConnectionError(f"연결 실패: {e}")
    
    def close(self):
        """연결 종료"""
        if self._connection:
            try:
                self._connection.close()
                self.logger.debug("Redshift connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing Redshift connection: {e}")
            finally:
                self._connection = None
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """커서 컨텍스트 매니저 (읽기 전용)"""
        conn = None
        cursor = None
        try:
            conn = self.connect()
            # 읽기 전용 트랜잭션 설정 (이미 연결 시 설정했지만 명시적으로)
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
        """
        쿼리 실행 및 결과 반환 (읽기 전용 - SELECT만 허용)
        
        Args:
            sql: SQL 쿼리
            params: 파라미터 리스트
            
        Returns:
            (columns, rows) 튜플
        """
        params = params or []
        
        # SELECT 쿼리만 허용
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
            raise RedshiftQueryError("읽기 전용 연결입니다. SELECT 쿼리만 실행 가능합니다.")
        
        conn = None
        cursor = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            # 쿼리 실행 전 로깅
            self.logger.debug(f"Executing Redshift query with {len(params)} parameters")
            
            # 쿼리 실행
            cursor.execute(sql, params)
            
            # SELECT 쿼리 결과 반환
            if cursor.description:
                cols = [desc[0] for desc in cursor.description]
                
                # 대용량 데이터 처리를 위한 배치 페치
                rows = []
                batch_size = 10000  # 배치 크기 설정
                
                while True:
                    batch = cursor.fetchmany(batch_size)
                    if not batch:
                        break
                    rows.extend(batch)
                    
                    # 메모리 사용량 체크 (옵션)
                    if len(rows) > 1000000:  # 100만 행 이상이면 경고
                        self.logger.warning(f"Large result set: {len(rows)} rows fetched")
                
                self.logger.debug(f"Query returned {len(rows)} rows")
                return cols, rows
            else:
                return [], []
                
        except psycopg2.OperationalError as e:
            # 연결 관련 에러
            self.logger.error(f"Redshift connection error during query: {e}")
            raise RedshiftQueryError(f"연결 오류: {self._parse_redshift_error(str(e))}")
            
        except psycopg2.ProgrammingError as e:
            # SQL 구문 에러
            self.logger.error(f"Redshift SQL error: {e}")
            raise RedshiftQueryError(f"SQL 오류: {str(e)}")
            
        except psycopg2.DataError as e:
            # 데이터 타입 에러
            self.logger.error(f"Redshift data error: {e}")
            raise RedshiftQueryError(f"데이터 오류: {str(e)}")
            
        except Exception as e:
            # 기타 예상치 못한 에러
            self.logger.exception(f"Unexpected error in Redshift query execution: {e}")
            raise RedshiftQueryError(f"쿼리 실행 실패: {str(e)}")
            
        finally:
            # 리소스 정리
            if cursor:
                try:
                    cursor.close()
                    self.logger.debug("Cursor closed")
                except Exception as e:
                    self.logger.warning(f"Error closing cursor: {e}")
            
            if conn:
                try:
                    conn.close()
                    self.logger.debug("Connection closed")
                except Exception as e:
                    self.logger.warning(f"Error closing connection: {e}")
    
    def execute_query_dict(self, sql: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """
        쿼리 실행 및 딕셔너리 형태로 결과 반환 (읽기 전용)
        
        Args:
            sql: SQL 쿼리
            params: 파라미터 리스트
            
        Returns:
            딕셔너리 리스트
        """
        params = params or []
        
        # SELECT 쿼리만 허용
        sql_upper = sql.strip().upper()
        if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
            raise RedshiftQueryError("읽기 전용 연결입니다. SELECT 쿼리만 실행 가능합니다.")
        
        with self.get_cursor(cursor_factory=RealDictCursor) as cursor:
            try:
                cursor.execute(sql, params)
                
                if cursor.description:
                    return cursor.fetchall()
                else:
                    return []
                    
            except psycopg2.Error as e:
                self.logger.exception(f"Redshift query execution failed: {e}")
                raise RedshiftQueryError(f"쿼리 실행 실패: {e}")
    
    def test_connection(self) -> bool:
        """연결 테스트 (읽기 전용)"""
        try:
            conn = self.connect()
            cursor = conn.cursor()
            # 간단한 읽기 전용 테스트 쿼리
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            self.close()
            return result[0] == 1
        except Exception as e:
            self.logger.error(f"Redshift connection test failed: {e}")
            return False
    
    @staticmethod
    def _parse_redshift_error(error_text: str) -> str:
        """Redshift 에러 메시지 파싱 및 사용자 친화적 메시지 변환"""
        error_lower = error_text.lower()
        
        error_mappings = {
            'could not connect to server': '연결 실패: 서버에 연결할 수 없습니다. 호스트와 포트를 확인하세요.',
            'authentication failed': '연결 실패: 사용자명 또는 비밀번호가 올바르지 않습니다.',
            'password authentication failed': '연결 실패: 사용자명 또는 비밀번호가 올바르지 않습니다.',
            'database': '연결 실패: 데이터베이스가 존재하지 않습니다.',
            'does not exist': '연결 실패: 데이터베이스가 존재하지 않습니다.',
            'timeout': '연결 실패: 연결 시간이 초과되었습니다.',
            'permission denied': '연결 실패: 권한이 없습니다. 읽기 권한을 확인하세요.',
            'ssl': '연결 실패: SSL 연결 오류가 발생했습니다.',
            'read-only': '연결 실패: 읽기 전용 권한 설정 오류입니다.',
            'readonly': '연결 실패: 읽기 전용 권한 설정 오류입니다.',
        }
        
        for key, message in error_mappings.items():
            if key in error_lower:
                return message
        
        return f'연결 실패: {error_text}'