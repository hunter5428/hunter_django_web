"""
Oracle database connection module
"""
import os
import re
import logging
from typing import Tuple, List, Optional, Dict, Any
from contextlib import contextmanager
from pathlib import Path

import jaydebeapi
from django.conf import settings

from .base import (
    DatabaseConnectionBase,
    ConnectionConfig,
    DatabaseConnectionError,
    DatabaseQueryError
)

logger = logging.getLogger(__name__)


class OracleConnectionError(DatabaseConnectionError):
    """Oracle 연결 관련 커스텀 예외"""
    pass


class OracleQueryError(DatabaseQueryError):
    """Oracle 쿼리 실행 관련 커스텀 예외"""
    pass


class OracleConnection(DatabaseConnectionBase):
    """Oracle 데이터베이스 연결 관리 클래스"""
    
    # 기본 드라이버 설정
    DEFAULT_DRIVER_CLASS = 'oracle.jdbc.driver.OracleDriver'
    DEFAULT_DRIVER_PATH = os.getenv('ORACLE_JAR', r'C:\ojdbc11-21.5.0.0.jar')
    
    def __init__(self, config: ConnectionConfig):
        """
        Oracle 연결 초기화
        
        Args:
            config: 연결 설정 객체
        """
        super().__init__(config)
        
        # Oracle 특화 설정
        self.driver_class = config.options.get('driver_class', self.DEFAULT_DRIVER_CLASS)
        self.driver_path = config.options.get('driver_path', self.DEFAULT_DRIVER_PATH)
        
        # JDBC URL 생성
        self.jdbc_url = self._build_jdbc_url()
    
    def _build_jdbc_url(self) -> str:
        """JDBC URL 생성"""
        if self.config.service_name:
            return (
                f"jdbc:oracle:thin:@//"
                f"{self.config.host}:{self.config.port}/"
                f"{self.config.service_name}"
            )
        elif self.config.database:
            # SID 방식
            return (
                f"jdbc:oracle:thin:@"
                f"{self.config.host}:{self.config.port}:"
                f"{self.config.database}"
            )
        else:
            raise OracleConnectionError(
                "Oracle 연결에는 service_name 또는 database(SID)가 필요합니다."
            )
    
    @classmethod
    def from_jdbc_url(cls, jdbc_url: str, username: str, password: str,
                     driver_path: Optional[str] = None,
                     driver_class: Optional[str] = None) -> 'OracleConnection':
        """JDBC URL로부터 연결 객체 생성"""
        # JDBC URL 파싱
        pattern = r'jdbc:oracle:thin:@//([^:]+):(\d+)/(.+)'
        match = re.match(pattern, jdbc_url)
        
        if match:
            host, port, service_name = match.groups()
            config = ConnectionConfig(
                host=host,
                port=port,
                username=username,
                password=password,
                service_name=service_name,
                options={
                    'driver_path': driver_path or cls.DEFAULT_DRIVER_PATH,
                    'driver_class': driver_class or cls.DEFAULT_DRIVER_CLASS,
                    'jdbc_url': jdbc_url  # 원본 URL 저장
                }
            )
            return cls(config)
        else:
            raise OracleConnectionError(f"Invalid JDBC URL format: {jdbc_url}")
    
    def connect(self):
        """데이터베이스 연결"""
        try:
            self._connection = jaydebeapi.connect(
                self.driver_class,
                self.jdbc_url,
                [self.config.username, self.config.password],
                self.driver_path
            )
            self.logger.debug("Oracle JDBC connected successfully")
            return self._connection
        except Exception as e:
            error_msg = self._parse_oracle_error(str(e))
            self.logger.exception(f"Oracle JDBC connection failed: {error_msg}")
            raise OracleConnectionError(error_msg)
    
    def close(self):
        """연결 종료"""
        if self._connection:
            try:
                self._connection.close()
                self.logger.debug("Oracle connection closed")
            except Exception as e:
                self.logger.warning(f"Error closing connection: {e}")
            finally:
                self._connection = None
    
    @contextmanager
    def get_cursor(self, prefetch: int = 1000):
        """커서 컨텍스트 매니저"""
        conn = None
        cursor = None
        try:
            conn = self.connect()
            # 성능 향상을 위한 prefetch 설정
            try:
                conn.jconn.setDefaultRowPrefetch(prefetch)
            except Exception:
                pass  # 지원하지 않는 경우 무시
            
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
        """
        쿼리 실행 및 결과 반환
        
        Args:
            sql: SQL 쿼리
            params: 파라미터 리스트
            
        Returns:
            (columns, rows) 튜플
        """
        params = params or []
        
        with self.get_cursor() as cursor:
            try:
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                cols = [d[0] for d in cursor.description] if cursor.description else []
                return cols, rows
            except Exception as e:
                self.logger.exception(f"Query execution failed: {e}")
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
        """Oracle 에러 메시지 파싱 및 사용자 친화적 메시지 변환"""
        error_mappings = {
            'ORA-12514': '연결 실패: SERVICE_NAME을 확인하세요. (ORA-12514)',
            'ORA-12154': '연결 실패: 호스트/포트/서비스명을 확인하세요. (ORA-12154)',
            'ORA-01017': '연결 실패: 사용자명 또는 비밀번호가 올바르지 않습니다. (ORA-01017)',
            'ORA-28000': '연결 실패: 계정이 잠겼습니다. (ORA-28000)',
            'ORA-12545': '연결 실패: 대상 호스트 또는 객체가 존재하지 않습니다. (ORA-12545)',
            'ORA-00942': '테이블 또는 뷰가 존재하지 않습니다. (ORA-00942)',
            'ORA-00904': '부적합한 식별자입니다. (ORA-00904)',
        }
        
        for ora_code, message in error_mappings.items():
            if ora_code in error_text:
                return message
        
        return f'연결 실패: {error_text}'