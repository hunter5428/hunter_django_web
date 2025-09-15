"""
Base database connection interface
"""
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple, Optional
from dataclasses import dataclass
from contextlib import contextmanager

logger = logging.getLogger(__name__)


@dataclass
class ConnectionConfig:
    """데이터베이스 연결 설정"""
    host: str
    port: str
    username: str
    password: str
    database: Optional[str] = None
    service_name: Optional[str] = None
    options: Dict[str, Any] = None

    def __post_init__(self):
        if self.options is None:
            self.options = {}

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            'host': self.host,
            'port': self.port,
            'username': self.username,
            'password': self.password,
            'database': self.database,
            'service_name': self.service_name,
            'options': self.options
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConnectionConfig':
        """딕셔너리에서 생성"""
        return cls(
            host=data.get('host', ''),
            port=data.get('port', ''),
            username=data.get('username', ''),
            password=data.get('password', ''),
            database=data.get('database'),
            service_name=data.get('service_name'),
            options=data.get('options', {})
        )


class DatabaseConnectionError(Exception):
    """데이터베이스 연결 에러 기본 클래스"""
    pass


class DatabaseQueryError(Exception):
    """데이터베이스 쿼리 에러 기본 클래스"""
    pass


class DatabaseConnectionBase(ABC):
    """데이터베이스 연결 추상 클래스"""
    
    def __init__(self, config: ConnectionConfig):
        """
        Args:
            config: 연결 설정 객체
        """
        self.config = config
        self._connection = None
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def connect(self):
        """데이터베이스 연결"""
        pass
    
    @abstractmethod
    def close(self):
        """연결 종료"""
        pass
    
    @abstractmethod
    def test_connection(self) -> bool:
        """연결 테스트"""
        pass
    
    @abstractmethod
    @contextmanager
    def get_cursor(self):
        """커서 컨텍스트 매니저"""
        pass
    
    @abstractmethod
    def execute_query(self, sql: str, params: Optional[List] = None) -> Tuple[List[str], List[List]]:
        """
        쿼리 실행
        
        Args:
            sql: SQL 쿼리
            params: 파라미터 리스트
            
        Returns:
            (columns, rows) 튜플
        """
        pass
    
    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return self._connection is not None
    
    def __enter__(self):
        """컨텍스트 매니저 진입"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """컨텍스트 매니저 종료"""
        self.close()
    
    def __repr__(self) -> str:
        """객체 표현"""
        return (
            f"{self.__class__.__name__}("
            f"host={self.config.host}, "
            f"port={self.config.port}, "
            f"user={self.config.username})"
        )