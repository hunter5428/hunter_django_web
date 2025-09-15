"""
STR Dashboard Core Module
"""

__version__ = '1.0.0'

# 핵심 모듈 임포트를 여기서 관리
from .database import OracleConnection, RedshiftConnection
from .utils import SessionManager

__all__ = [
    'OracleConnection',
    'RedshiftConnection', 
    'SessionManager',
]