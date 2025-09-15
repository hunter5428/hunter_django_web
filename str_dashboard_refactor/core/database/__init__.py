"""
Database connection management module
"""

from .oracle import OracleConnection, OracleConnectionError, OracleQueryError
from .redshift import RedshiftConnection, RedshiftConnectionError, RedshiftQueryError
from .base import DatabaseConnectionBase, ConnectionConfig

__all__ = [
    # Base
    'DatabaseConnectionBase',
    'ConnectionConfig',
    
    # Oracle
    'OracleConnection',
    'OracleConnectionError',
    'OracleQueryError',
    
    # Redshift  
    'RedshiftConnection',
    'RedshiftConnectionError',
    'RedshiftQueryError',
]