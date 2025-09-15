"""
SQL query file management utilities
"""
import re
import logging
from pathlib import Path
from typing import Tuple, Optional, Dict
from django.conf import settings

logger = logging.getLogger(__name__)


class SQLQueryManager:
    """SQL 쿼리 파일 관리 클래스"""
    
    # 기본 쿼리 디렉토리
    DEFAULT_QUERIES_DIR = settings.BASE_DIR / 'str_dashboard' / 'queries'
    
    def __init__(self, queries_dir: Optional[Path] = None):
        """
        Args:
            queries_dir: SQL 쿼리 파일 디렉토리 경로
        """
        self.queries_dir = queries_dir or self.DEFAULT_QUERIES_DIR
        self._cache = {}  # 쿼리 캐시
    
    def load_sql(self, filename: str, use_cache: bool = True) -> str:
        """
        SQL 파일 로드
        
        Args:
            filename: SQL 파일명
            use_cache: 캐시 사용 여부
            
        Returns:
            SQL 쿼리 문자열
        """
        if use_cache and filename in self._cache:
            logger.debug(f"Using cached SQL for {filename}")
            return self._cache[filename]
        
        sql_path = self.queries_dir / filename
        
        try:
            sql_content = sql_path.read_text(encoding='utf-8')
            
            if use_cache:
                self._cache[filename] = sql_content
                
            logger.debug(f"Loaded SQL from {filename}")
            return sql_content
            
        except FileNotFoundError:
            logger.error(f"SQL file not found: {filename}")
            raise FileNotFoundError(f"SQL 파일을 찾을 수 없습니다: {filename}")
        except Exception as e:
            logger.error(f"Failed to load SQL file {filename}: {e}")
            raise Exception(f"SQL 파일 로드 실패: {e}")
    
    @staticmethod
    def strip_comments(sql: str) -> str:
        """
        SQL 주석 제거
        
        Args:
            sql: 원본 SQL
            
        Returns:
            주석이 제거된 SQL
        """
        # 블록 주석 제거 (/* ... */)
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
        
        # 라인 주석 제거 (-- ...)
        sql = re.sub(r"--.*?$", "", sql, flags=re.MULTILINE)
        
        return sql.strip()
    
    @staticmethod
    def prepare_sql(sql: str, bind_params: Optional[Dict[str, str]] = None) -> Tuple[str, int]:
        """
        SQL 준비 (주석 제거, 바인드 변수 처리)
        
        Args:
            sql: 원본 SQL
            bind_params: 바인드 변수 매핑 (예: {':alert_id': '?'})
        
        Returns:
            (prepared_sql, param_count) 튜플
        """
        # 주석 제거
        sql = SQLQueryManager.strip_comments(sql)
        
        # 바인드 변수 처리
        param_count = 0
        if bind_params:
            # 각 바인드 변수의 사용 횟수를 계산
            for bind_var, placeholder in bind_params.items():
                # 바인드 변수가 SQL에서 몇 번 사용되는지 계산
                count = sql.count(bind_var)
                # 치환
                sql = sql.replace(bind_var, placeholder)
                # 총 파라미터 개수 누적
                param_count += count
                logger.debug(f"Bind variable {bind_var}: replaced {count} times")
        else:
            # bind_params가 없으면 기존 ? 개수 카운트
            param_count = sql.count("?")
        
        # 마지막 세미콜론 제거
        if sql.rstrip().endswith(";"):
            sql = sql.rstrip()[:-1]
        
        logger.debug(f"Prepared SQL - total parameter count: {param_count}")
        
        return sql, param_count
    
    def load_and_prepare(self, filename: str, 
                        bind_params: Optional[Dict[str, str]] = None,
                        use_cache: bool = True) -> Tuple[str, int]:
        """
        SQL 파일 로드 및 준비
        
        Args:
            filename: SQL 파일명
            bind_params: 바인드 변수 매핑
            use_cache: 캐시 사용 여부
            
        Returns:
            (prepared_sql, param_count) 튜플
        """
        raw_sql = self.load_sql(filename, use_cache)
        return self.prepare_sql(raw_sql, bind_params)
    
    def clear_cache(self) -> None:
        """SQL 캐시 초기화"""
        self._cache.clear()
        logger.debug("SQL cache cleared")
    
    @classmethod
    def validate_params(cls, param_count: int, provided_params: Optional[list]) -> bool:
        """
        파라미터 개수 검증
        
        Args:
            param_count: 필요한 파라미터 개수
            provided_params: 제공된 파라미터 리스트
            
        Returns:
            검증 성공 여부
        """
        provided_count = len(provided_params) if provided_params else 0
        
        if param_count > 0 and not provided_params:
            logger.error(f"Query requires {param_count} parameters but none provided")
            return False
        
        if param_count != provided_count:
            logger.error(f"Parameter count mismatch: required {param_count}, provided {provided_count}")
            return False
        
        return True