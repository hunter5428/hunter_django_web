# str_dashboard/db_utils.py
"""
Oracle 데이터베이스 연결 및 쿼리 실행 유틸리티
"""
import os
import re
import logging
from functools import wraps
from pathlib import Path
from typing import Tuple, List, Optional, Dict, Any
from contextlib import contextmanager

import jaydebeapi
from django.conf import settings
from django.http import JsonResponse

logger = logging.getLogger(__name__)


class OracleConnectionError(Exception):
    """Oracle 연결 관련 커스텀 예외"""
    pass


class OracleQueryError(Exception):
    """Oracle 쿼리 실행 관련 커스텀 예외"""
    pass


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
        """Oracle 에러 메시지 파싱 및 사용자 친화적 메시지 변환"""
        if 'ORA-12514' in error_text:
            return '연결 실패: SERVICE_NAME을 확인하세요. (ORA-12514)'
        elif 'ORA-12154' in error_text:
            return '연결 실패: 호스트/포트/서비스명을 확인하세요. (ORA-12154)'
        elif 'ORA-01017' in error_text:
            return '연결 실패: 사용자명 또는 비밀번호가 올바르지 않습니다. (ORA-01017)'
        elif 'ORA-28000' in error_text:
            return '연결 실패: 계정이 잠겼습니다. (ORA-28000)'
        return f'연결 실패: {error_text}'


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
        # 블록 주석 제거
        sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
        # 라인 주석 제거
        sql = re.sub(r"--.*?$", "", sql, flags=re.M)
        return sql.strip()
    
    # str_dashboard/db_utils.py의 SQLQueryManager.prepare_sql 메서드 수정

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
        if bind_params:
            import re
            
            # 바인드 변수별 사용 횟수 카운트 (디버깅용)
            bind_counts = {}
            
            # 모든 바인드 변수의 위치 찾기
            bind_positions = []
            for bind_var in bind_params.keys():
                # 이스케이프 처리
                escaped_var = re.escape(bind_var)
                # 바인드 변수 뒤에 단어 문자가 아닌 것이 오는 패턴
                pattern = escaped_var + r'(?![a-zA-Z0-9_])'
                
                count = 0
                for match in re.finditer(pattern, sql):
                    bind_positions.append((match.start(), bind_var))
                    count += 1
                
                bind_counts[bind_var] = count
                logger.debug(f"Bind variable {bind_var}: found {count} times")
            
            # 위치 순으로 정렬
            bind_positions.sort(key=lambda x: x[0])
            
            # 뒤에서부터 치환 (인덱스가 변하지 않도록)
            for _, bind_var in reversed(bind_positions):
                placeholder = bind_params[bind_var]
                # 정확한 패턴 매칭으로 치환
                pattern = re.escape(bind_var) + r'(?![a-zA-Z0-9_])'
                sql = re.sub(pattern, placeholder, sql, count=1)
            
            # 전체 바인드 변수 개수 (각 변수의 사용 횟수 합)
            total_bind_count = sum(bind_counts.values())
            logger.debug(f"Total bind variables: {total_bind_count}")
        
        # 마지막 세미콜론 제거
        if sql.rstrip().endswith(";"):
            sql = sql.rstrip()[:-1]
        
        # 파라미터 개수 계산
        param_count = sql.count("?")
        
        # 디버깅 로그 추가
        if bind_params:
            logger.debug(f"Prepared SQL param count: {param_count}")
            if param_count != sum(bind_counts.values()):
                logger.warning(f"Parameter count mismatch: expected {sum(bind_counts.values())}, got {param_count}")
        
        return sql, param_count

    
    @classmethod
    def load_and_prepare(cls, filename: str, bind_params: Optional[Dict[str, str]] = None) -> Tuple[str, int]:
        """SQL 파일 로드 및 준비"""
        raw_sql = cls.load_sql(filename)
        return cls.prepare_sql(raw_sql, bind_params)


def require_db_connection(func):
    """
    데이터베이스 연결이 필요한 뷰 데코레이터
    세션에서 연결 정보를 확인하고 OracleConnection 객체를 생성하여 전달
    """
    @wraps(func)
    def wrapper(request, *args, **kwargs):
        db_info = request.session.get('db_conn')
        if not db_info or request.session.get('db_conn_status') != 'ok':
            return JsonResponse({
                'success': False,
                'message': '먼저 DB Connection에서 연결을 완료해 주세요.'
            })
        
        try:
            oracle_conn = OracleConnection.from_session(db_info)
            # 함수에 oracle_conn 파라미터 추가
            return func(request, oracle_conn=oracle_conn, *args, **kwargs)
        except Exception as e:
            logger.exception(f"DB connection setup failed: {e}")
            return JsonResponse({
                'success': False,
                'message': f'데이터베이스 연결 설정 실패: {e}'
            })
    
    return wrapper


def execute_query_with_error_handling(
    oracle_conn: OracleConnection,
    sql_filename: str,
    bind_params: Optional[Dict[str, str]] = None,
    query_params: Optional[List] = None
) -> Dict[str, Any]:
    """
    쿼리 실행 및 에러 처리를 포함한 헬퍼 함수
    
    Returns:
        {'success': True/False, 'columns': [...], 'rows': [...], 'message': '...'}
    """
    try:
        # SQL 로드 및 준비
        prepared_sql, param_count = SQLQueryManager.load_and_prepare(sql_filename, bind_params)
        
        # 디버깅 로그 추가
        logger.debug(f"SQL File: {sql_filename}")
        logger.debug(f"Expected params: {param_count}, Provided params: {len(query_params) if query_params else 0}")
        
        # 파라미터 검증
        if param_count > 0 and not query_params:
            return {
                'success': False,
                'message': f'쿼리에 {param_count}개의 파라미터가 필요하지만 제공되지 않았습니다.'
            }
        
        # 파라미터 개수 불일치 검증
        if param_count != (len(query_params) if query_params else 0):
            return {
                'success': False,
                'message': f'파라미터 개수 불일치: 필요 {param_count}개, 제공 {len(query_params) if query_params else 0}개'
            }
        
        # 쿼리 실행
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