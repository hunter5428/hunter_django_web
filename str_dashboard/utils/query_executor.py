# str_dashboard/utils/query_executor.py
"""
개별 쿼리 실행 로직을 담당하는 모듈
각 쿼리의 파라미터 바인딩과 실행을 처리
"""

import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from django.conf import settings
import pandas as pd
from decimal import Decimal
import re

from .db import RedshiftConnection, SQLQueryManager

logger = logging.getLogger(__name__)


class QueryExecutor:
    """개별 쿼리 실행 클래스"""
    
    def execute_alert_info_query(self, db_conn, alert_id: str) -> Dict[str, Any]:
        """ALERT 정보 조회 쿼리 실행"""
        return self._execute_oracle_query(
            db_conn,
            'alert_info_by_alert_id.sql',
            [alert_id]
        )
    
    def execute_customer_info_query(self, db_conn, cust_id: str) -> Dict[str, Any]:
        """고객 정보 조회 쿼리 실행"""
        return self._execute_oracle_query(
            db_conn,
            'customer_unified_info.sql',
            [cust_id]
        )
    
    def execute_corp_related_query(self, db_conn, cust_id: str) -> Dict[str, Any]:
        """법인 관련인 조회 쿼리 실행"""
        return self._execute_oracle_query(
            db_conn,
            'corp_related_persons.sql',
            [cust_id]
        )
    
    def execute_person_related_query(self, db_conn, cust_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """개인 관련인 조회 쿼리 실행"""
        # 날짜 형식 정리
        cleaned_start = self._clean_datetime_string(start_date)
        cleaned_end = self._clean_datetime_string(end_date)
        
        if not cleaned_start or not cleaned_end:
            logger.warning(f"Invalid date for person related query: start={start_date}, end={end_date}")
            return {'success': False, 'message': 'Invalid date format'}
        
        # SQL 파일 로드 및 파라미터 순서 확인
        sql = SQLQueryManager.load_sql('person_related_summary.sql')
        
        # SQL에서 실제 파라미터 순서 파악
        # 일반적인 패턴: WHERE 절에서 start_date, end_date가 먼저 오고, cust_id가 뒤에 옴
        # 정확한 순서는 SQL 파일을 확인해야 함
        params = [cleaned_start, cleaned_end, cust_id, cust_id, cleaned_start, cleaned_end]
        
        logger.debug(f"person_related_query - cust_id: {cust_id}, period: {cleaned_start} ~ {cleaned_end}")
        
        return self._execute_oracle_query(
            db_conn,
            'person_related_summary.sql',
            params
        )
    
    def execute_ip_history_query(self, db_conn, mem_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """IP 이력 조회 쿼리 실행"""
        # 날짜 형식 정리 및 날짜 부분만 추출
        cleaned_start = self._clean_datetime_string(start_date)
        cleaned_end = self._clean_datetime_string(end_date)
        
        if not cleaned_start or not cleaned_end:
            logger.warning(f"Invalid date for IP history query: start={start_date}, end={end_date}")
            return {'success': False, 'message': 'Invalid date format'}
        
        start_date_only = cleaned_start.split(' ')[0]
        end_date_only = cleaned_end.split(' ')[0]
        
        return self._execute_oracle_query(
            db_conn,
            'query_ip_access_history.sql',
            [mem_id, start_date_only, end_date_only]
        )
    
    def execute_duplicate_query(self, db_conn, cust_id: str, dup_params: Dict) -> Dict[str, Any]:
        """중복 회원 조회 쿼리 실행"""
        # duplicate_unified.sql의 파라미터 순서에 맞게 조정
        # 기존 views.py 참고: 각 조건별로 current_cust_id가 반복적으로 사용됨
        params = [
            cust_id,                                    # :current_cust_id (첫번째)
            dup_params.get('address', ''),             # :address (주거지 주소 조건 1)
            dup_params.get('address', ''),             # :address (주거지 주소 조건 2)
            dup_params.get('detail_address', ''),      # :detail_address (상세주소)
            cust_id,                                    # :current_cust_id (workplace_name 조건)
            dup_params.get('workplace_name', ''),      # :workplace_name (조건 1)
            dup_params.get('workplace_name', ''),      # :workplace_name (조건 2)
            cust_id,                                    # :current_cust_id (workplace_address 조건)
            dup_params.get('workplace_address', ''),   # :workplace_address (조건 1)
            dup_params.get('workplace_address', ''),   # :workplace_address (조건 2)
            dup_params.get('workplace_detail_address', ''), # :workplace_detail_address (조건 1)
            dup_params.get('workplace_detail_address', ''), # :workplace_detail_address (조건 2)
            dup_params.get('phone_suffix', ''),        # :phone_suffix (조건 1)
            dup_params.get('phone_suffix', ''),        # :phone_suffix (조건 2)
        ]
        
        logger.debug(f"duplicate_query - cust_id: {cust_id}, params count: {len(params)}")
        
        return self._execute_oracle_query(
            db_conn,
            'duplicate_unified.sql',
            params
        )
    
    def execute_orderbook_query(self, redshift_info: Dict, mem_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
        """Redshift Orderbook 조회"""
        try:
            # 날짜 형식 정리
            cleaned_start = self._clean_datetime_string(start_date)
            cleaned_end = self._clean_datetime_string(end_date)
            
            if not cleaned_start or not cleaned_end:
                logger.warning(f"Invalid date for orderbook query: start={start_date}, end={end_date}")
                return {'success': False, 'message': 'Invalid date format'}
            
            redshift_conn = RedshiftConnection.from_session(redshift_info)
            sql_path = Path(settings.BASE_DIR) / 'str_dashboard' / 'queries' / 'redshift_orderbook.sql'
            sql_query = sql_path.read_text('utf-8')
            
            # pandas datetime으로 변환
            start_dt = pd.to_datetime(cleaned_start)
            end_dt = pd.to_datetime(cleaned_end)
            
            with redshift_conn.transaction() as rs_conn:
                with rs_conn.cursor() as cursor:
                    cursor.execute(sql_query, (start_dt, end_dt, mem_id))
                    if not cursor.description:
                        return {'success': True, 'columns': [], 'rows': []}
                    
                    cols = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    # Decimal 타입 처리
                    processed_rows = []
                    for row in rows:
                        processed_row = []
                        for value in row:
                            if isinstance(value, Decimal):
                                processed_row.append(float(value))
                            else:
                                processed_row.append(value)
                        processed_rows.append(processed_row)
                    
                    return {'success': True, 'columns': cols, 'rows': processed_rows}
                    
        except Exception as e:
            logger.error(f"Orderbook query failed: {e}")
            return {'success': False, 'message': str(e)}
    
    def _clean_datetime_string(self, datetime_str: str) -> str:
        """
        날짜/시간 문자열 정리
        한글이나 특수문자 제거하고 표준 형식으로 변환
        Oracle 호환 형식으로 변환 (마이크로초 제외)
        """
        if not datetime_str:
            return datetime_str
        
        # 문자열로 변환 (혹시 다른 타입일 경우 대비)
        datetime_str = str(datetime_str)
        
        # 한글 및 특수문자 제거 (숫자, 공백, 하이픈, 콜론, 점만 남김)
        # '?' 같은 깨진 문자도 제거
        cleaned = re.sub(r'[^0-9\s\-:.]', '', datetime_str)
        
        # 여러 공백을 하나로
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        # 빈 문자열이 되었으면 None 반환
        if not cleaned:
            logger.warning(f"Datetime string became empty after cleaning: {datetime_str}")
            return None
        
        # 날짜 형식 검증 및 표준화
        try:
            # 날짜 부분만 있는 경우 처리
            date_pattern = r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'
            date_match = re.search(date_pattern, cleaned)
            
            if date_match:
                year, month, day = date_match.groups()
                date_str = f"{year:0>4}-{month:0>2}-{day:0>2}"
                
                # 시간 부분 확인
                time_pattern = r'(\d{1,2}):(\d{1,2}):(\d{1,2})'
                time_match = re.search(time_pattern, cleaned)
                
                if time_match:
                    hour, minute, second = time_match.groups()
                    # 시간 값이 유효한지 확인
                    try:
                        hour_int = int(hour)
                        minute_int = int(minute) 
                        second_int = int(second)
                        
                        if 0 <= hour_int <= 23 and 0 <= minute_int <= 59 and 0 <= second_int <= 59:
                            # Oracle 호환 형식: 마이크로초 제외
                            return f"{date_str} {hour:0>2}:{minute:0>2}:{second:0>2}"
                    except ValueError:
                        pass
                
                # 시간이 없거나 잘못된 경우 기본값 사용
                return f"{date_str} 00:00:00"
            
            # 날짜 패턴을 찾지 못한 경우
            logger.warning(f"No valid date pattern found in: {cleaned} (original: {datetime_str})")
            return None
                
        except Exception as e:
            logger.warning(f"Error cleaning datetime string '{datetime_str}': {e}")
            return None
    
    def _execute_oracle_query(self, db_conn, sql_filename: str, params: List) -> Dict[str, Any]:
        """Oracle 쿼리 실행 헬퍼"""
        try:
            from decimal import Decimal
            
            raw_sql = SQLQueryManager.load_sql(sql_filename)
            
            # 바인드 변수를 ? 로 치환
            processed_sql = raw_sql
            param_pattern = re.compile(r':(\w+)')
            
            # 모든 바인드 변수를 ?로 치환
            processed_sql = param_pattern.sub('?', processed_sql)
            
            # 주석 제거
            processed_sql = re.sub(r"--.*?$", "", processed_sql, flags=re.M).strip()
            processed_sql = processed_sql.rstrip(';')
            
            logger.debug(f"Executing {sql_filename} with {len(params)} parameters")
            
            with db_conn.cursor() as cursor:
                cursor.execute(processed_sql, params)
                rows = cursor.fetchall()
                cols = [d[0] for d in cursor.description] if cursor.description else []
                
                # Decimal 타입 처리
                processed_rows = []
                for row in rows:
                    processed_row = []
                    for value in row:
                        if isinstance(value, Decimal):
                            processed_row.append(float(value))
                        else:
                            processed_row.append(value)
                    processed_rows.append(processed_row)
                
                return {'success': True, 'columns': cols, 'rows': processed_rows}
                
        except Exception as e:
            logger.exception(f"Oracle query execution failed ({sql_filename}): {e}")
            return {'success': False, 'message': f"{sql_filename} 쿼리 실행 실패: {str(e)}"}