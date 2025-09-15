"""
Customer information service
"""
import logging
from typing import Dict, Any, List, Optional, Tuple
from django.http import HttpRequest

from ..database import OracleConnection
from ..utils import SQLQueryManager, SessionManager, DataProcessor

logger = logging.getLogger(__name__)


class CustomerService:
    """고객 정보 관련 비즈니스 로직 서비스"""
    
    def __init__(self, oracle_conn: OracleConnection):
        """
        Args:
            oracle_conn: Oracle 데이터베이스 연결 객체
        """
        self.oracle_conn = oracle_conn
        self.sql_manager = SQLQueryManager()
    
    def get_unified_customer_info(self, cust_id: str) -> Dict[str, Any]:
        """
        통합 고객 정보 조회
        
        Args:
            cust_id: 고객 ID
            
        Returns:
            고객 정보 딕셔너리
        """
        if not cust_id:
            raise ValueError("Customer ID is required")
        
        logger.info(f"Fetching unified customer info for ID: {cust_id}")
        
        try:
            # SQL 실행
            prepared_sql, param_count = self.sql_manager.load_and_prepare(
                'customer_unified_info.sql',
                bind_params={':custId': '?'}
            )
            
            cols, rows = self.oracle_conn.execute_query(prepared_sql, [cust_id])
            
            # 고객 유형 판별
            customer_type = None
            if rows and len(rows) > 0:
                cust_type_idx = cols.index('고객구분') if '고객구분' in cols else -1
                if cust_type_idx >= 0 and cust_type_idx < len(rows[0]):
                    customer_type = rows[0][cust_type_idx]
            
            result = {
                'success': True,
                'columns': cols,
                'rows': rows,
                'customer_type': customer_type,
                'cust_id': cust_id
            }
            
            logger.info(f"Customer info fetched. Type: {customer_type}, Rows: {len(rows)}")
            
            return result
            
        except Exception as e:
            logger.exception(f"Error fetching customer info: {e}")
            return {
                'success': False,
                'message': f'고객 정보 조회 실패: {str(e)}',
                'columns': [],
                'rows': []
            }
    
    def get_duplicate_persons(self, cust_id: str, search_params: Dict[str, str]) -> Dict[str, Any]:
        """
        동일/차명의심 회원 조회
        
        Args:
            cust_id: 현재 고객 ID
            search_params: 검색 파라미터
            
        Returns:
            중복 회원 정보
        """
        logger.info(f"Searching duplicate persons for cust_id: {cust_id}")
        
        try:
            # SQL 파라미터 준비
            prepared_sql, param_count = self.sql_manager.load_and_prepare(
                'duplicate_unified.sql',
                bind_params={
                    ':current_cust_id': '?',
                    ':address': '?',
                    ':detail_address': '?',
                    ':workplace_name': '?',
                    ':workplace_address': '?',
                    ':workplace_detail_address': '?',
                    ':phone_suffix': '?'
                }
            )
            
            # 파라미터 값 추출
            address = search_params.get('address')
            detail_address = search_params.get('detail_address')
            workplace_name = search_params.get('workplace_name')
            workplace_address = search_params.get('workplace_address')
            workplace_detail_address = search_params.get('workplace_detail_address')
            phone_suffix = search_params.get('phone_suffix')
            
            # 쿼리 파라미터 구성 (SQL의 바인드 변수 순서대로)
            query_params = [
                cust_id, address, address, detail_address,
                cust_id, workplace_name, workplace_name,
                cust_id, workplace_address, workplace_address,
                workplace_detail_address, workplace_detail_address,
                phone_suffix, phone_suffix
            ]
            
            cols, rows = self.oracle_conn.execute_query(prepared_sql, query_params)
            
            result = {
                'success': True,
                'columns': cols,
                'rows': rows,
                'match_criteria': search_params
            }
            
            logger.info(f"Found {len(rows)} duplicate persons")
            
            return result
            
        except Exception as e:
            logger.exception(f"Error searching duplicate persons: {e}")
            return {
                'success': False,
                'message': f'중복 회원 조회 실패: {str(e)}',
                'columns': [],
                'rows': []
            }
    
    def get_corp_related_persons(self, cust_id: str) -> Dict[str, Any]:
        """
        법인 관련인 정보 조회
        
        Args:
            cust_id: 고객 ID
            
        Returns:
            법인 관련인 정보
        """
        logger.info(f"Fetching corp related persons for cust_id: {cust_id}")
        
        try:
            prepared_sql, param_count = self.sql_manager.load_and_prepare(
                'corp_related_persons.sql',
                bind_params={':cust_id': '?'}
            )
            
            cols, rows = self.oracle_conn.execute_query(prepared_sql, [cust_id])
            
            return {
                'success': True,
                'columns': cols,
                'rows': rows
            }
            
        except Exception as e:
            logger.exception(f"Error fetching corp related persons: {e}")
            return {
                'success': False,
                'message': f'법인 관련인 조회 실패: {str(e)}',
                'columns': [],
                'rows': []
            }
    
    def get_person_related_summary(self, cust_id: str, 
                                  start_date: str, 
                                  end_date: str) -> Dict[str, Any]:
        """
        개인 관련인 정보 조회 (내부입출금 거래)
        
        Args:
            cust_id: 고객 ID
            start_date: 시작일
            end_date: 종료일
            
        Returns:
            개인 관련인 정보
        """
        logger.info(f"Fetching person related summary for cust_id: {cust_id}")
        
        try:
            prepared_sql, param_count = self.sql_manager.load_and_prepare(
                'person_related_summary.sql',
                bind_params={
                    ':cust_id': '?',
                    ':start_date': '?',
                    ':end_date': '?'
                }
            )
            
            # 쿼리 파라미터 (SQL 구조에 맞게)
            query_params = [
                start_date, end_date, cust_id,
                cust_id, start_date, end_date
            ]
            
            cols, rows = self.oracle_conn.execute_query(prepared_sql, query_params)
            
            # 데이터 구조화
            related_persons = self._structure_related_persons(cols, rows)
            
            return {
                'success': True,
                'related_persons': related_persons,
                'raw_columns': cols,
                'raw_rows': rows
            }
            
        except Exception as e:
            logger.exception(f"Error fetching person related summary: {e}")
            return {
                'success': False,
                'message': f'개인 관련인 조회 실패: {str(e)}',
                'related_persons': {}
            }
    
    def get_ip_access_history(self, mem_id: str, 
                            start_date: str, 
                            end_date: str) -> Dict[str, Any]:
        """
        IP 접속 이력 조회
        
        Args:
            mem_id: 회원 ID
            start_date: 시작일
            end_date: 종료일
            
        Returns:
            IP 접속 이력
        """
        logger.info(f"Fetching IP access history for MID: {mem_id}")
        
        try:
            prepared_sql, param_count = self.sql_manager.load_and_prepare(
                'query_ip_access_history.sql',
                bind_params={
                    ':mem_id': '?',
                    ':start_date': '?',
                    ':end_date': '?'
                }
            )
            
            cols, rows = self.oracle_conn.execute_query(
                prepared_sql, 
                [mem_id, start_date, end_date]
            )
            
            return {
                'success': True,
                'columns': cols,
                'rows': rows
            }
            
        except Exception as e:
            logger.exception(f"Error fetching IP access history: {e}")
            return {
                'success': False,
                'message': f'IP 접속 이력 조회 실패: {str(e)}',
                'columns': [],
                'rows': []
            }
    
    def _structure_related_persons(self, cols: List[str], rows: List[List]) -> Dict[str, Any]:
        """관련인 데이터 구조화"""
        related_persons = {}
        
        for row in rows:
            record_type = row[0] if len(row) > 0 else None
            cust_id_val = row[1] if len(row) > 1 else None
            
            if not cust_id_val:
                continue
            
            if cust_id_val not in related_persons:
                related_persons[cust_id_val] = {
                    'info': None,
                    'transactions': []
                }
            
            if record_type == 'PERSON_INFO':
                # 개인 정보
                related_persons[cust_id_val]['info'] = {
                    'cust_id': cust_id_val,
                    'name': row[2] if len(row) > 2 else None,
                    'id_number': row[3] if len(row) > 3 else None,
                    'birth_date': row[4] if len(row) > 4 else None,
                    'age': row[5] if len(row) > 5 else None,
                    'gender': row[6] if len(row) > 6 else None,
                    'address': row[7] if len(row) > 7 else None,
                    'job': row[8] if len(row) > 8 else None,
                    'workplace': row[9] if len(row) > 9 else None,
                    'workplace_addr': row[10] if len(row) > 10 else None,
                    'income_source': row[11] if len(row) > 11 else None,
                    'tran_purpose': row[12] if len(row) > 12 else None,
                    'risk_grade': row[13] if len(row) > 13 else None,
                    'total_tran_count': row[14] if len(row) > 14 else None
                }
            elif record_type == 'TRAN_SUMMARY':
                # 거래 요약
                related_persons[cust_id_val]['transactions'].append({
                    'coin_symbol': row[15] if len(row) > 15 else None,
                    'tran_type': row[16] if len(row) > 16 else None,
                    'tran_qty': row[17] if len(row) > 17 else None,
                    'tran_amt': row[18] if len(row) > 18 else None,
                    'tran_cnt': row[19] if len(row) > 19 else None
                })
        
        return related_persons
    
    def save_to_session(self, request: HttpRequest, 
                       customer_data: Dict[str, Any],
                       data_type: str = 'unified') -> None:
        """
        고객 데이터를 세션에 저장
        
        Args:
            request: HttpRequest 객체
            customer_data: 저장할 고객 데이터
            data_type: 데이터 타입 ('unified', 'duplicate', 'corp_related', 'person_related', 'ip_history')
        """
        key_mapping = {
            'unified': SessionManager.Keys.CURRENT_CUSTOMER_DATA,
            'duplicate': SessionManager.Keys.DUPLICATE_PERSONS_DATA,
            'corp_related': SessionManager.Keys.CURRENT_CORP_RELATED_DATA,
            'person_related': SessionManager.Keys.CURRENT_PERSON_RELATED_DATA,
            'ip_history': SessionManager.Keys.IP_HISTORY_DATA
        }
        
        session_key = key_mapping.get(data_type)
        if not session_key:
            logger.warning(f"Unknown customer data type: {data_type}")
            return
        
        SessionManager.save_data(request, session_key, customer_data)
        logger.info(f"Customer data ({data_type}) saved to session")