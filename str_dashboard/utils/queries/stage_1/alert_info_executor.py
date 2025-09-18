# str_dashboard/utils/queries/stage_2/customer_executor.py
"""
고객 및 관련인 정보 쿼리 실행 모듈
통합 DataFrame 생성
"""

import logging
from typing import Dict, Any, List, Optional, Set, Tuple
from datetime import datetime
from decimal import Decimal


from .sql_templates import (
    INITIAL_ALERT_QUERY, 
    MONTHLY_ALERT_QUERY,
    RULE_HISTORY_QUERY,
    SIMILAR_RULE_COMBINATIONS_QUERY
)


logger = logging.getLogger(__name__)



class CustomerExecutor:
    """
    Stage 2: 고객 및 관련인 정보 조회 실행 클래스
    """
    
    def __init__(self, db_connection):
        self.db_conn = db_connection
        
    def execute(self, cust_id: str, stage_1_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        고객 및 관련인 정보 조회 메인 실행 함수
        """
        try:
            logger.info(f"[Stage 2] Starting customer info query for: {cust_id}")
            
            # Step 1: 주 고객 정보 조회
            customer_result = self._get_customer_info(cust_id)
            if not customer_result['success']:
                return customer_result
            
            # 고객 타입 확인
            customer_type = self._determine_customer_type(customer_result)
            logger.info(f"[Stage 2] Customer type: {customer_type}")
            
            # Step 2: 관련인 정보 조회
            related_persons_result = {'success': True, 'data': []}
            
            if customer_type == 'CORP':
                # 법인 관련인 조회
                related_persons_result = self._get_corp_related_persons(cust_id)
                
            else:  # PERSON
                # 개인 관련인 조회 (내부거래 상대방)
                tran_start = stage_1_metadata.get('tran_start')
                tran_end = stage_1_metadata.get('tran_end')
                
                if tran_start and tran_end:
                    related_persons_result = self._get_person_related_with_details(
                        cust_id, tran_start, tran_end
                    )
            
            # Step 3: 중복 의심 회원 조회
            duplicate_result = self._get_duplicate_persons(cust_id, customer_result)
            
            # Step 4: 통합 DataFrame 구성
            unified_result = self._create_unified_dataframe(
                customer_result,
                related_persons_result,
                customer_type
            )
            
            return {
                'success': True,
                'customer_info': customer_result,
                'related_persons': unified_result,
                'duplicate_persons': duplicate_result,
                'metadata': {
                    'customer_type': customer_type,
                    'mid': self._extract_mid(customer_result),
                    'kyc_datetime': self._extract_kyc_datetime(customer_result)
                },
                'summary': {
                    'cust_id': cust_id,
                    'customer_type': customer_type,
                    'related_count': len(unified_result.get('rows', [])),
                    'duplicate_count': len(duplicate_result.get('rows', []))
                }
            }
            
        except Exception as e:
            logger.exception(f"[Stage 2] Unexpected error: {e}")
            return {'success': False, 'message': str(e)}
    
    def _get_customer_info(self, cust_id: str) -> Dict[str, Any]:
        """고객 정보 조회 (재사용 가능)"""
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(CUSTOMER_UNIFIED_INFO_QUERY, [cust_id])
                rows = cursor.fetchall()
                
                if not rows:
                    return {'success': False, 'message': f'No data for {cust_id}'}
                
                cols = [desc[0] for desc in cursor.description]
                
                return {
                    'success': True,
                    'columns': cols,
                    'rows': [self._convert_row_types(rows[0])]
                }
                
        except Exception as e:
            logger.error(f"Error getting customer info: {e}")
            return {'success': False, 'message': str(e)}
    
    def _get_corp_related_persons(self, cust_id: str) -> Dict[str, Any]:
        """법인 관련인 조회"""
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(CORP_RELATED_PERSONS_QUERY, [cust_id])
                rows = cursor.fetchall()
                
                # 법인 관련인 정보를 통합 형식으로 변환
                related_data = []
                for row in rows:
                    related_person = {
                        'related_cust_id': row[0],  # 관련인고객ID
                        'relation_type': row[1],     # 관계유형
                        'name': row[2],              # 관련인성명
                        'name_en': row[3],           # 관련인영문명
                        'birth_date': row[4],        # 관련인생년월일
                        'gender': row[5],            # 관련인성별
                        'id_number': row[6],         # 관련인실명번호
                        'stake_rate': row[7],        # 지분율 (법인 특수)
                        'relation_code': row[8],     # 관계유형코드
                        # 개인 특수 필드는 NULL
                        'internal_deposit_amount': None,
                        'internal_withdraw_amount': None,
                        'transaction_count': None,
                        'customer_details': None  # 상세 정보는 별도 조회
                    }
                    related_data.append(related_person)
                
                return {'success': True, 'data': related_data}
                
        except Exception as e:
            logger.error(f"Error in corp related persons: {e}")
            return {'success': True, 'data': []}
    
    def _get_person_related_with_details(self, cust_id: str, 
                                        tran_start: str, tran_end: str) -> Dict[str, Any]:
        """개인 관련인 조회 및 상세 정보 포함"""
        try:
            # Step 1: 내부거래 상대방 조회
            with self.db_conn.cursor() as cursor:
                cursor.execute(PERSON_INTERNAL_TRANSACTION_QUERY,
                             [tran_start, tran_end, cust_id])
                transaction_rows = cursor.fetchall()
            
            if not transaction_rows:
                return {'success': True, 'data': []}
            
            # Step 2: 각 관련인의 상세 정보 조회
            related_data = []
            for tx_row in transaction_rows:
                related_cust_id = tx_row[0]
                deposit_amount = float(tx_row[1]) if tx_row[1] else 0
                withdraw_amount = float(tx_row[2]) if tx_row[2] else 0
                tx_count = tx_row[3]
                
                # 관련인의 전체 고객 정보 조회 (재사용)
                detail_result = self._get_customer_info(related_cust_id)
                
                if detail_result['success'] and detail_result['rows']:
                    detail_row = detail_result['rows'][0]
                    detail_cols = detail_result['columns']
                    
                    # 필요한 정보 추출
                    related_person = {
                        'related_cust_id': related_cust_id,
                        'relation_type': '내부거래상대방',
                        'name': self._get_value_by_column(detail_row, detail_cols, '성명'),
                        'name_en': self._get_value_by_column(detail_row, detail_cols, '영문명'),
                        'birth_date': self._get_value_by_column(detail_row, detail_cols, '생년월일'),
                        'gender': self._get_value_by_column(detail_row, detail_cols, '성별'),
                        'id_number': self._get_value_by_column(detail_row, detail_cols, '실명번호'),
                        # 법인 특수 필드는 NULL
                        'stake_rate': None,
                        'relation_code': 'INTERNAL',
                        # 개인 특수 필드
                        'internal_deposit_amount': deposit_amount,
                        'internal_withdraw_amount': withdraw_amount,
                        'transaction_count': tx_count,
                        # 전체 상세 정보 저장
                        'customer_details': {
                            'columns': detail_cols,
                            'values': detail_row
                        }
                    }
                else:
                    # 상세 정보 조회 실패 시 기본 정보만
                    related_person = {
                        'related_cust_id': related_cust_id,
                        'relation_type': '내부거래상대방',
                        'name': None,
                        'name_en': None,
                        'birth_date': None,
                        'gender': None,
                        'id_number': None,
                        'stake_rate': None,
                        'relation_code': 'INTERNAL',
                        'internal_deposit_amount': deposit_amount,
                        'internal_withdraw_amount': withdraw_amount,
                        'transaction_count': tx_count,
                        'customer_details': None
                    }
                
                related_data.append(related_person)
            
            return {'success': True, 'data': related_data}
            
        except Exception as e:
            logger.error(f"Error in person related query: {e}")
            return {'success': True, 'data': []}
    
    def _create_unified_dataframe(self, customer_result: Dict,
                                 related_result: Dict,
                                 customer_type: str) -> Dict[str, Any]:
        """통합 관련인 DataFrame 생성"""
        
        # 통합 컬럼 정의
        unified_columns = [
            '관련인고객ID',
            '관계유형',
            '관련인성명',
            '관련인영문명',
            '관련인생년월일',
            '관련인성별',
            '관련인실명번호',
            '관련인국적',
            '관련인연락처',
            '관련인이메일',
            '관련인거주지주소',
            '관련인직업',
            '관련인직장명',
            '관련인위험등급',
            # 법인 특수
            '지분율',
            # 개인 특수  
            '내부입고금액',
            '내부출고금액',
            '거래횟수',
            # 공통
            '관계유형코드'
        ]
        
        unified_rows = []
        
        for person in related_result.get('data', []):
            row = []
            
            # 기본 정보
            row.append(person.get('related_cust_id'))
            row.append(person.get('relation_type'))
            row.append(person.get('name'))
            row.append(person.get('name_en'))
            row.append(person.get('birth_date'))
            row.append(person.get('gender'))
            row.append(person.get('id_number'))
            
            # 상세 정보 (개인인 경우만 존재)
            if person.get('customer_details'):
                details = person['customer_details']
                cols = details['columns']
                vals = details['values']
                
                row.append(self._get_value_by_column(vals, cols, '국적'))
                row.append(self._get_value_by_column(vals, cols, '연락처'))
                row.append(self._get_value_by_column(vals, cols, '이메일'))
                row.append(self._get_value_by_column(vals, cols, '거주지주소'))
                row.append(self._get_value_by_column(vals, cols, '직업'))
                row.append(self._get_value_by_column(vals, cols, '직장명'))
                row.append(self._get_value_by_column(vals, cols, '위험등급'))
            else:
                # 상세 정보 없는 경우 NULL
                row.extend([None] * 7)
            
            # 법인 특수 필드
            row.append(person.get('stake_rate'))
            
            # 개인 특수 필드
            row.append(person.get('internal_deposit_amount'))
            row.append(person.get('internal_withdraw_amount'))
            row.append(person.get('transaction_count'))
            
            # 관계유형코드
            row.append(person.get('relation_code'))
            
            unified_rows.append(row)
        
        return {
            'success': True,
            'columns': unified_columns,
            'rows': unified_rows
        }
    
    def _get_value_by_column(self, row: list, columns: list, column_name: str):
        """컬럼명으로 값 추출"""
        try:
            if column_name in columns:
                idx = columns.index(column_name)
                return row[idx]
        except:
            pass
        return None
    
    def _determine_customer_type(self, customer_result: Dict) -> str:
        """고객 타입 판단"""
        if not customer_result.get('rows'):
            return 'UNKNOWN'
        
        cols = customer_result['columns']
        row = customer_result['rows'][0]
        
        if '고객구분' in cols:
            idx = cols.index('고객구분')
            cust_type = row[idx]
            if '법인' in str(cust_type):
                return 'CORP'
            elif '개인' in str(cust_type):
                return 'PERSON'
        
        return 'UNKNOWN'
    
    def _extract_mid(self, customer_result: Dict) -> Optional[str]:
        """MID 추출"""
        if customer_result.get('rows'):
            cols = customer_result['columns']
            row = customer_result['rows'][0]
            if 'MID' in cols:
                return row[cols.index('MID')]
        return None
    
    def _extract_kyc_datetime(self, customer_result: Dict) -> Optional[str]:
        """KYC 완료일시 추출"""
        if customer_result.get('rows'):
            cols = customer_result['columns']
            row = customer_result['rows'][0]
            if 'KYC완료일시' in cols:
                return row[cols.index('KYC완료일시')]
        return None
    
    def _get_duplicate_persons(self, cust_id: str, customer_result: Dict) -> Dict[str, Any]:
        """중복 의심 회원 조회 (기존 로직 유지)"""
        # 기존 구현 그대로 유지
        return {'success': True, 'columns': [], 'rows': []}
    
    def _convert_row_types(self, row: tuple) -> list:
        """타입 변환"""
        converted = []
        for value in row:
            if isinstance(value, Decimal):
                converted.append(float(value))
            else:
                converted.append(value)
        return converted
    

class AlertInfoExecutor:
    """
    ALERT 정보 및 Rule 히스토리 조회 클래스
    """
    
    def execute(self, alert_id: str) -> Dict[str, Any]:
        """
        ALERT 정보 조회 메인 실행 함수 (Rule 히스토리 포함)
        """
        try:
            logger.info(f"[Stage 1] Starting ALERT info query for: {alert_id}")
            
            # Step 1: 초기 정보 조회
            initial_result = self._get_initial_info(alert_id)
            if not initial_result['success']:
                return initial_result
            
            # Step 2: 년월 및 고객ID 추출
            year_month, cust_id = self._extract_key_info(initial_result)
            if not year_month or not cust_id:
                return {
                    'success': False,
                    'message': f"ALERT ID '{alert_id}'에서 년월 또는 고객ID를 추출할 수 없습니다."
                }
            
            logger.info(f"[Stage 1] Extracted - Year/Month: {year_month}, Customer ID: {cust_id}")
            
            # Step 3: 월별 전체 데이터 조회
            monthly_result = self._get_monthly_data(alert_id, year_month, cust_id)
            if not monthly_result['success']:
                return monthly_result
            
            # Step 4: 메타데이터 생성
            metadata = self._create_metadata(initial_result, monthly_result)
            
            # Step 5: Rule 히스토리 조회 (새로 추가)
            rule_history_result = {'success': True, 'exact_match': None, 'similar_matches': []}
            
            if metadata.get('unique_rule_ids'):
                # Rule 조합 생성 (정렬된 콤마 구분 문자열)
                rule_combo = ','.join(sorted(metadata['unique_rule_ids']))
                logger.info(f"[Stage 1] Querying rule history for combination: {rule_combo}")
                
                # 정확한 매칭 조회
                exact_match = self._get_exact_rule_history(rule_combo)
                rule_history_result['exact_match'] = exact_match
                
                # 유사 조합 조회
                similar_matches = self._get_similar_rule_combinations(
                    rule_combo, 
                    metadata['unique_rule_ids']
                )
                rule_history_result['similar_matches'] = similar_matches
                
                logger.info(f"[Stage 1] Rule history: {exact_match.get('occurrence_count', 0)} exact matches, "
                          f"{len(similar_matches.get('rows', []))} similar combinations")
            
            return {
                'success': True,
                'initial_info': initial_result,
                'monthly_data': monthly_result,
                'rule_history': rule_history_result,
                'metadata': metadata,
                'summary': {
                    'alert_id': alert_id,
                    'year_month': year_month,
                    'cust_id': cust_id,
                    'total_records': len(monthly_result.get('rows', [])),
                    'unique_rules': len(metadata.get('unique_rule_ids', [])),
                    'rule_combo': ','.join(sorted(metadata.get('unique_rule_ids', []))),
                    'date_range': f"{metadata.get('min_date')} ~ {metadata.get('max_date')}"
                }
            }
            
        except Exception as e:
            logger.exception(f"[Stage 1] Unexpected error in execute: {e}")
            return {
                'success': False,
                'message': f"ALERT 정보 조회 중 예외 발생: {str(e)}"
            }
    
    def _get_exact_rule_history(self, rule_combo: str) -> Dict[str, Any]:
        """정확히 일치하는 Rule 조합의 과거 이력 조회"""
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(RULE_HISTORY_QUERY, [rule_combo])
                rows = cursor.fetchall()
                
                if not rows:
                    return {
                        'success': True,
                        'occurrence_count': 0,
                        'message': 'No historical occurrences found'
                    }
                
                cols = [desc[0] for desc in cursor.description]
                row = rows[0]  # 단일 행 결과
                
                return {
                    'success': True,
                    'occurrence_count': row[1] if len(row) > 1 else 0,
                    'unique_customers': row[2] if len(row) > 2 else 0,
                    'first_occurrence': row[3] if len(row) > 3 else None,
                    'last_occurrence': row[4] if len(row) > 4 else None,
                    'str_reported_count': row[5] if len(row) > 5 else 0,
                    'not_reported_count': row[6] if len(row) > 6 else 0,
                    'uper_patterns': row[7] if len(row) > 7 else None,
                    'lwer_patterns': row[8] if len(row) > 8 else None,
                    'columns': cols,
                    'row': self._convert_row_types(row)
                }
                
        except Exception as e:
            logger.error(f"[Stage 1] Error in rule history query: {e}")
            return {
                'success': False,
                'occurrence_count': 0,
                'message': str(e)
            }
    
    def _get_similar_rule_combinations(self, rule_combo: str, 
                                      rule_ids: List[str]) -> Dict[str, Any]:
        """유사한 Rule 조합 검색"""
        try:
            # Oracle에서 리스트를 처리하기 위한 문자열 변환
            rule_ids_str = ','.join(rule_ids)
            
            with self.db_conn.cursor() as cursor:
                cursor.execute(SIMILAR_RULE_COMBINATIONS_QUERY, 
                             [rule_ids_str, rule_combo])
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                
                converted_rows = [self._convert_row_types(row) for row in rows]
                
                return {
                    'success': True,
                    'columns': cols,
                    'rows': converted_rows,
                    'count': len(converted_rows)
                }
                
        except Exception as e:
            logger.error(f"[Stage 1] Error in similar rules query: {e}")
            return {
                'success': True,
                'columns': [],
                'rows': [],
                'count': 0
            }
    
