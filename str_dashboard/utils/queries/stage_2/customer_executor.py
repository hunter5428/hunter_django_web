# str_dashboard/utils/queries/stage_2/customer_executor.py
"""
고객 및 관련인 정보 쿼리 실행 모듈
통합 DataFrame 생성
"""

import logging
from typing import Dict, Any, List, Optional, Set
from datetime import datetime
from decimal import Decimal
import json

from .sql_templates import (
    CUSTOMER_UNIFIED_INFO_QUERY,
    CORP_RELATED_PERSONS_QUERY,
    PERSON_INTERNAL_TRANSACTION_QUERY,
    PERSON_TRANSACTION_DETAIL_QUERY,
    DUPLICATE_PERSONS_QUERY
)

logger = logging.getLogger(__name__)


class CustomerExecutor:
    """
    Stage 2: 고객 및 관련인 정보 조회 실행 클래스
    """
    
    def __init__(self, db_connection):
        """
        Args:
            db_connection: Oracle 데이터베이스 연결 객체
        """
        self.db_conn = db_connection
        
    def execute(self, cust_id: str, stage_1_metadata: Dict[str, Any]) -> Dict[str, Any]:
        """고객 및 관련인 정보 조회 메인 실행 함수"""
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
                else:
                    logger.warning("[Stage 2] No transaction period for person related query")
            
            # Step 3: 중복 의심 회원 조회
            duplicate_result = self._get_duplicate_persons(cust_id, customer_result)
            
            # Step 4: 통합 DataFrame 구성
            unified_result = self._create_unified_dataframe(
                customer_result,
                related_persons_result,
                customer_type
            )
            
            # Step 5: 메타데이터 생성
            metadata = self._create_metadata(
                customer_result,
                unified_result,
                duplicate_result,
                customer_type
            )
            
            return {
                'success': True,
                'customer_info': customer_result,
                'related_persons': unified_result,
                'duplicate_persons': duplicate_result,
                'metadata': metadata,
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
        """고객 정보 조회 - Oracle 딕셔너리 바인딩"""
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(CUSTOMER_UNIFIED_INFO_QUERY, {'cust_id': cust_id})
                rows = cursor.fetchall()
                
                if not rows:
                    logger.warning(f"[Stage 2] No customer data found for: {cust_id}")
                    return {
                        'success': False,
                        'message': f"고객 ID '{cust_id}'에 해당하는 데이터가 없습니다."
                    }
                
                cols = [desc[0] for desc in cursor.description]
                
                logger.info(f"[Stage 2] Customer query found: {len(rows)} row(s)")
                
                return {
                    'success': True,
                    'columns': cols,
                    'rows': [self._convert_row_types(rows[0])]
                }
                
        except Exception as e:
            logger.exception(f"[Stage 2] Error in _get_customer_info: {e}")
            return {
                'success': False,
                'message': f"고객 정보 조회 실패: {str(e)}"
            }
    
    def _determine_customer_type(self, customer_result: Dict) -> str:
        """고객 타입 결정 (개인/법인)"""
        if not customer_result.get('rows'):
            return 'UNKNOWN'
        
        cols = customer_result['columns']
        row = customer_result['rows'][0]
        
        # CUST_TYPE_CD 컬럼 찾기
        if 'CUST_TYPE_CD' in cols:
            idx = cols.index('CUST_TYPE_CD')
            cust_type_cd = row[idx]
            
            if cust_type_cd == '01':
                return 'PERSON'
            elif cust_type_cd == '02':
                return 'CORP'
        
        # 고객구분 컬럼으로 판단
        if '고객구분' in cols:
            idx = cols.index('고객구분')
            cust_type = row[idx]
            if '법인' in str(cust_type):
                return 'CORP'
            elif '개인' in str(cust_type):
                return 'PERSON'
        
        return 'UNKNOWN'
    
    def _get_corp_related_persons(self, cust_id: str) -> Dict[str, Any]:
        """법인 관련인 조회 - Oracle 딕셔너리 바인딩"""
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(CORP_RELATED_PERSONS_QUERY, {'cust_id': cust_id})
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                
                related_data = []
                for row in rows:
                    related_cust_id = row[0] if len(row) > 0 else None
                    mid = None
                    if related_cust_id:
                        mid = self._get_mid_for_customer(related_cust_id)
                    
                    related_person = {
                        'related_cust_id': related_cust_id,
                        'mid': mid,
                        'relation_type': row[1] if len(row) > 1 else None,
                        'name': row[2] if len(row) > 2 else None,
                        'name_en': row[3] if len(row) > 3 else None,
                        'birth_date': row[4] if len(row) > 4 else None,
                        'gender': row[5] if len(row) > 5 else None,
                        'id_number': row[6] if len(row) > 6 else None,
                        'stake_rate': row[7] if len(row) > 7 else None,
                        'relation_code': row[8] if len(row) > 8 else None,
                        'internal_deposit_amount': None,
                        'internal_withdraw_amount': None,
                        'transaction_count': None,
                        'customer_details': None
                    }
                    related_data.append(related_person)
                
                logger.info(f"[Stage 2] Corp related query found: {len(related_data)} person(s)")
                return {'success': True, 'data': related_data}
                
        except Exception as e:
            logger.error(f"[Stage 2] Error in corp related persons: {e}")
            return {'success': True, 'data': []}
    
    def _get_mid_for_customer(self, cust_id: str) -> Optional[str]:
        """고객 ID로 MID 조회"""
        try:
            query = "SELECT MEM_ID FROM BTCAMLDB_OWN.KYC_MEM_BASE WHERE CUST_ID = :cust_id"
            with self.db_conn.cursor() as cursor:
                cursor.execute(query, {'cust_id': cust_id})
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"[Stage 2] Error getting MID for {cust_id}: {e}")
            return None


    def _get_duplicate_persons(self, cust_id: str, 
                            customer_result: Dict) -> Dict[str, Any]:
        """중복 의심 회원 조회 - Oracle 딕셔너리 바인딩"""
        try:
            dup_params = self._extract_duplicate_params(customer_result)
            
            if not dup_params:
                logger.warning("[Stage 2] No duplicate params extracted")
                return {'success': True, 'columns': [], 'rows': []}
            
            # Oracle은 named 바인딩에서 동일한 이름 재사용 가능
            params = {
                'cust_id': cust_id,
                'address': dup_params.get('address'),
                'detail_address': dup_params.get('detail_address'),
                'workplace_name': dup_params.get('workplace_name'),
                'workplace_address': dup_params.get('workplace_address'),
                'workplace_detail_address': dup_params.get('workplace_detail_address'),
                'phone_suffix': dup_params.get('phone_suffix')
            }
            
            with self.db_conn.cursor() as cursor:
                cursor.execute(DUPLICATE_PERSONS_QUERY, params)
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                
                logger.info(f"[Stage 2] Duplicate query found: {len(rows)} person(s)")
                
                converted_rows = [self._convert_row_types(row) for row in rows]
                
                return {
                    'success': True,
                    'columns': cols,
                    'rows': converted_rows
                }
                
        except Exception as e:
            logger.error(f"[Stage 2] Error in duplicate persons: {e}")
            return {'success': True, 'columns': [], 'rows': []}

    def _extract_duplicate_params(self, customer_result: Dict) -> Optional[Dict]:
        """중복 검색용 파라미터 추출"""
        if not customer_result.get('rows'):
            return None
        
        cols = customer_result['columns']
        row = customer_result['rows'][0]
        
        params = {}
        
        field_map = {
            '거주지주소': 'address',
            '거주지상세주소': 'detail_address',
            '직장명': 'workplace_name',
            '직장주소': 'workplace_address',
            '직장상세주소': 'workplace_detail_address',
            '연락처': 'phone'
        }
        
        for col_name, param_name in field_map.items():
            if col_name in cols:
                idx = cols.index(col_name)
                value = row[idx]
                
                if param_name == 'phone' and value:
                    params['phone_suffix'] = str(value)[-4:] if len(str(value)) >= 4 else ''
                else:
                    params[param_name] = str(value) if value else ''
        
        return params

    def _get_person_related_with_details(self, cust_id: str, 
                                        tran_start: str, tran_end: str) -> Dict[str, Any]:
        """개인 관련인 조회 - DM 테이블 사용"""
        try:
            start_dt = self._format_timestamp(tran_start)
            end_dt = self._format_timestamp(tran_end)
            
            # Oracle 딕셔너리 바인딩
            params = {
                'start_date': start_dt,
                'end_date': end_dt,
                'cust_id': cust_id
            }
            
            with self.db_conn.cursor() as cursor:
                cursor.execute(PERSON_INTERNAL_TRANSACTION_QUERY, params)
                transaction_rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
            
            if not transaction_rows:
                return {'success': True, 'data': []}
            
            related_data = []
            for tx_row in transaction_rows:
                related_cust_id = tx_row[0] if len(tx_row) > 0 else None
                name = tx_row[1] if len(tx_row) > 1 else None  # DM_CUST_BASE에서 조회된 이름
                deposit_amount = float(tx_row[2]) if len(tx_row) > 2 and tx_row[2] else 0
                withdraw_amount = float(tx_row[3]) if len(tx_row) > 3 and tx_row[3] else 0
                tx_count = tx_row[4] if len(tx_row) > 4 else 0
                
                # KYC 정보 조회 (신원 확인 정보)
                detail_result = self._get_customer_info(related_cust_id)
                
                # 종목별 거래 상세 조회
                coin_transactions = self._get_coin_transaction_details(
                    cust_id, related_cust_id, start_dt, end_dt
                )
                
                if detail_result['success'] and detail_result['rows']:
                    detail_row = detail_result['rows'][0]
                    detail_cols = detail_result['columns']
                    
                    mid_value = self._get_value_by_column(detail_row, detail_cols, 'MID')
                    
                    # DM에서 조회한 이름 우선 사용
                    related_name = name if name else self._get_value_by_column(detail_row, detail_cols, '성명')
                    
                    related_person = {
                        'related_cust_id': related_cust_id,
                        'mid': mid_value,
                        'relation_type': '내부거래상대방',
                        'name': related_name,
                        'name_en': self._get_value_by_column(detail_row, detail_cols, '영문명'),
                        'birth_date': self._get_value_by_column(detail_row, detail_cols, '생년월일'),
                        'gender': self._get_value_by_column(detail_row, detail_cols, '성별'),
                        'id_number': self._get_value_by_column(detail_row, detail_cols, '실명번호'),
                        'stake_rate': None,
                        'relation_code': 'INTERNAL',
                        'internal_deposit_amount': deposit_amount,
                        'internal_withdraw_amount': withdraw_amount,
                        'transaction_count': tx_count,
                        'coin_transactions': coin_transactions,
                        'customer_details': {
                            'columns': detail_cols,
                            'values': detail_row
                        }
                    }
                else:
                    # KYC 정보가 없어도 기본 정보 제공
                    related_person = {
                        'related_cust_id': related_cust_id,
                        'mid': None,
                        'relation_type': '내부거래상대방',
                        'name': name,  # DM에서 조회한 이름
                        'name_en': None,
                        'birth_date': None,
                        'gender': None,
                        'id_number': None,
                        'stake_rate': None,
                        'relation_code': 'INTERNAL',
                        'internal_deposit_amount': deposit_amount,
                        'internal_withdraw_amount': withdraw_amount,
                        'transaction_count': tx_count,
                        'coin_transactions': coin_transactions,
                        'customer_details': None
                    }
                
                related_data.append(related_person)
            
            return {'success': True, 'data': related_data}
            
        except Exception as e:
            logger.error(f"[Stage 2] Error in person related query: {e}")
            return {'success': True, 'data': []}

    def _get_coin_transaction_details(self, cust_id: str, related_cust_id: str,
                                    start_dt: str, end_dt: str) -> List[Dict]:
        """종목별 거래 상세 조회 - DM 테이블 사용"""
        try:
            params = {
                'cust_id': cust_id,
                'related_cust_id': related_cust_id,
                'start_date': start_dt,
                'end_date': end_dt
            }
            
            with self.db_conn.cursor() as cursor:
                cursor.execute(PERSON_TRANSACTION_DETAIL_QUERY, params)
                rows = cursor.fetchall()
                
                coin_details = []
                for row in rows:
                    coin_detail = {
                        '종목': row[1] if len(row) > 1 else None,
                        '거래구분': row[2] if len(row) > 2 else None,
                        '거래수량': float(row[3]) if len(row) > 3 and row[3] else 0,
                        '거래금액': float(row[4]) if len(row) > 4 and row[4] else 0,
                        '거래건수': int(row[5]) if len(row) > 5 and row[5] else 0
                    }
                    coin_details.append(coin_detail)
                
                return coin_details
                
        except Exception as e:
            logger.error(f"[Stage 2] Error getting coin transaction details: {e}")
            return []

    def _create_unified_dataframe(self, customer_result: Dict,
                                related_result: Dict,
                                customer_type: str) -> Dict[str, Any]:
        """통합 관련인 DataFrame 생성"""
        
        unified_columns = [
            '관련인고객ID',
            '관련인MID',
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
            '지분율',
            '내부입고금액',
            '내부출고금액',
            '거래횟수',
            '종목별거래상세',
            '관계유형코드'
        ]
        
        unified_rows = []
        
        for person in related_result.get('data', []):
            row = []
            
            row.append(person.get('related_cust_id'))
            row.append(person.get('mid'))
            row.append(person.get('relation_type'))
            row.append(person.get('name'))
            row.append(person.get('name_en'))
            row.append(person.get('birth_date'))
            row.append(person.get('gender'))
            row.append(person.get('id_number'))
            
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
                row.extend([None] * 7)
            
            row.append(person.get('stake_rate'))
            row.append(person.get('internal_deposit_amount'))
            row.append(person.get('internal_withdraw_amount'))
            row.append(person.get('transaction_count'))
            
            # 종목별 거래 상세
            coin_transactions = person.get('coin_transactions', [])
            if coin_transactions:
                row.append(json.dumps(coin_transactions, ensure_ascii=False))
            else:
                row.append(None)
            
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
    
    def _format_timestamp(self, date_str: str) -> str:
        """날짜 문자열을 타임스탬프 형식으로 변환"""
        try:
            if ' ' in date_str and ':' in date_str:
                return date_str
            
            if '-' in date_str and ' ' not in date_str:
                return f"{date_str} 00:00:00"
            
            return date_str
        except Exception:
            return date_str
    
    def _convert_row_types(self, row: tuple) -> list:
        """행 데이터 타입 변환"""
        converted = []
        for value in row:
            if isinstance(value, Decimal):
                converted.append(float(value))
            elif value is None:
                converted.append(None)
            else:
                converted.append(value)
        return converted
    
    def _create_metadata(self, customer_result: Dict, unified_result: Dict,
                        duplicate_result: Dict, customer_type: str) -> Dict[str, Any]:
        """메타데이터 생성"""
        metadata = {
            'customer_type': customer_type,
            'has_related_persons': len(unified_result.get('rows', [])) > 0,
            'has_duplicates': len(duplicate_result.get('rows', [])) > 0,
            'mid': None,
            'kyc_datetime': None
        }
        
        if customer_result.get('rows'):
            cols = customer_result['columns']
            row = customer_result['rows'][0]
            
            if 'MID' in cols:
                metadata['mid'] = row[cols.index('MID')]
            
            if 'KYC완료일시' in cols:
                metadata['kyc_datetime'] = row[cols.index('KYC완료일시')]
        
        return metadata