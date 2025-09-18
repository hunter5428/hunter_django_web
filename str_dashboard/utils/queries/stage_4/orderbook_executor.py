# str_dashboard/utils/queries/stage_4/orderbook_executor.py
"""
Orderbook 조회 실행 모듈
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import pandas as pd

from .sql_templates import ORDERBOOK_QUERY
from .special_range_rules import requires_extended_range

logger = logging.getLogger(__name__)


class OrderbookExecutor:
    """
    Stage 4: Orderbook 조회 실행 클래스
    """
    
    def __init__(self, redshift_connection):
        """
        Args:
            redshift_connection: Redshift 연결 객체
        """
        self.rs_conn = redshift_connection
        
    def execute(self, stage_1_data: Dict[str, Any], 
                stage_2_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Orderbook 조회 메인 실행 함수
        
        Args:
            stage_1_data: Stage 1 결과 데이터
            stage_2_data: Stage 2 결과 데이터
            
        Returns:
            실행 결과 딕셔너리
        """
        try:
            logger.info("[Stage 4] Starting Orderbook query")
            
            # Step 1: 날짜 변수 추출
            date_vars = self._extract_date_variables(stage_1_data, stage_2_data)
            if not date_vars['success']:
                return date_vars
            
            # Step 2: Orderbook 조회 날짜 계산
            tran_strt_orderbook = self._calculate_start_date(date_vars, stage_1_data)
            tran_end_orderbook = date_vars['TRAN_END_MAX']
            
            logger.info(f"[Stage 4] Orderbook period: {tran_strt_orderbook} ~ {tran_end_orderbook}")
            
            # Step 3: MID 목록 추출
            mid_list = self._extract_mid_list(stage_2_data)
            if not mid_list:
                return {
                    'success': False,
                    'message': 'No MID values found for orderbook query'
                }
            
            logger.info(f"[Stage 4] Querying orderbook for {len(mid_list)} MIDs")
            
            # Step 4: Orderbook 조회
            orderbook_result = self._query_orderbook(
                tran_strt_orderbook,
                tran_end_orderbook,
                mid_list
            )
            
            return {
                'success': True,
                'orderbook_data': orderbook_result,
                'metadata': {
                    'start_date': tran_strt_orderbook,
                    'end_date': tran_end_orderbook,
                    'mid_count': len(mid_list),
                    'primary_mid': mid_list[0] if mid_list else None,
                    'date_calculation': date_vars
                },
                'summary': {
                    'total_records': len(orderbook_result.get('rows', [])),
                    'unique_users': len(set([row[0] for row in orderbook_result.get('rows', [])])) if orderbook_result.get('rows') else 0
                }
            }
            
        except Exception as e:
            logger.exception(f"[Stage 4] Error in execute: {e}")
            return {
                'success': False,
                'message': f"Orderbook 조회 실패: {str(e)}"
            }
    
    def _extract_date_variables(self, stage_1_data: Dict, stage_2_data: Dict) -> Dict[str, Any]:
        """Stage 1과 Stage 2에서 날짜 변수 추출"""
        try:
            # Stage 1 monthly_df에서 날짜 추출
            monthly_df_data = stage_1_data.get('dataframes', {}).get('monthly', {})
            
            if not monthly_df_data.get('rows'):
                return {'success': False, 'message': 'No monthly data found'}
            
            rows = monthly_df_data['rows']
            cols = monthly_df_data['columns']
            
            # 컬럼 인덱스 찾기
            tran_strt_idx = cols.index('TRAN_STRT') if 'TRAN_STRT' in cols else None
            tran_end_idx = cols.index('TRAN_END') if 'TRAN_END' in cols else None
            is_target_idx = cols.index('IS_TARGET_ALERT') if 'IS_TARGET_ALERT' in cols else None
            
            if tran_strt_idx is None or tran_end_idx is None:
                return {'success': False, 'message': 'TRAN_STRT or TRAN_END column not found'}
            
            # 날짜 추출
            tran_strt_values = [row[tran_strt_idx] for row in rows if row[tran_strt_idx]]
            tran_end_values = [row[tran_end_idx] for row in rows if row[tran_end_idx]]
            
            # 최소/최대 날짜
            TRAN_STRT_MIN = min(tran_strt_values) if tran_strt_values else None
            TRAN_END_MAX = max(tran_end_values) if tran_end_values else None
            
            # 타겟 ALERT의 날짜
            TRAN_STRT_TARGET = None
            TRAN_END_TARGET = None
            
            if is_target_idx is not None:
                for row in rows:
                    if row[is_target_idx] == 'Y':
                        TRAN_STRT_TARGET = row[tran_strt_idx]
                        TRAN_END_TARGET = row[tran_end_idx]
                        break
            
            # TRAN_END_MAX 기준 -90일, -365일 계산
            if TRAN_END_MAX:
                end_date = pd.to_datetime(TRAN_END_MAX)
                TRAN_STRT_90D = (end_date - timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
                TRAN_STRT_365D = (end_date - timedelta(days=365)).strftime('%Y-%m-%d %H:%M:%S')
            else:
                TRAN_STRT_90D = None
                TRAN_STRT_365D = None
            
            # Stage 2에서 KYC 날짜 추출
            cust_id_kycdate = self._extract_kyc_date(stage_2_data)
            
            return {
                'success': True,
                'TRAN_STRT_MIN': TRAN_STRT_MIN,
                'TRAN_END_MAX': TRAN_END_MAX,
                'TRAN_STRT_TARGET': TRAN_STRT_TARGET,
                'TRAN_END_TARGET': TRAN_END_TARGET,
                'TRAN_STRT_90D': TRAN_STRT_90D,
                'TRAN_STRT_365D': TRAN_STRT_365D,
                'cust_id_kycdate': cust_id_kycdate
            }
            
        except Exception as e:
            logger.error(f"[Stage 4] Error extracting date variables: {e}")
            return {'success': False, 'message': str(e)}
    
    def _extract_kyc_date(self, stage_2_data: Dict) -> Optional[str]:
        """Stage 2에서 KYC 완료 날짜 추출"""
        try:
            customer_df = stage_2_data.get('dataframes', {}).get('customer', {})
            
            if not customer_df.get('rows'):
                return None
            
            cols = customer_df['columns']
            row = customer_df['rows'][0]  # PRIMARY 고객 정보
            
            if 'KYC완료일시' in cols:
                kyc_datetime = row[cols.index('KYC완료일시')]
                return kyc_datetime
            
            return None
            
        except Exception as e:
            logger.error(f"[Stage 4] Error extracting KYC date: {e}")
            return None
    
    def _calculate_start_date(self, date_vars: Dict, stage_1_data: Dict) -> str:
        """Orderbook 조회 시작 날짜 계산"""
        
        # Rule ID 목록 추출
        metadata = stage_1_data.get('metadata', {})
        rule_ids = metadata.get('unique_rule_ids', [])
        
        # 특별 Rule 확인
        use_extended_range = requires_extended_range(rule_ids)
        
        # 비교할 날짜 목록 생성
        dates_to_compare = []
        
        # TRAN_STRT_MIN 추가
        if date_vars.get('TRAN_STRT_MIN'):
            dates_to_compare.append(date_vars['TRAN_STRT_MIN'])
        
        # 90일 또는 365일 추가
        if use_extended_range:
            logger.info(f"[Stage 4] Using extended range (365 days) due to special rules: {rule_ids}")
            if date_vars.get('TRAN_STRT_365D'):
                dates_to_compare.append(date_vars['TRAN_STRT_365D'])
        else:
            logger.info("[Stage 4] Using standard range (90 days)")
            if date_vars.get('TRAN_STRT_90D'):
                dates_to_compare.append(date_vars['TRAN_STRT_90D'])
        
        # KYC 날짜 추가
        if date_vars.get('cust_id_kycdate'):
            dates_to_compare.append(date_vars['cust_id_kycdate'])
        
        # 가장 오래된 날짜 선택
        if dates_to_compare:
            # 날짜를 datetime으로 변환하여 비교
            datetime_list = [pd.to_datetime(d) for d in dates_to_compare]
            oldest_date = min(datetime_list)
            return oldest_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # 기본값: TRAN_STRT_MIN
        return date_vars.get('TRAN_STRT_MIN', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    
    def _extract_mid_list(self, stage_2_data: Dict) -> List[str]:
        """Stage 2에서 MID 목록 추출"""
        mid_list = []
        
        try:
            # PRIMARY 고객 MID
            metadata = stage_2_data.get('metadata', {})
            primary_mid = metadata.get('mid')
            if primary_mid:
                mid_list.append(primary_mid)
            
            # 관련인 MID
            related_df = stage_2_data.get('dataframes', {}).get('related_persons', {})
            if related_df.get('rows'):
                cols = related_df['columns']
                if '관련인MID' in cols:
                    mid_idx = cols.index('관련인MID')
                    for row in related_df['rows']:
                        if row[mid_idx]:
                            mid_list.append(row[mid_idx])
            
            # 중복 제거
            mid_list = list(set(mid_list))
            
        except Exception as e:
            logger.error(f"[Stage 4] Error extracting MID list: {e}")
        
        return mid_list
    
    def _query_orderbook(self, start_date: str, end_date: str, 
                        mid_list: List[str]) -> Dict[str, Any]:
        """Orderbook 조회"""
        try:
            # 날짜를 pandas datetime으로 변환
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            
            with self.rs_conn.transaction() as conn:
                with conn.cursor() as cursor:
                    # IN 절을 위한 튜플 생성
                    cursor.execute(ORDERBOOK_QUERY, (start_dt, end_dt, tuple(mid_list)))
                    
                    if not cursor.description:
                        return {'success': True, 'columns': [], 'rows': []}
                    
                    cols = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    logger.info(f"[Stage 4] Orderbook query found {len(rows)} records")
                    
                    return {
                        'success': True,
                        'columns': cols,
                        'rows': rows
                    }
                    
        except Exception as e:
            logger.error(f"[Stage 4] Error querying orderbook: {e}")
            return {
                'success': False,
                'columns': [],
                'rows': [],
                'error': str(e)
            }