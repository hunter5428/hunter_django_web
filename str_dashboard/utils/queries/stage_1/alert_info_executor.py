# str_dashboard/utils/queries/stage_1/alert_info_executor.py
"""
ALERT 정보 쿼리 실행 모듈
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from decimal import Decimal

from .sql_templates import (
    INITIAL_ALERT_QUERY,
    MONTHLY_ALERT_QUERY,
    RULE_HISTORY_QUERY
)

logger = logging.getLogger(__name__)


class AlertInfoExecutor:
    """
    Stage 1: ALERT 정보 및 Rule 히스토리 조회 클래스
    """
    
    def __init__(self, db_connection):
        """
        Args:
            db_connection: Oracle 데이터베이스 연결 객체
        """
        self.db_conn = db_connection
        
    def execute(self, alert_id: str) -> Dict[str, Any]:
        """
        ALERT 정보 조회 메인 실행 함수
        
        Args:
            alert_id: ALERT ID
            
        Returns:
            실행 결과 딕셔너리
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
            
            # Step 5: Rule 히스토리 조회
            rule_history_result = {'success': True, 'exact_match': None}
            
            if metadata.get('unique_rule_ids'):
                rule_combo = ','.join(sorted(metadata['unique_rule_ids']))
                logger.info(f"[Stage 1] Querying rule history for: {rule_combo}")
                
                exact_match = self._get_exact_rule_history(rule_combo)
                rule_history_result['exact_match'] = exact_match
            
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
                    'date_range': f"{metadata.get('min_date')} ~ {metadata.get('max_date')}"
                }
            }
            
        except Exception as e:
            logger.exception(f"[Stage 1] Error in execute: {e}")
            return {
                'success': False,
                'message': f"ALERT 정보 조회 중 오류: {str(e)}"
            }
    
    def _get_initial_info(self, alert_id: str) -> Dict[str, Any]:
        """초기 ALERT 정보 조회"""
        try:
            with self.db_conn.cursor() as cursor:
                cursor.execute(INITIAL_ALERT_QUERY, [alert_id])
                rows = cursor.fetchall()
                
                if not rows:
                    return {
                        'success': False,
                        'message': f"ALERT ID '{alert_id}'를 찾을 수 없습니다."
                    }
                
                cols = [desc[0] for desc in cursor.description]
                
                return {
                    'success': True,
                    'columns': cols,
                    'rows': [self._convert_row_types(rows[0])]
                }
                
        except Exception as e:
            logger.error(f"[Stage 1] Error in initial query: {e}")
            return {
                'success': False,
                'message': str(e)
            }
    
    def _extract_key_info(self, initial_result: Dict) -> tuple:
        """초기 조회 결과에서 년월과 고객ID 추출"""
        if not initial_result.get('rows'):
            return None, None
        
        cols = initial_result['columns']
        row = initial_result['rows'][0]
        
        year_month = None
        cust_id = None
        
        # STDS_DTM에서 년월 추출
        if 'STDS_DTM' in cols:
            idx = cols.index('STDS_DTM')
            date_str = row[idx]
            if date_str:
                # YYYY-MM 형식으로 추출
                year_month = str(date_str)[:7]
        
        # CUST_ID 추출
        if 'CUST_ID' in cols:
            idx = cols.index('CUST_ID')
            cust_id = row[idx]
        
        return year_month, cust_id
    
    def _get_monthly_data(self, alert_id: str, year_month: str, cust_id: str) -> Dict[str, Any]:
        """월별 ALERT 데이터 조회"""
        try:
            # 월의 시작일과 종료일 계산
            start_date = f"{year_month}-01"
            
            # 다음 달 계산
            year = int(year_month[:4])
            month = int(year_month[5:7])
            
            if month == 12:
                next_month = f"{year + 1:04d}-01-01"
            else:
                next_month = f"{year:04d}-{month + 1:02d}-01"
            
            with self.db_conn.cursor() as cursor:
                cursor.execute(
                    MONTHLY_ALERT_QUERY,
                    [alert_id, start_date, next_month, cust_id]
                )
                rows = cursor.fetchall()
                cols = [desc[0] for desc in cursor.description]
                
                # 타입 변환
                converted_rows = [self._convert_row_types(row) for row in rows]
                
                logger.info(f"[Stage 1] Monthly query found {len(rows)} records")
                
                return {
                    'success': True,
                    'columns': cols,
                    'rows': converted_rows
                }
                
        except Exception as e:
            logger.error(f"[Stage 1] Error in monthly query: {e}")
            return {
                'success': False,
                'message': str(e)
            }
    
    def _create_metadata(self, initial_result: Dict, monthly_result: Dict) -> Dict[str, Any]:
        """메타데이터 생성"""
        metadata = {}
        
        # 초기 정보에서 추출
        if initial_result.get('rows'):
            cols = initial_result['columns']
            row = initial_result['rows'][0]
            
            if 'CUST_ID' in cols:
                metadata['cust_id'] = row[cols.index('CUST_ID')]
            if 'STR_RPT_MNGT_NO' in cols:
                metadata['str_rpt_mngt_no'] = row[cols.index('STR_RPT_MNGT_NO')]
        
        # 월별 데이터에서 추출
        if monthly_result.get('rows'):
            cols = monthly_result['columns']
            rows = monthly_result['rows']
            
            # Rule ID 추출
            if 'STR_RULE_ID' in cols:
                rule_idx = cols.index('STR_RULE_ID')
                rule_ids = [row[rule_idx] for row in rows if row[rule_idx]]
                metadata['unique_rule_ids'] = list(set(rule_ids))
                metadata['canonical_ids'] = sorted(metadata['unique_rule_ids'])
            
            # 거래 기간 추출
            if 'TRAN_STRT' in cols and 'TRAN_END' in cols:
                start_idx = cols.index('TRAN_STRT')
                end_idx = cols.index('TRAN_END')
                
                start_dates = [row[start_idx] for row in rows if row[start_idx]]
                end_dates = [row[end_idx] for row in rows if row[end_idx]]
                
                if start_dates:
                    metadata['tran_start'] = min(start_dates)
                    metadata['min_date'] = metadata['tran_start']
                
                if end_dates:
                    metadata['tran_end'] = max(end_dates)
                    metadata['max_date'] = metadata['tran_end']
        
        return metadata
    
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
                row = rows[0]
                
                return {
                    'success': True,
                    'occurrence_count': row[1] if len(row) > 1 else 0,
                    'unique_customers': row[2] if len(row) > 2 else 0,
                    'first_occurrence': row[3] if len(row) > 3 else None,
                    'last_occurrence': row[4] if len(row) > 4 else None,
                    'str_reported_count': row[5] if len(row) > 5 else 0,
                    'not_reported_count': row[6] if len(row) > 6 else 0,
                    'uper_patterns': row[7] if len(row) > 7 else None,
                    'lwer_patt[erns': row[8] if len(row) > 8 else None,
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
    
    def _convert_row_types(self, row: tuple) -> list:
        """행 데이터 타입 변환"""
        converted = []
        for value in row:
            if isinstance(value, Decimal):
                converted.append(float(value))
            else:
                converted.append(value)
        return converted