"""
Rule related business logic service
"""
import logging
from typing import Dict, Any, List, Optional
import pandas as pd

from ..database import OracleConnection, ConnectionConfig
from ..utils import SessionManager

logger = logging.getLogger(__name__)


class RuleService:
    """Rule 관련 비즈니스 로직 서비스"""
    
    def __init__(self, oracle_conn: Optional[OracleConnection] = None):
        """
        Args:
            oracle_conn: Oracle 데이터베이스 연결 객체 (optional)
        """
        self.oracle_conn = oracle_conn
    
    def search_rule_history(self, rule_key: str, 
                           db_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Rule 히스토리 검색
        
        Args:
            rule_key: Rule ID 조합 (쉼표 구분)
            db_config: 데이터베이스 연결 설정
            
        Returns:
            히스토리 검색 결과
        """
        logger.info(f"Searching rule history for key: {rule_key}")
        
        try:
            # Rule 히스토리 검색 모듈 임포트
            from ..queries.rule_historic_search import (
                fetch_df_result_0,
                aggregate_by_rule_id_list,
                find_most_similar_rule_combinations
            )
            
            # DataFrame 조회
            df0 = fetch_df_result_0(
                jdbc_url=db_config['jdbc_url'],
                driver_class=db_config['driver_class'],
                driver_path=db_config['driver_path'],
                username=db_config['username'],
                password=db_config['password']
            )
            
            # 집계 처리
            df1 = aggregate_by_rule_id_list(df0)
            
            # 일치하는 행 찾기
            matching_rows = df1[df1["STR_RULE_ID_LIST"] == rule_key]
            
            columns = list(matching_rows.columns) if not matching_rows.empty else list(df1.columns)
            rows = matching_rows.values.tolist()
            
            # 유사한 조합 찾기 (일치하는 것이 없을 때)
            similar_list = []
            if len(rows) == 0 and not df1.empty:
                similar_list = find_most_similar_rule_combinations(rule_key, df1)
                logger.info(f"No exact match found. Found {len(similar_list)} similar combinations")
            
            result = {
                'success': True,
                'columns': columns,
                'rows': rows,
                'searched_rule': rule_key,
                'similar_list': similar_list
            }
            
            logger.info(f"Rule history search completed. Found {len(rows)} matching rows")
            
            return result
            
        except Exception as e:
            logger.exception(f"Error searching rule history: {e}")
            return {
                'success': False,
                'message': f'Rule 히스토리 검색 실패: {str(e)}',
                'columns': [],
                'rows': [],
                'searched_rule': rule_key,
                'similar_list': []
            }
    
    def get_rule_objectives_mapping(self) -> Dict[str, List[str]]:
        """
        Rule ID별 객관식 정보 매핑 가져오기
        
        Returns:
            Rule ID to objectives 매핑 딕셔너리
        """
        try:
            from ..queries.rule_objectives import build_rule_to_objectives
            return build_rule_to_objectives()
        except Exception as e:
            logger.error(f"Error building rule objectives mapping: {e}")
            return {}
    
    def analyze_rule_patterns(self, alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Alert 데이터에서 Rule 패턴 분석
        
        Args:
            alert_data: Alert 데이터
            
        Returns:
            Rule 패턴 분석 결과
        """
        try:
            cols = alert_data.get('columns', [])
            rows = alert_data.get('rows', [])
            canonical_ids = alert_data.get('canonical_ids', [])
            rep_rule_id = alert_data.get('rep_rule_id')
            
            # Rule 빈도 분석
            rule_frequency = {}
            if 'STR_RULE_ID' in cols:
                rule_idx = cols.index('STR_RULE_ID')
                for row in rows:
                    if rule_idx < len(row):
                        rule_id = str(row[rule_idx])
                        rule_frequency[rule_id] = rule_frequency.get(rule_id, 0) + 1
            
            # Rule별 거래 기간 분석
            rule_periods = {}
            if all(col in cols for col in ['STR_RULE_ID', 'TRAN_STRT', 'TRAN_END']):
                rule_idx = cols.index('STR_RULE_ID')
                start_idx = cols.index('TRAN_STRT')
                end_idx = cols.index('TRAN_END')
                
                for row in rows:
                    if all(idx < len(row) for idx in [rule_idx, start_idx, end_idx]):
                        rule_id = str(row[rule_idx])
                        if rule_id not in rule_periods:
                            rule_periods[rule_id] = {
                                'min_start': row[start_idx],
                                'max_end': row[end_idx]
                            }
                        else:
                            # 최소 시작일, 최대 종료일 업데이트
                            if row[start_idx] and row[start_idx] < rule_periods[rule_id]['min_start']:
                                rule_periods[rule_id]['min_start'] = row[start_idx]
                            if row[end_idx] and row[end_idx] > rule_periods[rule_id]['max_end']:
                                rule_periods[rule_id]['max_end'] = row[end_idx]
            
            # 결과 구성
            analysis = {
                'total_rules': len(canonical_ids),
                'representative_rule': rep_rule_id,
                'rule_frequency': rule_frequency,
                'rule_periods': rule_periods,
                'canonical_order': canonical_ids
            }
            
            logger.info(f"Rule pattern analysis completed. Total rules: {len(canonical_ids)}")
            
            return analysis
            
        except Exception as e:
            logger.exception(f"Error analyzing rule patterns: {e}")
            return {
                'total_rules': 0,
                'representative_rule': None,
                'rule_frequency': {},
                'rule_periods': {},
                'canonical_order': []
            }
    
    def validate_rule_combination(self, rule_ids: List[str]) -> Dict[str, Any]:
        """
        Rule ID 조합의 유효성 검증
        
        Args:
            rule_ids: Rule ID 리스트
            
        Returns:
            검증 결과
        """
        validation_result = {
            'is_valid': True,
            'warnings': [],
            'errors': []
        }
        
        # 빈 리스트 체크
        if not rule_ids:
            validation_result['is_valid'] = False
            validation_result['errors'].append("Rule ID 리스트가 비어있습니다.")
            return validation_result
        
        # 중복 체크
        if len(rule_ids) != len(set(rule_ids)):
            validation_result['warnings'].append("중복된 Rule ID가 있습니다.")
        
        # 특수 Rule ID 체크
        special_rules = {'IO000', 'IO111'}  # 12개월 조회 필요한 특수 Rule
        has_special = any(rule_id in special_rules for rule_id in rule_ids)
        
        if has_special:
            validation_result['warnings'].append(
                "특수 Rule ID(IO000, IO111)가 포함되어 있어 12개월 데이터 조회가 필요합니다."
            )
        
        # Rule ID 형식 검증
        for rule_id in rule_ids:
            if not self._is_valid_rule_id_format(rule_id):
                validation_result['errors'].append(f"잘못된 Rule ID 형식: {rule_id}")
                validation_result['is_valid'] = False
        
        logger.info(f"Rule combination validation: {validation_result}")
        
        return validation_result
    
    def _is_valid_rule_id_format(self, rule_id: str) -> bool:
        """Rule ID 형식 검증"""
        if not rule_id:
            return False
        
        # 기본적으로 알파벳과 숫자 조합
        import re
        pattern = r'^[A-Z]{2}\d{3}$'  # 예: IO000, ML001 등
        
        return bool(re.match(pattern, rule_id))