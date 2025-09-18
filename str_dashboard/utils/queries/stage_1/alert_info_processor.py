# str_dashboard/utils/queries/stage_2/customer_processor.py
"""
고객 및 관련인 정보 처리 모듈
통합 DataFrame 관리
"""

import logging
import pandas as pd
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class CustomerProcessor:
    """
    Stage 2: 고객 및 관련인 데이터 처리 클래스
    """
    
    def __init__(self):
        self.customer_df = None
        self.related_df = None
        self.duplicate_df = None
        self.metadata = {}
        
    def process(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        CustomerExecutor의 실행 결과를 처리
        """
        try:
            if not execution_result.get('success'):
                return execution_result
            
            # DataFrame 생성
            self._create_dataframes(execution_result)
            
            # 데이터 분석
            analysis = self._analyze_unified_data()
            
            # Export용 데이터 준비
            export_data = self._prepare_export_data(execution_result)
            
            return {
                'success': True,
                'dataframes': {
                    'customer': self.customer_df,
                    'related_persons': self.related_df,
                    'duplicate_persons': self.duplicate_df
                },
                'analysis': analysis,
                'export_data': export_data
            }
            
        except Exception as e:
            logger.exception(f"[Stage 2 Processor] Error: {e}")
            return {'success': False, 'message': str(e)}
    
    def _create_dataframes(self, execution_result: Dict[str, Any]):
        """DataFrame 생성"""
        
        # 고객 정보
        customer_data = execution_result.get('customer_info', {})
        if customer_data.get('columns') and customer_data.get('rows'):
            self.customer_df = pd.DataFrame(
                customer_data['rows'],
                columns=customer_data['columns']
            )
            logger.info(f"Customer DF: {self.customer_df.shape}")
        
        # 통합 관련인 정보
        related_data = execution_result.get('related_persons', {})
        if related_data.get('columns') and related_data.get('rows'):
            self.related_df = pd.DataFrame(
                related_data['rows'],
                columns=related_data['columns']
            )
            logger.info(f"Related persons DF: {self.related_df.shape}")
            
            # 데이터 타입 최적화
            self._optimize_datatypes()
        
        # 중복 의심 회원 (기존 유지)
        duplicate_data = execution_result.get('duplicate_persons', {})
        if duplicate_data.get('columns') and duplicate_data.get('rows'):
            self.duplicate_df = pd.DataFrame(
                duplicate_data['rows'],
                columns=duplicate_data['columns']
            )
        
        # 메타데이터
        self.metadata = execution_result.get('metadata', {})
    
    def _optimize_datatypes(self):
        """DataFrame 데이터 타입 최적화"""
        if self.related_df is None or self.related_df.empty:
            return
        
        # 숫자형 컬럼 변환
        numeric_columns = ['지분율', '내부입고금액', '내부출고금액', '거래횟수']
        for col in numeric_columns:
            if col in self.related_df.columns:
                self.related_df[col] = pd.to_numeric(self.related_df[col], errors='coerce')
    
    def _analyze_unified_data(self) -> Dict[str, Any]:
        """통합 데이터 분석"""
        analysis = {
            'customer_type': self.metadata.get('customer_type'),
            'related_persons_analysis': {}
        }
        
        if self.related_df is not None and not self.related_df.empty:
            customer_type = self.metadata.get('customer_type')
            
            if customer_type == 'CORP':
                # 법인 분석
                analysis['related_persons_analysis'] = self._analyze_corp_relations()
            else:
                # 개인 분석
                analysis['related_persons_analysis'] = self._analyze_person_relations()
        
        return analysis
    
    def _analyze_corp_relations(self) -> Dict[str, Any]:
        """법인 관련인 분석"""
        result = {}
        
        if '관계유형' in self.related_df.columns:
            result['by_relation'] = self.related_df['관계유형'].value_counts().to_dict()
        
        if '지분율' in self.related_df.columns:
            stake_df = self.related_df[self.related_df['지분율'].notna()]
            if not stake_df.empty:
                result['total_stake'] = stake_df['지분율'].sum()
                result['max_stake_holder'] = {
                    'name': stake_df.loc[stake_df['지분율'].idxmax(), '관련인성명'],
                    'stake': stake_df['지분율'].max()
                }
        
        return result
    
    def _analyze_person_relations(self) -> Dict[str, Any]:
        """개인 관련인 분석"""
        result = {}
        
        # 거래 금액 분석
        if '내부입고금액' in self.related_df.columns:
            total_deposit = self.related_df['내부입고금액'].sum()
            total_withdraw = self.related_df['내부출고금액'].sum()
            total_transactions = self.related_df['거래횟수'].sum()
            
            result['transaction_summary'] = {
                'total_deposit': float(total_deposit),
                'total_withdraw': float(total_withdraw),
                'total_amount': float(total_deposit + total_withdraw),
                'total_transactions': int(total_transactions),
                'unique_partners': len(self.related_df)
            }
            
            # TOP 5 거래 상대방
            top_5 = self.related_df.nlargest(
                5, ['내부입고금액', '내부출고금액']
            )[['관련인성명', '내부입고금액', '내부출고금액', '거래횟수']]
            
            result['top_partners'] = top_5.to_dict('records')
        
        # 위험등급 분포
        if '관련인위험등급' in self.related_df.columns:
            risk_dist = self.related_df['관련인위험등급'].value_counts().to_dict()
            result['risk_distribution'] = risk_dist
        
        return result
    
    def _prepare_export_data(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """Export용 데이터 준비"""
        export_data = {
            'stage': 'stage_2',
            'cust_id': execution_result.get('summary', {}).get('cust_id'),
            'customer_type': execution_result.get('summary', {}).get('customer_type'),
            'metadata': self.metadata,
            'dataframes': {}
        }
        
        # DataFrame을 dict로 변환
        if self.customer_df is not None:
            export_data['dataframes']['customer'] = {
                'columns': self.customer_df.columns.tolist(),
                'rows': self.customer_df.values.tolist()
            }
        
        if self.related_df is not None:
            # NaN을 None으로 변환
            related_export = self.related_df.copy()
            related_export = related_export.where(pd.notnull(related_export), None)
            
            export_data['dataframes']['related_persons'] = {
                'columns': related_export.columns.tolist(),
                'rows': related_export.values.tolist()
            }
        
        if self.duplicate_df is not None:
            export_data['dataframes']['duplicate_persons'] = {
                'columns': self.duplicate_df.columns.tolist(),
                'rows': self.duplicate_df.values.tolist()
            }
        
        return export_data
    

class AlertInfoProcessor:
    """
    ALERT 조회 결과를 처리하고 DataFrame으로 변환하는 클래스
    """
    
    def process(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        AlertInfoExecutor의 실행 결과를 처리 (Rule 히스토리 포함)
        """
        try:
            if not execution_result.get('success'):
                return execution_result
            
            # DataFrame 생성
            self._create_dataframes(execution_result)
            
            # Rule 히스토리 DataFrame 추가
            self._create_rule_history_dataframe(execution_result)
            
            # 데이터 분석
            analysis = self._analyze_data()
            
            # Export용 데이터 준비
            export_data = self._prepare_export_data(execution_result)
            
            return {
                'success': True,
                'dataframes': {
                    'initial': self.initial_df,
                    'monthly': self.monthly_df,
                    'rule_history_exact': self.rule_history_exact_df,
                    'rule_history_similar': self.rule_history_similar_df,
                    'summary': self._create_summary_df(analysis)
                },
                'analysis': analysis,
                'export_data': export_data
            }
            
        except Exception as e:
            logger.exception(f"[Stage 1 Processor] Error in process: {e}")
            return {
                'success': False,
                'message': f"데이터 처리 중 오류: {str(e)}"
            }
    
    def _create_rule_history_dataframe(self, execution_result: Dict[str, Any]):
        """Rule 히스토리 DataFrame 생성"""
        rule_history = execution_result.get('rule_history', {})
        
        # 정확한 매칭 DataFrame
        exact_match = rule_history.get('exact_match', {})
        if exact_match and exact_match.get('occurrence_count', 0) > 0:
            self.rule_history_exact_df = pd.DataFrame([{
                'RULE_COMBO': execution_result['summary']['rule_combo'],
                'OCCURRENCE_COUNT': exact_match.get('occurrence_count', 0),
                'UNIQUE_CUSTOMERS': exact_match.get('unique_customers', 0),
                'FIRST_OCCURRENCE': exact_match.get('first_occurrence'),
                'LAST_OCCURRENCE': exact_match.get('last_occurrence'),
                'STR_REPORTED': exact_match.get('str_reported_count', 0),
                'NOT_REPORTED': exact_match.get('not_reported_count', 0),
                'STR_RATIO': exact_match.get('str_reported_count', 0) / 
                            max(exact_match.get('occurrence_count', 1), 1),
                'UPER_PATTERNS': exact_match.get('uper_patterns'),
                'LWER_PATTERNS': exact_match.get('lwer_patterns')
            }])
        else:
            self.rule_history_exact_df = pd.DataFrame()
        
        # 유사 조합 DataFrame  
        similar_matches = rule_history.get('similar_matches', {})
        if similar_matches.get('rows'):
            self.rule_history_similar_df = pd.DataFrame(
                similar_matches['rows'],
                columns=similar_matches['columns']
            )
        else:
            self.rule_history_similar_df = pd.DataFrame()
        
        logger.info(f"[Stage 1 Processor] Rule history DFs created - "
                   f"Exact: {self.rule_history_exact_df.shape}, "
                   f"Similar: {self.rule_history_similar_df.shape}")