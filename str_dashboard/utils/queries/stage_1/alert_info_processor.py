# str_dashboard/utils/queries/stage_1/alert_info_processor.py
"""
ALERT 정보 처리 모듈
"""

import logging
import pandas as pd
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)


class AlertInfoProcessor:
    """
    Stage 1: ALERT 조회 결과를 처리하고 DataFrame으로 변환하는 클래스
    """
    
    def __init__(self):
        self.initial_df = None
        self.monthly_df = None
        self.rule_history_exact_df = None
        self.rule_history_similar_df = None
        self.metadata = {}
        
    def process(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        AlertInfoExecutor의 실행 결과를 처리
        
        Args:
            execution_result: AlertInfoExecutor.execute()의 반환값
            
        Returns:
            처리된 결과
        """
        try:
            if not execution_result.get('success'):
                return execution_result
            
            # DataFrame 생성
            self._create_dataframes(execution_result)
            
            # Rule 히스토리 DataFrame 생성
            self._create_rule_history_dataframes(execution_result)
            
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
            logger.exception(f"[Stage 1 Processor] Error: {e}")
            return {
                'success': False,
                'message': f"데이터 처리 중 오류: {str(e)}"
            }
    
    def _create_dataframes(self, execution_result: Dict[str, Any]):
        """쿼리 결과를 DataFrame으로 변환"""
        
        # 초기 정보 DataFrame
        initial_data = execution_result.get('initial_info', {})
        if initial_data.get('columns') and initial_data.get('rows'):
            self.initial_df = pd.DataFrame(
                initial_data['rows'],
                columns=initial_data['columns']
            )
            logger.info(f"[Stage 1 Processor] Initial DF: {self.initial_df.shape}")
        
        # 월별 데이터 DataFrame
        monthly_data = execution_result.get('monthly_data', {})
        if monthly_data.get('columns') and monthly_data.get('rows'):
            self.monthly_df = pd.DataFrame(
                monthly_data['rows'],
                columns=monthly_data['columns']
            )
            logger.info(f"[Stage 1 Processor] Monthly DF: {self.monthly_df.shape}")
        
        # 메타데이터
        self.metadata = execution_result.get('metadata', {})
    
    def _create_rule_history_dataframes(self, execution_result: Dict[str, Any]):
        """Rule 히스토리 DataFrame 생성"""
        rule_history = execution_result.get('rule_history', {})
        
        # 정확한 매칭 DataFrame
        exact_match = rule_history.get('exact_match', {})
        if exact_match and exact_match.get('occurrence_count', 0) > 0:
            self.rule_history_exact_df = pd.DataFrame([{
                'RULE_COMBO': execution_result.get('summary', {}).get('rule_combo', ''),
                'OCCURRENCE_COUNT': exact_match.get('occurrence_count', 0),
                'UNIQUE_CUSTOMERS': exact_match.get('unique_customers', 0),
                'FIRST_OCCURRENCE': exact_match.get('first_occurrence'),
                'LAST_OCCURRENCE': exact_match.get('last_occurrence'),
                'STR_REPORTED': exact_match.get('str_reported_count', 0),
                'NOT_REPORTED': exact_match.get('not_reported_count', 0),
                'STR_RATIO': (
                    exact_match.get('str_reported_count', 0) / 
                    max(exact_match.get('occurrence_count', 1), 1)
                )
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
        
        logger.info(
            f"[Stage 1 Processor] Rule history DFs - "
            f"Exact: {self.rule_history_exact_df.shape}, "
            f"Similar: {self.rule_history_similar_df.shape}"
        )
    
    def _analyze_data(self) -> Dict[str, Any]:
        """데이터 분석"""
        analysis = {}
        
        # 월별 데이터 분석
        if self.monthly_df is not None and not self.monthly_df.empty:
            # Rule ID별 집계
            if 'STR_RULE_ID' in self.monthly_df.columns:
                rule_counts = self.monthly_df['STR_RULE_ID'].value_counts()
                analysis['rule_distribution'] = rule_counts.to_dict()
            
            # 날짜 범위
            if 'TRAN_STRT' in self.monthly_df.columns:
                analysis['date_range'] = {
                    'start': self.monthly_df['TRAN_STRT'].min(),
                    'end': self.monthly_df['TRAN_END'].max() if 'TRAN_END' in self.monthly_df.columns else None
                }
            
            # 타겟 Alert 정보
            if 'IS_TARGET_ALERT' in self.monthly_df.columns:
                target_rows = self.monthly_df[self.monthly_df['IS_TARGET_ALERT'] == 'Y']
                analysis['target_alert_count'] = len(target_rows)
        
        # Rule 히스토리 분석
        if self.rule_history_exact_df is not None and not self.rule_history_exact_df.empty:
            analysis['rule_history'] = {
                'has_history': True,
                'occurrence_count': self.rule_history_exact_df.iloc[0]['OCCURRENCE_COUNT'],
                'str_ratio': self.rule_history_exact_df.iloc[0]['STR_RATIO']
            }
        else:
            analysis['rule_history'] = {'has_history': False}
        
        return analysis
    
    def _create_summary_df(self, analysis: Dict[str, Any]) -> pd.DataFrame:
        """분석 결과 요약 DataFrame 생성"""
        summary_data = []
        
        # Rule 분포
        if 'rule_distribution' in analysis:
            for rule_id, count in analysis['rule_distribution'].items():
                summary_data.append({
                    'Category': 'Rule Distribution',
                    'Value': rule_id,
                    'Count': count,
                    'Description': f'Rule {rule_id} 발생 횟수'
                })
        
        # Rule 히스토리
        if analysis.get('rule_history', {}).get('has_history'):
            history = analysis['rule_history']
            summary_data.append({
                'Category': 'Rule History',
                'Value': f"{history['occurrence_count']} occurrences",
                'Count': history['occurrence_count'],
                'Description': f"STR 비율: {history['str_ratio']:.2%}"
            })
        
        # 날짜 범위
        if 'date_range' in analysis:
            date_range = analysis['date_range']
            summary_data.append({
                'Category': 'Date Range',
                'Value': f"{date_range['start']} ~ {date_range['end']}",
                'Count': 0,
                'Description': '거래 기간'
            })
        
        return pd.DataFrame(summary_data)
    
    def _prepare_export_data(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """세션 저장 및 export를 위한 데이터 준비"""
        export_data = {
            'stage': 'stage_1',
            'alert_id': execution_result.get('summary', {}).get('alert_id'),
            'metadata': self.metadata,
            'dataframes': {}
        }
        
        # DataFrame을 dict로 변환
        if self.initial_df is not None:
            export_data['dataframes']['initial'] = {
                'columns': self.initial_df.columns.tolist(),
                'rows': self.initial_df.values.tolist()
            }
        
        if self.monthly_df is not None:
            export_data['dataframes']['monthly'] = {
                'columns': self.monthly_df.columns.tolist(),
                'rows': self.monthly_df.values.tolist()
            }
        
        if self.rule_history_exact_df is not None and not self.rule_history_exact_df.empty:
            export_data['dataframes']['rule_history_exact'] = {
                'columns': self.rule_history_exact_df.columns.tolist(),
                'rows': self.rule_history_exact_df.values.tolist()
            }
        
        if self.rule_history_similar_df is not None and not self.rule_history_similar_df.empty:
            export_data['dataframes']['rule_history_similar'] = {
                'columns': self.rule_history_similar_df.columns.tolist(),
                'rows': self.rule_history_similar_df.values.tolist()
            }
        
        return export_data