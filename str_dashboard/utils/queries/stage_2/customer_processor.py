# str_dashboard/utils/queries/stage_2/customer_processor.py
"""
고객 및 관련인 정보 처리 모듈
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Optional
from datetime import datetime

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
        
        Args:
            execution_result: CustomerExecutor.execute()의 반환값
            
        Returns:
            처리된 결과
        """
        try:
            if not execution_result.get('success'):
                return execution_result
            
            # DataFrame 생성
            self._create_dataframes(execution_result)
            
            # 데이터 분석
            analysis = self._analyze_data(execution_result.get('summary', {}))
            
            # Export용 데이터 준비
            export_data = self._prepare_export_data(execution_result)
            
            return {
                'success': True,
                'dataframes': {
                    'customer': self.customer_df,
                    'related_persons': self.related_df,
                    'duplicate_persons': self.duplicate_df,
                    'summary': self._create_summary_df(analysis)
                },
                'analysis': analysis,
                'export_data': export_data
            }
            
        except Exception as e:
            logger.exception(f"[Stage 2 Processor] Error in process: {e}")
            return {
                'success': False,
                'message': f"데이터 처리 중 오류: {str(e)}"
            }
    
    def _create_dataframes(self, execution_result: Dict[str, Any]):
        """쿼리 결과를 DataFrame으로 변환"""
        
        # 고객 정보
        customer_data = execution_result.get('customer_info', {})
        if customer_data.get('columns') and customer_data.get('rows'):
            self.customer_df = pd.DataFrame(
                customer_data['rows'],
                columns=customer_data['columns']
            )
            logger.info(f"[Stage 2 Processor] Customer DF: {self.customer_df.shape}")
        
        # 관련인 정보
        related_data = execution_result.get('related_persons', {})
        if related_data.get('columns') and related_data.get('rows'):
            self.related_df = pd.DataFrame(
                related_data['rows'],
                columns=related_data['columns']
            )
            logger.info(f"[Stage 2 Processor] Related DF: {self.related_df.shape}")
        
        # 중복 의심 회원
        duplicate_data = execution_result.get('duplicate_persons', {})
        if duplicate_data.get('columns') and duplicate_data.get('rows'):
            self.duplicate_df = pd.DataFrame(
                duplicate_data['rows'],
                columns=duplicate_data['columns']
            )
            logger.info(f"[Stage 2 Processor] Duplicate DF: {self.duplicate_df.shape}")
        
        # 메타데이터
        self.metadata = execution_result.get('metadata', {})
    
    def _analyze_data(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 분석"""
        analysis = {
            'customer_type': summary.get('customer_type', 'UNKNOWN'),
            'related_persons_analysis': {},
            'duplicate_analysis': {}
        }
        
        # 관련인 분석
        if self.related_df is not None and not self.related_df.empty:
            if '관계' in self.related_df.columns:
                # 법인 관련인
                relation_counts = self.related_df['관계'].value_counts()
                analysis['related_persons_analysis']['by_relation'] = relation_counts.to_dict()
                
                if '관계인지분율' in self.related_df.columns:
                    # 지분율 분석
                    stake_holders = self.related_df[
                        self.related_df['관계인지분율'].notna()
                    ]['관계인지분율'].sum()
                    analysis['related_persons_analysis']['total_stake'] = float(stake_holders)
            
            elif '거래횟수' in self.related_df.columns:
                # 개인 관련인 (거래 상대방)
                total_trans = self.related_df['거래횟수'].sum()
                analysis['related_persons_analysis']['total_transactions'] = int(total_trans)
                
                # TOP 5 거래 상대방
                top_5 = self.related_df.nlargest(5, '거래횟수')[
                    ['관련인명', '거래횟수']
                ].to_dict('records')
                analysis['related_persons_analysis']['top_5_partners'] = top_5
        
        # 중복 의심 분석
        if self.duplicate_df is not None and not self.duplicate_df.empty:
            if 'MATCH_TYPES' in self.duplicate_df.columns:
                match_type_counts = {}
                for match_types in self.duplicate_df['MATCH_TYPES']:
                    for match_type in str(match_types).split(','):
                        match_type = match_type.strip()
                        match_type_counts[match_type] = match_type_counts.get(match_type, 0) + 1
                
                analysis['duplicate_analysis']['match_types'] = match_type_counts
                analysis['duplicate_analysis']['total_duplicates'] = len(self.duplicate_df)
        
        return analysis
    
    def _create_summary_df(self, analysis: Dict[str, Any]) -> pd.DataFrame:
        """분석 결과 요약 DataFrame 생성"""
        summary_data = []
        
        # 고객 타입
        summary_data.append({
            'Category': 'Customer Type',
            'Value': analysis.get('customer_type'),
            'Description': '고객 구분 (개인/법인)'
        })
        
        # 관련인 정보
        related_analysis = analysis.get('related_persons_analysis', {})
        if related_analysis:
            if 'by_relation' in related_analysis:
                for relation, count in related_analysis['by_relation'].items():
                    summary_data.append({
                        'Category': f'Related Person - {relation}',
                        'Value': count,
                        'Description': f'{relation} 관계 인원 수'
                    })
            
            if 'total_stake' in related_analysis:
                summary_data.append({
                    'Category': 'Total Stake',
                    'Value': f"{related_analysis['total_stake']:.2f}%",
                    'Description': '총 지분율'
                })
        
        # 중복 의심
        duplicate_analysis = analysis.get('duplicate_analysis', {})
        if duplicate_analysis:
            summary_data.append({
                'Category': 'Duplicate Suspects',
                'Value': duplicate_analysis.get('total_duplicates', 0),
                'Description': '중복 의심 회원 수'
            })
        
        return pd.DataFrame(summary_data)
    
    def _prepare_export_data(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """세션 저장 및 export를 위한 데이터 준비"""
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
            export_data['dataframes']['related_persons'] = {
                'columns': self.related_df.columns.tolist(),
                'rows': self.related_df.values.tolist()
            }
        
        if self.duplicate_df is not None:
            export_data['dataframes']['duplicate_persons'] = {
                'columns': self.duplicate_df.columns.tolist(),
                'rows': self.duplicate_df.values.tolist()
            }
        
        return export_data
    
    def get_mid(self) -> Optional[str]:
        """MID 반환 (다음 단계에서 사용)"""
        if self.customer_df is not None and not self.customer_df.empty:
            if 'MID' in self.customer_df.columns:
                return str(self.customer_df.iloc[0]['MID'])
        return None
    
    def get_customer_type(self) -> str:
        """고객 타입 반환"""
        return self.metadata.get('customer_type', 'UNKNOWN')