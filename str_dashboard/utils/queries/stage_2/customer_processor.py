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
            logger.debug(f"[Stage 2 Processor] Customer columns: {self.customer_df.columns.tolist()}")
        
        # 관련인 정보
        related_data = execution_result.get('related_persons', {})
        if related_data.get('columns') and related_data.get('rows'):
            self.related_df = pd.DataFrame(
                related_data['rows'],
                columns=related_data['columns']
            )
            logger.info(f"[Stage 2 Processor] Related DF: {self.related_df.shape}")
            logger.info(f"[Stage 2 Processor] Related columns: {self.related_df.columns.tolist()}")  # 컬럼명 로깅
        
        # 중복 의심 회원
        duplicate_data = execution_result.get('duplicate_persons', {})
        if duplicate_data.get('columns') and duplicate_data.get('rows'):
            self.duplicate_df = pd.DataFrame(
                duplicate_data['rows'],
                columns=duplicate_data['columns']
            )
            logger.info(f"[Stage 2 Processor] Duplicate DF: {self.duplicate_df.shape}")
            logger.debug(f"[Stage 2 Processor] Duplicate columns: {self.duplicate_df.columns.tolist()}")
        
        # 메타데이터
        self.metadata = execution_result.get('metadata', {})
    


    # str_dashboard/utils/queries/stage_2/customer_processor.py
    # _analyze_data 메서드 수정 부분

    def _analyze_data(self, summary: Dict[str, Any]) -> Dict[str, Any]:
        """데이터 분석"""
        analysis = {
            'customer_type': summary.get('customer_type', 'UNKNOWN'),
            'related_persons_analysis': {},
            'duplicate_analysis': {}
        }
        
        try:
            # 관련인 분석
            if self.related_df is not None and not self.related_df.empty:
                # 사용 가능한 컬럼 로깅
                logger.debug(f"[Stage 2] Available related_df columns: {self.related_df.columns.tolist()}")
                
                # 법인 관련인 분석
                if '관계유형' in self.related_df.columns:
                    relation_counts = self.related_df['관계유형'].value_counts()
                    analysis['related_persons_analysis']['by_relation'] = relation_counts.to_dict()
                    
                    if '지분율' in self.related_df.columns:
                        # 지분율 분석 (NaN 제거)
                        stake_holders = self.related_df[
                            self.related_df['지분율'].notna()
                        ]['지분율'].sum()
                        analysis['related_persons_analysis']['total_stake'] = float(stake_holders)
                
                # 개인 관련인 분석 (내부거래)
                if '거래횟수' in self.related_df.columns:
                    # 거래횟수 합계
                    valid_trans = self.related_df[self.related_df['거래횟수'].notna()]['거래횟수']
                    if not valid_trans.empty:
                        total_trans = valid_trans.sum()
                        analysis['related_persons_analysis']['total_transactions'] = int(total_trans)
                    
                    # TOP 5 거래 상대방 - 안전한 처리
                    try:
                        # 거래횟수가 유효한 행만 필터링
                        valid_rows = self.related_df[
                            (self.related_df['거래횟수'].notna()) & 
                            (self.related_df['거래횟수'] > 0)
                        ].copy()
                        
                        if not valid_rows.empty:
                            # 내부거래 금액 합계 계산 (선택적)
                            if '내부입고금액' in valid_rows.columns and '내부출고금액' in valid_rows.columns:
                                valid_rows['총거래금액'] = (
                                    valid_rows['내부입고금액'].fillna(0) + 
                                    valid_rows['내부출고금액'].fillna(0)
                                )
                            
                            # 거래횟수 기준으로 정렬
                            top_5_rows = valid_rows.nlargest(5, '거래횟수')
                            
                            # TOP 5 리스트 생성
                            top_5_list = []
                            for idx, row in top_5_rows.iterrows():
                                partner_info = {
                                    '거래횟수': int(row['거래횟수'])
                                }
                                
                                # 이름 찾기 - 통합 DataFrame의 실제 컬럼명 체크
                                name_found = False
                                # _create_unified_dataframe에서 생성되는 실제 컬럼명 사용
                                for name_col in ['관련인성명']:  # 통합 DataFrame에서 사용하는 정확한 컬럼명
                                    if name_col in row.index and pd.notna(row[name_col]):
                                        partner_info['관련인명'] = str(row[name_col])
                                        name_found = True
                                        break
                                
                                if not name_found:
                                    # 이름이 없으면 고객ID 사용
                                    if '관련인고객ID' in row.index:
                                        # 고객ID가 있으면 사용
                                        cust_id_value = row['관련인고객ID']
                                        if pd.notna(cust_id_value):
                                            partner_info['관련인명'] = f"고객ID: {cust_id_value}"
                                        else:
                                            partner_info['관련인명'] = "Unknown"
                                    else:
                                        partner_info['관련인명'] = "Unknown"
                                
                                # 추가 정보 (있는 경우)
                                if '총거래금액' in row.index:
                                    partner_info['총거래금액'] = float(row['총거래금액'])
                                
                                top_5_list.append(partner_info)
                            
                            analysis['related_persons_analysis']['top_5_partners'] = top_5_list
                            logger.info(f"[Stage 2] Top 5 partners: {len(top_5_list)} found")
                        else:
                            logger.info("[Stage 2] No valid transaction data for top 5 partners")
                            
                    except Exception as e:
                        logger.warning(f"[Stage 2] Error calculating top 5 partners: {e}")
                        # 에러가 발생해도 다른 분석은 계속 진행
        
            # 중복 의심 분석
            if self.duplicate_df is not None and not self.duplicate_df.empty:
                if 'MATCH_TYPES' in self.duplicate_df.columns:
                    match_type_counts = {}
                    for match_types in self.duplicate_df['MATCH_TYPES']:
                        if pd.notna(match_types):  # NaN 체크
                            for match_type in str(match_types).split(','):
                                match_type = match_type.strip()
                                if match_type:  # 빈 문자열 제외
                                    match_type_counts[match_type] = match_type_counts.get(match_type, 0) + 1
                    
                    analysis['duplicate_analysis']['match_types'] = match_type_counts
                    analysis['duplicate_analysis']['total_duplicates'] = len(self.duplicate_df)
                    
        except Exception as e:
            logger.error(f"[Stage 2] Error in _analyze_data: {e}", exc_info=True)
            # 에러가 발생해도 부분적인 분석 결과는 반환
        
        return analysis
        


    def _create_summary_df(self, analysis: Dict[str, Any]) -> pd.DataFrame:
        """분석 결과 요약 DataFrame 생성"""
        summary_data = []
        
        try:
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
                
                if 'total_transactions' in related_analysis:
                    summary_data.append({
                        'Category': 'Total Transactions',
                        'Value': related_analysis['total_transactions'],
                        'Description': '총 거래 횟수'
                    })
                
                if 'top_5_partners' in related_analysis and related_analysis['top_5_partners']:
                    for idx, partner in enumerate(related_analysis['top_5_partners'], 1):
                        summary_data.append({
                            'Category': f'Top {idx} Partner',
                            'Value': partner.get('관련인명', 'Unknown'),
                            'Description': f"거래 횟수: {partner.get('거래횟수', 0)}회"
                        })
            
            # 중복 의심
            duplicate_analysis = analysis.get('duplicate_analysis', {})
            if duplicate_analysis:
                summary_data.append({
                    'Category': 'Duplicate Suspects',
                    'Value': duplicate_analysis.get('total_duplicates', 0),
                    'Description': '중복 의심 회원 수'
                })
                
                if 'match_types' in duplicate_analysis:
                    match_types = duplicate_analysis['match_types']
                    if match_types:
                        match_desc = ', '.join([f"{k}({v})" for k, v in match_types.items()])
                        summary_data.append({
                            'Category': 'Match Types',
                            'Value': len(match_types),
                            'Description': match_desc[:100]  # 너무 길면 자르기
                        })
                        
        except Exception as e:
            logger.error(f"[Stage 2] Error creating summary DataFrame: {e}")
        
        return pd.DataFrame(summary_data) if summary_data else pd.DataFrame()
    
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