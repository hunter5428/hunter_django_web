# str_dashboard/utils/query_manager.py
"""
통합 쿼리 관리 모듈
Stage 기반 실행을 중앙에서 관리
"""

import logging
from typing import Dict, Any, Optional
from decimal import Decimal

from .db import OracleConnection
from .df_manager import DataFrameManager
from .query_executor import QueryExecutor

logger = logging.getLogger(__name__)


class QueryManager:
    """
    Stage 기반 통합 쿼리 관리 클래스
    """
    
    def __init__(self, oracle_info: Dict[str, Any], redshift_info: Optional[Dict[str, Any]] = None):
        self.oracle_info = oracle_info
        self.redshift_info = redshift_info
        self.df_manager = DataFrameManager()
        self.executor = QueryExecutor()
        
    def execute_all_queries(self, alert_id: str) -> Dict[str, Any]:
        """
        모든 Stage를 순차적으로 실행
        """
        self.df_manager.set_alert_id(alert_id)
        oracle_conn = OracleConnection.from_session(self.oracle_info)
        
        try:
            with oracle_conn.transaction() as db_conn:
                logger.info(f"Starting integrated query for ALERT ID: {alert_id}")
                
                # Stage 1: ALERT 정보 조회
                stage_1_result = self._execute_stage_1(db_conn, alert_id)
                if not stage_1_result['success']:
                    return stage_1_result
                
                # Stage 1에서 고객 ID 추출
                cust_id = stage_1_result['metadata'].get('cust_id')
                if not cust_id:
                    return {
                        'success': False,
                        'message': 'ALERT에 연결된 고객 정보가 없습니다.'
                    }
                
                # Stage 2: 고객 및 관련인 정보 조회
                stage_2_result = self._execute_stage_2(db_conn, cust_id)
                if not stage_2_result['success']:
                    return stage_2_result
                
                # Stage 3: IP/거래 이력 (필요시)
                mid = stage_2_result['metadata'].get('mid')
                customer_type = stage_2_result['metadata'].get('customer_type')
                
                if mid and customer_type == 'PERSON':
                    self._execute_stage_3(db_conn, cust_id, mid)
                
                # Stage 4: Orderbook (Redshift - 옵션)
                if self.redshift_info and mid:
                    self._execute_stage_4(mid)
                
                # 최종 결과 정리
                summary = self.df_manager.get_all_datasets_summary()
                df_manager_data = self._prepare_export_data()
                
                logger.info(f"Integrated query completed for ALERT ID: {alert_id}")
                
                return {
                    'success': True,
                    'summary': summary,
                    'df_manager_data': df_manager_data,
                    'dataset_count': len(summary['datasets']),
                    'alert_id': alert_id
                }
                
        except Exception as e:
            logger.exception(f"Error in execute_all_queries: {e}")
            return {
                'success': False,
                'message': str(e)
            }
    
    def _execute_stage_1(self, db_conn, alert_id: str) -> Dict[str, Any]:
        """Stage 1 실행 및 DataFrame 저장"""
        result = self.executor.execute_stage_1(db_conn, alert_id)
        
        if result['success']:
            # DataFrame Manager에 저장
            self.df_manager.add_dataset(
                'alert_info',
                result['columns'],
                result['rows'],
                **result.get('metadata', {})
            )
            
            # 메타데이터 업데이트
            if 'stage_data' in result:
                self.df_manager.metadata['stage_1'] = result['stage_data']
                self.df_manager.metadata.update(result['metadata'])
        
        return result
    
    def _execute_stage_2(self, db_conn, cust_id: str) -> Dict[str, Any]:
        """Stage 2 실행 및 DataFrame 저장"""
        result = self.executor.execute_stage_2(db_conn, cust_id)
        
        if result['success']:
            # 고객 정보 저장
            self.df_manager.add_dataset(
                'customer_info',
                result['columns'],
                result['rows'],
                **result.get('metadata', {})
            )
            
            # 관련인 정보 저장
            if 'related_persons' in result:
                related = result['related_persons']
                if related.get('columns') and related.get('rows'):
                    self.df_manager.add_dataset(
                        'related_persons',
                        related['columns'],
                        related['rows']
                    )
            
            # 중복 의심 회원 저장
            if 'duplicate_persons' in result:
                duplicate = result['duplicate_persons']
                if duplicate.get('columns') and duplicate.get('rows'):
                    self.df_manager.add_dataset(
                        'duplicate_persons',
                        duplicate['columns'],
                        duplicate['rows']
                    )
            
            # 메타데이터 업데이트
            if 'stage_data' in result:
                self.df_manager.metadata['stage_2'] = result['stage_data']
                self.df_manager.metadata.update(result['metadata'])
        
        return result
    
    def _execute_stage_3(self, db_conn, cust_id: str, mid: str):
        """Stage 3: IP/거래 이력 조회 (향후 구현)"""
        # TODO: Stage 3 구현
        logger.info(f"Stage 3 placeholder for customer {cust_id}, MID {mid}")
    
    def _execute_stage_4(self, mid: str):
        """Stage 4: Orderbook 조회"""
        try:
            # 거래 기간 가져오기
            tran_start = self.df_manager.metadata.get('tran_start')
            tran_end = self.df_manager.metadata.get('tran_end')
            
            if not tran_start or not tran_end:
                logger.warning("No transaction period for Orderbook query")
                return
            
            result = self.executor.execute_orderbook_query(
                self.redshift_info,
                mid,
                tran_start,
                tran_end
            )
            
            if result['success'] and result.get('rows'):
                self.df_manager.add_dataset(
                    'orderbook',
                    result['columns'],
                    result['rows'],
                    user_id=mid,
                    start_date=tran_start,
                    end_date=tran_end
                )
                
        except Exception as e:
            logger.error(f"Stage 4 (Orderbook) failed: {e}")
            # Orderbook은 옵션이므로 실패해도 계속 진행
    
    def _prepare_export_data(self) -> Dict[str, Any]:
        """DataFrame Manager 데이터를 export 형식으로 변환"""
        export_data = self.df_manager.export_to_dict()
        return self._convert_types(export_data)
    
    def _convert_types(self, obj):
        """Decimal 등 특수 타입을 JSON 직렬화 가능한 형태로 변환"""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_types(item) for item in obj]
        return obj