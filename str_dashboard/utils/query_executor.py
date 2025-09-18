# str_dashboard/utils/query_executor.py
"""
Stage 기반 쿼리 실행 모듈
각 Stage별 Executor와 Processor를 호출하여 처리
"""

import logging
from typing import Dict, Any, Optional
from pathlib import Path
from django.conf import settings

from .db import RedshiftConnection
from .queries.stage_1 import AlertInfoExecutor, AlertInfoProcessor
from .queries.stage_2 import CustomerExecutor, CustomerProcessor

logger = logging.getLogger(__name__)


class QueryExecutor:
    """Stage 기반 쿼리 실행 클래스"""
    
    def __init__(self):
        self.stage_results = {}
        
    def execute_stage_1(self, db_conn, alert_id: str) -> Dict[str, Any]:
        """
        Stage 1: ALERT 정보 조회
        """
        try:
            # Stage 1 Executor 실행
            executor = AlertInfoExecutor(db_conn)
            execution_result = executor.execute(alert_id)
            
            if not execution_result['success']:
                return execution_result
            
            # Stage 1 Processor 처리
            processor = AlertInfoProcessor()
            processed_result = processor.process(execution_result)
            
            if not processed_result['success']:
                return processed_result
            
            # Stage 결과 저장
            self.stage_results['stage_1'] = processed_result['export_data']
            
            # 기존 형식으로 변환 (호환성 유지)
            monthly_data = processed_result['export_data']['dataframes'].get('monthly', {})
            
            return {
                'success': True,
                'columns': monthly_data.get('columns', []),
                'rows': monthly_data.get('rows', []),
                'metadata': processed_result['export_data']['metadata'],
                'stage_data': processed_result['export_data']
            }
            
        except Exception as e:
            logger.exception(f"Error in Stage 1: {e}")
            return {
                'success': False,
                'message': f"ALERT 정보 조회 실패: {str(e)}"
            }
    
    def execute_stage_2(self, db_conn, cust_id: str) -> Dict[str, Any]:
        """
        Stage 2: 고객 및 관련인 정보 조회
        """
        try:
            # Stage 1 메타데이터 가져오기
            stage_1_metadata = self.stage_results.get('stage_1', {}).get('metadata', {})
            
            # Stage 2 Executor 실행
            executor = CustomerExecutor(db_conn)
            execution_result = executor.execute(cust_id, stage_1_metadata)
            
            if not execution_result['success']:
                return execution_result
            
            # Stage 2 Processor 처리
            processor = CustomerProcessor()
            processed_result = processor.process(execution_result)
            
            if not processed_result['success']:
                return processed_result
            
            # Stage 결과 저장
            self.stage_results['stage_2'] = processed_result['export_data']
            
            # 호환성을 위한 형식 변환
            customer_data = processed_result['export_data']['dataframes'].get('customer', {})
            
            return {
                'success': True,
                'columns': customer_data.get('columns', []),
                'rows': customer_data.get('rows', []),
                'metadata': processed_result['export_data']['metadata'],
                'stage_data': processed_result['export_data'],
                'related_persons': processed_result['export_data']['dataframes'].get('related_persons', {}),
                'duplicate_persons': processed_result['export_data']['dataframes'].get('duplicate_persons', {})
            }
            
        except Exception as e:
            logger.exception(f"Error in Stage 2: {e}")
            return {
                'success': False,
                'message': f"고객 정보 조회 실패: {str(e)}"
            }
    
    def execute_orderbook_query(self, redshift_info: Dict, mem_id: str, 
                               start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Redshift Orderbook 조회 (Stage 4에서 사용 예정)
        """
        try:
            redshift_conn = RedshiftConnection.from_session(redshift_info)
            
            # SQL 파일 경로 (추후 stage_4로 이동 예정)
            sql_path = Path(settings.BASE_DIR) / 'str_dashboard' / 'queries' / 'redshift_orderbook.sql'
            
            if not sql_path.exists():
                logger.warning("Orderbook SQL file not found, using empty result")
                return {'success': True, 'columns': [], 'rows': []}
            
            sql_query = sql_path.read_text('utf-8')
            
            with redshift_conn.transaction() as rs_conn:
                with rs_conn.cursor() as cursor:
                    cursor.execute(sql_query, (start_date, end_date, mem_id))
                    
                    if not cursor.description:
                        return {'success': True, 'columns': [], 'rows': []}
                    
                    cols = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    
                    return {'success': True, 'columns': cols, 'rows': rows}
                    
        except Exception as e:
            logger.error(f"Orderbook query failed: {e}")
            # Redshift 실패는 크리티컬하지 않으므로 빈 결과 반환
            return {'success': True, 'columns': [], 'rows': []}
    
    def get_stage_results(self) -> Dict[str, Any]:
        """모든 Stage 결과 반환"""
        return self.stage_results