# str_dashboard/utils/query_manager.py
"""
쿼리 실행 및 데이터 처리를 관리하는 모듈
각 쿼리별 로직을 분리하여 관리
"""

import os
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from django.conf import settings
import pandas as pd
from decimal import Decimal
from datetime import timedelta
import re

from .db import OracleConnection, RedshiftConnection
from .df_manager import DataFrameManager
from .ledger_manager import OrderbookAnalyzer
from .query_executor import QueryExecutor

logger = logging.getLogger(__name__)


class QueryManager:
    """
    통합 쿼리 관리 클래스
    모든 쿼리 실행 로직을 중앙에서 관리
    """
    
    def __init__(self, oracle_info: Dict[str, Any], redshift_info: Optional[Dict[str, Any]] = None):
        self.oracle_info = oracle_info
        self.redshift_info = redshift_info
        self.df_manager = DataFrameManager()
        self.executor = QueryExecutor()
        
    def execute_all_queries(self, alert_id: str) -> Dict[str, Any]:
        """
        ALERT ID에 대한 모든 쿼리 실행
        
        Returns:
            실행 결과 딕셔너리
        """
        self.df_manager.set_alert_id(alert_id)
        oracle_conn = OracleConnection.from_session(self.oracle_info)
        
        try:
            with oracle_conn.transaction() as db_conn:
                logger.info(f"Starting integrated query for ALERT ID: {alert_id}")
                
                # 1단계: ALERT 정보 조회
                alert_metadata = self._query_alert_info(db_conn, alert_id)
                if not alert_metadata:
                    return {'success': False, 'message': 'ALERT 정보 조회 실패'}
                
                if not alert_metadata.get('cust_id'):
                    return {'success': False, 'message': 'ALERT에 연결된 고객 정보가 없습니다.'}
                
                # 2단계: 고객 정보 조회
                customer_metadata = self._query_customer_info(db_conn, alert_metadata['cust_id'])
                if not customer_metadata:
                    return {'success': False, 'message': '고객 정보 조회 실패'}
                
                # 3단계: 추가 쿼리 실행
                self._execute_additional_queries(db_conn, alert_metadata, customer_metadata)
                
                # 4단계: 결과 정리
                summary = self.df_manager.get_all_datasets_summary()
                df_manager_data = self._prepare_export_data()
                
                logger.info(f"Integrated query completed for ALERT ID: {alert_id}")
                
                return {
                    'success': True,
                    'summary': summary,
                    'df_manager_data': df_manager_data,
                    'dataset_count': len(summary['datasets'])
                }
                
        except Exception as e:
            logger.exception(f"Error in execute_all_queries: {e}")
            return {'success': False, 'message': str(e)}
    
    def _query_alert_info(self, db_conn, alert_id: str) -> Optional[Dict[str, Any]]:
        """ALERT 정보 조회"""
        result = self.executor.execute_alert_info_query(db_conn, alert_id)
        if not result.get('success'):
            logger.error(f"Alert info query failed: {result.get('message')}")
            return None
        
        self.df_manager.add_dataset('alert_info', result['columns'], result['rows'])
        return self.df_manager.process_alert_data(result)
    
    def _query_customer_info(self, db_conn, cust_id: str) -> Optional[Dict[str, Any]]:
        """고객 정보 조회"""
        result = self.executor.execute_customer_info_query(db_conn, cust_id)
        if not result.get('success'):
            logger.error(f"Customer info query failed: {result.get('message')}")
            return None
        
        self.df_manager.add_dataset('customer_info', result['columns'], result['rows'])
        return self.df_manager.process_customer_data(result)
    
    def _execute_additional_queries(self, db_conn, alert_metadata: Dict, customer_metadata: Dict):
        """추가 쿼리들 실행"""
        cust_id = alert_metadata.get('cust_id')
        customer_type = customer_metadata.get('customer_type')
        
        # Rule 히스토리
        self._query_rule_history(alert_metadata)
        
        # 법인/개인별 분기 처리
        if customer_type == '법인':
            self._query_corp_related(db_conn, cust_id)
        else:  # 개인
            self._query_person_related(db_conn, cust_id, customer_metadata)
        
        # 중복 회원 조회
        self._query_duplicate_persons(db_conn, cust_id)
    
    def _query_rule_history(self, alert_metadata: Dict):
        """Rule 히스토리 조회"""
        if not alert_metadata.get('canonical_ids'):
            return
        
        try:
            # Rule 히스토리 조회 로직
            from ..queries.rule_historic_search import fetch_df_result_0, aggregate_by_rule_id_list
            
            df0 = fetch_df_result_0(
                jdbc_url=self.oracle_info['jdbc_url'],
                driver_class=self.oracle_info.get('driver_class', 'oracle.jdbc.driver.OracleDriver'),
                driver_path=self.oracle_info.get('driver_path', r'C:\ojdbc11-21.5.0.0.jar'),
                username=self.oracle_info['username'],
                password=self.oracle_info['password']
            )
            df1 = aggregate_by_rule_id_list(df0)
            rule_key = ','.join(sorted(alert_metadata['canonical_ids']))
            matching_rows = df1[df1["STR_RULE_ID_LIST"] == rule_key]
            
            self.df_manager.add_dataset('rule_history', list(matching_rows.columns), matching_rows.values.tolist())
        except Exception as e:
            logger.error(f"Rule history query failed: {e}")
    
    def _query_corp_related(self, db_conn, cust_id: str):
        """법인 관련인 조회"""
        result = self.executor.execute_corp_related_query(db_conn, cust_id)
        if result.get('success'):
            self.df_manager.add_dataset('corp_related', result['columns'], result['rows'])
    
    def _query_person_related(self, db_conn, cust_id: str, customer_metadata: Dict):
        """개인 관련 정보 조회"""
        tran_start, tran_end = self.df_manager.calculate_transaction_period()
        if not tran_start or not tran_end:
            logger.warning("Transaction period not found, skipping person related queries")
            return
        
        # 개인 관련인
        result = self.executor.execute_person_related_query(db_conn, cust_id, tran_start, tran_end)
        if result.get('success'):
            self.df_manager.add_dataset('person_related', result['columns'], result['rows'])
        
        # IP 이력 및 Orderbook 조회
        mid = customer_metadata.get('mid')
        if mid:
            # IP 이력
            result = self.executor.execute_ip_history_query(db_conn, mid, tran_start, tran_end)
            if result.get('success'):
                self.df_manager.add_dataset('ip_history', result['columns'], result['rows'])
            
            # Redshift Orderbook
            if self.redshift_info:
                self._query_orderbook(mid, tran_start, tran_end)
    
    def _query_orderbook(self, mem_id: str, start_date: str, end_date: str):
        """Redshift Orderbook 조회"""
        try:
            result = self.executor.execute_orderbook_query(
                self.redshift_info, mem_id, start_date, end_date
            )
            if result.get('success'):
                self.df_manager.add_dataset('orderbook', result['columns'], result['rows'],
                                          user_id=mem_id,
                                          start_date=start_date,
                                          end_date=end_date)
                
                # 분석 실행
                df = self.df_manager.get_dataframe('orderbook')
                if df is not None and not df.empty:
                    analyzer = OrderbookAnalyzer(df)
                    analyzer.analyze()
                    analysis_result = {
                        'text_summary': analyzer.generate_text_summary(),
                        'patterns': analyzer.get_pattern_analysis(),
                        'daily_summary': analyzer.get_daily_summary().to_dict('records')
                    }
                    self.df_manager.add_dataset('orderbook_analysis',
                                              ['analysis_type', 'data'],
                                              [['summary', analysis_result]])
        except Exception as e:
            logger.error(f"Orderbook query failed: {e}")
    
    def _query_duplicate_persons(self, db_conn, cust_id: str):
        """중복 회원 조회"""
        customer_df = self.df_manager.get_dataframe('customer_info')
        if customer_df is None or customer_df.empty:
            return
        
        dup_params = self._extract_duplicate_params(customer_df.iloc[0])
        if not dup_params:
            return
        
        result = self.executor.execute_duplicate_query(db_conn, cust_id, dup_params)
        if result.get('success'):
            self.df_manager.add_dataset('duplicate_persons', result['columns'], result['rows'])
    
    def _extract_duplicate_params(self, customer_row: pd.Series) -> Dict[str, Any]:
        """고객 정보에서 중복 검색 파라미터 추출"""
        phone = str(customer_row.get('연락처', ''))
        return {
            'address': str(customer_row.get('거주지주소', '')),
            'detail_address': str(customer_row.get('거주지상세주소', '')),
            'workplace_name': str(customer_row.get('직장명', '')),
            'workplace_address': str(customer_row.get('직장주소', '')),
            'workplace_detail_address': str(customer_row.get('직장상세주소', '')),
            'phone_suffix': phone[-4:] if len(phone) >= 4 else '',
        }
    
    def _prepare_export_data(self) -> Dict[str, Any]:
        """
        DataFrame Manager 데이터를 JSON 직렬화 가능한 형태로 변환
        Decimal 타입 처리 포함
        """
        export_data = self.df_manager.export_to_dict()
        return self._convert_decimals_to_float(export_data)
    
    def _convert_decimals_to_float(self, obj):
        """Decimal 타입을 float로 변환 (재귀적)"""
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, dict):
            return {k: self._convert_decimals_to_float(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals_to_float(item) for item in obj]
        return obj