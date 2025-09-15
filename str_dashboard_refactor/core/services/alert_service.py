"""
Alert related business logic service
"""
import logging
from typing import Dict, Any, List, Optional, Tuple
from django.http import HttpRequest

from ..database import OracleConnection, OracleQueryError
from ..utils import SQLQueryManager, SessionManager, DataProcessor

logger = logging.getLogger(__name__)


class AlertService:
    """Alert 관련 비즈니스 로직 서비스"""
    
    def __init__(self, oracle_conn: OracleConnection):
        """
        Args:
            oracle_conn: Oracle 데이터베이스 연결 객체
        """
        self.oracle_conn = oracle_conn
        self.sql_manager = SQLQueryManager()
    
    def get_alert_info(self, alert_id: str) -> Dict[str, Any]:
        """
        Alert ID로 Alert 정보 조회
        
        Args:
            alert_id: Alert ID
            
        Returns:
            Alert 정보 딕셔너리
        """
        if not alert_id:
            raise ValueError("Alert ID is required")
        
        logger.info(f"Fetching alert info for ID: {alert_id}")
        
        try:
            # SQL 로드 및 실행
            prepared_sql, param_count = self.sql_manager.load_and_prepare(
                'alert_info_by_alert_id.sql',
                bind_params={':alert_id': '?'}
            )
            
            if not SQLQueryManager.validate_params(param_count, [alert_id]):
                raise ValueError(f"Parameter count mismatch. Expected {param_count}")
            
            cols, rows = self.oracle_conn.execute_query(prepared_sql, [alert_id])
            
            if not rows:
                logger.warning(f"No data found for alert_id: {alert_id}")
                return {
                    'success': False,
                    'message': f'Alert ID {alert_id}에 대한 데이터가 없습니다.',
                    'columns': [],
                    'rows': []
                }
            
            # Alert 데이터 처리
            processed_data = DataProcessor.process_alert_data(cols, rows, alert_id)
            
            # Rule 객관식 매핑 추가
            from ..queries.rule_objectives import build_rule_to_objectives
            rule_obj_map = build_rule_to_objectives()
            
            result = {
                'success': True,
                'alert_id': alert_id,
                'columns': cols,
                'rows': rows,
                'canonical_ids': processed_data['canonical_ids'],
                'rep_rule_id': processed_data['rep_rule_id'],
                'cust_id': processed_data['cust_id_for_person'],
                'rule_obj_map': rule_obj_map
            }
            
            logger.info(f"Successfully fetched alert info. Rows: {len(rows)}, Rules: {len(processed_data['canonical_ids'])}")
            
            return result
            
        except OracleQueryError as e:
            logger.error(f"Query error while fetching alert info: {e}")
            return {
                'success': False,
                'message': f'조회 실행 실패: {str(e)}'
            }
        except Exception as e:
            logger.exception(f"Unexpected error in get_alert_info: {e}")
            return {
                'success': False,
                'message': f'예상치 못한 오류: {str(e)}'
            }
    
    def get_alert_orderbook_detail(self, alert_id: str, 
                                  start_date: str, 
                                  end_date: str,
                                  orderbook_df: Any) -> Dict[str, Any]:
        """
        특정 Alert의 Orderbook 상세 분석
        
        Args:
            alert_id: Alert ID
            start_date: 시작일
            end_date: 종료일
            orderbook_df: Orderbook DataFrame
            
        Returns:
            분석 결과 딕셔너리
        """
        import pandas as pd
        from ..analyzers import OrderbookAnalyzer
        
        if orderbook_df is None or orderbook_df.empty:
            return {
                'summary': self._empty_summary(),
                'by_ticker': {}
            }
        
        try:
            # 기간 필터링
            df_filtered = orderbook_df[
                (pd.to_datetime(orderbook_df['trade_date']) >= pd.to_datetime(start_date)) &
                (pd.to_datetime(orderbook_df['trade_date']) <= pd.to_datetime(end_date))
            ].copy()
            
            if df_filtered.empty:
                return {
                    'summary': self._empty_summary(),
                    'by_ticker': {}
                }
            
            # 분석 실행
            analyzer = OrderbookAnalyzer(df_filtered)
            analyzer.analyze()
            patterns = analyzer.get_pattern_analysis()
            
            # 종목별 상세 정보 구성
            by_ticker = {
                'buy': self._format_ticker_details(patterns.get('buy_details', [])),
                'sell': self._format_ticker_details(patterns.get('sell_details', [])),
                'deposit': self._format_ticker_details(patterns.get('deposit_crypto_details', [])),
                'withdraw': self._format_ticker_details(patterns.get('withdraw_crypto_details', []))
            }
            
            # 요약 정보
            summary = {
                'buy_amount': patterns.get('total_buy_amount', 0),
                'buy_count': patterns.get('total_buy_count', 0),
                'sell_amount': patterns.get('total_sell_amount', 0),
                'sell_count': patterns.get('total_sell_count', 0),
                'deposit_krw': patterns.get('total_deposit_krw', 0),
                'deposit_krw_count': patterns.get('total_deposit_krw_count', 0),
                'withdraw_krw': patterns.get('total_withdraw_krw', 0),
                'withdraw_krw_count': patterns.get('total_withdraw_krw_count', 0),
                'deposit_crypto': patterns.get('total_deposit_crypto', 0),
                'deposit_crypto_count': patterns.get('total_deposit_crypto_count', 0),
                'withdraw_crypto': patterns.get('total_withdraw_crypto', 0),
                'withdraw_crypto_count': patterns.get('total_withdraw_crypto_count', 0)
            }
            
            return {
                'summary': summary,
                'by_ticker': by_ticker
            }
            
        except Exception as e:
            logger.exception(f"Error analyzing alert orderbook: {e}")
            return {
                'summary': self._empty_summary(),
                'by_ticker': {},
                'error': str(e)
            }
    
    def _empty_summary(self) -> Dict[str, int]:
        """빈 요약 정보 생성"""
        return {
            'buy_amount': 0, 'buy_count': 0,
            'sell_amount': 0, 'sell_count': 0,
            'deposit_krw': 0, 'deposit_krw_count': 0,
            'withdraw_krw': 0, 'withdraw_krw_count': 0,
            'deposit_crypto': 0, 'deposit_crypto_count': 0,
            'withdraw_crypto': 0, 'withdraw_crypto_count': 0
        }
    
    def _format_ticker_details(self, details: List, max_items: int = 10) -> List[Dict]:
        """종목 상세 정보 포맷팅"""
        formatted = []
        for ticker, data in details[:max_items]:
            formatted.append({
                'ticker': ticker,
                'amount': data.get('amount_krw', 0),
                'count': data.get('count', 0)
            })
        return formatted
    
    def save_to_session(self, request: HttpRequest, alert_data: Dict[str, Any]) -> None:
        """
        Alert 데이터를 세션에 저장
        
        Args:
            request: HttpRequest 객체
            alert_data: 저장할 Alert 데이터
        """
        alert_id = alert_data.get('alert_id')
        
        if not alert_id:
            logger.warning("No alert_id in data to save to session")
            return
        
        # 세션에 저장
        SessionManager.save_multiple(request, {
            SessionManager.Keys.CURRENT_ALERT_ID: alert_id,
            SessionManager.Keys.CURRENT_ALERT_DATA: {
                'alert_id': alert_id,
                'cols': alert_data.get('columns', []),
                'rows': alert_data.get('rows', []),
                'canonical_ids': alert_data.get('canonical_ids', []),
                'rep_rule_id': alert_data.get('rep_rule_id'),
                'custIdForPerson': alert_data.get('cust_id'),
                'rule_obj_map': alert_data.get('rule_obj_map', {})
            }
        })
        
        logger.info(f"Alert data saved to session for ID: {alert_id}")