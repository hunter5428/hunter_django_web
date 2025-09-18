# str_dashboard/utils/queries/stage_4/orderbook_processor.py
"""
Orderbook 데이터 처리 모듈
"""

import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)


class OrderbookProcessor:
    """
    Stage 4: Orderbook 데이터 처리 클래스
    """
    
    def __init__(self):
        self.orderbook_df = None
        self.metadata = {}
        
    def process(self, execution_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        OrderbookExecutor의 실행 결과를 처리
        """
        try:
            if not execution_result.get('success'):
                return execution_result
            
            # DataFrame 생성
            self._create_dataframe(execution_result)
            
            # 데이터 분석
            analysis = self._analyze_orderbook()
            
            # Export용 데이터 준비
            export_data = self._prepare_export_data(execution_result, analysis)
            
            return {
                'success': True,
                'dataframes': {
                    'orderbook': self.orderbook_df
                },
                'analysis': analysis,
                'export_data': export_data
            }
            
        except Exception as e:
            logger.exception(f"[Stage 4 Processor] Error: {e}")
            return {
                'success': False,
                'message': str(e)
            }
    
    def _create_dataframe(self, execution_result: Dict[str, Any]):
        """DataFrame 생성"""
        orderbook_data = execution_result.get('orderbook_data', {})
        
        if orderbook_data.get('columns') and orderbook_data.get('rows'):
            self.orderbook_df = pd.DataFrame(
                orderbook_data['rows'],
                columns=orderbook_data['columns']
            )
            
            # trade_date를 datetime으로 변환
            if 'trade_date' in self.orderbook_df.columns:
                self.orderbook_df['trade_date'] = pd.to_datetime(
                    self.orderbook_df['trade_date'],
                    errors='coerce'
                )
            
            logger.info(f"[Stage 4 Processor] Orderbook DF: {self.orderbook_df.shape}")
        else:
            self.orderbook_df = pd.DataFrame()
        
        self.metadata = execution_result.get('metadata', {})
    
    def _analyze_orderbook(self) -> Dict[str, Any]:
        """Orderbook 데이터 분석"""
        if self.orderbook_df is None or self.orderbook_df.empty:
            return {'has_data': False}
        
        analysis = {
            'has_data': True,
            'total_transactions': len(self.orderbook_df),
            'unique_users': self.orderbook_df['user_id'].nunique(),
            'unique_markets': self.orderbook_df['market_nm'].nunique(),
            'unique_tickers': self.orderbook_df['ticker_nm'].nunique(),
            'date_range': {
                'start': self.orderbook_df['trade_date'].min(),
                'end': self.orderbook_df['trade_date'].max()
            }
        }
        
        # 사용자별 집계
        user_summary = self.orderbook_df.groupby('user_id').agg({
            'trade_amount_krw': ['sum', 'mean', 'count'],
            'ticker_nm': 'nunique'
        }).to_dict()
        
        analysis['user_summary'] = user_summary
        
        # 종목별 거래 요약
        ticker_summary = self.orderbook_df.groupby('ticker_nm')['trade_amount_krw'].sum().nlargest(10).to_dict()
        analysis['top_10_tickers'] = ticker_summary
        
        return analysis
    
    def _prepare_export_data(self, execution_result: Dict[str, Any],
                           analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Export용 데이터 준비"""
        export_data = {
            'stage': 'stage_4',
            'metadata': self.metadata,
            'analysis': analysis,
            'dataframes': {}
        }
        
        if self.orderbook_df is not None and not self.orderbook_df.empty:
            df_export = self.orderbook_df.copy()
            
            # datetime을 문자열로 변환
            if 'trade_date' in df_export.columns:
                df_export['trade_date'] = df_export['trade_date'].dt.strftime('%Y-%m-%d')
            
            export_data['dataframes']['orderbook'] = {
                'columns': df_export.columns.tolist(),
                'rows': df_export.values.tolist()
            }
        
        return export_data