# str_dashboard/utils/ledger_manager/orderbook_analyzer.py
"""
Orderbook 데이터 분석 모듈
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger(__name__)


class OrderbookAnalyzer:
    """Orderbook DataFrame 분석 클래스"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.patterns = {}
        self.daily_summary = pd.DataFrame()
        
    def analyze(self):
        """전체 분석 실행"""
        self._prepare_data()
        self._analyze_patterns()
        self._create_daily_summary()
        
    def _prepare_data(self):
        """데이터 전처리"""
        # 날짜 컬럼 변환
        if 'trade_date' in self.df.columns:
            self.df['trade_date'] = pd.to_datetime(self.df['trade_date'])
            self.df['trade_day'] = self.df['trade_date'].dt.date
        
        # 숫자 컬럼 변환
        numeric_cols = ['trade_quantity', 'trade_price', 'trade_amount', 'trade_amount_krw']
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce').fillna(0)
    
    def _analyze_patterns(self):
        """거래 패턴 분석"""
        patterns = {}
        
        # 매수/매도 분석
        if 'trans_cat' in self.df.columns:
            buy_df = self.df[self.df['trans_cat'] == 'BUY']
            sell_df = self.df[self.df['trans_cat'] == 'SELL']
            
            patterns['total_buy_amount'] = buy_df['trade_amount_krw'].sum()
            patterns['total_buy_count'] = len(buy_df)
            patterns['total_sell_amount'] = sell_df['trade_amount_krw'].sum()
            patterns['total_sell_count'] = len(sell_df)
            
            # 종목별 매수 TOP
            if not buy_df.empty:
                buy_by_ticker = buy_df.groupby('ticker_nm').agg({
                    'trade_amount_krw': 'sum',
                    'trade_quantity': 'sum',
                    'trans_cat': 'count'
                }).rename(columns={'trans_cat': 'count'})
                buy_by_ticker = buy_by_ticker.sort_values('trade_amount_krw', ascending=False)
                
                patterns['buy_details'] = [
                    (ticker, {
                        'amount_krw': row['trade_amount_krw'],
                        'quantity': row['trade_quantity'],
                        'count': row['count']
                    })
                    for ticker, row in buy_by_ticker.head(10).iterrows()
                ]
            
            # 종목별 매도 TOP
            if not sell_df.empty:
                sell_by_ticker = sell_df.groupby('ticker_nm').agg({
                    'trade_amount_krw': 'sum',
                    'trade_quantity': 'sum',
                    'trans_cat': 'count'
                }).rename(columns={'trans_cat': 'count'})
                sell_by_ticker = sell_by_ticker.sort_values('trade_amount_krw', ascending=False)
                
                patterns['sell_details'] = [
                    (ticker, {
                        'amount_krw': row['trade_amount_krw'],
                        'quantity': row['trade_quantity'],
                        'count': row['count']
                    })
                    for ticker, row in sell_by_ticker.head(10).iterrows()
                ]
        
        # 입출금 분석
        if 'trans_cat' in self.df.columns:
            # KRW 입출금
            deposit_krw = self.df[self.df['trans_cat'] == 'DEPOSIT_KRW']
            withdraw_krw = self.df[self.df['trans_cat'] == 'WITHDRAW_KRW']
            
            patterns['total_deposit_krw'] = deposit_krw['trade_amount_krw'].sum()
            patterns['total_deposit_krw_count'] = len(deposit_krw)
            patterns['total_withdraw_krw'] = withdraw_krw['trade_amount_krw'].sum()
            patterns['total_withdraw_krw_count'] = len(withdraw_krw)
            
            # 가상자산 입출고
            deposit_crypto = self.df[self.df['trans_cat'] == 'DEPOSIT_CRYPTO']
            withdraw_crypto = self.df[self.df['trans_cat'] == 'WITHDRAW_CRYPTO']
            
            patterns['total_deposit_crypto'] = deposit_crypto['trade_amount_krw'].sum()
            patterns['total_deposit_crypto_count'] = len(deposit_crypto)
            patterns['total_withdraw_crypto'] = withdraw_crypto['trade_amount_krw'].sum()
            patterns['total_withdraw_crypto_count'] = len(withdraw_crypto)
            
            # 종목별 입출고 상세
            if not deposit_crypto.empty:
                deposit_by_ticker = deposit_crypto.groupby('ticker_nm').agg({
                    'trade_amount_krw': 'sum',
                    'trade_quantity': 'sum',
                    'trans_cat': 'count'
                }).rename(columns={'trans_cat': 'count'})
                deposit_by_ticker = deposit_by_ticker.sort_values('trade_amount_krw', ascending=False)
                
                patterns['deposit_crypto_details'] = [
                    (ticker, {
                        'amount_krw': row['trade_amount_krw'],
                        'quantity': row['trade_quantity'],
                        'count': row['count']
                    })
                    for ticker, row in deposit_by_ticker.head(10).iterrows()
                ]
            
            if not withdraw_crypto.empty:
                withdraw_by_ticker = withdraw_crypto.groupby('ticker_nm').agg({
                    'trade_amount_krw': 'sum',
                    'trade_quantity': 'sum',
                    'trans_cat': 'count'
                }).rename(columns={'trans_cat': 'count'})
                withdraw_by_ticker = withdraw_by_ticker.sort_values('trade_amount_krw', ascending=False)
                
                patterns['withdraw_crypto_details'] = [
                    (ticker, {
                        'amount_krw': row['trade_amount_krw'],
                        'quantity': row['trade_quantity'],
                        'count': row['count']
                    })
                    for ticker, row in withdraw_by_ticker.head(10).iterrows()
                ]
        
        self.patterns = patterns
    
    def _create_daily_summary(self):
        """일별 요약 생성"""
        if 'trade_day' not in self.df.columns:
            return
        
        daily = self.df.groupby(['trade_day', 'trans_cat']).agg({
            'trade_amount_krw': 'sum',
            'trade_quantity': 'count'
        }).reset_index()
        
        daily = daily.pivot_table(
            index='trade_day',
            columns='trans_cat',
            values='trade_amount_krw',
            aggfunc='sum',
            fill_value=0
        ).reset_index()
        
        self.daily_summary = daily
    
    def get_pattern_analysis(self) -> Dict[str, Any]:
        """패턴 분석 결과 반환"""
        return self.patterns
    
    def get_daily_summary(self) -> pd.DataFrame:
        """일별 요약 반환"""
        return self.daily_summary
    
    def generate_text_summary(self) -> str:
        """텍스트 요약 생성"""
        lines = []
        lines.append("=" * 80)
        lines.append("【 Orderbook 거래 분석 요약 】")
        lines.append("=" * 80)
        
        # 기간 정보
        if 'trade_date' in self.df.columns:
            min_date = self.df['trade_date'].min()
            max_date = self.df['trade_date'].max()
            lines.append(f"• 분석 기간: {min_date.strftime('%Y-%m-%d')} ~ {max_date.strftime('%Y-%m-%d')}")
        
        lines.append(f"• 총 거래 건수: {len(self.df):,}건")
        
        # 매수/매도 요약
        if self.patterns.get('total_buy_amount'):
            lines.append(f"\n▶ 매수: {self.patterns['total_buy_amount']:,.0f}원 ({self.patterns.get('total_buy_count', 0):,}건)")
            if 'buy_details' in self.patterns:
                lines.append("  [주요 매수 종목]")
                for ticker, data in self.patterns['buy_details'][:5]:
                    lines.append(f"    - {ticker}: {data['amount_krw']:,.0f}원 ({data['count']}건)")
        
        if self.patterns.get('total_sell_amount'):
            lines.append(f"\n▶ 매도: {self.patterns['total_sell_amount']:,.0f}원 ({self.patterns.get('total_sell_count', 0):,}건)")
            if 'sell_details' in self.patterns:
                lines.append("  [주요 매도 종목]")
                for ticker, data in self.patterns['sell_details'][:5]:
                    lines.append(f"    - {ticker}: {data['amount_krw']:,.0f}원 ({data['count']}건)")
        
        # 입출금 요약
        if self.patterns.get('total_deposit_krw'):
            lines.append(f"\n▶ KRW 입금: {self.patterns['total_deposit_krw']:,.0f}원 ({self.patterns.get('total_deposit_krw_count', 0):,}건)")
        
        if self.patterns.get('total_withdraw_krw'):
            lines.append(f"▶ KRW 출금: {self.patterns['total_withdraw_krw']:,.0f}원 ({self.patterns.get('total_withdraw_krw_count', 0):,}건)")
        
        if self.patterns.get('total_deposit_crypto'):
            lines.append(f"\n▶ 가상자산 입고: {self.patterns['total_deposit_crypto']:,.0f}원 ({self.patterns.get('total_deposit_crypto_count', 0):,}건)")
            if 'deposit_crypto_details' in self.patterns:
                lines.append("  [주요 입고 종목]")
                for ticker, data in self.patterns['deposit_crypto_details'][:3]:
                    lines.append(f"    - {ticker}: {data['amount_krw']:,.0f}원 ({data['count']}건)")
        
        if self.patterns.get('total_withdraw_crypto'):
            lines.append(f"\n▶ 가상자산 출고: {self.patterns['total_withdraw_crypto']:,.0f}원 ({self.patterns.get('total_withdraw_crypto_count', 0):,}건)")
            if 'withdraw_crypto_details' in self.patterns:
                lines.append("  [주요 출고 종목]")
                for ticker, data in self.patterns['withdraw_crypto_details'][:3]:
                    lines.append(f"    - {ticker}: {data['amount_krw']:,.0f}원 ({data['count']}건)")
        
        lines.append("\n" + "=" * 80)
        return "\n".join(lines)