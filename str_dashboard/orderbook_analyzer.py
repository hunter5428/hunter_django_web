# str_dashboard/orderbook_analyzer.py
"""
Orderbook(거래원장) 분석 및 요약 모듈
거래원장을 구간별로 요약하여 매매패턴을 분석
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import logging

logger = logging.getLogger(__name__)


class OrderbookAnalyzer:
    """거래원장 분석 클래스"""
    
    # trans_cat 매핑
    TRANS_CAT_MAP = {
        1: '매수',
        2: '매도',
        3: '원화입금',
        4: '원화출금',
        5: '가상자산입금',
        6: '가상자산출금'
    }
    
    def __init__(self, df: pd.DataFrame):
        """
        Args:
            df: Orderbook DataFrame
        """
        self.df_original = df.copy()
        self.df = self._preprocess(df)
        self.summary_df = None
        self.text_summary = None
        self.detail_by_action = {}  # 행동별 상세 데이터 저장
        self.daily_summary = None  # 일자별 요약 추가

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터 전처리"""
        # 포인트(P) 제거
        df_clean = df[df['ticker_nm'] != 'P'].copy()
        
        # ticker_nm이 비어있는 행 제거
        df_clean = df_clean[df_clean['ticker_nm'].notna()].copy()
        df_clean = df_clean[df_clean['ticker_nm'] != ''].copy()
        
        # trans_from 컬럼에 한글이 포함된 행 제거
        if 'trans_from' in df_clean.columns:
            import re
            korean_pattern = re.compile('[가-힣]+')
            
            mask_has_korean = df_clean['trans_from'].apply(
                lambda x: bool(korean_pattern.search(str(x))) if pd.notna(x) else False
            )
            
            korean_rows_count = mask_has_korean.sum()
            if korean_rows_count > 0:
                logger.info(f"Removing {korean_rows_count} rows with Korean characters in trans_from")
            
            df_clean = df_clean[~mask_has_korean].copy()
        
        # 숫자형 컬럼들을 float64로 변환
        numeric_columns = ['trade_quantity', 'trade_price', 'trade_amount', 
                        'trade_amount_krw', 'balance_market', 'balance_asset']
        
        for col in numeric_columns:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0).astype(np.float64)
        
        # trans_cat을 정수로 변환
        if 'trans_cat' in df_clean.columns:
            df_clean['trans_cat'] = pd.to_numeric(df_clean['trans_cat'], errors='coerce').fillna(0).astype(int)
        
        # 날짜/시간 컬럼 결합
        df_clean['trade_datetime'] = pd.to_datetime(
            df_clean['trade_date'].astype(str) + ' ' + df_clean['trade_time'].astype(str)
        )
        
        # 인덱스 리셋
        df_clean.reset_index(drop=True, inplace=True)
        
        removed_count = len(df) - len(df_clean)
        logger.info(f"Preprocessed orderbook: {len(df)} -> {len(df_clean)} rows (removed {removed_count} rows total)")
        
        return df_clean

    def analyze_segments(self) -> pd.DataFrame:
        """
        연속된 동일 trans_cat을 구간으로 묶어서 분석
        
        Returns:
            구간별 요약 DataFrame
        """
        if self.df.empty:
            return pd.DataFrame()
        
        segments = []
        current_segment = None
        segment_counter = 0
        
        for idx, row in self.df.iterrows():
            trans_cat = row['trans_cat']
            
            # 새로운 구간 시작
            if current_segment is None or current_segment['trans_cat'] != trans_cat:
                # 이전 구간 저장
                if current_segment is not None:
                    current_segment['segment_num'] = segment_counter
                    segments.append(self._finalize_segment(current_segment))
                    segment_counter += 1
                
                # 새 구간 초기화
                current_segment = {
                    'trans_cat': trans_cat,
                    'trans_cat_name': self.TRANS_CAT_MAP.get(trans_cat, f'Unknown({trans_cat})'),
                    'start_idx': idx,
                    'end_idx': idx,
                    'start_datetime': row['trade_datetime'],
                    'end_datetime': row['trade_datetime'],
                    'rows': [row],
                    'ticker_details': {},
                    'total_krw_amount': 0,
                    'count': 0
                }
            else:
                # 기존 구간에 추가
                current_segment['end_idx'] = idx
                current_segment['end_datetime'] = row['trade_datetime']
                current_segment['rows'].append(row)
            
            # 종목별 집계
            ticker = row.get('ticker_nm', None)
            
            if pd.isna(ticker) or ticker is None or ticker == '':
                logger.warning(f"Skipping row {idx} with empty ticker_nm")
                continue
            
            ticker = str(ticker).strip()
            
            if ticker not in current_segment['ticker_details']:
                current_segment['ticker_details'][ticker] = {
                    'quantity': 0,
                    'amount': 0,
                    'amount_krw': 0,
                    'count': 0
                }
            
            # 수량 처리 수정: 가상자산 입출금(5,6)은 trade_amount 사용
            if trans_cat in [5, 6]:  # 가상자산 입출금
                quantity = row['trade_amount'] if pd.notna(row['trade_amount']) else 0
            elif trans_cat in [3, 4]:  # 원화입출금
                quantity = 0
            else:  # 매수(1), 매도(2)
                quantity = row['trade_quantity'] if pd.notna(row['trade_quantity']) else 0
            
            amount = row['trade_amount'] if pd.notna(row['trade_amount']) else 0
            amount_krw = row['trade_amount_krw'] if pd.notna(row['trade_amount_krw']) else 0
            
            current_segment['ticker_details'][ticker]['quantity'] += quantity
            current_segment['ticker_details'][ticker]['amount'] += amount
            current_segment['ticker_details'][ticker]['amount_krw'] += amount_krw
            current_segment['ticker_details'][ticker]['count'] += 1
            
            current_segment['total_krw_amount'] += amount_krw
            current_segment['count'] += 1
        
        # 마지막 구간 저장
        if current_segment is not None:
            current_segment['segment_num'] = segment_counter
            segments.append(self._finalize_segment(current_segment))
        
        # DataFrame 생성
        summary_data = []
        for idx, seg in enumerate(segments, 1):
            summary_data.append({
                '구간': idx,
                'trans_cat': seg['trans_cat'],
                '행동': seg['trans_cat_name'],
                '시작시간': seg['start_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                '종료시간': seg['end_datetime'].strftime('%Y-%m-%d %H:%M:%S'),
                '소요시간': seg['duration'],
                '건수': seg['count'],
                '종목수': seg['ticker_count'],
                '주요종목': seg['main_tickers'],
                '총금액(KRW)': float(seg['total_krw_amount']),
                '상세내역': seg['detail_text']
            })
        
        self.summary_df = pd.DataFrame(summary_data)
        
        if not self.summary_df.empty:
            self.summary_df['총금액(KRW)'] = pd.to_numeric(self.summary_df['총금액(KRW)'], errors='coerce').fillna(0)
        
        logger.info(f"Created {len(segments)} segments from {len(self.df)} rows")
        
        # 행동별 상세 데이터 집계
        self._aggregate_by_action()
        
        # 일자별 요약 추가
        self._aggregate_by_date()
        
        return self.summary_df

    def _finalize_segment(self, segment: Dict) -> Dict:
        """구간 정보 최종 정리"""
        # 소요시간 계산
        duration = segment['end_datetime'] - segment['start_datetime']
        hours = duration.total_seconds() // 3600
        minutes = (duration.total_seconds() % 3600) // 60
        seconds = duration.total_seconds() % 60
        
        if hours > 0:
            segment['duration'] = f"{int(hours)}시간 {int(minutes)}분"
        elif minutes > 0:
            segment['duration'] = f"{int(minutes)}분 {int(seconds)}초"
        else:
            segment['duration'] = f"{int(seconds)}초"
        
        # 종목 정보 정리
        segment['ticker_count'] = len(segment['ticker_details'])
        
        # 주요 종목 (금액 기준 상위 3개)
        sorted_tickers = sorted(
            segment['ticker_details'].items(),
            key=lambda x: abs(x[1]['amount_krw']),
            reverse=True
        )[:3]
        
        segment['main_tickers'] = ', '.join([ticker for ticker, _ in sorted_tickers])
        
        # 상세 내역 텍스트
        detail_lines = []
        for ticker, detail in sorted(segment['ticker_details'].items(),
                                    key=lambda x: abs(x[1]['amount_krw']), reverse=True):
            quantity = detail['quantity']
            amount_krw = detail['amount_krw']
            count = detail['count']
            
            if segment['trans_cat'] in [1, 2]:  # 매수/매도
                if amount_krw > 0:
                    detail_lines.append(
                        f"  - {ticker}: 수량 {quantity:,.4f}개, "
                        f"금액 {int(amount_krw):,}원, "
                        f"횟수 {count:,}건"
                    )
            elif segment['trans_cat'] in [3, 4]:  # 원화입출금
                if amount_krw > 0:
                    detail_lines.append(
                        f"  - {ticker}: 금액 {int(amount_krw):,}원, "
                        f"횟수 {count:,}건"
                    )
            elif segment['trans_cat'] in [5, 6]:  # 가상자산입출금
                if amount_krw > 0 or quantity > 0:
                    parts = [f"  - {ticker}:"]
                    if quantity > 0:
                        parts.append(f"수량 {quantity:,.4f}개")
                    if amount_krw > 0:
                        parts.append(f"금액 {int(amount_krw):,}원")
                    parts.append(f"횟수 {count:,}건")
                    detail_lines.append(' '.join(parts))
        
        segment['detail_text'] = '\n'.join(detail_lines) if detail_lines else ''
        segment['total_krw_amount'] = float(segment['total_krw_amount'])
        segment.pop('rows', None)
        
        return segment

    def _aggregate_by_action(self):
        """행동별로 종목 상세 데이터 집계"""
        self.detail_by_action = {}
        
        if self.df.empty:
            return
        
        # 각 trans_cat별로 집계
        for trans_cat in [1, 2, 3, 4, 5, 6]:
            action_name = self.TRANS_CAT_MAP.get(trans_cat, f'Unknown({trans_cat})')
            action_data = self.df[self.df['trans_cat'] == trans_cat]
            
            if action_data.empty:
                continue
            
            # 종목별 집계
            ticker_summary = {}
            for _, row in action_data.iterrows():
                ticker = row.get('ticker_nm', None)
                if pd.isna(ticker) or ticker is None or ticker == '':
                    continue
                
                if ticker not in ticker_summary:
                    ticker_summary[ticker] = {
                        'quantity': 0,
                        'amount_krw': 0,
                        'count': 0
                    }
                
                # 수량 처리 수정: 가상자산 입출금(5,6)은 trade_amount 사용
                if trans_cat in [5, 6]:  # 가상자산 입출금
                    quantity = row['trade_amount'] if pd.notna(row['trade_amount']) else 0
                elif trans_cat in [3, 4]:  # 원화입출금 - 수량 무시
                    quantity = 0
                else:  # 매수(1), 매도(2)
                    quantity = row['trade_quantity'] if pd.notna(row['trade_quantity']) else 0
                
                amount_krw = row['trade_amount_krw'] if pd.notna(row['trade_amount_krw']) else 0
                
                ticker_summary[ticker]['quantity'] += quantity
                ticker_summary[ticker]['amount_krw'] += amount_krw
                ticker_summary[ticker]['count'] += 1
            
            # 금액 기준 정렬
            sorted_tickers = sorted(
                ticker_summary.items(),
                key=lambda x: abs(x[1]['amount_krw']),
                reverse=True
            )
            
            self.detail_by_action[action_name] = {
                'total_amount': sum(d['amount_krw'] for d in ticker_summary.values()),
                'total_count': sum(d['count'] for d in ticker_summary.values()),
                'tickers': sorted_tickers
            }

    def _aggregate_by_date(self):
        """일자별 매수/매도, 입출금 현황 집계"""
        if self.df.empty:
            self.daily_summary = pd.DataFrame()
            return
        
        # 날짜 컬럼 생성
        self.df['date'] = pd.to_datetime(self.df['trade_date']).dt.date
        
        daily_data = []
        
        for date in sorted(self.df['date'].unique()):
            day_df = self.df[self.df['date'] == date]
            
            day_summary = {
                '날짜': date.strftime('%Y-%m-%d'),
                '매수': self._get_daily_action_summary(day_df, 1),
                '매도': self._get_daily_action_summary(day_df, 2),
                '원화입금': self._get_daily_action_summary(day_df, 3),
                '원화출금': self._get_daily_action_summary(day_df, 4),
                '가상자산내부입금': self._get_daily_crypto_summary(day_df, 5, is_internal=True),
                '가상자산내부출금': self._get_daily_crypto_summary(day_df, 6, is_internal=True),
                '가상자산외부입금': self._get_daily_crypto_summary(day_df, 5, is_internal=False),
                '가상자산외부출금': self._get_daily_crypto_summary(day_df, 6, is_internal=False),
            }
            
            daily_data.append(day_summary)
        
        self.daily_summary = pd.DataFrame(daily_data)

    def _get_daily_action_summary(self, day_df: pd.DataFrame, trans_cat: int) -> Dict:
        """일자별 특정 행동의 요약"""
        action_df = day_df[day_df['trans_cat'] == trans_cat]
        
        if action_df.empty:
            return {'total_amount': 0, 'details': []}
        
        # 종목별 집계
        ticker_summary = {}
        for _, row in action_df.iterrows():
            ticker = row.get('ticker_nm', 'Unknown')
            if pd.isna(ticker) or ticker == '':
                ticker = 'Unknown'
            
            if ticker not in ticker_summary:
                ticker_summary[ticker] = {
                    'quantity': 0,
                    'amount_krw': 0,
                    'count': 0
                }
            
            # 수량 처리 수정
            if trans_cat in [5, 6]:  # 가상자산 입출금
                quantity = row['trade_amount'] if pd.notna(row['trade_amount']) else 0
            elif trans_cat in [3, 4]:  # 원화입출금
                quantity = 0
            else:  # 매수, 매도
                quantity = row['trade_quantity'] if pd.notna(row['trade_quantity']) else 0
            
            amount_krw = row['trade_amount_krw'] if pd.notna(row['trade_amount_krw']) else 0
            
            ticker_summary[ticker]['quantity'] += quantity
            ticker_summary[ticker]['amount_krw'] += amount_krw
            ticker_summary[ticker]['count'] += 1
        
        # 금액 기준 정렬
        sorted_details = sorted(
            ticker_summary.items(),
            key=lambda x: abs(x[1]['amount_krw']),
            reverse=True
        )
        
        # 정수형으로 변환
        total = int(sum(d['amount_krw'] for _, d in sorted_details))
        
        return {
            'total_amount': total,
            'details': sorted_details
        }

    def _get_daily_crypto_summary(self, day_df: pd.DataFrame, trans_cat: int, is_internal: bool) -> Dict:
        """일자별 가상자산 입출금 요약 (내부/외부 구분)"""
        action_df = day_df[day_df['trans_cat'] == trans_cat]
        
        if action_df.empty:
            return {'total_amount': 0, 'details': []}
        
        # 내부/외부 구분
        if is_internal:
            # trans_from과 trans_to가 모두 'A'로 시작하고 'A'로 끝나는 경우
            mask = (
                action_df['trans_from'].fillna('').str.match(r'^A.*A$', na=False) &
                action_df['trans_to'].fillna('').str.match(r'^A.*A$', na=False)
            )
        else:
            # 그 외의 경우
            mask = ~(
                action_df['trans_from'].fillna('').str.match(r'^A.*A$', na=False) &
                action_df['trans_to'].fillna('').str.match(r'^A.*A$', na=False)
            )
        
        filtered_df = action_df[mask]
        
        if filtered_df.empty:
            return {'total_amount': 0, 'details': []}
        
        # 종목별, 시간별 집계
        details = []
        for _, row in filtered_df.iterrows():
            ticker = row.get('ticker_nm', 'Unknown')
            datetime_str = row['trade_datetime'].strftime('%Y-%m-%d %H:%M:%S')
            # 가상자산 입출금은 trade_amount 사용
            quantity = row['trade_amount'] if pd.notna(row['trade_amount']) else 0
            amount_krw = row['trade_amount_krw'] if pd.notna(row['trade_amount_krw']) else 0
            
            details.append({
                'datetime': datetime_str,
                'ticker': ticker,
                'quantity': quantity,
                'amount_krw': amount_krw,
                'count': 1
            })
        
        # 금액 기준 정렬
        details.sort(key=lambda x: abs(x['amount_krw']), reverse=True)
        
        # 정수형으로 변환
        total = int(sum(d['amount_krw'] for d in details))
        
        return {
            'total_amount': total,
            'details': details
        }

    def get_pattern_analysis(self) -> Dict:
        """거래 패턴 분석"""
        if self.summary_df is None:
            self.analyze_segments()
        
        if self.summary_df.empty:
            return {}
        
        patterns = {}
        
        # 각 행동별 금액과 횟수 계산 - 정수형으로 변환
        for trans_cat, action_name in self.TRANS_CAT_MAP.items():
            if action_name in self.detail_by_action:
                data = self.detail_by_action[action_name]
                if action_name == '매수':
                    patterns['total_buy_amount'] = int(data['total_amount'])
                    patterns['total_buy_count'] = int(data['total_count'])
                    patterns['buy_details'] = data['tickers']
                elif action_name == '매도':
                    patterns['total_sell_amount'] = int(data['total_amount'])
                    patterns['total_sell_count'] = int(data['total_count'])
                    patterns['sell_details'] = data['tickers']
                elif action_name == '원화입금':
                    patterns['total_deposit_krw'] = int(data['total_amount'])
                    patterns['total_deposit_krw_count'] = int(data['total_count'])
                    patterns['deposit_krw_details'] = data['tickers']
                elif action_name == '원화출금':
                    patterns['total_withdraw_krw'] = int(data['total_amount'])
                    patterns['total_withdraw_krw_count'] = int(data['total_count'])
                    patterns['withdraw_krw_details'] = data['tickers']
                elif action_name == '가상자산입금':
                    patterns['total_deposit_crypto'] = int(data['total_amount'])
                    patterns['total_deposit_crypto_count'] = int(data['total_count'])
                    patterns['deposit_crypto_details'] = data['tickers']
                elif action_name == '가상자산출금':
                    patterns['total_withdraw_crypto'] = int(data['total_amount'])
                    patterns['total_withdraw_crypto_count'] = int(data['total_count'])
                    patterns['withdraw_crypto_details'] = data['tickers']
        
        # 기본값 설정 (정수형)
        patterns.setdefault('total_buy_amount', 0)
        patterns.setdefault('total_buy_count', 0)
        patterns.setdefault('total_sell_amount', 0)
        patterns.setdefault('total_sell_count', 0)
        patterns.setdefault('total_deposit_krw', 0)
        patterns.setdefault('total_deposit_krw_count', 0)
        patterns.setdefault('total_withdraw_krw', 0)
        patterns.setdefault('total_withdraw_krw_count', 0)
        patterns.setdefault('total_deposit_crypto', 0)
        patterns.setdefault('total_deposit_crypto_count', 0)
        patterns.setdefault('total_withdraw_crypto', 0)
        patterns.setdefault('total_withdraw_crypto_count', 0)
        
        return patterns

    def get_daily_summary(self) -> pd.DataFrame:
        """일자별 요약 반환"""
        if self.daily_summary is None:
            self._aggregate_by_date()
        return self.daily_summary

    def generate_text_summary(self) -> str:
        """분석 결과를 텍스트 형식으로 요약"""
        if self.summary_df is None or self.summary_df.empty:
            return "분석할 데이터가 없습니다."
        
        lines = []
        lines.append("=" * 80)
        lines.append("【 거래원장(Orderbook) 분석 요약 】")
        lines.append("=" * 80)
        lines.append("")
        
        # 전체 거래 기간
        if not self.df.empty:
            start_time = self.df['trade_datetime'].min()
            end_time = self.df['trade_datetime'].max()
            lines.append(f"◆ 분석 기간: {start_time} ~ {end_time}")
            lines.append(f"◆ 전체 거래 건수: {len(self.df):,}건")
            lines.append(f"◆ 거래 구간 수: {len(self.summary_df)}개")
            lines.append("")
        
        # 패턴 분석 결과
        patterns = self.get_pattern_analysis()
        if patterns:
            lines.append("【 거래 패턴 요약 】")
            lines.append("-" * 60)
            
            # 매수/매도
            if patterns.get('total_buy_amount', 0) > 0:
                lines.append(f"  • 총 매수: {patterns['total_buy_amount']:,}원 ({patterns.get('total_buy_count', 0):,}건)")
            if patterns.get('total_sell_amount', 0) > 0:
                lines.append(f"  • 총 매도: {patterns['total_sell_amount']:,}원 ({patterns.get('total_sell_count', 0):,}건)")
            
            # 입출금
            if patterns.get('total_deposit_krw', 0) > 0:
                lines.append(f"  • 원화 입금: {patterns['total_deposit_krw']:,}원 ({patterns.get('total_deposit_krw_count', 0):,}건)")
            if patterns.get('total_withdraw_krw', 0) > 0:
                lines.append(f"  • 원화 출금: {patterns['total_withdraw_krw']:,}원 ({patterns.get('total_withdraw_krw_count', 0):,}건)")
            
            # 가상자산 입출금
            if patterns.get('total_deposit_crypto', 0) > 0:
                lines.append(f"  • 가상자산 입금: {patterns['total_deposit_crypto']:,}원 ({patterns.get('total_deposit_crypto_count', 0):,}건)")
            if patterns.get('total_withdraw_crypto', 0) > 0:
                lines.append(f"  • 가상자산 출금: {patterns['total_withdraw_crypto']:,}원 ({patterns.get('total_withdraw_crypto_count', 0):,}건)")
            
            lines.append("")
        
        # 주요 거래 구간 (금액 기준 상위 5개)
        if not self.summary_df.empty:
            lines.append("【 주요 거래 구간 (금액 기준 TOP 5) 】")
            lines.append("-" * 60)
            
            self.summary_df['총금액(KRW)'] = pd.to_numeric(self.summary_df['총금액(KRW)'], errors='coerce').fillna(0)
            
            top_segments = self.summary_df.nlargest(5, '총금액(KRW)')
            for idx, row in top_segments.iterrows():
                amount = float(row['총금액(KRW)'])
                lines.append(f"  [{row['구간']}] {row['행동']} - {row['시작시간']} ~ {row['종료시간']}")
                lines.append(f"      금액: {amount:,.0f}원, 건수: {row['건수']:,}건, 종목: {row['주요종목']}")
            
            lines.append("")
        
        lines.append("=" * 80)
        
        self.text_summary = "\n".join(lines)
        return self.text_summary

    def export_to_html_table(self) -> str:
        """분석 결과를 HTML 테이블로 변환"""
        if self.summary_df is None or self.summary_df.empty:
            return "<p>분석할 데이터가 없습니다.</p>"
        
        html = self.summary_df.to_html(
            index=False,
            classes='table orderbook-summary-table',
            table_id='orderbook-summary',
            escape=False
        )
        
        import re
        
        def format_number(match):
            num = float(match.group(1))
            return f"{num:,.0f}"
        
        html = re.sub(r'<td>(\d+\.?\d*)</td>(?=.*</tr>)', 
                    lambda m: f"<td>{format_number(m)}</td>" if m.group(1).replace('.', '').isdigit() else m.group(0), 
                    html)
        
        return html