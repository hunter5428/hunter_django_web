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
        1: '매도',
        2: '매수',
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

    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """데이터 전처리"""
        # 포인트(P) 제거
        df_clean = df[df['ticker_nm'] != 'P'].copy()
        
        # ticker_nm이 비어있는 행 제거
        df_clean = df_clean[df_clean['ticker_nm'].notna()].copy()
        df_clean = df_clean[df_clean['ticker_nm'] != ''].copy()
        
        # trans_from 컬럼에 한글이 포함된 행 제거 (추가)
        if 'trans_from' in df_clean.columns:
            import re
            # 한글 패턴 정의 (가-힣 범위)
            korean_pattern = re.compile('[가-힣]+')
            
            # trans_from이 None이 아니고 한글이 포함된 행 찾기
            mask_has_korean = df_clean['trans_from'].apply(
                lambda x: bool(korean_pattern.search(str(x))) if pd.notna(x) else False
            )
            
            # 한글이 포함된 행 개수 로깅
            korean_rows_count = mask_has_korean.sum()
            if korean_rows_count > 0:
                logger.info(f"Removing {korean_rows_count} rows with Korean characters in trans_from")
                # 디버깅을 위해 몇 개 샘플 로깅
                if logger.isEnabledFor(logging.DEBUG):
                    sample_korean = df_clean[mask_has_korean]['trans_from'].head(5).tolist()
                    logger.debug(f"Sample trans_from values with Korean: {sample_korean}")
            
            # 한글이 없는 행만 유지
            df_clean = df_clean[~mask_has_korean].copy()
        
        # 숫자형 컬럼들을 float64로 변환
        numeric_columns = ['trade_quantity', 'trade_price', 'trade_amount', 
                        'trade_amount_krw', 'balance_market', 'balance_asset']
        
        for col in numeric_columns:
            if col in df_clean.columns:
                # None/NaN 값을 0으로 채우고 float64로 변환
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0).astype(np.float64)
        
        # trans_cat을 정수로 변환
        if 'trans_cat' in df_clean.columns:
            df_clean['trans_cat'] = pd.to_numeric(df_clean['trans_cat'], errors='coerce').fillna(0).astype(int)
        
        # 날짜/시간 컬럼 결합
        df_clean['trade_datetime'] = pd.to_datetime(
            df_clean['trade_date'].astype(str) + ' ' + df_clean['trade_time'].astype(str)
        )
        
        # 인덱스 리셋 (원본 순서 유지)
        df_clean.reset_index(drop=True, inplace=True)
        
        removed_count = len(df) - len(df_clean)
        logger.info(f"Preprocessed orderbook: {len(df)} -> {len(df_clean)} rows (removed {removed_count} rows total)")
        logger.debug(f"Data types after preprocessing: {df_clean.dtypes.to_dict()}")
        
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
            # trans_cat을 정수로 변환 (이미 _preprocess에서 처리됨)
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
                    'ticker_details': {},  # 종목별 상세
                    'total_krw_amount': 0,
                    'count': 0
                }
            else:
                # 기존 구간에 추가
                current_segment['end_idx'] = idx
                current_segment['end_datetime'] = row['trade_datetime']
                current_segment['rows'].append(row)
            
            # 종목별 집계 - ticker_nm만 사용 (market_nm 사용하지 않음)
            ticker = row.get('ticker_nm', None)  # ticker_nm 컬럼만 사용
            
            # ticker가 None이거나 빈 값인 경우 건너뛰기
            if pd.isna(ticker) or ticker is None or ticker == '':
                logger.warning(f"Skipping row {idx} with empty ticker_nm")
                continue
            
            # ticker를 문자열로 변환
            ticker = str(ticker).strip()
            
            if ticker not in current_segment['ticker_details']:
                current_segment['ticker_details'][ticker] = {
                    'quantity': 0,
                    'amount': 0,
                    'amount_krw': 0,
                    'count': 0
                }
            
            # None 값 체크 및 변환
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
        
        # 데이터 타입 확인 및 변환
        if not self.summary_df.empty:
            self.summary_df['총금액(KRW)'] = pd.to_numeric(self.summary_df['총금액(KRW)'], errors='coerce').fillna(0)
        
        logger.info(f"Created {len(segments)} segments from {len(self.df)} rows")
        
        # 행동별 상세 데이터 집계
        self._aggregate_by_action()
        
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
        
        # 상세 내역 텍스트 - trans_cat별로 적절한 처리
        detail_lines = []
        for ticker, detail in sorted(segment['ticker_details'].items()):
            quantity = detail['quantity']
            amount_krw = detail['amount_krw']
            count = detail['count']
            
            if segment['trans_cat'] in [1, 2]:  # 매도/매수
                # 수량과 금액 모두 표시
                if amount_krw > 0:
                    detail_lines.append(
                        f"  - {ticker}: 수량 {quantity:,.4f}개, "
                        f"금액 {int(amount_krw):,}원, "
                        f"횟수 {count:,}건"
                    )
            elif segment['trans_cat'] in [3, 4]:  # 원화입출금
                # 금액만 표시 (수량은 의미 없음)
                if amount_krw > 0:
                    detail_lines.append(
                        f"  - {ticker}: 금액 {int(amount_krw):,}원, "
                        f"횟수 {count:,}건"
                    )
            elif segment['trans_cat'] in [5, 6]:  # 가상자산입출금
                # 수량과 금액 표시 (수량이 0이면 금액만)
                if amount_krw > 0 or quantity > 0:
                    parts = [f"  - {ticker}:"]
                    if quantity > 0:
                        parts.append(f"수량 {quantity:,.4f}개")
                    if amount_krw > 0:
                        parts.append(f"금액 {int(amount_krw):,}원")
                    parts.append(f"횟수 {count:,}건")
                    detail_lines.append(' '.join(parts))
        
        segment['detail_text'] = '\n'.join(detail_lines) if detail_lines else ''
        
        # total_krw_amount를 float로 저장
        segment['total_krw_amount'] = float(segment['total_krw_amount'])
        
        # 불필요한 임시 데이터 제거
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
                
                # trans_cat별로 다른 처리
                if trans_cat in [3, 4]:  # 원화입출금 - 수량 무시
                    quantity = 0  # 원화입출금은 수량이 의미 없음
                else:
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




    def get_pattern_analysis(self) -> Dict:
        """
        거래 패턴 분석
        
        Returns:
            패턴 분석 결과
        """
        if self.summary_df is None:
            self.analyze_segments()
        
        if self.summary_df.empty:
            return {}
        
        patterns = {}
        
        # 각 행동별 금액과 횟수 계산
        for trans_cat, action_name in self.TRANS_CAT_MAP.items():
            if action_name in self.detail_by_action:
                data = self.detail_by_action[action_name]
                if action_name == '매수':
                    patterns['total_buy_amount'] = data['total_amount']
                    patterns['total_buy_count'] = data['total_count']
                    patterns['buy_details'] = data['tickers']
                elif action_name == '매도':
                    patterns['total_sell_amount'] = data['total_amount']
                    patterns['total_sell_count'] = data['total_count']
                    patterns['sell_details'] = data['tickers']
                elif action_name == '원화입금':
                    patterns['total_deposit_krw'] = data['total_amount']
                    patterns['total_deposit_krw_count'] = data['total_count']
                    patterns['deposit_krw_details'] = data['tickers']
                elif action_name == '원화출금':
                    patterns['total_withdraw_krw'] = data['total_amount']
                    patterns['total_withdraw_krw_count'] = data['total_count']
                    patterns['withdraw_krw_details'] = data['tickers']
                elif action_name == '가상자산입금':
                    patterns['total_deposit_crypto'] = data['total_amount']
                    patterns['total_deposit_crypto_count'] = data['total_count']
                    patterns['deposit_crypto_details'] = data['tickers']
                elif action_name == '가상자산출금':
                    patterns['total_withdraw_crypto'] = data['total_amount']
                    patterns['total_withdraw_crypto_count'] = data['total_count']
                    patterns['withdraw_crypto_details'] = data['tickers']
        
        # 기본값 설정
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

    def generate_text_summary(self) -> str:
        """
        분석 결과를 텍스트 형식으로 요약
        
        Returns:
            텍스트 요약 문자열
        """
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
                lines.append(f"  • 총 매수: {patterns['total_buy_amount']:,.0f}원 ({patterns.get('total_buy_count', 0):,}건)")
            if patterns.get('total_sell_amount', 0) > 0:
                lines.append(f"  • 총 매도: {patterns['total_sell_amount']:,.0f}원 ({patterns.get('total_sell_count', 0):,}건)")
            
            # 입출금
            if patterns.get('total_deposit_krw', 0) > 0:
                lines.append(f"  • 원화 입금: {patterns['total_deposit_krw']:,.0f}원 ({patterns.get('total_deposit_krw_count', 0):,}건)")
            if patterns.get('total_withdraw_krw', 0) > 0:
                lines.append(f"  • 원화 출금: {patterns['total_withdraw_krw']:,.0f}원 ({patterns.get('total_withdraw_krw_count', 0):,}건)")
            
            # 가상자산 입출금
            if patterns.get('total_deposit_crypto', 0) > 0:
                lines.append(f"  • 가상자산 입금: {patterns['total_deposit_crypto']:,.0f}원 ({patterns.get('total_deposit_crypto_count', 0):,}건)")
            if patterns.get('total_withdraw_crypto', 0) > 0:
                lines.append(f"  • 가상자산 출금: {patterns['total_withdraw_crypto']:,.0f}원 ({patterns.get('total_withdraw_crypto_count', 0):,}건)")
            
            lines.append("")
        
        # 주요 거래 구간 (금액 기준 상위 5개)
        if not self.summary_df.empty:
            lines.append("【 주요 거래 구간 (금액 기준 TOP 5) 】")
            lines.append("-" * 60)
            
            # 안전한 타입 변환
            self.summary_df['총금액(KRW)'] = pd.to_numeric(self.summary_df['총금액(KRW)'], errors='coerce').fillna(0)
            
            top_segments = self.summary_df.nlargest(5, '총금액(KRW)')
            for idx, row in top_segments.iterrows():
                amount = float(row['총금액(KRW)'])  # 숫자로 변환
                lines.append(f"  [{row['구간']}] {row['행동']} - {row['시작시간']} ~ {row['종료시간']}")
                lines.append(f"      금액: {amount:,.0f}원, 건수: {row['건수']:,}건, 종목: {row['주요종목']}")
            
            lines.append("")
        
        lines.append("=" * 80)
        
        # 텍스트 요약 저장
        self.text_summary = "\n".join(lines)
        return self.text_summary

    def export_to_html_table(self) -> str:
        """
        분석 결과를 HTML 테이블로 변환
        
        Returns:
            HTML 테이블 문자열
        """
        if self.summary_df is None or self.summary_df.empty:
            return "<p>분석할 데이터가 없습니다.</p>"
        
        # pandas DataFrame의 to_html 메서드 활용
        html = self.summary_df.to_html(
            index=False,
            classes='table orderbook-summary-table',
            table_id='orderbook-summary',
            escape=False  # 상세내역의 줄바꿈 등을 유지
        )
        
        # 금액 컬럼 포맷팅 (숫자에 천단위 구분자 추가)
        import re
        
        def format_number(match):
            num = float(match.group(1))
            return f"{num:,.0f}"
        
        # 총금액(KRW) 컬럼의 숫자 포맷팅
        html = re.sub(r'<td>(\d+\.?\d*)</td>(?=.*</tr>)', 
                    lambda m: f"<td>{format_number(m)}</td>" if m.group(1).replace('.', '').isdigit() else m.group(0), 
                    html)
        
        return html