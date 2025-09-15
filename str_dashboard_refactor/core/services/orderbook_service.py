"""
Orderbook analysis service
"""
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

from django.conf import settings
from ..database import RedshiftConnection
from ..analyzers import OrderbookAnalyzer
from ..utils import DataProcessor

logger = logging.getLogger(__name__)


class OrderbookService:
    """Orderbook 분석 서비스"""
    
    # 메모리 캐시 (클래스 변수)
    _cache: Dict[str, Dict[str, Any]] = {}
    
    def __init__(self, redshift_conn: Optional[RedshiftConnection] = None):
        """
        Args:
            redshift_conn: Redshift 연결 객체
        """
        self.redshift_conn = redshift_conn
    
    def query_orderbook(self, user_id: str, 
                        tran_start: str, 
                        tran_end: str) -> Dict[str, Any]:
        """
        Redshift에서 Orderbook 데이터 조회
        
        Args:
            user_id: 사용자 ID
            tran_start: 거래 시작일 (YYYY-MM-DD)
            tran_end: 거래 종료일 (YYYY-MM-DD)
            
        Returns:
            조회 결과 및 캐시 정보
        """
        logger.info(f"Querying orderbook for user: {user_id}, period: {tran_start} ~ {tran_end}")
        
        if not self.redshift_conn:
            raise ValueError("Redshift connection is required")
        
        try:
            # 날짜 변환 (+1일 처리)
            start_date = datetime.strptime(tran_start, '%Y-%m-%d')
            end_date = datetime.strptime(tran_end, '%Y-%m-%d')
            
            start_date_plus1 = start_date + timedelta(days=1)
            end_date_plus1 = end_date + timedelta(days=1)
            
            start_time = start_date_plus1.strftime('%Y-%m-%d 00:00:00')
            end_time = end_date_plus1.strftime('%Y-%m-%d 23:59:59')
            
            # SQL 로드
            sql_path = Path(settings.BASE_DIR) / 'str_dashboard' / 'queries' / 'redshift_orderbook.sql'
            with open(sql_path, 'r', encoding='utf-8') as f:
                sql_query = f.read()
            
            # 쿼리 실행
            cols, rows = self.redshift_conn.execute_query(
                sql_query,
                params=[start_time, end_time, user_id]
            )
            
            if not cols or not rows:
                logger.info(f"No orderbook data found for user: {user_id}")
                return {
                    'success': True,
                    'message': 'No data found',
                    'rows_count': 0,
                    'cached': False
                }
            
            # DataFrame 생성 및 캐싱
            df = pd.DataFrame(rows, columns=cols)
            cache_key = self._generate_cache_key(user_id, tran_start, tran_end)
            
            # 캐시에 저장
            self._cache[cache_key] = {
                'dataframe': df,
                'created_at': datetime.now(),
                'user_id': user_id,
                'start_date': tran_start,
                'end_date': tran_end,
                'start_time': start_time,
                'end_time': end_time,
                'rows_count': len(df),
                'columns': list(df.columns)
            }
            
            logger.info(f"Orderbook data cached. Key: {cache_key}, Rows: {len(df)}")
            
            return {
                'success': True,
                'message': 'Orderbook 데이터를 메모리에 저장했습니다.',
                'cache_key': cache_key,
                'rows_count': len(df),
                'columns': list(df.columns),
                'period': {
                    'original': f"{tran_start} ~ {tran_end}",
                    'queried': f"{start_time} ~ {end_time}"
                },
                'cached': True
            }
            
        except Exception as e:
            logger.exception(f"Error querying orderbook: {e}")
            return {
                'success': False,
                'message': f'Orderbook 조회 실패: {str(e)}',
                'cached': False
            }
    
    def analyze_orderbook(self, cache_key: str) -> Dict[str, Any]:
        """
        캐시된 Orderbook 데이터 분석
        
        Args:
            cache_key: 캐시 키
            
        Returns:
            분석 결과
        """
        logger.info(f"Analyzing orderbook for cache key: {cache_key}")
        
        df = self.get_cached_dataframe(cache_key)
        
        if df is None:
            return {
                'success': False,
                'message': f'캐시된 데이터를 찾을 수 없습니다: {cache_key}'
            }
        
        try:
            # 분석 실행
            analyzer = OrderbookAnalyzer(df)
            analyzer.analyze()
            
            # 결과 수집
            text_summary = analyzer.generate_text_summary()
            patterns = analyzer.get_pattern_analysis()
            daily_summary = analyzer.get_daily_summary()
            
            # 기간 정보
            period_info = {}
            if cache_key in self._cache:
                cache_data = self._cache[cache_key]
                period_info = {
                    'start_date': cache_data['start_date'],
                    'end_date': cache_data['end_date'],
                    'query_start': cache_data['start_time'],
                    'query_end': cache_data['end_time']
                }
            
            # 분석 결과 캐시에 저장
            if cache_key in self._cache:
                self._cache[cache_key]['analysis'] = {
                    'text_summary': text_summary,
                    'patterns': patterns,
                    'daily_summary': daily_summary,
                    'period_info': period_info,
                    'analyzed_at': datetime.now()
                }
            
            # 일자별 요약을 JSON 형식으로 변환
            daily_json = daily_summary.to_dict('records') if not daily_summary.empty else []
            
            result = {
                'success': True,
                'cache_key': cache_key,
                'daily_summary': daily_json,
                'text_summary': text_summary,
                'patterns': patterns,
                'period_info': period_info,
                'alert_details': {}
            }
            
            logger.info(f"Orderbook analysis completed for {cache_key}")
            
            return result
            
        except Exception as e:
            logger.exception(f"Error analyzing orderbook: {e}")
            return {
                'success': False,
                'message': f'분석 중 오류 발생: {str(e)}'
            }
    
    def analyze_stds_dtm(self, stds_date: str, cache_key: str) -> Dict[str, Any]:
        """
        특정 STDS_DTM 날짜의 Orderbook 분석
        
        Args:
            stds_date: STDS_DTM 날짜
            cache_key: 캐시 키
            
        Returns:
            STDS_DTM 분석 결과
        """
        logger.info(f"Analyzing STDS_DTM orderbook for date: {stds_date}")
        
        df = self.get_cached_dataframe(cache_key)
        
        if df is None:
            return {
                'success': False,
                'message': f'캐시된 데이터를 찾을 수 없습니다: {cache_key}'
            }
        
        try:
            # 해당 날짜만 필터링
            target_date = pd.to_datetime(stds_date).date()
            df_filtered = df[pd.to_datetime(df['trade_date']).dt.date == target_date].copy()
            
            if df_filtered.empty:
                return {
                    'success': True,
                    'date': stds_date,
                    'summary': {
                        'total_records': 0,
                        'message': '해당 날짜의 거래 데이터가 없습니다.'
                    }
                }
            
            # 분석 실행
            analyzer = OrderbookAnalyzer(df_filtered)
            analyzer.analyze()
            patterns = analyzer.get_pattern_analysis()
            
            # 결과 구성
            summary = {
                'date': stds_date,
                'total_records': len(df_filtered),
                'buy_amount': patterns.get('total_buy_amount', 0),
                'buy_count': patterns.get('total_buy_count', 0),
                'buy_details': self._format_details(patterns.get('buy_details', [])),
                'sell_amount': patterns.get('total_sell_amount', 0),
                'sell_count': patterns.get('total_sell_count', 0),
                'sell_details': self._format_details(patterns.get('sell_details', [])),
                'deposit_krw_amount': patterns.get('total_deposit_krw', 0),
                'deposit_krw_count': patterns.get('total_deposit_krw_count', 0),
                'withdraw_krw_amount': patterns.get('total_withdraw_krw', 0),
                'withdraw_krw_count': patterns.get('total_withdraw_krw_count', 0),
                'deposit_crypto_amount': patterns.get('total_deposit_crypto', 0),
                'deposit_crypto_count': patterns.get('total_deposit_crypto_count', 0),
                'deposit_crypto_details': self._format_details(patterns.get('deposit_crypto_details', [])),
                'withdraw_crypto_amount': patterns.get('total_withdraw_crypto', 0),
                'withdraw_crypto_count': patterns.get('total_withdraw_crypto_count', 0),
                'withdraw_crypto_details': self._format_details(patterns.get('withdraw_crypto_details', []))
            }
            
            logger.info(f"STDS_DTM analysis completed for {stds_date}")
            
            return {
                'success': True,
                'summary': summary
            }
            
        except Exception as e:
            logger.exception(f"Error analyzing STDS_DTM orderbook: {e}")
            return {
                'success': False,
                'message': f'분석 중 오류: {str(e)}'
            }
    
    def get_cached_dataframe(self, cache_key: str) -> Optional[pd.DataFrame]:
        """캐시에서 DataFrame 가져오기"""
        if cache_key in self._cache:
            return self._cache[cache_key]['dataframe']
        return None
    
    def get_cache_info(self) -> List[Dict[str, Any]]:
        """모든 캐시 정보 조회"""
        cache_info = []
        
        for key, value in self._cache.items():
            info = {
                'cache_key': key,
                'user_id': value['user_id'],
                'period': f"{value['start_date']} ~ {value['end_date']}",
                'rows_count': value['rows_count'],
                'created_at': value['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
                'columns_count': len(value['columns']),
                'analyzed': 'analysis' in value
            }
            
            if 'analysis' in value:
                info['analyzed_at'] = value['analysis']['analyzed_at'].strftime('%Y-%m-%d %H:%M:%S')
            
            cache_info.append(info)
        
        return cache_info
    
    def clear_cache(self, cache_key: Optional[str] = None) -> Dict[str, Any]:
        """캐시 삭제"""
        if cache_key:
            if cache_key in self._cache:
                del self._cache[cache_key]
                logger.info(f"Cleared cache: {cache_key}")
                return {
                    'success': True,
                    'message': f'캐시 {cache_key}를 삭제했습니다.'
                }
            else:
                return {
                    'success': False,
                    'message': '해당 캐시를 찾을 수 없습니다.'
                }
        else:
            count = len(self._cache)
            self._cache.clear()
            logger.info(f"Cleared all cache ({count} items)")
            return {
                'success': True,
                'message': f'전체 캐시 {count}개를 삭제했습니다.'
            }
    
    def _generate_cache_key(self, user_id: str, start_date: str, end_date: str) -> str:
        """캐시 키 생성"""
        start_clean = start_date.replace('-', '')
        end_clean = end_date.replace('-', '')
        return f"df_orderbook_{start_clean}_{end_clean}_{user_id}"
    
    def _format_details(self, details: List, max_items: int = 10) -> List[Dict]:
        """상세 정보 포맷팅"""
        formatted = []
        for ticker, data in details[:max_items]:
            formatted.append({
                'ticker': ticker,
                'amount': data.get('amount_krw', 0),
                'count': data.get('count', 0)
            })
        return formatted