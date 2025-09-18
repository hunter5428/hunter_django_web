# str_dashboard/utils/df_manager/dataframe_manager.py
"""
DataFrame 기반 통합 데이터 관리 모듈
모든 쿼리 결과를 DataFrame으로 저장하고 중앙에서 관리
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from django.conf import settings
from decimal import Decimal
import re

logger = logging.getLogger(__name__)


class DataFrameManager:
    """
    ALERT ID 기반 데이터를 DataFrame으로 통합 관리하는 클래스
    """
    
    def __init__(self):
        self.datasets = {}  # 모든 데이터셋을 저장하는 딕셔너리
        self.alert_id = None
        self.metadata = {}  # 각종 메타데이터 저장
        
    def reset(self):
        """모든 데이터 초기화"""
        self.datasets.clear()
        self.metadata.clear()
        self.alert_id = None
        logger.info("DataFrameManager reset completed")
    
    def set_alert_id(self, alert_id: str):
        """현재 작업중인 ALERT ID 설정"""
        self.alert_id = alert_id
        self.metadata['alert_id'] = alert_id
        self.metadata['query_time'] = datetime.now().isoformat()
    
    def add_dataset(self, name: str, columns: List[str], rows: List[List], **extra_metadata):
        """
        데이터셋 추가
        
        Args:
            name: 데이터셋 이름
            columns: 컬럼 리스트
            rows: 행 데이터 리스트
            **extra_metadata: 추가 메타데이터
        """
        try:
            if not columns or not rows:
                df = pd.DataFrame(columns=columns if columns else [])
            else:
                # Decimal 타입을 float로 변환
                processed_rows = []
                for row in rows:
                    processed_row = []
                    for value in row:
                        if isinstance(value, Decimal):
                            processed_row.append(float(value))
                        elif isinstance(value, (np.integer, np.floating)):
                            processed_row.append(float(value))
                        else:
                            processed_row.append(value)
                    processed_rows.append(processed_row)
                
                df = pd.DataFrame(processed_rows, columns=columns)
            
            self.datasets[name] = {
                'dataframe': df,
                'columns': columns,
                'row_count': len(rows),
                'created_at': datetime.now().isoformat(),
                'metadata': extra_metadata
            }
            
            logger.info(f"Dataset '{name}' added: {len(rows)} rows, {len(columns)} columns")
            return True
            
        except Exception as e:
            logger.error(f"Failed to add dataset '{name}': {e}")
            return False
    
    def get_dataframe(self, name: str) -> Optional[pd.DataFrame]:
        """특정 데이터셋의 DataFrame 반환"""
        if name in self.datasets:
            return self.datasets[name]['dataframe']
        return None
    
    def get_dataset_info(self, name: str) -> Dict[str, Any]:
        """데이터셋 정보 반환"""
        if name in self.datasets:
            dataset = self.datasets[name]
            df = dataset['dataframe']
            return {
                'name': name,
                'shape': df.shape,
                'columns': list(df.columns),
                'dtypes': {col: str(dtype) for col, dtype in df.dtypes.to_dict().items()},
                'memory_usage': int(df.memory_usage(deep=True).sum()),
                'created_at': dataset['created_at'],
                'metadata': dataset.get('metadata', {})
            }
        return {}
    
    def _clean_date_string(self, date_str):
        """날짜 문자열 정리 헬퍼 함수 - Oracle 호환"""
        if pd.isna(date_str) or not date_str:
            return None
        
        # 문자열로 변환
        date_str = str(date_str)
        
        # 한글, ?, 기타 특수문자 제거
        cleaned = re.sub(r'[^0-9\s\-:.]', '', date_str)
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        if not cleaned:
            return None
        
        # 날짜 부분만 추출 시도
        date_pattern = r'(\d{4})[-/](\d{1,2})[-/](\d{1,2})'
        date_match = re.search(date_pattern, cleaned)
        
        if date_match:
            year, month, day = date_match.groups()
            base_date = f"{year:0>4}-{month:0>2}-{day:0>2}"
            
            # 시간 부분 확인
            time_pattern = r'(\d{1,2}):(\d{1,2}):(\d{1,2})'
            time_match = re.search(time_pattern, cleaned)
            
            if time_match:
                hour, minute, second = time_match.groups()
                try:
                    # 시간 값 유효성 확인
                    h, m, s = int(hour), int(minute), int(second)
                    if 0 <= h <= 23 and 0 <= m <= 59 and 0 <= s <= 59:
                        # Oracle 호환 형식: 마이크로초 제외
                        return f"{base_date} {hour:0>2}:{minute:0>2}:{second:0>2}"
                except ValueError:
                    pass
            
            # 시간이 없거나 유효하지 않으면 기본값
            return f"{base_date} 00:00:00"
        
        return None
    
    def process_alert_data(self, alert_data: Dict) -> Dict[str, Any]:
        """
        ALERT 데이터 처리 및 메타데이터 추출
        """
        cols = alert_data.get('columns', [])
        rows = alert_data.get('rows', [])
        
        # 메타데이터 초기화
        metadata = {
            'canonical_ids': [],
            'rep_rule_id': None,
            'cust_id': None,
            'tran_start': None,
            'tran_end': None,
            'kyc_datetime': None
        }
        
        if not cols or not rows:
            return metadata
        
        df = self.get_dataframe('alert_info')
        if df is None:
            return metadata
        
        # Rule ID 추출
        if 'STR_RULE_ID' in df.columns:
            canonical_ids = df['STR_RULE_ID'].dropna().unique().tolist()
            metadata['canonical_ids'] = [str(rid) for rid in canonical_ids]
            
            # 대표 Rule ID (ALERT ID가 일치하는 행)
            if 'STR_ALERT_ID' in df.columns and self.alert_id:
                rep_rows = df[df['STR_ALERT_ID'] == self.alert_id]
                if not rep_rows.empty:
                    metadata['rep_rule_id'] = str(rep_rows.iloc[0]['STR_RULE_ID'])
        
        # 고객 ID
        if 'CUST_ID' in df.columns:
            cust_ids = df['CUST_ID'].dropna().unique()
            if len(cust_ids) > 0:
                metadata['cust_id'] = str(cust_ids[0])
        
        # 거래 기간 추출 (날짜 문자열 정리)
        if 'TRAN_STRT' in df.columns and 'TRAN_END' in df.columns:
            # 날짜 문자열 정리 후 파싱
            tran_start_strs = df['TRAN_STRT'].apply(self._clean_date_string).dropna()
            tran_end_strs = df['TRAN_END'].apply(self._clean_date_string).dropna()
            
            if not tran_start_strs.empty:
                tran_starts = pd.to_datetime(tran_start_strs, errors='coerce').dropna()
                if not tran_starts.empty:
                    # Oracle 호환 형식 (마이크로초 제외)
                    metadata['tran_start'] = tran_starts.min().strftime('%Y-%m-%d %H:%M:%S')
            
            if not tran_end_strs.empty:
                tran_ends = pd.to_datetime(tran_end_strs, errors='coerce').dropna()
                if not tran_ends.empty:
                    # Oracle 호환 형식 (마이크로초 제외)
                    metadata['tran_end'] = tran_ends.max().strftime('%Y-%m-%d %H:%M:%S')
        
        # 메타데이터 저장
        self.metadata.update(metadata)
        
        return metadata
    
    def process_customer_data(self, customer_data: Dict) -> Dict[str, Any]:
        """고객 데이터 처리"""
        cols = customer_data.get('columns', [])
        rows = customer_data.get('rows', [])
        
        metadata = {
            'customer_type': None,
            'kyc_datetime': None,
            'mid': None
        }
        
        df = self.get_dataframe('customer_info')
        if df is not None and not df.empty:
            # 고객 유형
            if '고객구분' in df.columns:
                metadata['customer_type'] = df.iloc[0]['고객구분']
            
            # KYC 완료일시 - 날짜 정리 적용
            if 'KYC완료일시' in df.columns:
                kyc_dt = df.iloc[0]['KYC완료일시']
                if pd.notna(kyc_dt):
                    cleaned_kyc = self._clean_date_string(kyc_dt)
                    if cleaned_kyc:
                        metadata['kyc_datetime'] = cleaned_kyc
                        self.metadata['kyc_datetime'] = cleaned_kyc
            
            # MID
            if 'MID' in df.columns:
                mid_val = df.iloc[0]['MID']
                if pd.notna(mid_val):
                    metadata['mid'] = str(mid_val)
                    self.metadata['mid'] = str(mid_val)
        
        return metadata
    
    def calculate_transaction_period(self) -> Tuple[Optional[str], Optional[str]]:
        """
        거래 기간 계산 (KYC 시점 고려)
        
        Returns:
            (start_date, end_date) 튜플 (Oracle 호환 형식)
        """
        tran_end_str = self.metadata.get('tran_end')
        if not tran_end_str:
            logger.warning("No transaction end date found")
            return None, None
        
        try:
            end_date = pd.to_datetime(tran_end_str)
            if pd.isna(end_date):
                logger.error(f"Failed to parse tran_end date: {tran_end_str}")
                return None, None
            
            # 기본값: 종료일 기준 3개월 전
            start_date = end_date - timedelta(days=90)
            
            # KYC 완료일시가 있으면 더 이른 날짜 사용
            kyc_datetime_str = self.metadata.get('kyc_datetime')
            if kyc_datetime_str:
                kyc_date = pd.to_datetime(kyc_datetime_str, errors='coerce')
                if not pd.isna(kyc_date) and kyc_date < start_date:
                    start_date = kyc_date
            
            # 원래 시작일이 있으면 비교
            tran_start_str = self.metadata.get('tran_start')
            if tran_start_str:
                orig_start = pd.to_datetime(tran_start_str, errors='coerce')
                if not pd.isna(orig_start) and orig_start < start_date:
                    start_date = orig_start
            
            # Oracle 호환 형식으로 반환 (마이크로초 제외)
            return start_date.strftime('%Y-%m-%d %H:%M:%S'), end_date.strftime('%Y-%m-%d %H:%M:%S')
            
        except Exception as e:
            logger.error(f"Error calculating transaction period: {e}")
            return None, None
    
    def get_all_datasets_summary(self) -> Dict[str, Any]:
        """
        모든 데이터셋 요약 정보 반환
        콘솔에서 조회 가능한 형태로 정리
        """
        summary = {
            'alert_id': self.alert_id,
            'metadata': self.metadata,
            'datasets': {},
            'total_memory': 0
        }
        
        for name in self.datasets.keys():
            info = self.get_dataset_info(name)
            summary_info = {
                'shape': info.get('shape'),
                'columns': info.get('columns'),
                'memory_usage_bytes': info.get('memory_usage', 0),
                'created_at': info.get('created_at'),
                'has_data': info.get('shape', (0,0))[0] > 0
            }
            summary['datasets'][name] = summary_info
            summary['total_memory'] += summary_info.get('memory_usage_bytes', 0)
        
        # 메모리 크기를 읽기 쉽게 변환
        summary['total_memory_mb'] = round(summary['total_memory'] / 1024 / 1024, 2)
        
        return summary
    
    def export_to_dict(self) -> Dict[str, Any]:
        """
        모든 데이터를 딕셔너리로 내보내기
        (세션 저장 또는 TOML 변환용)
        """
        export_data = {
            'alert_id': self.alert_id,
            'metadata': self.metadata,
            'datasets': {}
        }
        
        for name, dataset in self.datasets.items():
            df = dataset['dataframe']
            
            # NaN, NaT, Decimal 값 처리
            df_cleaned = df.copy()
            
            # 각 컬럼별로 타입 확인 및 변환
            for col in df_cleaned.columns:
                # Decimal 타입 변환
                if df_cleaned[col].dtype == object:
                    df_cleaned[col] = df_cleaned[col].apply(
                        lambda x: float(x) if isinstance(x, Decimal) else x
                    )
                # NaN, NaT를 None으로 변환
                df_cleaned[col] = df_cleaned[col].replace({pd.NaT: None})
                if pd.api.types.is_numeric_dtype(df_cleaned[col]):
                    df_cleaned[col] = df_cleaned[col].replace({np.nan: None})
            
            export_data['datasets'][name] = {
                'columns': list(df_cleaned.columns),
                'rows': df_cleaned.values.tolist() if not df_cleaned.empty else [],
                'metadata': dataset.get('metadata', {})
            }
        
        return export_data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'DataFrameManager':
        """딕셔너리에서 DataFrameManager 인스턴스 복원"""
        manager = cls()
        manager.alert_id = data.get('alert_id')
        manager.metadata = data.get('metadata', {})
        
        for name, dataset_data in data.get('datasets', {}).items():
            manager.add_dataset(
                name=name,
                columns=dataset_data.get('columns', []),
                rows=dataset_data.get('rows', []),
                **dataset_data.get('metadata', {})
            )
        return manager