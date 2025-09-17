# str_dashboard/dataframe_manager.py
"""
DataFrame 기반 통합 데이터 관리 모듈
모든 쿼리 결과를 DataFrame으로 저장하고 중앙에서 관리
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from django.conf import settings

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
                df = pd.DataFrame()
            else:
                df = pd.DataFrame(rows, columns=columns)
            
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
                'dtypes': df.dtypes.to_dict(),
                'memory_usage': df.memory_usage(deep=True).sum(),
                'created_at': dataset['created_at'],
                'metadata': dataset.get('metadata', {})
            }
        return {}
    
    def process_alert_data(self, alert_data: Dict) -> Dict[str, Any]:
        """
        ALERT 데이터 처리 및 메타데이터 추출
        
        Returns:
            처리된 메타데이터 (canonical_ids, rep_rule_id, cust_id, 거래기간 등)
        """
        cols = alert_data.get('columns', [])
        rows = alert_data.get('rows', [])
        
        # DataFrame 생성 및 저장
        self.add_dataset('alert_info', cols, rows)
        
        # 메타데이터 추출
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
        
        df = pd.DataFrame(rows, columns=cols)
        
        # Rule ID 추출
        if 'STR_RULE_ID' in df.columns:
            canonical_ids = df['STR_RULE_ID'].dropna().unique().tolist()
            metadata['canonical_ids'] = [str(rid) for rid in canonical_ids]
            
            # 대표 Rule ID (ALERT ID가 일치하는 행)
            if 'STR_ALERT_ID' in df.columns:
                rep_rows = df[df['STR_ALERT_ID'] == self.alert_id]
                if not rep_rows.empty:
                    metadata['rep_rule_id'] = str(rep_rows.iloc[0]['STR_RULE_ID'])
        
        # 고객 ID
        if 'CUST_ID' in df.columns:
            cust_ids = df['CUST_ID'].dropna().unique()
            if len(cust_ids) > 0:
                metadata['cust_id'] = str(cust_ids[0])
        
        # 거래 기간 추출
        if 'TRAN_STRT' in df.columns and 'TRAN_END' in df.columns:
            tran_starts = pd.to_datetime(df['TRAN_STRT'].dropna(), errors='coerce')
            tran_ends = pd.to_datetime(df['TRAN_END'].dropna(), errors='coerce')
            
            if len(tran_starts) > 0:
                metadata['tran_start'] = tran_starts.min().strftime('%Y-%m-%d %H:%M:%S.%f')
            if len(tran_ends) > 0:
                metadata['tran_end'] = tran_ends.max().strftime('%Y-%m-%d %H:%M:%S.%f')
        
        # 메타데이터 저장
        self.metadata.update(metadata)
        
        return metadata
    
    def process_customer_data(self, customer_data: Dict) -> Dict[str, Any]:
        """고객 데이터 처리"""
        cols = customer_data.get('columns', [])
        rows = customer_data.get('rows', [])
        
        self.add_dataset('customer_info', cols, rows)
        
        metadata = {
            'customer_type': None,
            'kyc_datetime': None,
            'mid': None
        }
        
        if cols and rows:
            df = pd.DataFrame(rows, columns=cols)
            
            # 고객 유형
            if '고객구분' in df.columns and len(df) > 0:
                metadata['customer_type'] = df.iloc[0]['고객구분']
            
            # KYC 완료일시
            if 'KYC완료일시' in df.columns and len(df) > 0:
                metadata['kyc_datetime'] = df.iloc[0]['KYC완료일시']
                self.metadata['kyc_datetime'] = metadata['kyc_datetime']
            
            # MID
            if 'MID' in df.columns and len(df) > 0:
                metadata['mid'] = df.iloc[0]['MID']
                self.metadata['mid'] = metadata['mid']
        
        return metadata
    
    def calculate_transaction_period(self) -> Tuple[Optional[str], Optional[str]]:
        """
        거래 기간 계산 (KYC 시점 고려)
        
        Returns:
            (start_date, end_date) 튜플
        """
        tran_start = self.metadata.get('tran_start')
        tran_end = self.metadata.get('tran_end')
        kyc_datetime = self.metadata.get('kyc_datetime')
        
        if not tran_end:
            return None, None
        
        # 기본값: 종료일 기준 3개월 전
        end_date = pd.to_datetime(tran_end.split(' ')[0])
        start_date = end_date - timedelta(days=90)
        
        # KYC 완료일시가 있으면 더 이른 날짜 사용
        if kyc_datetime:
            kyc_date = pd.to_datetime(kyc_datetime.split(' ')[0])
            if kyc_date < start_date:
                start_date = kyc_date
        
        # 원래 시작일이 있으면 비교
        if tran_start:
            orig_start = pd.to_datetime(tran_start.split(' ')[0])
            if orig_start < start_date:
                start_date = orig_start
        
        return (
            start_date.strftime('%Y-%m-%d %H:%M:%S.%f'),
            end_date.strftime('%Y-%m-%d %H:%M:%S.%f')
        )
    
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
        
        for name, dataset in self.datasets.items():
            df = dataset['dataframe']
            info = {
                'shape': df.shape,
                'columns': list(df.columns),
                'memory_usage_bytes': df.memory_usage(deep=True).sum(),
                'created_at': dataset['created_at'],
                'has_data': not df.empty
            }
            
            # 샘플 데이터 (처음 5행)
            if not df.empty:
                info['sample'] = df.head(5).to_dict('records')
            
            summary['datasets'][name] = info
            summary['total_memory'] += info['memory_usage_bytes']
        
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
            export_data['datasets'][name] = {
                'columns': list(df.columns),
                'rows': df.values.tolist() if not df.empty else [],
                'metadata': dataset.get('metadata', {})
            }
        
        return export_data
    
    def import_from_dict(self, data: Dict[str, Any]):
        """딕셔너리에서 데이터 가져오기"""
        self.reset()
        
        self.alert_id = data.get('alert_id')
        self.metadata = data.get('metadata', {})
        
        for name, dataset in data.get('datasets', {}).items():
            self.add_dataset(
                name=name,
                columns=dataset.get('columns', []),
                rows=dataset.get('rows', []),
                **dataset.get('metadata', {})
            )


# 싱글톤 인스턴스
df_manager = DataFrameManager()