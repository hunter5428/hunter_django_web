# str_dashboard/utils/df_manager/dataframe_manager.py
"""
DataFrame 기반 통합 데이터 관리 모듈
Stage별 결과를 DataFrame으로 저장하고 관리
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any, Optional, List
from decimal import Decimal

logger = logging.getLogger(__name__)


class DataFrameManager:
    """
    Stage 기반 DataFrame 관리 클래스
    """
    
    def __init__(self):
        self.datasets = {}  # 모든 데이터셋
        self.metadata = {}  # 메타데이터
        self.alert_id = None
        
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
            # DataFrame 생성
            if not columns or not rows:
                df = pd.DataFrame(columns=columns if columns else [])
            else:
                # 타입 변환 처리
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
            
            # 데이터셋 저장
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
                'memory_usage': int(df.memory_usage(deep=True).sum()),
                'created_at': dataset['created_at'],
                'metadata': dataset.get('metadata', {})
            }
        return {}
    
    def get_all_datasets_summary(self) -> Dict[str, Any]:
        """모든 데이터셋 요약 정보"""
        summary = {
            'alert_id': self.alert_id,
            'metadata': self.metadata,
            'datasets': {},
            'total_memory': 0
        }
        
        for name in self.datasets.keys():
            info = self.get_dataset_info(name)
            if info:
                summary['datasets'][name] = {
                    'shape': info.get('shape'),
                    'columns': info.get('columns'),
                    'memory_usage_bytes': info.get('memory_usage', 0),
                    'created_at': info.get('created_at'),
                    'has_data': info.get('shape', (0, 0))[0] > 0
                }
                summary['total_memory'] += info.get('memory_usage', 0)
        
        summary['total_memory_mb'] = round(summary['total_memory'] / 1024 / 1024, 2)
        
        return summary
    
    def export_to_dict(self) -> Dict[str, Any]:
        """모든 데이터를 딕셔너리로 export (세션 저장용)"""
        export_data = {
            'alert_id': self.alert_id,
            'metadata': self.metadata,
            'datasets': {}
        }
        
        for name, dataset in self.datasets.items():
            df = dataset['dataframe']
            
            # DataFrame을 리스트로 변환
            df_cleaned = df.copy()
            
            # NaN, NaT 처리
            for col in df_cleaned.columns:
                if df_cleaned[col].dtype == object:
                    df_cleaned[col] = df_cleaned[col].apply(
                        lambda x: float(x) if isinstance(x, Decimal) else x
                    )
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
        """딕셔너리에서 DataFrameManager 복원"""
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