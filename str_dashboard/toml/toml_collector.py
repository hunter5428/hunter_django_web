# str_dashboard/toml/toml_collector.py
"""
TOML 데이터 수집 및 조합 로직
세션 데이터를 수집하여 TOML 형식으로 변환
"""

import logging
from typing import Dict, Any, List, Optional
import pandas as pd  # <--- 수정된 부분: pandas import 추가

from .toml_config import toml_config
from .toml_processor import toml_processor
from ..dataframe_manager import DataFrameManager

logger = logging.getLogger(__name__)


class TomlDataCollector:
    """
    렌더링된 데이터를 수집하고 TOML 형식으로 변환하는 클래스
    """
    
    def __init__(self):
        self.config = toml_config
        self.processor = toml_processor

    def collect_all_data(self, df_manager_data: Dict[str, Any]) -> Dict[str, Any]:
        """세션 데이터를 수집하여 TOML 형식으로 변환"""
        
        logger.info("Starting TOML data collection...")
        collected_data = {}
        
        df_manager = DataFrameManager.from_dict(df_manager_data)
        
        customer_df = df_manager.get_dataframe('customer_info')
        if customer_df is not None and not customer_df.empty:
            logger.info("Processing customer data...")
            collected_data = self._process_customer_data(customer_df, df_manager.metadata)
            
            dup_df = df_manager.get_dataframe('duplicate_persons')
            if dup_df is not None and not dup_df.empty:
                logger.info("Processing duplicate persons...")
                dup_info = self._process_duplicate_persons(dup_df)
                if '혐의대상자_고객_정보' in collected_data:
                    collected_data['혐의대상자_고객_정보']['동일_차명_의심회원'] = dup_info
        
        logger.info(f"Data collection completed. Sections: {list(collected_data.keys())}")
        return collected_data

    def _process_customer_data(self, customer_df: pd.DataFrame, metadata: Dict) -> Dict[str, Any]:
        """고객 데이터 처리"""
        if customer_df.empty:
            return {"혐의대상자_고객_정보": {}}

        row = customer_df.iloc[0]
        cust_id = metadata.get('cust_id')
        mid = metadata.get('mid')
        
        processed = {}
        for col, value in row.items():
            if pd.isna(value) or value == '':
                continue
            
            masked_value = self.processor.mask_customer_field(col, value, cust_id, mid)
            
            if masked_value is not None:
                # toml_config.py에 field_mapping을 추가해야 합니다.
                # 임시로 기존 로직을 유지하되, 설정을 외부 파일로 분리하는 것을 권장합니다.
                field_mapping = self.config.CUSTOMER_INFO.get('field_mapping', {})
                new_field_name = field_mapping.get(col, col)
                processed[new_field_name] = masked_value
        
        return {"혐의대상자_고객_정보": processed}

    def _process_duplicate_persons(self, duplicate_df: pd.DataFrame) -> str:
        """중복 회원 데이터를 설명 문자열로 처리"""
        if duplicate_df.empty:
            return "없음"

        all_match_reasons = set()
        for match_types in duplicate_df['MATCH_TYPES'].dropna():
            reasons = self.processor.process_duplicate_matches(match_types)
            if reasons:
                all_match_reasons.update(reasons.split(', '))
        
        count = len(duplicate_df)
        
        if all_match_reasons:
            formatted_desc = ", ".join(sorted(list(all_match_reasons)))
            return f"{formatted_desc}이(가) 동일한 회원이 {count}명 존재"
        
        return f"정보가 일치하는 회원이 {count}명 존재"


# 싱글톤 인스턴스
toml_collector = TomlDataCollector()