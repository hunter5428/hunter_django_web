# str_dashboard/toml_exporter.py
"""
TOML 파일 저장 및 내보내기 로직
"""

import toml
import logging
from datetime import datetime
from typing import Dict, Any
from pathlib import Path

from .toml_collector import toml_collector

logger = logging.getLogger(__name__)


class TomlExporter:
    """TOML 파일 내보내기 클래스"""
    
    def __init__(self):
        self.collector = toml_collector
    
    def export_to_toml(self, session_data: Dict[str, Any], filepath: str) -> bool:
        """
        세션 데이터를 TOML 파일로 내보내기
        
        Args:
            session_data: 세션에서 가져온 데이터
            filepath: 저장할 파일 경로
        
        Returns:
            성공 여부
        """
        try:
            # 데이터 수집 - 현재는 고객 정보만 포함
            collected_data = self.collector.collect_all_data(session_data)
            
            # 파일 저장
            return self.save_to_file(collected_data, filepath)
            
        except Exception as e:
            logger.exception(f"Failed to export TOML: {e}")
            return False
    
    def save_to_file(self, data: Dict[str, Any], filepath: str) -> bool:
        """
        데이터를 TOML 파일로 저장
        
        Args:
            data: 저장할 데이터
            filepath: 파일 경로
        
        Returns:
            성공 여부
        """
        try:
            path = Path(filepath)
            path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(path, 'w', encoding='utf-8') as f:
                toml.dump(data, f)
            
            logger.info(f"TOML file saved successfully: {filepath}")
            return True
            
        except Exception as e:
            logger.exception(f"Failed to save TOML file: {e}")
            return False
    
    def generate_filename(self, alert_id: str = None) -> str:
        """
        TOML 파일명 생성
        
        Args:
            alert_id: Alert ID (옵션)
        
        Returns:
            생성된 파일명
        """
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if alert_id:
            return f"str_data_{alert_id}_{timestamp}.toml"
        else:
            return f"str_data_{timestamp}.toml"


# 싱글톤 인스턴스
toml_exporter = TomlExporter()