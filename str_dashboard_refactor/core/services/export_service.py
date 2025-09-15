"""
Data export service (TOML)
"""
import toml
import tempfile
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

from django.http import HttpRequest
from ..utils import SessionManager
from ..toml import TomlDataCollector, TomlConfig

logger = logging.getLogger(__name__)


class ExportService:
    """데이터 내보내기 서비스"""
    
    def __init__(self):
        """초기화"""
        self.toml_collector = TomlDataCollector()
        self.toml_config = TomlConfig()
    
    def prepare_toml_data(self, request: HttpRequest) -> Dict[str, Any]:
        """
        화면에 렌더링된 데이터를 수집하여 TOML 형식으로 준비
        
        Args:
            request: HttpRequest 객체
            
        Returns:
            준비 결과
        """
        logger.info("Preparing TOML data for export")
        
        try:
            # 세션에서 모든 관련 데이터 수집
            session_data = self._collect_session_data(request)
            
            # 데이터 존재 여부 로깅
            self._log_session_data_status(session_data)
            
            # TOML 데이터 수집 및 처리
            collected_data = self.toml_collector.collect_all_data(session_data)
            
            if not collected_data:
                return {
                    'success': False,
                    'message': '수집할 데이터가 없습니다.'
                }
            
            # 임시 파일에 저장
            temp_file = self._save_to_temp_file(collected_data)
            
            # 세션에 임시 파일 경로 저장
            SessionManager.save_data(request, SessionManager.Keys.TOML_TEMP_PATH, temp_file)
            
            result = {
                'success': True,
                'message': 'TOML 데이터 준비 완료',
                'data_count': len(collected_data),
                'sections': list(collected_data.keys()),
                'temp_path': temp_file
            }
            
            logger.info(f"TOML data prepared successfully. Sections: {result['sections']}")
            
            return result
            
        except Exception as e:
            logger.exception(f"Error preparing TOML data: {e}")
            return {
                'success': False,
                'message': f'TOML 데이터 준비 실패: {str(e)}'
            }
    
    def get_toml_download_info(self, request: HttpRequest) -> Dict[str, Any]:
        """
        TOML 다운로드 정보 가져오기
        
        Args:
            request: HttpRequest 객체
            
        Returns:
            다운로드 정보
        """
        try:
            temp_path = SessionManager.get_data(request, SessionManager.Keys.TOML_TEMP_PATH)
            
            if not temp_path or not Path(temp_path).exists():
                return {
                    'success': False,
                    'message': 'TOML 파일을 찾을 수 없습니다.'
                }
            
            # 파일명 생성
            alert_id = SessionManager.get_data(request, SessionManager.Keys.CURRENT_ALERT_ID, 'unknown')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f'str_data_{alert_id}_{timestamp}.toml'
            
            return {
                'success': True,
                'file_path': temp_path,
                'filename': filename,
                'alert_id': alert_id
            }
            
        except Exception as e:
            logger.exception(f"Error getting TOML download info: {e}")
            return {
                'success': False,
                'message': f'다운로드 정보 조회 실패: {str(e)}'
            }
    
    def cleanup_temp_file(self, file_path: str) -> None:
        """
        임시 파일 정리
        
        Args:
            file_path: 파일 경로
        """
        try:
            path = Path(file_path)
            if path.exists():
                path.unlink()
                logger.debug(f"Temp file deleted: {file_path}")
        except Exception as e:
            logger.warning(f"Failed to delete temp file {file_path}: {e}")
    
    def _collect_session_data(self, request: HttpRequest) -> Dict[str, Any]:
        """세션에서 데이터 수집"""
        keys_to_collect = [
            SessionManager.Keys.CURRENT_ALERT_DATA,
            SessionManager.Keys.CURRENT_ALERT_ID,
            SessionManager.Keys.CURRENT_CUSTOMER_DATA,
            SessionManager.Keys.CURRENT_CORP_RELATED_DATA,
            SessionManager.Keys.CURRENT_PERSON_RELATED_DATA,
            SessionManager.Keys.CURRENT_RULE_HISTORY_DATA,
            SessionManager.Keys.DUPLICATE_PERSONS_DATA,
            SessionManager.Keys.IP_HISTORY_DATA,
            SessionManager.Keys.CURRENT_ORDERBOOK_ANALYSIS,
            SessionManager.Keys.CURRENT_STDS_DTM_SUMMARY
        ]
        
        session_data = {}
        for key in keys_to_collect:
            # 세션 키를 변수명으로 변환 (소문자)
            var_name = key.lower()
            session_data[var_name] = SessionManager.get_data(request, key, {})
        
        return session_data
    
    def _log_session_data_status(self, session_data: Dict[str, Any]) -> None:
        """세션 데이터 상태 로깅"""
        logger.info("=== Session Data Status ===")
        for key, value in session_data.items():
            if value:
                size = len(str(value)) if value else 0
                logger.info(f"{key}: {type(value).__name__}, size: {size:,} bytes")
                if isinstance(value, dict):
                    logger.info(f"  keys: {list(value.keys())}")
    
    def _save_to_temp_file(self, data: Dict[str, Any]) -> str:
        """
        데이터를 임시 파일에 저장
        
        Args:
            data: 저장할 데이터
            
        Returns:
            임시 파일 경로
        """
        with tempfile.NamedTemporaryFile(
            mode='w',
            suffix='.toml',
            delete=False,
            encoding='utf-8'
        ) as tmp:
            toml.dump(data, tmp)
            return tmp.name
    
    def export_to_json(self, request: HttpRequest) -> Dict[str, Any]:
        """
        JSON 형식으로 내보내기 (향후 구현)
        
        Args:
            request: HttpRequest 객체
            
        Returns:
            내보내기 결과
        """
        # TODO: JSON 내보내기 구현
        return {
            'success': False,
            'message': 'JSON 내보내기는 아직 구현되지 않았습니다.'
        }
    
    def export_to_excel(self, request: HttpRequest) -> Dict[str, Any]:
        """
        Excel 형식으로 내보내기 (향후 구현)
        
        Args:
            request: HttpRequest 객체
            
        Returns:
            내보내기 결과
        """
        # TODO: Excel 내보내기 구현
        return {
            'success': False,
            'message': 'Excel 내보내기는 아직 구현되지 않았습니다.'
        }