"""
데이터 내보내기 관련 API Views
"""
import json
import logging
import tempfile
from pathlib import Path
from datetime import datetime
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, FileResponse
from django.views.decorators.http import require_POST

from ..core.services.export_service import ExportService
from ..core.utils.session_manager import SessionManager

logger = logging.getLogger(__name__)


@login_required
@require_POST
def prepare_toml_data(request):
    """화면에 렌더링된 데이터를 수집하여 TOML 형식으로 준비"""
    try:
        # 세션에서 모든 관련 데이터 수집
        session_keys = [
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
        for key in session_keys:
            # Keys enum을 문자열로 변환
            key_str = key if isinstance(key, str) else key
            session_data[key_str] = SessionManager.get_data(request, key, {})
        
        # 서비스 레이어 사용
        export_service = ExportService()
        result = export_service.prepare_toml_export(session_data)
        
        if result['success']:
            # 임시 파일 경로 저장
            SessionManager.save_data(request, SessionManager.Keys.TOML_TEMP_PATH, 
                                    result['temp_path'])
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error preparing TOML data: {e}")
        return JsonResponse({
            'success': False,
            'message': f'TOML 데이터 준비 실패: {str(e)}'
        })


@login_required
def download_toml(request):
    """준비된 TOML 파일 다운로드"""
    try:
        tmp_path = SessionManager.get_data(request, SessionManager.Keys.TOML_TEMP_PATH)
        if not tmp_path or not Path(tmp_path).exists():
            return JsonResponse({
                'success': False,
                'message': 'TOML 파일을 찾을 수 없습니다.'
            })
        
        alert_id = SessionManager.get_data(request, SessionManager.Keys.CURRENT_ALERT_ID, 'unknown')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'str_data_{alert_id}_{timestamp}.toml'
        
        response = FileResponse(
            open(tmp_path, 'rb'),
            as_attachment=True,
            filename=filename
        )
        
        # 파일 전송 후 정리
        def cleanup():
            try:
                Path(tmp_path).unlink()
                SessionManager.clear_keys(request, [SessionManager.Keys.TOML_TEMP_PATH])
            except:
                pass
        
        import atexit
        atexit.register(cleanup)
        
        return response
        
    except Exception as e:
        logger.exception(f"Error downloading TOML: {e}")
        return JsonResponse({
            'success': False,
            'message': f'TOML 다운로드 실패: {str(e)}'
        })


@login_required
@require_POST
def save_to_session(request):
    """JavaScript에서 세션에 데이터 저장"""
    key = request.POST.get('key', '').strip()
    data = request.POST.get('data', '').strip()
    
    if not key:
        return JsonResponse({
            'success': False,
            'message': 'Key is required'
        })
    
    try:
        parsed_data = json.loads(data) if data else {}
        SessionManager.save_data(request, key, parsed_data)
        
        return JsonResponse({
            'success': True,
            'key': key
        })
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Invalid JSON data: {e}'
        })
    except Exception as e:
        logger.exception(f"Error saving to session: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Failed to save: {e}'
        })