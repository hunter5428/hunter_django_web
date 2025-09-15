"""
Orderbook 관련 API Views
"""
import logging
from datetime import datetime, timedelta
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse
from django.views.decorators.http import require_POST

from ..core.services.orderbook_service import OrderbookService
from ..core.utils.session_manager import SessionManager
from ..core.utils.decorators import require_redshift_connection

logger = logging.getLogger(__name__)


@login_required
@require_POST
@require_redshift_connection
def query_redshift_orderbook(request, redshift_conn=None):
    """Redshift에서 Orderbook 데이터 조회"""
    user_id = request.POST.get('user_id', '').strip()
    tran_start = request.POST.get('tran_start', '').strip()
    tran_end = request.POST.get('tran_end', '').strip()
    
    if not all([user_id, tran_start, tran_end]):
        return JsonResponse({
            'success': False,
            'message': 'Missing required parameters'
        })
    
    try:
        # 서비스 레이어 사용
        orderbook_service = OrderbookService(redshift_conn)
        result = orderbook_service.query_orderbook(user_id, tran_start, tran_end)
        
        if result['success'] and result.get('cache_key'):
            logger.info(f"Orderbook data cached with key: {result['cache_key']}")
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error in query_redshift_orderbook: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Orderbook 조회 중 오류: {str(e)}'
        })


@login_required
def get_cached_orderbook_info(request):
    """캐시된 Orderbook 정보 조회"""
    try:
        orderbook_service = OrderbookService(None)
        cache_info = orderbook_service.get_cache_info()
        
        return JsonResponse({
            'success': True,
            'cache_count': len(cache_info),
            'cached_data': cache_info
        })
    except Exception as e:
        logger.exception(f"Error getting cache info: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        })


@login_required
@require_POST
def clear_orderbook_cache(request):
    """Orderbook 캐시 초기화"""
    cache_key = request.POST.get('cache_key', '').strip()
    
    try:
        orderbook_service = OrderbookService(None)
        result = orderbook_service.clear_cache(cache_key)
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error clearing cache: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        })


@login_required
@require_POST
def analyze_cached_orderbook(request):
    """캐시된 Orderbook 데이터 분석"""
    cache_key = request.POST.get('cache_key', '').strip()
    
    if not cache_key:
        return JsonResponse({
            'success': False,
            'message': 'cache_key is required'
        })
    
    try:
        orderbook_service = OrderbookService(None)
        result = orderbook_service.analyze_orderbook(cache_key)
        
        if result['success']:
            # 세션에 저장
            SessionManager.save_data(request, SessionManager.Keys.CURRENT_ORDERBOOK_ANALYSIS, {
                'patterns': result.get('patterns'),
                'period_info': result.get('period_info'),
                'text_summary': result.get('text_summary'),
                'cache_key': cache_key
            })
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error analyzing orderbook: {e}")
        return JsonResponse({
            'success': False,
            'message': f'분석 중 오류 발생: {str(e)}'
        })


@login_required
def get_orderbook_summary(request):
    """캐시된 Orderbook의 분석 결과 조회"""
    cache_key = request.GET.get('cache_key', '').strip()
    
    if not cache_key:
        return JsonResponse({
            'success': False,
            'message': 'cache_key is required'
        })
    
    try:
        orderbook_service = OrderbookService(None)
        result = orderbook_service.get_analysis_summary(cache_key)
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error getting summary: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        })


@login_required
@require_POST
def analyze_alert_orderbook(request):
    """특정 ALERT ID에 대한 Orderbook 상세 분석"""
    alert_id = request.POST.get('alert_id', '').strip()
    start_date = request.POST.get('start_date', '').strip()
    end_date = request.POST.get('end_date', '').strip()
    cache_key = request.POST.get('cache_key', '').strip()
    
    if not all([alert_id, start_date, end_date, cache_key]):
        return JsonResponse({
            'success': False,
            'message': 'Missing required parameters'
        })
    
    try:
        orderbook_service = OrderbookService(None)
        result = orderbook_service.analyze_alert_orderbook(
            alert_id, start_date, end_date, cache_key
        )
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error analyzing alert orderbook: {e}")
        return JsonResponse({
            'success': False,
            'message': f'분석 중 오류: {str(e)}'
        })


@login_required
@require_POST
def analyze_stds_dtm_orderbook(request):
    """대표 ALERT의 STDS_DTM 날짜에 대한 Orderbook 요약"""
    stds_date = request.POST.get('stds_date', '').strip()
    cache_key = request.POST.get('cache_key', '').strip()
    
    if not all([stds_date, cache_key]):
        return JsonResponse({
            'success': False,
            'message': 'Missing required parameters'
        })
    
    try:
        orderbook_service = OrderbookService(None)
        result = orderbook_service.analyze_stds_dtm(stds_date, cache_key)
        
        if result['success']:
            # 세션에 저장
            SessionManager.save_data(request, SessionManager.Keys.CURRENT_STDS_DTM_SUMMARY, 
                                    result.get('summary', {}))
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error analyzing STDS_DTM orderbook: {e}")
        return JsonResponse({
            'success': False,
            'message': f'분석 중 오류: {str(e)}'
        })