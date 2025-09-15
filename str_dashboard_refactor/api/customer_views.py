"""
고객 정보 관련 API Views
"""
import logging
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.http import require_POST

from ..core.services.customer_service import CustomerService
from ..core.utils.session_manager import SessionManager
from ..core.utils.decorators import require_db_connection

logger = logging.getLogger(__name__)


@login_required
@require_POST
@require_db_connection
def query_customer_unified_info(request, oracle_conn=None):
    """통합 고객 정보 조회"""
    cust_id = request.POST.get('cust_id', '').strip()
    
    if not cust_id:
        return HttpResponseBadRequest('Missing cust_id.')
    
    logger.info(f"Querying unified customer info for cust_id: {cust_id}")
    
    try:
        # 서비스 레이어 사용
        customer_service = CustomerService(oracle_conn)
        result = customer_service.get_unified_customer_info(cust_id)
        
        if result['success']:
            # 세션에 저장
            SessionManager.save_data(request, SessionManager.Keys.CURRENT_CUSTOMER_DATA, {
                'columns': result['columns'],
                'rows': result['rows'],
                'customer_type': result.get('customer_type'),
                'cust_id': cust_id
            })
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error in query_customer_unified_info: {e}")
        return JsonResponse({
            'success': False,
            'message': f'고객 정보 조회 중 오류: {e}'
        })


@login_required
@require_POST
@require_db_connection
def query_duplicate_unified(request, oracle_conn=None):
    """통합 중복 회원 조회"""
    current_cust_id = request.POST.get('current_cust_id', '').strip()
    
    if not current_cust_id:
        return JsonResponse({
            'success': True,
            'columns': [],
            'rows': []
        })
    
    # 파라미터 수집
    params = {
        'current_cust_id': current_cust_id,
        'full_email': request.POST.get('full_email', '').strip() or None,
        'phone_suffix': request.POST.get('phone_suffix', '').strip() or None,
        'address': request.POST.get('address', '').strip() or None,
        'detail_address': request.POST.get('detail_address', '').strip() or None,
        'workplace_name': request.POST.get('workplace_name', '').strip() or None,
        'workplace_address': request.POST.get('workplace_address', '').strip() or None,
        'workplace_detail_address': request.POST.get('workplace_detail_address', '').strip() or None
    }
    
    logger.debug(f"Duplicate search params - cust_id: {current_cust_id}")
    
    try:
        # 서비스 레이어 사용
        customer_service = CustomerService(oracle_conn)
        result = customer_service.find_duplicate_customers(params)
        
        if result['success']:
            # 세션에 저장
            SessionManager.save_data(request, SessionManager.Keys.DUPLICATE_PERSONS_DATA, {
                'columns': result['columns'],
                'rows': result['rows'],
                'match_criteria': params
            })
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error in query_duplicate_unified: {e}")
        return JsonResponse({
            'success': False,
            'message': f'중복 회원 조회 중 오류: {e}'
        })


@login_required
@require_POST
@require_db_connection
def query_corp_related_persons(request, oracle_conn=None):
    """법인 관련인 정보 조회"""
    cust_id = request.POST.get('cust_id', '').strip()
    
    if not cust_id:
        return HttpResponseBadRequest('Missing cust_id.')
    
    logger.info(f"Querying corp related persons for cust_id: {cust_id}")
    
    try:
        # 서비스 레이어 사용
        customer_service = CustomerService(oracle_conn)
        result = customer_service.get_corp_related_persons(cust_id)
        
        if result['success']:
            # 세션에 저장
            SessionManager.save_data(request, SessionManager.Keys.CURRENT_CORP_RELATED_DATA, {
                'columns': result['columns'],
                'rows': result['rows']
            })
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error in query_corp_related_persons: {e}")
        return JsonResponse({
            'success': False,
            'message': f'법인 관련인 조회 중 오류: {e}'
        })


@login_required
@require_POST
@require_db_connection
def query_person_related_summary(request, oracle_conn=None):
    """개인 고객의 관련인 정보 조회"""
    cust_id = request.POST.get('cust_id', '').strip()
    start_date = request.POST.get('start_date', '').strip()
    end_date = request.POST.get('end_date', '').strip()
    
    if not all([cust_id, start_date, end_date]):
        return JsonResponse({
            'success': False,
            'message': 'Missing required parameters'
        })
    
    logger.info(f"Querying person related summary - cust_id: {cust_id}")
    
    try:
        # 서비스 레이어 사용
        customer_service = CustomerService(oracle_conn)
        result = customer_service.get_person_related_summary(cust_id, start_date, end_date)
        
        if result['success']:
            # 세션에 저장
            SessionManager.save_data(request, SessionManager.Keys.CURRENT_PERSON_RELATED_DATA, 
                                    result.get('related_persons', {}))
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error in query_person_related_summary: {e}")
        return JsonResponse({
            'success': False,
            'message': f'관련인 조회 중 오류: {e}'
        })


@login_required
@require_POST
@require_db_connection
def query_ip_access_history(request, oracle_conn=None):
    """IP 접속 이력 조회"""
    mem_id = request.POST.get('mem_id', '').strip()
    start_date = request.POST.get('start_date', '').strip()
    end_date = request.POST.get('end_date', '').strip()
    
    if not all([mem_id, start_date, end_date]):
        return JsonResponse({
            'success': False,
            'message': 'Missing required parameters'
        })
    
    logger.info(f"Querying IP access history - MID: {mem_id}")
    
    try:
        # 서비스 레이어 사용
        customer_service = CustomerService(oracle_conn)
        result = customer_service.get_ip_access_history(mem_id, start_date, end_date)
        
        if result['success']:
            # 세션에 저장
            SessionManager.save_data(request, SessionManager.Keys.IP_HISTORY_DATA, {
                'columns': result['columns'],
                'rows': result['rows']
            })
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error in query_ip_access_history: {e}")
        return JsonResponse({
            'success': False,
            'message': f'IP 조회 중 오류: {e}'
        })