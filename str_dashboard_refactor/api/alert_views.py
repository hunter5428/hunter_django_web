"""
Alert 관련 API Views
"""
import json
import logging
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.http import require_POST

from ..core.services.alert_service import AlertService
from ..core.services.rule_service import RuleService
from ..core.utils.session_manager import SessionManager
from ..core.utils.decorators import require_db_connection
from ..queries.rule_objectives import build_rule_to_objectives

logger = logging.getLogger(__name__)


@login_required
@require_POST
@require_db_connection
def query_alert_info(request, oracle_conn=None):
    """ALERT ID 기반 정보 조회"""
    alert_id = request.POST.get('alert_id', '').strip()
    if not alert_id:
        return HttpResponseBadRequest('Missing alert_id.')
    
    logger.info(f"Querying alert info for alert_id: {alert_id}")
    
    try:
        # 서비스 레이어 사용
        alert_service = AlertService(oracle_conn)
        result = alert_service.get_alert_info(alert_id)
        
        if result['success']:
            # Rule 객관식 매핑 추가
            rule_obj_map = build_rule_to_objectives()
            result['rule_obj_map'] = rule_obj_map
            
            # 세션에 저장
            SessionManager.save_multiple(request, {
                SessionManager.Keys.CURRENT_ALERT_ID: alert_id,
                SessionManager.Keys.CURRENT_ALERT_DATA: {
                    'alert_id': alert_id,
                    'cols': result['columns'],
                    'rows': result['rows'],
                    'canonical_ids': result['canonical_ids'],
                    'rep_rule_id': result['rep_rule_id'],
                    'custIdForPerson': result['cust_id'],
                    'rule_obj_map': rule_obj_map
                }
            })
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error in query_alert_info: {e}")
        return JsonResponse({
            'success': False,
            'message': f'쿼리 실행 중 오류: {e}'
        })


@login_required
@require_POST
@require_db_connection
def rule_history_search(request, oracle_conn=None):
    """RULE 히스토리 검색"""
    rule_key = request.POST.get('rule_key', '').strip()
    if not rule_key:
        return HttpResponseBadRequest('Missing rule_key.')
    
    try:
        # 서비스 레이어 사용
        rule_service = RuleService(oracle_conn)
        result = rule_service.search_rule_history(rule_key)
        
        if result['success']:
            # 세션에 저장
            SessionManager.save_data(request, SessionManager.Keys.CURRENT_RULE_HISTORY_DATA, {
                'columns': result['columns'],
                'rows': result['rows'],
                'searched_rule': result['searched_rule'],
                'similar_list': result.get('similar_list', [])
            })
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Rule history search failed: {e}")
        return JsonResponse({
            'success': False,
            'message': f'히스토리 조회 실패: {e}'
        })


@login_required
def menu1_1(request):
    """ALERT ID 조회 페이지"""
    from django.shortcuts import render
    
    # 이전 연결 정보에서 서비스명 추출
    default_service = 'PRDAMLKR.OCIAMLPRODDBA.OCIAMLPROD.ORACLEVCN.COM'
    
    db_info = SessionManager.get_data(request, SessionManager.Keys.DB_CONN)
    if db_info and isinstance(db_info, dict):
        default_service = db_info.get('service_name', default_service)
    
    rs_info = SessionManager.get_data(request, SessionManager.Keys.RS_CONN)
    rule_obj_map = build_rule_to_objectives()
    
    context = {
        'active_top_menu': 'menu1',
        'active_sub_menu': 'menu1_1',
        'db_status': SessionManager.get_data(request, SessionManager.Keys.DB_CONN_STATUS, 'need'),
        'default_host': '127.0.0.1',
        'default_port': '40112',
        'default_service': default_service,
        'default_username': db_info.get('username', '') if db_info else '',
        'rs_status': SessionManager.get_data(request, SessionManager.Keys.RS_CONN_STATUS, 'need'),
        'default_rs_host': '127.0.0.1',
        'default_rs_port': '40127',
        'default_rs_dbname': 'prod',
        'default_rs_username': rs_info.get('username', '') if rs_info else '',
        'rule_obj_map_json': json.dumps(rule_obj_map, ensure_ascii=False),
    }
    return render(request, 'str_dashboard/menu1_1/main.html', context)