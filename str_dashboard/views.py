# str_dashboard/views.py

import json
import logging
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.decorators.http import require_POST
from django.conf import settings

from .db_utils import (
    OracleConnection, 
    OracleConnectionError,
    require_db_connection, 
    execute_query_with_error_handling
)
from .queries.rule_objectives import build_rule_to_objectives
from .queries.rule_historic_search import (
    fetch_df_result_0, 
    aggregate_by_rule_id_list
)

logger = logging.getLogger(__name__)


@login_required
def home(request):
    """홈 페이지"""
    context = {
        'active_top_menu': '',
        'active_sub_menu': ''
    }
    return render(request, 'str_dashboard/home.html', context)


@login_required
def menu1_1(request):
    """ALERT ID 조회 페이지"""
    # 이전 연결 정보에서 서비스명 추출
    default_service = 'PRDAMLKR.OCIAMLPRODDBA.OCIAMLPROD.ORACLEVCN.COM'
    
    db_info = request.session.get('db_conn')
    if db_info and isinstance(db_info, dict):
        jdbc_url = db_info.get('jdbc_url', '')
        if jdbc_url.startswith('jdbc:oracle:thin:@//'):
            try:
                # jdbc:oracle:thin:@//host:port/service_name 형식에서 service_name 추출
                default_service = jdbc_url.split('/', 3)[-1]
            except Exception:
                pass
    
    # Rule 객관식 매핑 데이터 생성
    rule_obj_map = build_rule_to_objectives()
    
    context = {
        'active_top_menu': 'menu1',
        'active_sub_menu': 'menu1_1',
        'db_status': request.session.get('db_conn_status', 'need'),
        'default_host': '127.0.0.1',
        'default_port': '40112',
        'default_service': default_service,
        'default_username': db_info.get('username', '') if db_info else '',
        'rule_obj_map_json': json.dumps(rule_obj_map, ensure_ascii=False),
    }
    return render(request, 'str_dashboard/menu1_1/main.html', context)


@login_required
@require_POST
def test_oracle_connection(request):
    """Oracle 데이터베이스 연결 테스트"""
    # 파라미터 검증
    required_params = ['host', 'port', 'service_name', 'username', 'password']
    params = {}
    
    for param in required_params:
        value = request.POST.get(param, '').strip() if param != 'password' else request.POST.get(param, '')
        if not value:
            return HttpResponseBadRequest(f'Missing parameter: {param}')
        params[param] = value
    
    # JDBC URL 생성
    jdbc_url = OracleConnection.build_jdbc_url(
        params['host'], 
        params['port'], 
        params['service_name']
    )
    
    # 연결 테스트
    try:
        oracle_conn = OracleConnection(
            jdbc_url=jdbc_url,
            username=params['username'],
            password=params['password']
        )
        
        if oracle_conn.test_connection():
            # 연결 성공 - 세션에 저장
            request.session['db_conn_status'] = 'ok'
            request.session['db_conn'] = {
                'jdbc_url': jdbc_url,
                'driver_path': oracle_conn.driver_path,
                'driver_class': oracle_conn.driver_class,
                'username': params['username'],
                'password': params['password'],
            }
            
            logger.info(f"Oracle connection successful for user: {params['username']}")
            return JsonResponse({
                'success': True,
                'message': '연결에 성공했습니다.'
            })
        else:
            raise OracleConnectionError("연결 테스트 실패")
            
    except OracleConnectionError as e:
        request.session['db_conn_status'] = 'need'
        logger.warning(f"Oracle connection failed: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        })
    except Exception as e:
        request.session['db_conn_status'] = 'need'
        logger.exception(f"Unexpected error during connection test: {e}")
        return JsonResponse({
            'success': False,
            'message': f'연결 실패: {str(e)}'
        })


@login_required
@require_POST
@require_db_connection
def query_alert_info(request, oracle_conn=None):
    """ALERT ID 기반 정보 조회"""
    alert_id = request.POST.get('alert_id', '').strip()
    if not alert_id:
        return HttpResponseBadRequest('Missing alert_id.')
    
    # 쿼리 실행
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='alert_info_by_alert_id.sql',
        bind_params={':alert_id': '?'},
        query_params=[alert_id] * 1  # SQL에서 :alert_id가 사용되는 횟수만큼
    )
    
    # 실제 SQL을 보면 :alert_id가 한 번만 사용되므로
    # 파라미터 개수를 정확히 계산하기 위해 수정
    if not result['success']:
        return JsonResponse(result)
    
    # prepared_sql의 실제 ? 개수에 맞춰 파라미터 조정
    try:
        from .db_utils import SQLQueryManager
        _, param_count = SQLQueryManager.load_and_prepare(
            'alert_info_by_alert_id.sql',
            {':alert_id': '?'}
        )
        
        if param_count > 0:
            result = execute_query_with_error_handling(
                oracle_conn=oracle_conn,
                sql_filename='alert_info_by_alert_id.sql',
                bind_params={':alert_id': '?'},
                query_params=[alert_id] * param_count
            )
    except Exception as e:
        logger.exception(f"Error in query_alert_info: {e}")
        return JsonResponse({
            'success': False,
            'message': f'쿼리 실행 중 오류: {e}'
        })
    
    return JsonResponse(result)


@login_required
@require_POST
@require_db_connection
def query_person_info(request, oracle_conn=None):
    """고객 정보 조회"""
    cust_id = request.POST.get('cust_id', '').strip()
    if not cust_id:
        return HttpResponseBadRequest('Missing cust_id.')
    
    # 쿼리 실행
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='person_info.sql',
        bind_params={':custId': '?'},
        query_params=[cust_id]
    )
    
    return JsonResponse(result)


@login_required
@require_POST
@require_db_connection
def rule_history_search(request, oracle_conn=None):
    """
    RULE 히스토리 검색
    
    POST Parameters:
        rule_key: 'ID1,ID2,...' (오름차순 정렬된 RULE ID 목록)
    """
    rule_key = request.POST.get('rule_key', '').strip()
    if not rule_key:
        return HttpResponseBadRequest('Missing rule_key.')
    
    try:
        # DataFrame 기반 처리는 기존 함수 활용
        # 연결 정보 추출
        db_info = request.session.get('db_conn')
        
        # 전체 집계 데이터 조회
        df0 = fetch_df_result_0(
            jdbc_url=db_info['jdbc_url'],
            driver_class=db_info['driver_class'],
            driver_path=db_info['driver_path'],
            username=db_info['username'],
            password=db_info['password']
        )
        
        # 집계 처리
        df1 = aggregate_by_rule_id_list(df0)
        
        # 일치하는 행 필터링
        matching_rows = df1[df1["STR_RULE_ID_LIST"] == rule_key]
        
        # 결과 변환
        columns = list(matching_rows.columns)
        rows = matching_rows.values.tolist()
        
        logger.info(f"Rule history search completed. Found {len(rows)} matching rows for key: {rule_key}")
        
        return JsonResponse({
            'success': True,
            'columns': columns,
            'rows': rows
        })
        
    except Exception as e:
        logger.exception(f"Rule history search failed: {e}")
        return JsonResponse({
            'success': False,
            'message': f'히스토리 조회 실패: {e}'
        })


# 추가 유틸리티 함수들

def get_db_status(request) -> dict:
    """현재 데이터베이스 연결 상태 조회"""
    db_info = request.session.get('db_conn')
    status = request.session.get('db_conn_status', 'need')
    
    if status == 'ok' and db_info:
        return {
            'connected': True,
            'status': status,
            'username': db_info.get('username', 'Unknown'),
            'jdbc_url': db_info.get('jdbc_url', '')
        }
    
    return {
        'connected': False,
        'status': status,
        'username': None,
        'jdbc_url': None
    }


@login_required
def check_db_status(request):
    """데이터베이스 연결 상태 확인 API (AJAX용)"""
    return JsonResponse(get_db_status(request))


def clear_db_session(request):
    """데이터베이스 세션 정보 초기화"""
    keys_to_clear = ['db_conn', 'db_conn_status']
    for key in keys_to_clear:
        if key in request.session:
            del request.session[key]
    logger.info("Database session cleared")



@login_required
@require_POST
@require_db_connection
def query_person_detail_info(request, oracle_conn=None):
    """고객 상세 정보 조회 (개인/법인 구분)"""
    cust_id = request.POST.get('cust_id', '').strip()
    cust_type = request.POST.get('cust_type', '').strip()  # '개인' or '법인'
    
    if not cust_id:
        return HttpResponseBadRequest('Missing cust_id.')
    
    # 고객 유형에 따라 다른 SQL 파일 사용
    if cust_type == '법인':
        sql_filename = 'corp_detail_info.sql'
    else:
        sql_filename = 'person_detail_info.sql'
    
    # 쿼리 실행
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename=sql_filename,
        bind_params={':custId': '?'},
        query_params=[cust_id]
    )
    
    return JsonResponse(result)

