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
    
    logger.info(f"Querying alert info for alert_id: {alert_id}")
    
    try:
        # alert_info_by_alert_id.sql을 보면 :alert_id가 한 번만 사용됨
        # WITH 절의 WHERE STR_ALERT_ID = :alert_id 부분
        result = execute_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='alert_info_by_alert_id.sql',
            bind_params={':alert_id': '?'},
            query_params=[alert_id]  # 한 번만 사용
        )
        
        logger.info(f"Alert query result - success: {result.get('success')}, rows: {len(result.get('rows', []))}")
        
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
def query_person_detail_info(request, oracle_conn=None):
    """고객 상세 정보 조회 (개인/법인 구분)"""
    cust_id = request.POST.get('cust_id', '').strip()
    cust_type = request.POST.get('cust_type', '').strip()  # '개인' or '법인'
    
    if not cust_id:
        return HttpResponseBadRequest('Missing cust_id.')
    
    # 고객 유형에 따라 다른 SQL 파일 사용
    if cust_type == '법인':
        sql_filename = 'corp_detail_info.sql'
        # corp_detail_info.sql은 :custId를 한 번만 사용
        bind_params = {':custId': '?'}
        query_params = [cust_id]
    else:
        sql_filename = 'person_detail_info.sql'
        # person_detail_info.sql도 :custId를 한 번만 사용
        bind_params = {':custId': '?'}
        query_params = [cust_id]
    
    # 쿼리 실행
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename=sql_filename,
        bind_params=bind_params,
        query_params=query_params
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
        columns = list(matching_rows.columns) if not matching_rows.empty else list(df1.columns) if not df1.empty else []
        rows = matching_rows.values.tolist()
        
        # 결과가 없는 경우 유사 조합 검색
        similar_list = []
        if len(rows) == 0 and not df1.empty:
            from .queries.rule_historic_search import find_most_similar_rule_combinations
            similar_list = find_most_similar_rule_combinations(rule_key, df1)
        
        logger.info(f"Rule history search completed. Found {len(rows)} matching rows for key: {rule_key}")
        
        if similar_list:
            logger.info(f"Found {len(similar_list)} similar combinations with similarity: {similar_list[0]['similarity']:.2f}")
        
        return JsonResponse({
            'success': True,
            'columns': columns,
            'rows': rows,
            'searched_rule': rule_key,
            'similar_list': similar_list  # 리스트로 변경
        })
        
    except Exception as e:
        logger.exception(f"Rule history search failed: {e}")
        return JsonResponse({
            'success': False,
            'message': f'히스토리 조회 실패: {e}'
        })
    
    
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


# views.py에 추가할 통합 중복 조회 함수

@login_required
@require_POST
@require_db_connection
def query_duplicate_unified(request, oracle_conn=None):
    """통합 중복 회원 조회 - 단일 쿼리로 모든 조건 처리"""
    
    # 파라미터 추출
    current_cust_id = request.POST.get('current_cust_id', '').strip()
    email_prefix = request.POST.get('email_prefix', '').strip() or None
    phone_suffix = request.POST.get('phone_suffix', '').strip() or None
    address = request.POST.get('address', '').strip() or None
    detail_address = request.POST.get('detail_address', '').strip() or None
    workplace_name = request.POST.get('workplace_name', '').strip() or None
    workplace_address = request.POST.get('workplace_address', '').strip() or None
    workplace_detail_address = request.POST.get('workplace_detail_address', '').strip() or None
    
    if not current_cust_id:
        return JsonResponse({
            'success': True,
            'columns': [],
            'rows': []
        })
    
    # 통합 쿼리 실행
    # SQL에서 각 조건의 IS NOT NULL 체크와 OR 조건 때문에 일부 파라미터가 반복됨
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='duplicate_unified.sql',
        bind_params={
            ':current_cust_id': '?',
            ':email_prefix': '?',
            ':address': '?',
            ':detail_address': '?',
            ':workplace_name': '?',
            ':workplace_address': '?',
            ':workplace_detail_address': '?',
            ':phone_suffix': '?'
        },
        query_params=[
            # 총 16개 파라미터 (SQL에서 사용되는 순서대로)
            current_cust_id,  # 1. 이메일 조건의 CUST_ID != :current_cust_id
            email_prefix,     # 2. :email_prefix IS NOT NULL
            email_prefix,     # 3. = :email_prefix
            
            current_cust_id,  # 4. 주소 조건의 CUST_ID != :current_cust_id  
            address,          # 5. :address IS NOT NULL
            address,          # 6. CUST_ADDR = :address
            detail_address,   # 7. CUST_DTL_ADDR = :detail_address
            
            current_cust_id,  # 8. 직장명 조건의 CUST_ID != :current_cust_id
            workplace_name,   # 9. :workplace_name IS NOT NULL
            workplace_name,   # 10. WPLC_NM = :workplace_name
            
            current_cust_id,  # 11. 직장주소 조건의 CUST_ID != :current_cust_id
            workplace_address,  # 12. :workplace_address IS NOT NULL
            workplace_address,  # 13. WPLC_ADDR = :workplace_address
            workplace_detail_address,  # 14. :workplace_detail_address IS NULL (OR 조건 첫 번째)
            workplace_detail_address,  # 15. WPLC_DTL_ADDR = :workplace_detail_address (OR 조건 두 번째)
            
            phone_suffix,     # 16. WHERE절의 :phone_suffix IS NULL
            phone_suffix      # 17. SUBSTR(...) = :phone_suffix
        ]
    )
    
    return JsonResponse(result)



@login_required
@require_POST
@require_db_connection
def query_corp_related_persons(request, oracle_conn=None):
    """법인 관련인 정보 조회"""
    cust_id = request.POST.get('cust_id', '').strip()
    
    if not cust_id:
        return HttpResponseBadRequest('Missing cust_id.')
    
    # 쿼리 실행
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='corp_related_persons.sql',
        bind_params={':cust_id': '?'},
        query_params=[cust_id]
    )
    
    return JsonResponse(result)


@login_required
@require_POST
@require_db_connection
def query_duplicate_by_email(request, oracle_conn=None):
    """이메일 기준 중복 회원 조회"""
    email_prefix = request.POST.get('email_prefix', '').strip()
    phone_suffix = request.POST.get('phone_suffix', '').strip() or None
    current_cust_id = request.POST.get('current_cust_id', '').strip()
    
    if not current_cust_id or not email_prefix:
        return JsonResponse({
            'success': True,
            'columns': [],
            'rows': []
        })
    
    # :phone_suffix가 OR 조건에서 2번 사용됨
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='duplicate_by_email.sql',
        bind_params={
            ':current_cust_id': '?',
            ':email_prefix': '?',
            ':phone_suffix': '?'
        },
        query_params=[current_cust_id, email_prefix, phone_suffix, phone_suffix]
    )
    
    return JsonResponse(result)


@login_required
@require_POST
@require_db_connection
def query_duplicate_by_address(request, oracle_conn=None):
    """주소 기준 중복 회원 조회"""
    address = request.POST.get('address', '').strip()
    detail_address = request.POST.get('detail_address', '').strip()
    phone_suffix = request.POST.get('phone_suffix', '').strip() or None
    current_cust_id = request.POST.get('current_cust_id', '').strip()
    
    if not current_cust_id or not address:
        return JsonResponse({
            'success': True,
            'columns': [],
            'rows': []
        })
    
    # :phone_suffix가 OR 조건에서 2번 사용됨
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='duplicate_by_address.sql',
        bind_params={
            ':current_cust_id': '?',
            ':address': '?',
            ':detail_address': '?',
            ':phone_suffix': '?'
        },
        query_params=[current_cust_id, address, detail_address, phone_suffix, phone_suffix]
    )
    
    return JsonResponse(result)


@login_required
@require_POST
@require_db_connection
def query_duplicate_by_workplace(request, oracle_conn=None):
    """직장명 기준 중복 회원 조회"""
    workplace_name = request.POST.get('workplace_name', '').strip()
    phone_suffix = request.POST.get('phone_suffix', '').strip() or None
    current_cust_id = request.POST.get('current_cust_id', '').strip()
    
    if not current_cust_id or not workplace_name:
        return JsonResponse({
            'success': True,
            'columns': [],
            'rows': []
        })
    
    # :phone_suffix가 OR 조건에서 2번 사용됨
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='duplicate_by_workplace.sql',
        bind_params={
            ':current_cust_id': '?',
            ':workplace_name': '?',
            ':phone_suffix': '?'
        },
        query_params=[current_cust_id, workplace_name, phone_suffix, phone_suffix]
    )
    
    return JsonResponse(result)


@login_required
@require_POST
@require_db_connection
def query_duplicate_by_workplace_address(request, oracle_conn=None):
    """직장주소 기준 중복 회원 조회"""
    workplace_address = request.POST.get('workplace_address', '').strip()
    workplace_detail_address = request.POST.get('workplace_detail_address', '').strip() or None
    phone_suffix = request.POST.get('phone_suffix', '').strip() or None
    current_cust_id = request.POST.get('current_cust_id', '').strip()
    
    if not current_cust_id or not workplace_address:
        return JsonResponse({
            'success': True,
            'columns': [],
            'rows': []
        })
    
    # :workplace_detail_address와 :phone_suffix가 각각 OR 조건에서 2번씩 사용됨
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='duplicate_by_workplace_address.sql',
        bind_params={
            ':current_cust_id': '?',
            ':workplace_address': '?',
            ':workplace_detail_address': '?',
            ':phone_suffix': '?'
        },
        query_params=[
            current_cust_id,
            workplace_address,
            workplace_detail_address,
            workplace_detail_address,  # OR 조건에서 재사용
            phone_suffix,
            phone_suffix  # OR 조건에서 재사용
        ]
    )
    
    return JsonResponse(result)