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




# views.py에 추가할 함수

@login_required
@require_POST
@require_db_connection
def query_customer_unified_info(request, oracle_conn=None):
    """통합 고객 정보 조회 (기본 + 상세)"""
    cust_id = request.POST.get('cust_id', '').strip()
    
    if not cust_id:
        return HttpResponseBadRequest('Missing cust_id.')
    
    logger.info(f"Querying unified customer info for cust_id: {cust_id}")
    
    # 통합 쿼리 실행
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='customer_unified_info.sql',
        bind_params={':custId': '?'},
        query_params=[cust_id]
    )
    
    if result.get('success'):
        # 결과에서 고객 구분 추출
        columns = result.get('columns', [])
        rows = result.get('rows', [])
        
        customer_type = None
        if rows and len(rows) > 0:
            cust_type_idx = columns.index('고객구분') if '고객구분' in columns else -1
            if cust_type_idx >= 0:
                customer_type = rows[0][cust_type_idx]
        
        # 응답에 고객 유형 추가
        result['customer_type'] = customer_type
        
        logger.info(f"Unified query successful - customer_type: {customer_type}, rows: {len(rows)}")
    else:
        logger.error(f"Unified query failed: {result.get('message')}")
    
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


@login_required
@require_POST
@require_db_connection
def query_duplicate_unified(request, oracle_conn=None):
    """통합 중복 회원 조회 - 이메일 제외 버전"""
    
    # 파라미터 추출
    current_cust_id = request.POST.get('current_cust_id', '').strip()
    # full_email은 받지만 사용하지 않음 (향후 복구 가능성을 위해 유지)
    full_email = request.POST.get('full_email', '').strip() or None
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
    
    # 파라미터 로깅 (이메일 제외)
    logger.debug(f"Duplicate search params - cust_id: {current_cust_id}")
    logger.debug(f"  phone: {phone_suffix}")
    logger.debug(f"  address: {bool(address)}, workplace: {bool(workplace_name)}")
    logger.info("Note: Email-based duplicate search is currently disabled")
    
    # SQL 쿼리의 바인드 변수 사용 순서 (duplicate_unified.sql 기준)
    # 이메일 부분이 주석 처리되어 바인드 변수 개수 감소
    # WITH 절 내부:
    # 1. :current_cust_id (주소 섹션)
    # 2. :address (IS NOT NULL)
    # 3. :address (= 비교)
    # 4. :detail_address
    # 5. :current_cust_id (직장명 섹션)
    # 6. :workplace_name (IS NOT NULL)
    # 7. :workplace_name (= 비교)
    # 8. :current_cust_id (직장주소 섹션)
    # 9. :workplace_address (IS NOT NULL)
    # 10. :workplace_address (= 비교)
    # 11. :workplace_detail_address (IS NULL)
    # 12. :workplace_detail_address (= 비교)
    # WHERE 절:
    # 13. :phone_suffix (IS NULL)
    # 14. :phone_suffix (= 비교)
    
    # 통합 쿼리 실행 (이메일 파라미터 제외)
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='duplicate_unified.sql',
        bind_params={
            ':current_cust_id': '?',           # 3번 사용 (이메일 제외로 4→3)
            # ':encrypted_email': '?',         # 제거됨
            ':address': '?',                    # 2번 사용
            ':detail_address': '?',             # 1번 사용
            ':workplace_name': '?',             # 2번 사용
            ':workplace_address': '?',          # 2번 사용
            ':workplace_detail_address': '?',   # 2번 사용
            ':phone_suffix': '?'                # 2번 사용
        },
        query_params=[
            # 이메일 조건 제거됨 (3개 삭제)
            
            # 주소 조건 (4개)
            current_cust_id,
            address,
            address,
            detail_address,
            
            # 직장명 조건 (3개)
            current_cust_id,
            workplace_name,
            workplace_name,
            
            # 직장주소 조건 (5개)
            current_cust_id,
            workplace_address,
            workplace_address,
            workplace_detail_address,
            workplace_detail_address,
            
            # 전화번호 필터 (2개)
            phone_suffix,
            phone_suffix
        ]  # 총 14개 (기존 17개에서 3개 감소)
    )
    
    if result.get('success'):
        logger.info(f"Duplicate search successful - found {len(result.get('rows', []))} records")
        if full_email:
            logger.info("Note: Email was provided but not used in search due to encryption issues")
    else:
        logger.error(f"Duplicate search failed: {result.get('message')}")
    
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
def query_person_related_summary(request, oracle_conn=None):
    """개인 고객의 관련인(내부입출금 거래 상대방) 정보 조회"""
    
    cust_id = request.POST.get('cust_id', '').strip()
    start_date = request.POST.get('start_date', '').strip()
    end_date = request.POST.get('end_date', '').strip()
    
    if not all([cust_id, start_date, end_date]):
        return JsonResponse({
            'success': False,
            'message': 'Missing required parameters: cust_id, start_date, end_date'
        })
    
    logger.info(f"Querying person related summary - cust_id: {cust_id}, period: {start_date} ~ {end_date}")
    
    try:
        # person_related_summary.sql 실행
        # 바인드 변수 순서 확인:
        # 1. :start_date (RELATED_PERSONS CTE)
        # 2. :end_date (RELATED_PERSONS CTE)
        # 3. :cust_id (RELATED_PERSONS CTE)
        # 4. :cust_id (TRANSACTION_SUMMARY CTE)
        # 5. :start_date (TRANSACTION_SUMMARY CTE)
        # 6. :end_date (TRANSACTION_SUMMARY CTE)
        
        result = execute_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='person_related_summary.sql',
            bind_params={
                ':cust_id': '?',
                ':start_date': '?',
                ':end_date': '?'
            },
            query_params=[
                start_date,     # 첫 번째 :start_date
                end_date,       # 첫 번째 :end_date
                cust_id,        # 첫 번째 :cust_id
                cust_id,        # 두 번째 :cust_id
                start_date,     # 두 번째 :start_date
                end_date        # 두 번째 :end_date
            ]
        )
        
        if not result.get('success'):
            logger.error(f"Person related summary query failed: {result.get('message')}")
            return JsonResponse(result)
        
        # 결과 데이터 처리 및 포맷팅
        columns = result.get('columns', [])
        rows = result.get('rows', [])
        
        # 관련인별로 데이터 그룹화
        related_persons = {}
        
        for row in rows:
            record_type = row[0] if len(row) > 0 else None
            cust_id_val = row[1] if len(row) > 1 else None
            
            if not cust_id_val:
                continue
            
            if cust_id_val not in related_persons:
                related_persons[cust_id_val] = {
                    'info': None,
                    'transactions': []
                }
            
            if record_type == 'PERSON_INFO':
                # 개인 정보 레코드
                related_persons[cust_id_val]['info'] = {
                    'cust_id': cust_id_val,
                    'name': row[2],
                    'id_number': row[3],
                    'birth_date': row[4],
                    'age': row[5],
                    'gender': row[6],
                    'address': row[7],
                    'job': row[8],
                    'workplace': row[9],
                    'workplace_addr': row[10],
                    'income_source': row[11],
                    'tran_purpose': row[12],
                    'risk_grade': row[13],
                    'total_tran_count': row[14]
                }
            elif record_type == 'TRAN_SUMMARY':
                # 거래 요약 레코드
                related_persons[cust_id_val]['transactions'].append({
                    'coin_symbol': row[15],
                    'tran_type': row[16],
                    'tran_qty': row[17],
                    'tran_amt': row[18],
                    'tran_cnt': row[19]
                })
        
        # 텍스트 형식으로 변환
        summary_text = format_related_person_summary(related_persons)
        
        logger.info(f"Person related summary completed - found {len(related_persons)} related persons")
        
        return JsonResponse({
            'success': True,
            'related_persons': related_persons,
            'summary_text': summary_text,
            'raw_columns': columns,
            'raw_rows': rows
        })
        
    except Exception as e:
        logger.exception(f"Error in query_person_related_summary: {e}")
        return JsonResponse({
            'success': False,
            'message': f'관련인 조회 중 오류: {e}'
        })


def format_related_person_summary(related_persons):
    """관련인 정보를 읽기 쉬운 텍스트 형식으로 변환"""
    
    if not related_persons:
        return "내부입출금 거래 관련인이 없습니다."
    
    lines = []
    lines.append("=" * 80)
    lines.append("【 개인 고객 관련인 정보 (내부입출금 거래 상대방) 】")
    lines.append("=" * 80)
    lines.append("")
    
    for idx, (cust_id, data) in enumerate(related_persons.items(), 1):
        info = data.get('info')
        transactions = data.get('transactions', [])
        
        if not info:
            continue
        
        # 관련인 기본 정보
        lines.append(f"◆ 관련인 {idx}: {info.get('name', 'N/A')} (CID: {cust_id})")
        lines.append("-" * 60)
        
        # 기본 정보 출력
        lines.append(f"  • 실명번호: {info.get('id_number', 'N/A')}")
        lines.append(f"  • 생년월일: {info.get('birth_date', 'N/A')} (만 {info.get('age', 'N/A')}세)")
        lines.append(f"  • 성별: {info.get('gender', 'N/A')}")
        lines.append(f"  • 거주지: {info.get('address', 'N/A')}")
        
        if info.get('job'):
            lines.append(f"  • 직업: {info.get('job')}")
        if info.get('workplace'):
            lines.append(f"  • 직장명: {info.get('workplace')}")
        if info.get('workplace_addr'):
            lines.append(f"  • 직장주소: {info.get('workplace_addr')}")
        
        lines.append(f"  • 자금의 원천: {info.get('income_source', 'N/A')}")
        lines.append(f"  • 거래목적: {info.get('tran_purpose', 'N/A')}")
        lines.append(f"  • 위험등급: {info.get('risk_grade', 'N/A')}")
        lines.append(f"  • 총 거래횟수: {info.get('total_tran_count', 0)}회")
        lines.append("")
        
        # 거래 내역 요약
        if transactions:
            lines.append("  ▶ 거래 내역 (종목별)")
            lines.append("  " + "-" * 56)
            
            # 내부입고/출고 분리
            deposits = [t for t in transactions if t.get('tran_type') == '내부입고']
            withdrawals = [t for t in transactions if t.get('tran_type') == '내부출고']
            
            if deposits:
                lines.append("  [내부입고]")
                for t in sorted(deposits, key=lambda x: float(x.get('tran_amt', 0) or 0), reverse=True):
                    qty = float(t.get('tran_qty', 0) or 0)
                    amt = float(t.get('tran_amt', 0) or 0)
                    cnt = int(t.get('tran_cnt', 0) or 0)
                    lines.append(f"    - {t.get('coin_symbol', 'N/A')}: "
                               f"수량 {qty:,.4f}, "
                               f"금액 {amt:,.0f}원, "
                               f"건수 {cnt}건")
            
            if withdrawals:
                lines.append("  [내부출고]")
                for t in sorted(withdrawals, key=lambda x: float(x.get('tran_amt', 0) or 0), reverse=True):
                    qty = float(t.get('tran_qty', 0) or 0)
                    amt = float(t.get('tran_amt', 0) or 0)
                    cnt = int(t.get('tran_cnt', 0) or 0)
                    lines.append(f"    - {t.get('coin_symbol', 'N/A')}: "
                               f"수량 {qty:,.4f}, "
                               f"금액 {amt:,.0f}원, "
                               f"건수 {cnt}건")
        
        lines.append("")
    
    lines.append("=" * 80)
    
    return "\n".join(lines)