# str_dashboard/views.py

import json
import logging
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.decorators.http import require_POST
from django.conf import settings
from datetime import datetime, timedelta
import pandas as pd

from .db_utils import (
    OracleConnection, 
    OracleConnectionError,
    require_db_connection, 
    execute_query_with_error_handling
)
from .queries.rule_objectives import build_rule_to_objectives
from .queries.rule_historic_search import (
    fetch_df_result_0, 
    aggregate_by_rule_id_list,
    find_most_similar_rule_combinations
)

from .redshift_utils import (
    RedshiftConnection,
    RedshiftConnectionError,
    require_redshift_connection,
    execute_redshift_query_with_error_handling
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
    # Redshift 세션 정보 추가
    rs_info = request.session.get('rs_conn')
    # Rule 객관식 매핑 데이터 생성
    rule_obj_map = build_rule_to_objectives()
    
    context = {
        'active_top_menu': 'menu1',
        'active_sub_menu': 'menu1_1',
        # Oracle 상태 및 기본값
        'db_status': request.session.get('db_conn_status', 'need'),
        'default_host': '127.0.0.1',
        'default_port': '40112',
        'default_service': default_service,
        'default_username': db_info.get('username', '') if db_info else '',
        # Redshift 상태 및 기본값 추가
        'rs_status': request.session.get('rs_conn_status', 'need'),
        'default_rs_host': '127.0.0.1',
        'default_rs_port': '40127',
        'default_rs_dbname': 'prod',
        'default_rs_username': rs_info.get('username', '') if rs_info else '',
        # 기타
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
            
            # 세션 타임아웃 설정 (1시간)
            request.session.set_expiry(3600)
            
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
        result = execute_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='alert_info_by_alert_id.sql',
            bind_params={':alert_id': '?'},
            query_params=[alert_id]
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
            similar_list = find_most_similar_rule_combinations(rule_key, df1)
        
        logger.info(f"Rule history search completed. Found {len(rows)} matching rows for key: {rule_key}")
        
        if similar_list:
            logger.info(f"Found {len(similar_list)} similar combinations with similarity: {similar_list[0]['similarity']:.2f}")
        
        return JsonResponse({
            'success': True,
            'columns': columns,
            'rows': rows,
            'searched_rule': rule_key,
            'similar_list': similar_list
        })
        
    except Exception as e:
        logger.exception(f"Rule history search failed: {e}")
        return JsonResponse({
            'success': False,
            'message': f'히스토리 조회 실패: {e}'
        })


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
    
    # 통합 쿼리 실행 (이메일 파라미터 제외)
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='duplicate_unified.sql',
        bind_params={
            ':current_cust_id': '?',
            ':address': '?',
            ':detail_address': '?',
            ':workplace_name': '?',
            ':workplace_address': '?',
            ':workplace_detail_address': '?',
            ':phone_suffix': '?'
        },
        query_params=[
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
        ]  # 총 14개
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
    
    logger.info(f"Querying corp related persons for cust_id: {cust_id}")
    
    # 쿼리 실행
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='corp_related_persons.sql',
        bind_params={':cust_id': '?'},
        query_params=[cust_id]
    )
    
    if result.get('success'):
        logger.info(f"Corp related persons query successful - found {len(result.get('rows', []))} persons")
    else:
        logger.error(f"Corp related persons query failed: {result.get('message')}")
    
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
    """데이터베이스 세션 정보 초기화 (Oracle + Redshift)"""
    keys_to_clear = ['db_conn', 'db_conn_status', 'rs_conn', 'rs_conn_status']
    for key in keys_to_clear:
        if key in request.session:
            del request.session[key]
    logger.info("All database sessions cleared")


# str_dashboard/views.py에 추가할 함수
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
            'message': 'Missing required parameters: mem_id, start_date, end_date'
        })
    
    logger.info(f"Querying IP access history - MID: {mem_id}, period: {start_date} ~ {end_date}")
    
    try:
        # query_ip_access_history.sql 실행
        result = execute_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='query_ip_access_history.sql',  # 실제 파일명으로 수정
            bind_params={
                ':mem_id': '?',
                ':start_date': '?', 
                ':end_date': '?'
            },
            query_params=[mem_id, start_date, end_date]
        )
        
        if result.get('success'):
            # 해외 접속 건수 로깅
            rows = result.get('rows', [])
            columns = result.get('columns', [])
            
            if rows and columns:
                country_idx = columns.index('국가한글명') if '국가한글명' in columns else -1
                if country_idx >= 0:
                    foreign_count = sum(1 for row in rows 
                                      if row[country_idx] and 
                                      row[country_idx] not in ['대한민국', '한국'])
                    if foreign_count > 0:
                        logger.info(f"Found {foreign_count} foreign IP access records out of {len(rows)} total")
            
            logger.info(f"IP access history query successful - found {len(rows)} records")
        else:
            logger.error(f"IP access history query failed: {result.get('message')}")
        
        return JsonResponse(result)
        
    except Exception as e:
        logger.exception(f"Error in query_ip_access_history: {e}")
        return JsonResponse({
            'success': False,
            'message': f'IP 조회 중 오류: {e}'
        })
    


@login_required
@require_POST
def test_redshift_connection(request):
    """Redshift 데이터베이스 연결 테스트"""
    # 파라미터 검증
    required_params = ['host', 'port', 'dbname', 'username', 'password']
    params = {}
    
    for param in required_params:
        value = request.POST.get(param, '').strip() if param != 'password' else request.POST.get(param, '')
        if not value:
            return JsonResponse({
                'success': False,
                'message': f'{param}을(를) 입력해주세요.'
            })
        params[param] = value
    
    # 연결 테스트
    try:
        redshift_conn = RedshiftConnection(
            host=params['host'],
            port=params['port'],
            dbname=params['dbname'],
            username=params['username'],
            password=params['password']
        )
        
        if redshift_conn.test_connection():
            # 연결 성공 - 세션에 저장
            request.session['rs_conn_status'] = 'ok'
            request.session['rs_conn'] = {
                'host': params['host'],
                'port': params['port'],
                'dbname': params['dbname'],
                'username': params['username'],
                'password': params['password'],
            }
            
            logger.info(f"Redshift connection successful for user: {params['username']}")
            return JsonResponse({
                'success': True,
                'message': 'Redshift 연결에 성공했습니다.'
            })
        else:
            raise RedshiftConnectionError("연결 테스트 실패")
            
    except RedshiftConnectionError as e:
        request.session['rs_conn_status'] = 'need'
        logger.warning(f"Redshift connection failed: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        })
    except Exception as e:
        request.session['rs_conn_status'] = 'need'
        logger.exception(f"Unexpected error during Redshift connection test: {e}")
        return JsonResponse({
            'success': False,
            'message': f'연결 실패: {str(e)}'
        })
    



@login_required
@require_POST
def connect_all_databases(request):
    """Oracle과 Redshift 모두 연결"""
    oracle_status = 'fail'
    oracle_error = None
    redshift_status = 'fail'
    redshift_error = None
    
    # Oracle 연결
    try:
        oracle_params = {
            'host': request.POST.get('oracle_host', '').strip(),
            'port': request.POST.get('oracle_port', '').strip(),
            'service_name': request.POST.get('oracle_service_name', '').strip(),
            'username': request.POST.get('oracle_username', '').strip(),
            'password': request.POST.get('oracle_password', ''),
        }
        
        # 필수 파라미터 확인
        for key, value in oracle_params.items():
            if not value:
                oracle_error = f'Oracle {key} 누락'
                raise ValueError(oracle_error)
        
        # JDBC URL 생성
        jdbc_url = OracleConnection.build_jdbc_url(
            oracle_params['host'], 
            oracle_params['port'], 
            oracle_params['service_name']
        )
        
        # 연결 테스트
        oracle_conn = OracleConnection(
            jdbc_url=jdbc_url,
            username=oracle_params['username'],
            password=oracle_params['password']
        )
        
        if oracle_conn.test_connection():
            # 세션 저장
            request.session['db_conn_status'] = 'ok'
            request.session['db_conn'] = {
                'jdbc_url': jdbc_url,
                'driver_path': oracle_conn.driver_path,
                'driver_class': oracle_conn.driver_class,
                'username': oracle_params['username'],
                'password': oracle_params['password'],
            }
            oracle_status = 'ok'
            logger.info(f"Oracle connected: {oracle_params['username']}")
        else:
            oracle_error = 'Oracle 연결 테스트 실패'
            
    except Exception as e:
        oracle_error = str(e)
        request.session['db_conn_status'] = 'need'
        logger.error(f"Oracle connection failed: {e}")
    
    # Redshift 연결
    try:
        redshift_params = {
            'host': request.POST.get('redshift_host', '').strip(),
            'port': request.POST.get('redshift_port', '').strip(),
            'dbname': request.POST.get('redshift_dbname', '').strip(),
            'username': request.POST.get('redshift_username', '').strip(),
            'password': request.POST.get('redshift_password', ''),
        }
        
        # 필수 파라미터 확인
        for key, value in redshift_params.items():
            if not value:
                redshift_error = f'Redshift {key} 누락'
                raise ValueError(redshift_error)
        
        # 연결 테스트
        redshift_conn = RedshiftConnection(
            host=redshift_params['host'],
            port=redshift_params['port'],
            dbname=redshift_params['dbname'],
            username=redshift_params['username'],
            password=redshift_params['password']
        )
        
        if redshift_conn.test_connection():
            # 세션 저장
            request.session['rs_conn_status'] = 'ok'
            request.session['rs_conn'] = redshift_params
            redshift_status = 'ok'
            logger.info(f"Redshift connected: {redshift_params['username']}")
        else:
            redshift_error = 'Redshift 연결 테스트 실패'
            
    except Exception as e:
        redshift_error = str(e)
        request.session['rs_conn_status'] = 'need'
        logger.error(f"Redshift connection failed: {e}")
    
    # 세션 타임아웃 설정 (1시간)
    request.session.set_expiry(3600)
    
    # 결과 반환
    if oracle_status == 'ok' and redshift_status == 'ok':
        return JsonResponse({
            'success': True,
            'message': '모든 데이터베이스 연결 성공',
            'oracle_status': oracle_status,
            'redshift_status': redshift_status
        })
    else:
        return JsonResponse({
            'success': False,
            'message': '일부 데이터베이스 연결 실패',
            'oracle_status': oracle_status,
            'oracle_error': oracle_error,
            'redshift_status': redshift_status,
            'redshift_error': redshift_error
        })
    

    

def get_db_status(request) -> dict:
    """현재 데이터베이스 연결 상태 조회 (Oracle + Redshift)"""
    # Oracle 상태
    db_info = request.session.get('db_conn')
    oracle_status = request.session.get('db_conn_status', 'need')
    
    # Redshift 상태
    rs_info = request.session.get('rs_conn')
    redshift_status = request.session.get('rs_conn_status', 'need')
    
    return {
        'oracle': {
            'connected': oracle_status == 'ok',
            'status': oracle_status,
            'username': db_info.get('username', 'Unknown') if db_info else None,
            'jdbc_url': db_info.get('jdbc_url', '') if db_info else None
        },
        'redshift': {
            'connected': redshift_status == 'ok',
            'status': redshift_status,
            'username': rs_info.get('username', 'Unknown') if rs_info else None,
            'host': rs_info.get('host', '') if rs_info else None,
            'dbname': rs_info.get('dbname', '') if rs_info else None
        }
    }

# views.py에 추가할 내용

# Import 섹션에 추가
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# 전역 변수로 DataFrame 저장소 추가 (클래스나 캐시 시스템으로 개선 가능)
ORDERBOOK_CACHE = {}

@login_required
@require_POST
def query_redshift_orderbook(request):
    """
    Redshift에서 Orderbook 데이터 조회
    Alert 데이터의 거래 기간 + 1일을 기준으로 조회
    """
    # 파라미터 추출
    user_id = request.POST.get('user_id', '').strip()
    tran_start = request.POST.get('tran_start', '').strip()  # YYYY-MM-DD 형식
    tran_end = request.POST.get('tran_end', '').strip()      # YYYY-MM-DD 형식
    
    if not all([user_id, tran_start, tran_end]):
        return JsonResponse({
            'success': False,
            'message': 'Missing required parameters: user_id, tran_start, tran_end'
        })
    
    # Redshift 연결 확인
    rs_info = request.session.get('rs_conn')
    if not rs_info or request.session.get('rs_conn_status') != 'ok':
        return JsonResponse({
            'success': False,
            'message': 'Redshift 연결이 필요합니다.'
        })
    
    try:
        # 날짜 파싱 및 +1일 처리
        start_date = datetime.strptime(tran_start, '%Y-%m-%d')
        end_date = datetime.strptime(tran_end, '%Y-%m-%d')
        
        # +1일 적용
        start_date_plus1 = start_date + timedelta(days=1)
        end_date_plus1 = end_date + timedelta(days=1)
        
        # 타임스탬프 형식으로 변환
        start_time = start_date_plus1.strftime('%Y-%m-%d 00:00:00')
        end_time = end_date_plus1.strftime('%Y-%m-%d 23:59:59')
        
        logger.info(f"Querying Redshift orderbook - user_id: {user_id}, period: {start_time} ~ {end_time}")
        
        # Redshift 연결 생성
        redshift_conn = RedshiftConnection.from_session(rs_info)
        
        # SQL 파일 로드
        sql_path = Path(settings.BASE_DIR) / 'str_dashboard' / 'queries' / 'redshift_orderbook.sql'
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()
        
        # 쿼리 실행
        cols, rows = redshift_conn.execute_query(
            sql_query,
            params=[start_time, end_time, user_id]
        )
        
        if not cols or not rows:
            logger.info(f"No orderbook data found for user_id: {user_id}")
            return JsonResponse({
                'success': True,
                'message': 'No data found',
                'rows_count': 0,
                'cached': False
            })
        
        # DataFrame 생성
        df = pd.DataFrame(rows, columns=cols)
        
        # 캐시 키 생성 (날짜 형식 통일)
        cache_key = f"df_orderbook_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{user_id}"
        
        # 메모리에 저장
        ORDERBOOK_CACHE[cache_key] = {
            'dataframe': df,
            'created_at': datetime.now(),
            'user_id': user_id,
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d'),
            'start_time': start_time,
            'end_time': end_time,
            'rows_count': len(df),
            'columns': list(df.columns)
        }
        
        logger.info(f"Orderbook data cached with key: {cache_key}, rows: {len(df)}")
        
        # 응답 생성 (현재는 메타데이터만 반환)
        return JsonResponse({
            'success': True,
            'message': f'Orderbook 데이터를 메모리에 저장했습니다.',
            'cache_key': cache_key,
            'rows_count': len(df),
            'columns': list(df.columns),
            'period': {
                'original': f"{tran_start} ~ {tran_end}",
                'queried': f"{start_time} ~ {end_time}"
            },
            'cached': True
        })
        
    except Exception as e:
        logger.exception(f"Error in query_redshift_orderbook: {e}")
        return JsonResponse({
            'success': False,
            'message': f'Orderbook 조회 중 오류: {str(e)}'
        })


@login_required
def get_cached_orderbook_info(request):
    """
    캐시된 Orderbook 정보 조회
    """
    cache_info = []
    
    for key, value in ORDERBOOK_CACHE.items():
        cache_info.append({
            'cache_key': key,
            'user_id': value['user_id'],
            'period': f"{value['start_date']} ~ {value['end_date']}",
            'rows_count': value['rows_count'],
            'created_at': value['created_at'].strftime('%Y-%m-%d %H:%M:%S'),
            'columns_count': len(value['columns'])
        })
    
    return JsonResponse({
        'success': True,
        'cache_count': len(cache_info),
        'cached_data': cache_info
    })


@login_required
@require_POST
def clear_orderbook_cache(request):
    """
    Orderbook 캐시 초기화
    """
    cache_key = request.POST.get('cache_key', '').strip()
    
    if cache_key and cache_key in ORDERBOOK_CACHE:
        # 특정 캐시만 삭제
        del ORDERBOOK_CACHE[cache_key]
        logger.info(f"Cleared orderbook cache: {cache_key}")
        message = f'캐시 {cache_key}를 삭제했습니다.'
    elif not cache_key:
        # 전체 캐시 삭제
        count = len(ORDERBOOK_CACHE)
        ORDERBOOK_CACHE.clear()
        logger.info(f"Cleared all orderbook cache ({count} items)")
        message = f'전체 캐시 {count}개를 삭제했습니다.'
    else:
        return JsonResponse({
            'success': False,
            'message': '해당 캐시를 찾을 수 없습니다.'
        })
    
    return JsonResponse({
        'success': True,
        'message': message
    })


def get_orderbook_dataframe(cache_key: str) -> Optional[pd.DataFrame]:
    """
    캐시에서 DataFrame 가져오기 (내부 사용용)
    """
    if cache_key in ORDERBOOK_CACHE:
        return ORDERBOOK_CACHE[cache_key]['dataframe']
    return None






