# str_dashboard/views.py (리팩토링 버전)

import json
import logging
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.decorators.http import require_POST
from django.conf import settings
from datetime import datetime, timedelta
import pandas as pd
from typing import Optional, Dict, Any, List
from pathlib import Path
from .orderbook_analyzer import OrderbookAnalyzer
from .toml import toml_collector, toml_exporter
import tempfile
from django.http import FileResponse

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


# ==================== 세션 관리 헬퍼 클래스 ====================
class SessionManager:
    """세션 데이터 관리를 위한 헬퍼 클래스"""
    
    @staticmethod
    def save_data(request, key: str, data: Any, force_update: bool = True) -> None:
        """
        세션에 데이터 저장
        
        Args:
            request: Django request 객체
            key: 세션 키
            data: 저장할 데이터
            force_update: 세션 수정 플래그 강제 설정 여부
        """
        request.session[key] = data
        if force_update:
            request.session.modified = True
        logger.debug(f"Session saved: {key} (size: {len(str(data)) if data else 0} bytes)")
    
    @staticmethod
    def save_multiple(request, data_dict: Dict[str, Any]) -> None:
        """
        여러 데이터를 한번에 세션에 저장
        
        Args:
            request: Django request 객체
            data_dict: {key: value} 형태의 딕셔너리
        """
        for key, value in data_dict.items():
            request.session[key] = value
        request.session.modified = True
        logger.debug(f"Session saved: {len(data_dict)} items")
    
    @staticmethod
    def get_data(request, key: str, default: Any = None) -> Any:
        """세션에서 데이터 가져오기"""
        return request.session.get(key, default)
    
    @staticmethod
    def clear_keys(request, keys: List[str]) -> None:
        """특정 키들 삭제"""
        for key in keys:
            if key in request.session:
                del request.session[key]
        request.session.modified = True
    
    @staticmethod
    def clear_pattern(request, pattern: str) -> None:
        """특정 패턴으로 시작하는 모든 키 삭제"""
        keys_to_remove = [k for k in request.session.keys() if k.startswith(pattern)]
        for key in keys_to_remove:
            del request.session[key]
        if keys_to_remove:
            request.session.modified = True
            logger.debug(f"Cleared {len(keys_to_remove)} session keys with pattern: {pattern}")


# ==================== 뷰 함수들 ====================

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
    
    db_info = SessionManager.get_data(request, 'db_conn')
    if db_info and isinstance(db_info, dict):
        jdbc_url = db_info.get('jdbc_url', '')
        if jdbc_url.startswith('jdbc:oracle:thin:@//'):
            try:
                default_service = jdbc_url.split('/', 3)[-1]
            except Exception:
                pass
    
    rs_info = SessionManager.get_data(request, 'rs_conn')
    rule_obj_map = build_rule_to_objectives()
    
    context = {
        'active_top_menu': 'menu1',
        'active_sub_menu': 'menu1_1',
        'db_status': SessionManager.get_data(request, 'db_conn_status', 'need'),
        'default_host': '127.0.0.1',
        'default_port': '40112',
        'default_service': default_service,
        'default_username': db_info.get('username', '') if db_info else '',
        'rs_status': SessionManager.get_data(request, 'rs_conn_status', 'need'),
        'default_rs_host': '127.0.0.1',
        'default_rs_port': '40127',
        'default_rs_dbname': 'prod',
        'default_rs_username': rs_info.get('username', '') if rs_info else '',
        'rule_obj_map_json': json.dumps(rule_obj_map, ensure_ascii=False),
    }
    return render(request, 'str_dashboard/menu1_1/main.html', context)


@login_required
@require_POST
def test_oracle_connection(request):
    """Oracle 데이터베이스 연결 테스트"""
    required_params = ['host', 'port', 'service_name', 'username', 'password']
    params = {}
    
    for param in required_params:
        value = request.POST.get(param, '').strip() if param != 'password' else request.POST.get(param, '')
        if not value:
            return HttpResponseBadRequest(f'Missing parameter: {param}')
        params[param] = value
    
    jdbc_url = OracleConnection.build_jdbc_url(
        params['host'], 
        params['port'], 
        params['service_name']
    )
    
    try:
        oracle_conn = OracleConnection(
            jdbc_url=jdbc_url,
            username=params['username'],
            password=params['password']
        )
        
        if oracle_conn.test_connection():
            SessionManager.save_multiple(request, {
                'db_conn_status': 'ok',
                'db_conn': {
                    'jdbc_url': jdbc_url,
                    'driver_path': oracle_conn.driver_path,
                    'driver_class': oracle_conn.driver_class,
                    'username': params['username'],
                    'password': params['password'],
                }
            })
            
            request.session.set_expiry(3600)
            logger.info(f"Oracle connection successful for user: {params['username']}")
            
            return JsonResponse({
                'success': True,
                'message': '연결에 성공했습니다.'
            })
        else:
            raise OracleConnectionError("연결 테스트 실패")
            
    except OracleConnectionError as e:
        SessionManager.save_data(request, 'db_conn_status', 'need')
        logger.warning(f"Oracle connection failed: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        })
    except Exception as e:
        SessionManager.save_data(request, 'db_conn_status', 'need')
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
        result = execute_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='alert_info_by_alert_id.sql',
            bind_params={':alert_id': '?'},
            query_params=[alert_id]
        )
        
        if result.get('success'):
            # Rule 객관식 매핑 추가
            rule_obj_map = build_rule_to_objectives()
            
            # Alert 데이터 처리
            cols = result.get('columns', [])
            rows = result.get('rows', [])
            
            # canonical_ids와 rep_rule_id 추출
            canonical_ids = []
            rep_rule_id = None
            cust_id = None
            
            if cols and rows:
                rule_idx = cols.index('STR_RULE_ID') if 'STR_RULE_ID' in cols else -1
                alert_idx = cols.index('STR_ALERT_ID') if 'STR_ALERT_ID' in cols else -1
                cust_idx = cols.index('CUST_ID') if 'CUST_ID' in cols else -1
                
                if rule_idx >= 0:
                    seen = set()
                    for row in rows:
                        rule_id = row[rule_idx]
                        if rule_id and rule_id not in seen:
                            seen.add(rule_id)
                            canonical_ids.append(rule_id)
                
                # 대표 ALERT의 RULE ID 찾기
                if alert_idx >= 0:
                    for row in rows:
                        if str(row[alert_idx]) == str(alert_id):
                            if rule_idx >= 0:
                                rep_rule_id = row[rule_idx]
                            if cust_idx >= 0:
                                cust_id = row[cust_idx]
                            break
            
            # 세션에 저장 - 구조화된 데이터
            SessionManager.save_multiple(request, {
                'current_alert_id': alert_id,
                'current_alert_data': {
                    'alert_id': alert_id,
                    'cols': cols,
                    'rows': rows,
                    'canonical_ids': canonical_ids,
                    'rep_rule_id': rep_rule_id,
                    'custIdForPerson': cust_id,
                    'rule_obj_map': rule_obj_map  # 객관식 정보 추가
                }
            })
            
            result['canonical_ids'] = canonical_ids
            result['rep_rule_id'] = rep_rule_id
        
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
    """통합 고객 정보 조회"""
    cust_id = request.POST.get('cust_id', '').strip()
    
    if not cust_id:
        return HttpResponseBadRequest('Missing cust_id.')
    
    logger.info(f"Querying unified customer info for cust_id: {cust_id}")
    
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='customer_unified_info.sql',
        bind_params={':custId': '?'},
        query_params=[cust_id]
    )
    
    if result.get('success'):
        columns = result.get('columns', [])
        rows = result.get('rows', [])
        
        customer_type = None
        if rows and len(rows) > 0:
            cust_type_idx = columns.index('고객구분') if '고객구분' in columns else -1
            if cust_type_idx >= 0:
                customer_type = rows[0][cust_type_idx]
        
        result['customer_type'] = customer_type
        
        SessionManager.save_data(request, 'current_customer_data', {
            'columns': columns,
            'rows': rows,
            'customer_type': customer_type,
            'cust_id': cust_id
        })
        
        logger.info(f"Unified query successful - customer_type: {customer_type}, rows: {len(rows)}")
    
    return JsonResponse(result)


@login_required
@require_POST
@require_db_connection
def rule_history_search(request, oracle_conn=None):
    """RULE 히스토리 검색"""
    rule_key = request.POST.get('rule_key', '').strip()
    if not rule_key:
        return HttpResponseBadRequest('Missing rule_key.')
    
    try:
        db_info = SessionManager.get_data(request, 'db_conn')
        
        df0 = fetch_df_result_0(
            jdbc_url=db_info['jdbc_url'],
            driver_class=db_info['driver_class'],
            driver_path=db_info['driver_path'],
            username=db_info['username'],
            password=db_info['password']
        )
        
        df1 = aggregate_by_rule_id_list(df0)
        matching_rows = df1[df1["STR_RULE_ID_LIST"] == rule_key]
        
        columns = list(matching_rows.columns) if not matching_rows.empty else list(df1.columns) if not df1.empty else []
        rows = matching_rows.values.tolist()
        
        similar_list = []
        if len(rows) == 0 and not df1.empty:
            similar_list = find_most_similar_rule_combinations(rule_key, df1)
        
        SessionManager.save_data(request, 'current_rule_history_data', {
            'columns': columns,
            'rows': rows,
            'searched_rule': rule_key,
            'similar_list': similar_list
        })
        
        logger.info(f"Rule history search completed. Found {len(rows)} matching rows for key: {rule_key}")
        
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
    """통합 중복 회원 조회"""
    current_cust_id = request.POST.get('current_cust_id', '').strip()
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
    
    logger.debug(f"Duplicate search params - cust_id: {current_cust_id}")
    
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
            current_cust_id, address, address, detail_address,
            current_cust_id, workplace_name, workplace_name,
            current_cust_id, workplace_address, workplace_address, 
            workplace_detail_address, workplace_detail_address,
            phone_suffix, phone_suffix
        ]
    )
    
    if result.get('success'):
        SessionManager.save_data(request, 'duplicate_persons_data', {
            'columns': result.get('columns', []),
            'rows': result.get('rows', []),
            'match_criteria': {
                'email': full_email,
                'phone_suffix': phone_suffix,
                'address': address,
                'detail_address': detail_address,
                'workplace_name': workplace_name,
                'workplace_address': workplace_address
            }
        })
        
        logger.info(f"Duplicate search successful - found {len(result.get('rows', []))} records")
    
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
    
    result = execute_query_with_error_handling(
        oracle_conn=oracle_conn,
        sql_filename='corp_related_persons.sql',
        bind_params={':cust_id': '?'},
        query_params=[cust_id]
    )
    
    if result.get('success'):
        SessionManager.save_data(request, 'current_corp_related_data', {
            'columns': result.get('columns', []),
            'rows': result.get('rows', [])
        })
        logger.info(f"Corp related persons query successful - found {len(result.get('rows', []))} persons")
    
    return JsonResponse(result)


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
        result = execute_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='person_related_summary.sql',
            bind_params={
                ':cust_id': '?',
                ':start_date': '?',
                ':end_date': '?'
            },
            query_params=[
                start_date, end_date, cust_id,
                cust_id, start_date, end_date
            ]
        )
        
        if not result.get('success'):
            return JsonResponse(result)
        
        columns = result.get('columns', [])
        rows = result.get('rows', [])
        
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
                related_persons[cust_id_val]['transactions'].append({
                    'coin_symbol': row[15],
                    'tran_type': row[16],
                    'tran_qty': row[17],
                    'tran_amt': row[18],
                    'tran_cnt': row[19]
                })
        
        SessionManager.save_data(request, 'current_person_related_data', related_persons)
        
        summary_text = format_related_person_summary(related_persons)
        
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
    """관련인 정보를 텍스트 형식으로 변환"""
    if not related_persons:
        return "내부입출금 거래 관련인이 없습니다."
    
    lines = []
    lines.append("=" * 80)
    lines.append("【 개인 고객 관련인 정보 (내부입출금 거래 상대방) 】")
    lines.append("=" * 80)
    
    for idx, (cust_id, data) in enumerate(related_persons.items(), 1):
        info = data.get('info')
        transactions = data.get('transactions', [])
        
        if not info:
            continue
        
        lines.append(f"\n◆ 관련인 {idx}: {info.get('name', 'N/A')} (CID: {cust_id})")
        lines.append("-" * 60)
        
        # 기본 정보
        lines.append(f"  • 실명번호: {info.get('id_number', 'N/A')}")
        lines.append(f"  • 생년월일: {info.get('birth_date', 'N/A')} (만 {info.get('age', 'N/A')}세)")
        lines.append(f"  • 성별: {info.get('gender', 'N/A')}")
        lines.append(f"  • 거주지: {info.get('address', 'N/A')}")
        
        if info.get('job'):
            lines.append(f"  • 직업: {info.get('job')}")
        if info.get('workplace'):
            lines.append(f"  • 직장명: {info.get('workplace')}")
        
        lines.append(f"  • 위험등급: {info.get('risk_grade', 'N/A')}")
        lines.append(f"  • 총 거래횟수: {info.get('total_tran_count', 0)}회")
        
        # 거래 내역
        if transactions:
            lines.append("\n  ▶ 거래 내역")
            deposits = [t for t in transactions if t.get('tran_type') == '내부입고']
            withdrawals = [t for t in transactions if t.get('tran_type') == '내부출고']
            
            if deposits:
                lines.append("  [내부입고]")
                for t in deposits:
                    qty = float(t.get('tran_qty', 0) or 0)
                    amt = float(t.get('tran_amt', 0) or 0)
                    cnt = int(t.get('tran_cnt', 0) or 0)
                    lines.append(f"    - {t.get('coin_symbol')}: {qty:,.4f}개, {amt:,.0f}원, {cnt}건")
            
            if withdrawals:
                lines.append("  [내부출고]")
                for t in withdrawals:
                    qty = float(t.get('tran_qty', 0) or 0)
                    amt = float(t.get('tran_amt', 0) or 0)
                    cnt = int(t.get('tran_cnt', 0) or 0)
                    lines.append(f"    - {t.get('coin_symbol')}: {qty:,.4f}개, {amt:,.0f}원, {cnt}건")
    
    lines.append("\n" + "=" * 80)
    return "\n".join(lines)


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
        result = execute_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='query_ip_access_history.sql',
            bind_params={
                ':mem_id': '?',
                ':start_date': '?', 
                ':end_date': '?'
            },
            query_params=[mem_id, start_date, end_date]
        )
        
        if result.get('success'):
            SessionManager.save_data(request, 'ip_history_data', {
                'columns': result.get('columns', []),
                'rows': result.get('rows', [])
            })
            logger.info(f"IP access history query successful - found {len(result.get('rows', []))} records")
        
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
    
    try:
        redshift_conn = RedshiftConnection(
            host=params['host'],
            port=params['port'],
            dbname=params['dbname'],
            username=params['username'],
            password=params['password']
        )
        
        if redshift_conn.test_connection():
            SessionManager.save_multiple(request, {
                'rs_conn_status': 'ok',
                'rs_conn': params
            })
            
            logger.info(f"Redshift connection successful for user: {params['username']}")
            return JsonResponse({
                'success': True,
                'message': 'Redshift 연결에 성공했습니다.'
            })
        else:
            raise RedshiftConnectionError("연결 테스트 실패")
            
    except Exception as e:
        SessionManager.save_data(request, 'rs_conn_status', 'need')
        logger.exception(f"Redshift connection test failed: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
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
        
        for key, value in oracle_params.items():
            if not value:
                oracle_error = f'Oracle {key} 누락'
                raise ValueError(oracle_error)
        
        jdbc_url = OracleConnection.build_jdbc_url(
            oracle_params['host'], 
            oracle_params['port'], 
            oracle_params['service_name']
        )
        
        oracle_conn = OracleConnection(
            jdbc_url=jdbc_url,
            username=oracle_params['username'],
            password=oracle_params['password']
        )
        
        if oracle_conn.test_connection():
            SessionManager.save_multiple(request, {
                'db_conn_status': 'ok',
                'db_conn': {
                    'jdbc_url': jdbc_url,
                    'driver_path': oracle_conn.driver_path,
                    'driver_class': oracle_conn.driver_class,
                    'username': oracle_params['username'],
                    'password': oracle_params['password'],
                }
            })
            oracle_status = 'ok'
            logger.info(f"Oracle connected: {oracle_params['username']}")
            
    except Exception as e:
        oracle_error = str(e)
        SessionManager.save_data(request, 'db_conn_status', 'need')
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
        
        for key, value in redshift_params.items():
            if not value:
                redshift_error = f'Redshift {key} 누락'
                raise ValueError(redshift_error)
        
        redshift_conn = RedshiftConnection(
            host=redshift_params['host'],
            port=redshift_params['port'],
            dbname=redshift_params['dbname'],
            username=redshift_params['username'],
            password=redshift_params['password']
        )
        
        if redshift_conn.test_connection():
            SessionManager.save_multiple(request, {
                'rs_conn_status': 'ok',
                'rs_conn': redshift_params
            })
            redshift_status = 'ok'
            logger.info(f"Redshift connected: {redshift_params['username']}")
            
    except Exception as e:
        redshift_error = str(e)
        SessionManager.save_data(request, 'rs_conn_status', 'need')
        logger.error(f"Redshift connection failed: {e}")
    
    request.session.set_expiry(3600)
    
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
    """현재 데이터베이스 연결 상태 조회"""
    db_info = SessionManager.get_data(request, 'db_conn')
    oracle_status = SessionManager.get_data(request, 'db_conn_status', 'need')
    
    rs_info = SessionManager.get_data(request, 'rs_conn')
    redshift_status = SessionManager.get_data(request, 'rs_conn_status', 'need')
    
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


@login_required
def check_db_status(request):
    """데이터베이스 연결 상태 확인 API"""
    return JsonResponse(get_db_status(request))


def clear_db_session(request):
    """데이터베이스 세션 정보 초기화"""
    SessionManager.clear_keys(request, ['db_conn', 'db_conn_status', 'rs_conn', 'rs_conn_status'])
    logger.info("All database sessions cleared")


# 전역 변수로 DataFrame 저장소
ORDERBOOK_CACHE = {}


@login_required
@require_POST
def query_redshift_orderbook(request):
    """Redshift에서 Orderbook 데이터 조회"""
    user_id = request.POST.get('user_id', '').strip()
    tran_start = request.POST.get('tran_start', '').strip()
    tran_end = request.POST.get('tran_end', '').strip()
    
    if not all([user_id, tran_start, tran_end]):
        return JsonResponse({
            'success': False,
            'message': 'Missing required parameters'
        })
    
    rs_info = SessionManager.get_data(request, 'rs_conn')
    if not rs_info or SessionManager.get_data(request, 'rs_conn_status') != 'ok':
        return JsonResponse({
            'success': False,
            'message': 'Redshift 연결이 필요합니다.'
        })
    
    try:
        start_date = datetime.strptime(tran_start, '%Y-%m-%d')
        end_date = datetime.strptime(tran_end, '%Y-%m-%d')
        
        start_date_plus1 = start_date + timedelta(days=1)
        end_date_plus1 = end_date + timedelta(days=1)
        
        start_time = start_date_plus1.strftime('%Y-%m-%d 00:00:00')
        end_time = end_date_plus1.strftime('%Y-%m-%d 23:59:59')
        
        logger.info(f"Querying Redshift orderbook - user_id: {user_id}")
        
        redshift_conn = RedshiftConnection.from_session(rs_info)
        
        sql_path = Path(settings.BASE_DIR) / 'str_dashboard' / 'queries' / 'redshift_orderbook.sql'
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()
        
        cols, rows = redshift_conn.execute_query(
            sql_query,
            params=[start_time, end_time, user_id]
        )
        
        if not cols or not rows:
            return JsonResponse({
                'success': True,
                'message': 'No data found',
                'rows_count': 0,
                'cached': False
            })
        
        df = pd.DataFrame(rows, columns=cols)
        cache_key = f"df_orderbook_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{user_id}"
        
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
    """캐시된 Orderbook 정보 조회"""
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
    """Orderbook 캐시 초기화"""
    cache_key = request.POST.get('cache_key', '').strip()
    
    if cache_key and cache_key in ORDERBOOK_CACHE:
        del ORDERBOOK_CACHE[cache_key]
        logger.info(f"Cleared orderbook cache: {cache_key}")
        message = f'캐시 {cache_key}를 삭제했습니다.'
    elif not cache_key:
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
    """캐시에서 DataFrame 가져오기"""
    if cache_key in ORDERBOOK_CACHE:
        return ORDERBOOK_CACHE[cache_key]['dataframe']
    return None


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
    
    df = get_orderbook_dataframe(cache_key)
    
    if df is None:
        return JsonResponse({
            'success': False,
            'message': f'캐시된 데이터를 찾을 수 없습니다: {cache_key}'
        })
    
    try:
        analyzer = OrderbookAnalyzer(df)
        analyzer.analyze()
        
        text_summary = analyzer.generate_text_summary()
        patterns = analyzer.get_pattern_analysis()
        daily_summary = analyzer.get_daily_summary()
        
        period_info = {}
        if cache_key in ORDERBOOK_CACHE:
            cache_data = ORDERBOOK_CACHE[cache_key]
            period_info = {
                'start_date': cache_data['start_date'],
                'end_date': cache_data['end_date'],
                'query_start': cache_data['start_time'],
                'query_end': cache_data['end_time']
            }
        
        if cache_key in ORDERBOOK_CACHE:
            ORDERBOOK_CACHE[cache_key]['analysis'] = {
                'text_summary': text_summary,
                'patterns': patterns,
                'daily_summary': daily_summary,
                'period_info': period_info,
                'analyzed_at': datetime.now()
            }
        
        SessionManager.save_data(request, 'current_orderbook_analysis', {
            'patterns': patterns,
            'period_info': period_info,
            'text_summary': text_summary,
            'cache_key': cache_key
        })
        
        logger.info(f"Orderbook analysis completed for {cache_key}")
        
        daily_json = daily_summary.to_dict('records') if not daily_summary.empty else []
        
        return JsonResponse({
            'success': True,
            'cache_key': cache_key,
            'daily_summary': daily_json,
            'text_summary': text_summary,
            'patterns': patterns,
            'period_info': period_info,
            'alert_details': {}
        })
        
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
    
    if cache_key not in ORDERBOOK_CACHE:
        return JsonResponse({
            'success': False,
            'message': f'캐시된 데이터를 찾을 수 없습니다: {cache_key}'
        })
    
    cache_data = ORDERBOOK_CACHE[cache_key]
    
    if 'analysis' not in cache_data:
        return JsonResponse({
            'success': False,
            'message': '분석이 수행되지 않았습니다.',
            'analyzed': False
        })
    
    analysis = cache_data['analysis']
    
    return JsonResponse({
        'success': True,
        'cache_key': cache_key,
        'user_id': cache_data['user_id'],
        'period': f"{cache_data['start_date']} ~ {cache_data['end_date']}",
        'rows_count': cache_data['rows_count'],
        'text_summary': analysis['text_summary'],
        'patterns': analysis['patterns'],
        'analyzed_at': analysis['analyzed_at'].strftime('%Y-%m-%d %H:%M:%S'),
        'analyzed': True
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
    
    df = get_orderbook_dataframe(cache_key)
    
    if df is None:
        return JsonResponse({
            'success': False,
            'message': f'캐시된 데이터를 찾을 수 없습니다: {cache_key}'
        })
    
    try:
        df_filtered = df[
            (pd.to_datetime(df['trade_date']) >= pd.to_datetime(start_date)) &
            (pd.to_datetime(df['trade_date']) <= pd.to_datetime(end_date))
        ].copy()
        
        if df_filtered.empty:
            return JsonResponse({
                'success': True,
                'alert_id': alert_id,
                'detail': {
                    'summary': {
                        'buy_amount': 0, 'buy_count': 0,
                        'sell_amount': 0, 'sell_count': 0,
                        'deposit_krw': 0, 'deposit_krw_count': 0,
                        'withdraw_krw': 0, 'withdraw_krw_count': 0,
                        'deposit_crypto': 0, 'deposit_crypto_count': 0,
                        'withdraw_crypto': 0, 'withdraw_crypto_count': 0
                    },
                    'by_ticker': {}
                }
            })
        
        analyzer = OrderbookAnalyzer(df_filtered)
        analyzer.analyze()
        patterns = analyzer.get_pattern_analysis()
        
        by_ticker = {
            'buy': patterns.get('buy_details', [])[:10],
            'sell': patterns.get('sell_details', [])[:10],
            'deposit': patterns.get('deposit_crypto_details', [])[:10],
            'withdraw': patterns.get('withdraw_crypto_details', [])[:10]
        }
        
        for action in by_ticker:
            by_ticker[action] = [
                {
                    'ticker': ticker,
                    'amount': data['amount_krw'],
                    'count': data['count']
                }
                for ticker, data in by_ticker[action]
            ]
        
        detail = {
            'summary': {
                'buy_amount': patterns.get('total_buy_amount', 0),
                'buy_count': patterns.get('total_buy_count', 0),
                'sell_amount': patterns.get('total_sell_amount', 0),
                'sell_count': patterns.get('total_sell_count', 0),
                'deposit_krw': patterns.get('total_deposit_krw', 0),
                'deposit_krw_count': patterns.get('total_deposit_krw_count', 0),
                'withdraw_krw': patterns.get('total_withdraw_krw', 0),
                'withdraw_krw_count': patterns.get('total_withdraw_krw_count', 0),
                'deposit_crypto': patterns.get('total_deposit_crypto', 0),
                'deposit_crypto_count': patterns.get('total_deposit_crypto_count', 0),
                'withdraw_crypto': patterns.get('total_withdraw_crypto', 0),
                'withdraw_crypto_count': patterns.get('total_withdraw_crypto_count', 0)
            },
            'by_ticker': by_ticker
        }
        
        return JsonResponse({
            'success': True,
            'alert_id': alert_id,
            'period': f"{start_date} ~ {end_date}",
            'detail': detail
        })
        
    except Exception as e:
        logger.exception(f"Error analyzing alert orderbook: {e}")
        return JsonResponse({
            'success': False,
            'message': f'분석 중 오류: {str(e)}'
        })



@login_required
@require_POST
def analyze_stds_dtm_orderbook(request):
    """대표 ALERT의 거래 기간(TRAN_STRT ~ TRAN_END)에 대한 Orderbook 요약"""
    # 단일 날짜 또는 기간 범위를 모두 지원
    stds_date = request.POST.get('stds_date', '').strip()
    start_date = request.POST.get('start_date', '').strip()
    end_date = request.POST.get('end_date', '').strip()
    cache_key = request.POST.get('cache_key', '').strip()
    
    # 파라미터 검증: cache_key는 필수, 날짜는 단일 날짜 또는 기간 중 하나 필요
    if not cache_key or (not stds_date and not (start_date and end_date)):
        return JsonResponse({
            'success': False,
            'message': 'Missing required parameters'
        })
    
    df = get_orderbook_dataframe(cache_key)
    
    if df is None:
        return JsonResponse({
            'success': False,
            'message': f'캐시된 데이터를 찾을 수 없습니다: {cache_key}'
        })
    
    try:
        # 날짜 필터링 방식 결정: 단일 날짜 또는 기간 범위
        if stds_date:
            # 기존 단일 날짜 방식
            target_date = pd.to_datetime(stds_date).date()
            df_filtered = df[pd.to_datetime(df['trade_date']).dt.date == target_date].copy()
            date_display = stds_date  # 화면 표시용
        else:
            # 새로운 기간 범위 방식
            start_dt = pd.to_datetime(start_date).date()
            end_dt = pd.to_datetime(end_date).date()
            df_filtered = df[
                (pd.to_datetime(df['trade_date']).dt.date >= start_dt) & 
                (pd.to_datetime(df['trade_date']).dt.date <= end_dt)
            ].copy()
            date_display = f"{start_date} ~ {end_date}"  # 화면 표시용
        
        if df_filtered.empty:
            return JsonResponse({
                'success': True,
                'date': date_display,
                'summary': {
                    'total_records': 0,
                    'message': '해당 기간의 거래 데이터가 없습니다.'
                }
            })
        
        analyzer = OrderbookAnalyzer(df_filtered)
        analyzer.analyze()
        patterns = analyzer.get_pattern_analysis()
        
        summary = {
            'date': date_display,  # 단일 날짜 또는 기간 범위
            'total_records': len(df_filtered),
            'buy_amount': patterns.get('total_buy_amount', 0),
            'buy_count': patterns.get('total_buy_count', 0),
            'buy_details': [
                {'ticker': t, 'amount': d['amount_krw'], 'count': d['count']}
                for t, d in (patterns.get('buy_details', [])[:10])
            ],
            'sell_amount': patterns.get('total_sell_amount', 0),
            'sell_count': patterns.get('total_sell_count', 0),
            'sell_details': [
                {'ticker': t, 'amount': d['amount_krw'], 'count': d['count']}
                for t, d in (patterns.get('sell_details', [])[:10])
            ],
            'deposit_krw_amount': patterns.get('total_deposit_krw', 0),
            'deposit_krw_count': patterns.get('total_deposit_krw_count', 0),
            'withdraw_krw_amount': patterns.get('total_withdraw_krw', 0),
            'withdraw_krw_count': patterns.get('total_withdraw_krw_count', 0),
            'deposit_crypto_amount': patterns.get('total_deposit_crypto', 0),
            'deposit_crypto_count': patterns.get('total_deposit_crypto_count', 0),
            'deposit_crypto_details': [
                {'ticker': t, 'amount': d['amount_krw'], 'count': d['count']}
                for t, d in (patterns.get('deposit_crypto_details', [])[:10])
            ],
            'withdraw_crypto_amount': patterns.get('total_withdraw_crypto', 0),
            'withdraw_crypto_count': patterns.get('total_withdraw_crypto_count', 0),
            'withdraw_crypto_details': [
                {'ticker': t, 'amount': d['amount_krw'], 'count': d['count']}
                for t, d in (patterns.get('withdraw_crypto_details', [])[:10])
            ]
        }
        
        SessionManager.save_data(request, 'current_stds_dtm_summary', summary)
        
        return JsonResponse({
            'success': True,
            'summary': summary
        })
        
    except Exception as e:
        logger.exception(f"Error analyzing STDS_DTM orderbook: {e}")
        return JsonResponse({
            'success': False,
            'message': f'분석 중 오류: {str(e)}'
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
    


@login_required
def download_toml(request):
    """준비된 TOML 파일 다운로드"""
    try:
        tmp_path = SessionManager.get_data(request, 'toml_temp_path')
        if not tmp_path or not Path(tmp_path).exists():
            return JsonResponse({
                'success': False,
                'message': 'TOML 파일을 찾을 수 없습니다.'
            })
        
        alert_id = SessionManager.get_data(request, 'current_alert_id', 'unknown')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'str_data_{alert_id}_{timestamp}.toml'
        
        response = FileResponse(
            open(tmp_path, 'rb'),
            as_attachment=True,
            filename=filename
        )
        
        def cleanup():
            try:
                Path(tmp_path).unlink()
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
def prepare_toml_data(request):
    """화면에 렌더링된 데이터를 수집하여 TOML 형식으로 준비"""
    try:
        # 세션에서 모든 관련 데이터 수집
        session_data = {
            'current_alert_data': SessionManager.get_data(request, 'current_alert_data', {}),
            'current_alert_id': SessionManager.get_data(request, 'current_alert_id', ''),
            'current_customer_data': SessionManager.get_data(request, 'current_customer_data', {}),
            'current_corp_related_data': SessionManager.get_data(request, 'current_corp_related_data', {}),
            'current_person_related_data': SessionManager.get_data(request, 'current_person_related_data', {}),
            'current_rule_history_data': SessionManager.get_data(request, 'current_rule_history_data', {}),
            'duplicate_persons_data': SessionManager.get_data(request, 'duplicate_persons_data', {}),
            'ip_history_data': SessionManager.get_data(request, 'ip_history_data', {}),
            'current_orderbook_analysis': SessionManager.get_data(request, 'current_orderbook_analysis', {}),
            'current_stds_dtm_summary': SessionManager.get_data(request, 'current_stds_dtm_summary', {})
        }
        
        # 디버깅 로그
        logger.info("=== Session Data Keys ===")
        for key, value in session_data.items():
            if value:
                logger.info(f"{key}: {type(value)}, size: {len(str(value))}")
                if isinstance(value, dict):
                    logger.info(f"  keys: {list(value.keys())}")
        
        # TOML 파일명 생성
        alert_id = SessionManager.get_data(request, 'current_alert_id', 'unknown')
        filename = toml_exporter.generate_filename(alert_id)
        
        # 임시 파일 경로 생성
        tmp_dir = tempfile.gettempdir()
        tmp_path = Path(tmp_dir) / filename
        
        # TOML 내보내기 (toml_exporter 사용)
        success = toml_exporter.export_to_toml(session_data, str(tmp_path))
        
        if success:
            # 세션에 임시 파일 경로 저장
            SessionManager.save_data(request, 'toml_temp_path', str(tmp_path))
            
            # 수집된 데이터 정보 가져오기
            collected_data = toml_collector.collect_all_data(session_data)
            
            return JsonResponse({
                'success': True,
                'message': 'TOML 데이터 준비 완료',
                'data_count': len(collected_data),
                'sections': list(collected_data.keys()),
                'filename': filename
            })
        else:
            return JsonResponse({
                'success': False,
                'message': 'TOML 데이터 준비 실패'
            })
            
    except Exception as e:
        logger.exception(f"Error preparing TOML data: {e}")
        return JsonResponse({
            'success': False,
            'message': f'TOML 데이터 준비 실패: {str(e)}'
        })
    






