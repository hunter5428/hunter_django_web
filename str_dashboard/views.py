# str_dashboard/views.py
import json
import logging
import pandas as pd
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.decorators.http import require_POST

from .db.oracle import OracleConnection, OracleConnectionError, require_db_connection, SQLQueryManager
from .db.redshift import RedshiftConnection, RedshiftConnectionError

logger = logging.getLogger(__name__)

# (home, menu1_1, test_oracle_connection, test_redshift_connection 함수는 이전과 동일)
@login_required
def home(request):
    """홈 페이지"""
    return render(request, 'str_dashboard/home.html')


@login_required
def menu1_1(request):
    """ALERT ID 조회 페이지"""
    context = {
        'active_top_menu': 'menu1',
        'active_sub_menu': 'menu1_1',
        'db_status': request.session.get('db_conn_status', 'need'),
        'rs_status': request.session.get('rs_conn_status', 'need'),
        # 기본값 설정
        'default_host': '127.0.0.1',
        'default_port': '40112',
        'default_service': 'PRDAMLKR.OCIAMLPRODDBA.OCIAMLPROD.ORACLEVCN.COM',
        'default_rs_host': '127.0.0.1',
        'default_rs_port': '40127',
        'default_rs_dbname': 'prod',
    }
    return render(request, 'str_dashboard/menu1_1/main.html', context)


@login_required
@require_POST
def test_oracle_connection(request):
    """Oracle DB 연결 테스트"""
    data = json.loads(request.body)
    host = data.get('host')
    port = data.get('port')
    service = data.get('service_name')
    username = data.get('username')
    password = data.get('password')

    if not all([host, port, service, username, password]):
        return JsonResponse({'success': False, 'message': '모든 필드를 입력해주세요.'})

    jdbc_url = OracleConnection.build_jdbc_url(host, port, service)
    try:
        conn = OracleConnection(jdbc_url, username, password)
        if conn.test_connection():
            request.session['db_conn'] = {'jdbc_url': jdbc_url, 'username': username, 'password': password}
            request.session['db_conn_status'] = 'ok'
            return JsonResponse({'success': True, 'message': 'Oracle 연결에 성공했습니다.'})
        else:
            raise OracleConnectionError("연결 테스트 실패")
    except OracleConnectionError as e:
        request.session['db_conn_status'] = 'error'
        return JsonResponse({'success': False, 'message': str(e)})


@login_required
@require_POST
def test_redshift_connection(request):
    """Redshift DB 연결 테스트"""
    data = json.loads(request.body)
    host = data.get('host')
    port = data.get('port')
    dbname = data.get('dbname')
    username = data.get('username')
    password = data.get('password')

    if not all([host, port, dbname, username, password]):
        return JsonResponse({'success': False, 'message': '모든 필드를 입력해주세요.'})

    try:
        conn = RedshiftConnection(host, port, dbname, username, password)
        if conn.test_connection():
            request.session['rs_conn'] = {'host': host, 'port': port, 'dbname': dbname, 'username': username, 'password': password}
            request.session['rs_conn_status'] = 'ok'
            return JsonResponse({'success': True, 'message': 'Redshift 연결에 성공했습니다.'})
        else:
            raise RedshiftConnectionError("연결 테스트 실패")
    except RedshiftConnectionError as e:
        request.session['rs_conn_status'] = 'error'
        return JsonResponse({'success': False, 'message': str(e)})


@login_required
@require_POST
@require_db_connection
def query_alert_info(request, oracle_conn: OracleConnection):
    """ALERT ID로 모든 관련 정보 조회 및 DF 저장 (핵심 로직)"""
    alert_id = request.POST.get('alert_id', '').strip()
    if not alert_id:
        return HttpResponseBadRequest('ALERT ID를 입력해주세요.')

    try:
        # 1. Alert 정보 조회 -> CUST_ID, TRAN_STRT, TRAN_END 확보
        sql, params = SQLQueryManager.load_and_prepare('alert_info_by_alert_id.sql', {':alert_id': alert_id})
        cols, rows = oracle_conn.execute_query(sql, params)
        if not rows:
            return JsonResponse({'success': False, 'message': '해당 ALERT ID의 정보가 없습니다.'})
        
        df_alert = pd.DataFrame(rows, columns=cols)
        cust_id = df_alert['CUST_ID'].iloc[0]
        tran_start = df_alert['TRAN_STRT'].iloc[0]
        tran_end = df_alert['TRAN_END'].iloc[0]
        
        request.session['df_alert'] = df_alert.to_json()
        logger.info(f"DF_ALERT for CUST_ID {cust_id} saved to session.")

        # 2. 통합 고객 정보 조회 -> MID, 고객구분 등 확보
        sql, params = SQLQueryManager.load_and_prepare('customer_unified_info.sql', {':custId': cust_id})
        cols, rows = oracle_conn.execute_query(sql, params)
        df_customer = pd.DataFrame(rows, columns=cols)
        mem_id = df_customer['MID'].iloc[0] if not df_customer.empty else None
        
        request.session['df_customer'] = df_customer.to_json()
        logger.info(f"DF_CUSTOMER for CUST_ID {cust_id} saved to session.")

        # 3. 고객 구분에 따른 분기 처리
        if not df_customer.empty:
            customer_type = df_customer['고객구분'].iloc[0]
            if customer_type == '법인':
                # 3-1. 법인 관련인 정보 조회
                sql, params = SQLQueryManager.load_and_prepare('corp_related_persons.sql', {':cust_id': cust_id})
                cols, rows = oracle_conn.execute_query(sql, params)
                df_corp_related = pd.DataFrame(rows, columns=cols)
                request.session['df_corp_related'] = df_corp_related.to_json()
                logger.info("DF_CORP_RELATED saved to session.")
            else: # 개인
                # 3-2. 개인 관련인 (내부입출금) 정보 조회
                params_dict = {':cust_id': cust_id, ':start_date': tran_start, ':end_date': tran_end}
                sql, params = SQLQueryManager.load_and_prepare('person_related_summary.sql', params_dict)
                cols, rows = oracle_conn.execute_query(sql, params)
                df_person_related = pd.DataFrame(rows, columns=cols)
                request.session['df_person_related'] = df_person_related.to_json()
                logger.info("DF_PERSON_RELATED saved to session.")

        # 4. IP 접속 이력 조회 (MID가 있는 경우)
        if mem_id:
            params_dict = {':mem_id': mem_id, ':start_date': tran_start, ':end_date': tran_end}
            sql, params = SQLQueryManager.load_and_prepare('query_ip_access_history.sql', params_dict)
            cols, rows = oracle_conn.execute_query(sql, params)
            df_ip_history = pd.DataFrame(rows, columns=cols)
            request.session['df_ip_history'] = df_ip_history.to_json()
            logger.info("DF_IP_HISTORY saved to session.")

        return JsonResponse({
            'success': True,
            'message': f'CUST_ID: {cust_id}에 대한 모든 정보 조회가 완료되었습니다.',
            'data_summary': {
                'alert_rows': len(df_alert),
                'customer_rows': len(df_customer)
            }
        })

    except Exception as e:
        logger.exception(f"데이터 조회 중 오류 발생: {e}")
        return JsonResponse({'success': False, 'message': f'오류가 발생했습니다: {str(e)}'})