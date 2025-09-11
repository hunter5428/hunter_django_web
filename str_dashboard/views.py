# str_dashboard/views.py
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseBadRequest
from django.shortcuts import render
from django.views.decorators.http import require_POST
from django.conf import settings
import os, re, json
import jaydebeapi

from .queries.rule_objectives import build_rule_to_objectives
from .queries.rule_historic_search import (
    fetch_df_result_0, aggregate_by_rule_id_list
)

@login_required
def home(request):
    ctx = {'active_top_menu':'', 'active_sub_menu':''}
    return render(request, 'str_dashboard/home.html', ctx)

@login_required
def menu1_1(request):
    prev = request.session.get('db_conn')
    default_service = 'PRDAMLKR.OCIAMLPRODDBA.OCIAMLPROD.ORACLEVCN.COM'
    if prev and isinstance(prev, dict):
        jdbc_prev = prev.get('jdbc_url', '')
        if jdbc_prev.startswith('jdbc:oracle:thin:@//'):
            try: default_service = jdbc_prev.split('/', 3)[-1]
            except Exception: pass

    rule_obj_map = build_rule_to_objectives()
    ctx = {
        'active_top_menu': 'menu1',
        'active_sub_menu': 'menu1_1',
        'db_status': request.session.get('db_conn_status', 'need'),
        'default_host': '127.0.0.1',
        'default_port': '40112',
        'default_service': default_service,
        'default_username': '',
        'rule_obj_map_json': json.dumps(rule_obj_map, ensure_ascii=False),
    }
    return render(request, 'str_dashboard/menu1_1/main.html', ctx)

@login_required
@require_POST
def test_oracle_connection(request):
    host = (request.POST.get('host') or '').strip()
    port = (request.POST.get('port') or '').strip()
    service_name = (request.POST.get('service_name') or '').strip()
    username = (request.POST.get('username') or '').strip()
    password = request.POST.get('password') or ''
    if not (host and port and service_name and username and password):
        return HttpResponseBadRequest('Missing parameters.')

    driver_path = os.getenv('ORACLE_JAR', r'C:\ojdbc11-21.5.0.0.jar')
    driver_class = os.getenv('ORACLE_DRIVER', 'oracle.jdbc.driver.OracleDriver')
    jdbc_url = f"jdbc:oracle:thin:@//{host}:{port}/{service_name}"

    try:
        conn = jaydebeapi.connect(driver_class, jdbc_url, [username, password], driver_path)
        try: conn.close()
        except Exception: pass
        request.session['db_conn_status'] = 'ok'
        request.session['db_conn'] = {
            'jdbc_url': jdbc_url, 'driver_path': driver_path, 'driver_class': driver_class,
            'username': username, 'password': password,
        }
        return JsonResponse({'success': True, 'message': '연결에 성공했습니다.'})
    except Exception as e:
        request.session['db_conn_status'] = 'need'
        err_txt = str(e)
        msg = '연결에 실패했습니다.'
        if 'ORA-12514' in err_txt: msg += '\n(ORA-12514: SERVICE_NAME을 확인하세요.)'
        elif 'ORA-12154' in err_txt: msg += '\n(ORA-12154: 호스트/포트/서비스명을 다시 확인하세요.)'
        msg += f'\n{err_txt}'
        return JsonResponse({'success': False, 'message': msg})

def _strip_sql_comments(sql: str) -> str:
    no_block = re.sub(r"/\*.*?\*/", "", sql, flags=re.S)
    no_line = re.sub(r"--.*?$", "", no_block, flags=re.M)
    return no_line

@login_required
@require_POST
def query_alert_info(request):
    alert_id = (request.POST.get('alert_id') or '').strip()
    if not alert_id:
        return HttpResponseBadRequest('Missing alert_id.')

    db = request.session.get('db_conn')
    if not db or request.session.get('db_conn_status') != 'ok':
        return JsonResponse({'success': False, 'message': '먼저 DB Connection에서 연결을 완료해 주세요.'})

    jdbc_url   = db['jdbc_url']
    driver_path= db['driver_path']
    driver_class=db['driver_class']
    username  = db['username']
    password  = db['password']

    sql_path = settings.BASE_DIR / 'str_dashboard' / 'queries' / 'alert_info_by_alert_id.sql'
    try:
        raw_sql = sql_path.read_text(encoding='utf-8')
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'SQL 파일 로드 실패: {e}'})

    sql_no_comments = _strip_sql_comments(raw_sql).strip()
    prepared_sql = re.sub(r":alert_id\b", "?", sql_no_comments)
    if prepared_sql.rstrip().endswith(";"): prepared_sql = prepared_sql.rstrip()[:-1]
    param_count = prepared_sql.count("?")
    if param_count == 0:
        return JsonResponse({'success': False, 'message': '바인드 변수가 없습니다. SQL을 확인하세요.'})
    params = [alert_id] * param_count

    try:
        conn = jaydebeapi.connect(driver_class, jdbc_url, [username, password], driver_path)
        try:
            cur = conn.cursor()
            cur.execute(prepared_sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
        finally:
            try: conn.close()
            except Exception: pass
        return JsonResponse({'success': True, 'columns': cols, 'rows': rows})
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'조회 실패: {e}'})

@login_required
@require_POST
def query_person_info(request):
    cust_id = (request.POST.get('cust_id') or '').strip()
    if not cust_id:
        return HttpResponseBadRequest('Missing cust_id.')

    db = request.session.get('db_conn')
    if not db or request.session.get('db_conn_status') != 'ok':
        return JsonResponse({'success': False, 'message': '먼저 DB Connection에서 연결을 완료해 주세요.'})

    jdbc_url   = db['jdbc_url']
    driver_path= db['driver_path']
    driver_class=db['driver_class']
    username  = db['username']
    password  = db['password']

    sql_path = settings.BASE_DIR / 'str_dashboard' / 'queries' / 'person_info.sql'
    try:
        raw_sql = sql_path.read_text(encoding='utf-8')
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'SQL 파일 로드 실패: {e}'})

    sql_no_comments = _strip_sql_comments(raw_sql).strip()
    prepared_sql = re.sub(r":custId\b", "?", sql_no_comments)
    if prepared_sql.rstrip().endswith(";"): prepared_sql = prepared_sql.rstrip()[:-1]
    params = [cust_id]

    try:
        conn = jaydebeapi.connect(driver_class, jdbc_url, [username, password], driver_path)
        try:
            cur = conn.cursor()
            cur.execute(prepared_sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description] if cur.description else []
        finally:
            try: conn.close()
            except Exception: pass
        return JsonResponse({'success': True, 'columns': cols, 'rows': rows})
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'조회 실패: {e}'})

@login_required
@require_POST
def rule_history_search(request):
    """
    POST:
      - rule_key : 'ID1,ID2,...' (문자열, 오름차순 정렬된 코드들을 콤마로 연결)
    반환:
      { success: True, columns:[...], rows:[ [...] ] }  # 0~1행
    """
    rule_key = (request.POST.get('rule_key') or '').strip()
    if not rule_key:
        return HttpResponseBadRequest('Missing rule_key.')

    db = request.session.get('db_conn')
    if not db or request.session.get('db_conn_status') != 'ok':
        return JsonResponse({'success': False, 'message': '먼저 DB Connection에서 연결을 완료해 주세요.'})

    jdbc_url   = db['jdbc_url']
    driver_path= db['driver_path']
    driver_class=db['driver_class']
    username  = db['username']
    password  = db['password']

    try:
        # 1) 전체 집계 취득
        df0 = fetch_df_result_0(jdbc_url, driver_class, driver_path, username, password)
        df1 = aggregate_by_rule_id_list(df0)
        # 2) STR_RULE_ID_LIST 일치 행 필터
        sub = df1[df1["STR_RULE_ID_LIST"] == rule_key]
        cols = list(sub.columns)
        rows = sub.values.tolist()
        return JsonResponse({'success': True, 'columns': cols, 'rows': rows})
    except Exception as e:
        return JsonResponse({'success': False, 'message': f'히스토리 조회 실패: {e}'})
