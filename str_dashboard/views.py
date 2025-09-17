# str_dashboard/views.py

import os
import json
import logging
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse, HttpResponseBadRequest, HttpResponseNotFound, FileResponse
from django.shortcuts import render
from django.views.decorators.http import require_POST
from django.conf import settings
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, Any, List
from pathlib import Path
import tempfile
import io

# 내부 모듈 import
from .utils.df_manager import DataFrameManager
from .utils.ledger_manager import OrderbookAnalyzer
from .toml import toml_collector, toml_exporter

# 데이터베이스 모듈
from .utils.db import (
    OracleConnection,
    OracleConnectionError,
    OracleQueryError,
    RedshiftConnection,
    RedshiftConnectionError,
    RedshiftQueryError,
    execute_oracle_query,
    get_default_config,
)

# 쿼리 모듈
from .queries.rule_historic_search import (
    fetch_df_result_0, 
    aggregate_by_rule_id_list,
)

logger = logging.getLogger(__name__)


# ==================== 페이지 뷰 ====================

@login_required
def home(request):
    """홈 페이지"""
    return render(request, 'str_dashboard/home.html')

@login_required
def menu1_1(request):
    """ALERT ID 조회 페이지"""
    # 기본값 설정
    context = {
        'active_top_menu': 'menu1',
        'active_sub_menu': 'menu1_1',
        'db_status': request.session.get('db_conn_status', 'need'),
        'rs_status': request.session.get('rs_conn_status', 'need'),
        'db_info': request.session.get('db_conn', {}),
        'rs_info': request.session.get('rs_conn', {}),
        # 기본 연결 정보 (환경변수 또는 설정에서 가져오기)
        'default_host': os.getenv('ORACLE_HOST', ''),
        'default_port': os.getenv('ORACLE_PORT', '1521'),
        'default_service': os.getenv('ORACLE_SERVICE', ''),
        'default_username': os.getenv('ORACLE_USERNAME', ''),
        'default_rs_host': os.getenv('REDSHIFT_HOST', ''),
        'default_rs_port': os.getenv('REDSHIFT_PORT', '5439'),
        'default_rs_dbname': os.getenv('REDSHIFT_DBNAME', ''),
        'default_rs_username': os.getenv('REDSHIFT_USERNAME', ''),
    }
    return render(request, 'str_dashboard/menu1_1/main.html', context)


# ==================== 데이터베이스 연결 ====================

@require_POST
@login_required
def test_oracle_connection(request):
    """Oracle 데이터베이스 연결 테스트"""
    params = {k: request.POST.get(k, '').strip() for k in ['host', 'port', 'service_name', 'username', 'password']}
    if not all(params.values()):
        return JsonResponse({'success': False, 'message': '모든 필드를 입력해주세요.'})

    try:
        conn_details = {
            'jdbc_url': OracleConnection.build_jdbc_url(params['host'], params['port'], params['service_name']),
            'username': params['username'],
            'password': params['password']
        }
        oracle_conn = OracleConnection(**conn_details)
        
        if oracle_conn.test_connection():
            request.session['db_conn_status'] = 'ok'
            request.session['db_conn'] = conn_details
            return JsonResponse({'success': True, 'message': '연결에 성공했습니다.'})
        else:
            raise OracleConnectionError("연결 테스트 실패")
            
    except OracleConnectionError as e:
        request.session['db_conn_status'] = 'need'
        return JsonResponse({'success': False, 'message': str(e)})


@require_POST
@login_required
def test_redshift_connection(request):
    """Redshift 데이터베이스 연결 테스트"""
    params = {k: request.POST.get(k, '').strip() for k in ['host', 'port', 'dbname', 'username', 'password']}
    if not all(params.values()):
        return JsonResponse({'success': False, 'message': '모든 필드를 입력해주세요.'})
    
    try:
        redshift_conn = RedshiftConnection(**params)
        if redshift_conn.test_connection():
            request.session['rs_conn_status'] = 'ok'
            request.session['rs_conn'] = params
            return JsonResponse({'success': True, 'message': 'Redshift 연결에 성공했습니다.'})
        else:
            raise RedshiftConnectionError("연결 테스트 실패")
    except RedshiftConnectionError as e:
        request.session['rs_conn_status'] = 'need'
        return JsonResponse({'success': False, 'message': str(e)})


@require_POST
@login_required
def connect_all_databases(request):
    """Oracle과 Redshift 모두 연결"""
    oracle_params = {
        'host': request.POST.get('oracle_host', '').strip(),
        'port': request.POST.get('oracle_port', '').strip(),
        'service_name': request.POST.get('oracle_service_name', '').strip(),
        'username': request.POST.get('oracle_username', '').strip(),
        'password': request.POST.get('oracle_password', '').strip(),
    }
    
    redshift_params = {
        'host': request.POST.get('redshift_host', '').strip(),
        'port': request.POST.get('redshift_port', '').strip(),
        'dbname': request.POST.get('redshift_dbname', '').strip(),
        'username': request.POST.get('redshift_username', '').strip(),
        'password': request.POST.get('redshift_password', '').strip(),
    }
    
    result = {'success': False, 'oracle_status': 'fail', 'redshift_status': 'fail'}
    
    # Oracle 연결 시도
    if all(oracle_params.values()):
        try:
            oracle_conn_details = {
                'jdbc_url': OracleConnection.build_jdbc_url(
                    oracle_params['host'], 
                    oracle_params['port'], 
                    oracle_params['service_name']
                ),
                'username': oracle_params['username'],
                'password': oracle_params['password']
            }
            oracle_conn = OracleConnection(**oracle_conn_details)
            
            if oracle_conn.test_connection():
                request.session['db_conn_status'] = 'ok'
                request.session['db_conn'] = oracle_conn_details
                result['oracle_status'] = 'ok'
        except OracleConnectionError as e:
            result['oracle_error'] = str(e)
            request.session['db_conn_status'] = 'need'
    
    # Redshift 연결 시도
    if all(redshift_params.values()):
        try:
            redshift_conn = RedshiftConnection(**redshift_params)
            if redshift_conn.test_connection():
                request.session['rs_conn_status'] = 'ok'
                request.session['rs_conn'] = redshift_params
                result['redshift_status'] = 'ok'
        except RedshiftConnectionError as e:
            result['redshift_error'] = str(e)
            request.session['rs_conn_status'] = 'need'
    
    # 전체 성공 여부
    result['success'] = (result['oracle_status'] == 'ok' and result['redshift_status'] == 'ok')
    
    return JsonResponse(result)


# ==================== 통합 데이터 처리 (메인 API) ====================

@require_POST
@login_required
def query_all_data_integrated(request):
    """
    ALERT ID 기반 모든 데이터를 한번에 조회하는 통합 함수
    DataFrame Manager를 사용하여 요청별로 데이터를 중앙 관리
    """
    alert_id = request.POST.get('alert_id', '').strip()
    if not alert_id:
        return JsonResponse({'success': False, 'message': 'ALERT ID를 입력하세요.'})
    
    db_info = request.session.get('db_conn')
    if not db_info or request.session.get('db_conn_status') != 'ok':
        return JsonResponse({'success': False, 'message': '먼저 Oracle 연결을 완료해 주세요.'})
    
    # 요청마다 새로운 DataFrameManager 인스턴스 생성
    df_manager = DataFrameManager()
    df_manager.set_alert_id(alert_id)
    
    oracle_conn = OracleConnection.from_session(db_info)

    try:
        with oracle_conn.transaction() as db_conn:
            logger.info(f"Starting integrated query for ALERT ID: {alert_id}")
            
            # 1. ALERT 정보 조회
            alert_result = execute_oracle_query(
                db_conn, 'alert_info_by_alert_id.sql', 
                {':alert_id': '?'}, 
                [alert_id]
            )
            if not alert_result.get('success'): 
                return JsonResponse(alert_result)
            
            df_manager.add_dataset('alert_info', alert_result['columns'], alert_result['rows'])
            alert_metadata = df_manager.process_alert_data(alert_result)
            
            if not alert_metadata.get('cust_id'):
                return JsonResponse({'success': False, 'message': 'ALERT에 연결된 고객 정보가 없습니다.'})
            
            # 2. 고객 정보 조회
            cust_id = alert_metadata['cust_id']
            customer_result = execute_oracle_query(
                db_conn, 'customer_unified_info.sql', 
                {':custId': '?'}, 
                [cust_id]
            )
            if not customer_result.get('success'): 
                return JsonResponse(customer_result)

            df_manager.add_dataset('customer_info', customer_result['columns'], customer_result['rows'])
            customer_metadata = df_manager.process_customer_data(customer_result)
            
            # 3. 추가 쿼리 실행
            _execute_additional_queries(request, db_conn, df_manager, alert_metadata, customer_metadata)

        # 4. 전체 데이터 요약 및 세션 저장
        summary = df_manager.get_all_datasets_summary()
        request.session['df_manager_data'] = df_manager.export_to_dict()
        
        logger.info(f"Integrated query completed for ALERT ID: {alert_id}")
        
        return JsonResponse({
            'success': True, 
            'alert_id': alert_id, 
            'summary': summary,
            'message': f"데이터 조회 완료: {len(summary['datasets'])}개 데이터셋"
        })
        
    except (OracleConnectionError, OracleQueryError) as e:
        logger.exception(f"DB Error in integrated query: {e}")
        return JsonResponse({'success': False, 'message': f'데이터베이스 처리 중 오류: {str(e)}'})
    except Exception as e:
        logger.exception(f"Error in integrated query: {e}")
        return JsonResponse({'success': False, 'message': f'통합 조회 중 오류: {str(e)}'})


def _execute_additional_queries(request, db_conn, df_manager, alert_metadata, customer_metadata):
    """추가 쿼리들 실행 (내부 함수)"""
    cust_id = alert_metadata.get('cust_id')
    customer_type = customer_metadata.get('customer_type')

    # Rule 히스토리
    if alert_metadata.get('canonical_ids'):
        try:
            db_info = request.session.get('db_conn', {})
            df0 = fetch_df_result_0(
                jdbc_url=db_info['jdbc_url'],
                driver_class=db_info.get('driver_class', 'oracle.jdbc.driver.OracleDriver'),
                driver_path=db_info.get('driver_path', os.getenv('ORACLE_JAR', r'C:\ojdbc11-21.5.0.0.jar')),
                username=db_info['username'], 
                password=db_info['password']
            )
            df1 = aggregate_by_rule_id_list(df0)
            rule_key = ','.join(sorted(alert_metadata['canonical_ids']))
            matching_rows = df1[df1["STR_RULE_ID_LIST"] == rule_key]
            
            df_manager.add_dataset('rule_history', list(matching_rows.columns), matching_rows.values.tolist())
        except Exception as e:
            logger.error(f"Rule history query failed: {e}")
    
    # 법인/개인별 분기 처리
    if customer_type == '법인':
        result = execute_oracle_query(
            db_conn, 'corp_related_persons.sql', 
            {':cust_id': '?'}, 
            [cust_id]
        )
        if result.get('success'):
            df_manager.add_dataset('corp_related', result['columns'], result['rows'])
    else:  # 개인
        tran_start, tran_end = df_manager.calculate_transaction_period()
        if tran_start and tran_end:
            # 개인 관련인
            result = execute_oracle_query(
                db_conn, 'person_related_summary.sql', 
                {':start_date': '?', ':end_date': '?', ':cust_id': '?'},
                [tran_start, tran_end, cust_id, cust_id, tran_start, tran_end, cust_id]
            )
            if result.get('success'):
                df_manager.add_dataset('person_related', result['columns'], result['rows'])
            
            # IP 이력
            mid = customer_metadata.get('mid')
            if mid:
                result = execute_oracle_query(
                    db_conn, 'query_ip_access_history.sql',
                    {':mem_id': '?', ':start_date': '?', ':end_date': '?'},
                    [mid, tran_start.split(' ')[0], tran_end.split(' ')[0]]
                )
                if result.get('success'):
                    df_manager.add_dataset('ip_history', result['columns'], result['rows'])
                
                # Redshift Orderbook
                if request.session.get('rs_conn_status') == 'ok':
                    _query_orderbook(request, df_manager, mid, tran_start, tran_end)

    # 중복 회원
    customer_df = df_manager.get_dataframe('customer_info')
    if customer_df is not None and not customer_df.empty:
        dup_params = _extract_duplicate_params(customer_df.iloc[0])
        if dup_params:
            # 바인드 변수 개수와 일치하도록 수정
            bind_vars = {
                ':current_cust_id': '?',
                ':address': '?',
                ':detail_address': '?',
                ':current_cust_id_wpn': '?',
                ':workplace_name': '?',
                ':current_cust_id_wpa': '?',
                ':workplace_address': '?',
                ':workplace_detail_address': '?',
                ':phone_suffix': '?',
                ':current_cust_id_final': '?'
            }
            
            bind_values = [
                cust_id,  # :current_cust_id
                dup_params['address'],
                dup_params['detail_address'],
                cust_id,  # :current_cust_id_wpn
                dup_params['workplace_name'],
                cust_id,  # :current_cust_id_wpa
                dup_params['workplace_address'],
                dup_params['workplace_detail_address'],
                dup_params['phone_suffix'],
                cust_id   # :current_cust_id_final
            ]
            
            result = execute_oracle_query(
                db_conn, 'duplicate_unified.sql',
                bind_vars,
                bind_values
            )
            if result.get('success'):
                df_manager.add_dataset('duplicate_persons', result['columns'], result['rows'])


def _query_orderbook(request, df_manager, mem_id, start_date, end_date):
    """Redshift Orderbook 조회 (내부 함수)"""
    rs_info = request.session.get('rs_conn')
    if not rs_info: 
        return

    try:
        redshift_conn = RedshiftConnection.from_session(rs_info)
        sql_path = Path(settings.BASE_DIR) / 'str_dashboard' / 'queries' / 'redshift_orderbook.sql'
        sql_query = sql_path.read_text('utf-8')
        
        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        with redshift_conn.transaction() as rs_conn:
            with rs_conn.cursor() as cursor:
                cursor.execute(sql_query, (start_dt, end_dt, mem_id))
                if not cursor.description:
                    cols, rows = [], []
                else:
                    cols = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

        if cols and rows:
            df_manager.add_dataset('orderbook', cols, rows, 
                                    user_id=mem_id, 
                                    start_date=start_date, 
                                    end_date=end_date)
            df = df_manager.get_dataframe('orderbook')
            if df is not None and not df.empty:
                analyzer = OrderbookAnalyzer(df)
                analyzer.analyze()
                analysis_result = {
                    'text_summary': analyzer.generate_text_summary(),
                    'patterns': analyzer.get_pattern_analysis(),
                    'daily_summary': analyzer.get_daily_summary().to_dict('records')
                }
                df_manager.add_dataset('orderbook_analysis', 
                                      ['analysis_type', 'data'], 
                                      [['summary', analysis_result]])
            
    except (RedshiftConnectionError, RedshiftQueryError) as e:
        logger.error(f"Orderbook query failed: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error in orderbook query: {e}")


def _extract_duplicate_params(customer_row: pd.Series) -> Dict[str, Any]:
    """고객 정보에서 중복 검색 파라미터 추출"""
    phone = customer_row.get('연락처', '')
    return {
        'address': customer_row.get('거주지주소', ''),
        'detail_address': customer_row.get('거주지상세주소', ''),
        'workplace_name': customer_row.get('직장명', ''),
        'workplace_address': customer_row.get('직장주소', ''),
        'workplace_detail_address': customer_row.get('직장상세주소', ''),
        'phone_suffix': str(phone)[-4:] if phone and len(str(phone)) >= 4 else None,
    }


# ==================== DataFrame Manager APIs ====================

@login_required
def get_dataframe_manager_status(request):
    """세션에 저장된 DataFrame Manager 상태 조회 API"""
    df_manager_data = request.session.get('df_manager_data')
    if not df_manager_data:
        return JsonResponse({'success': False, 'message': '조회된 데이터가 없습니다.'})
    
    df_manager = DataFrameManager.from_dict(df_manager_data)
    summary = df_manager.get_all_datasets_summary()
    
    return JsonResponse({
        'success': True, 
        'summary': summary,
        'datasets_list': list(df_manager.datasets.keys()),
        'alert_id': df_manager.alert_id,
        'total_memory_mb': summary.get('total_memory_mb', 0)
    })


@login_required
def export_dataframe_to_csv(request):
    """특정 DataFrame을 CSV로 내보내기"""
    dataset_name = request.GET.get('dataset', '').strip()
    if not dataset_name:
        return HttpResponseBadRequest('Dataset name required')

    df_manager_data = request.session.get('df_manager_data')
    if not df_manager_data:
        return HttpResponseNotFound('No data found in session.')

    df_manager = DataFrameManager.from_dict(df_manager_data)
    df = df_manager.get_dataframe(dataset_name)
    
    if df is None:
        return HttpResponseNotFound(f'Dataset {dataset_name} not found')
    
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8-sig')
    
    response = HttpResponse(output.getvalue(), content_type='text/csv; charset=utf-8-sig')
    response['Content-Disposition'] = f'attachment; filename="{dataset_name}_{datetime.now().strftime("%Y%m%d")}.csv"'
    return response


# ==================== TOML Export ====================

@require_POST
@login_required
def prepare_toml_data(request):
    """세션 데이터를 기반으로 TOML 파일 생성 준비"""
    df_manager_data = request.session.get('df_manager_data')
    if not df_manager_data:
        return JsonResponse({'success': False, 'message': 'TOML로 내보낼 데이터가 없습니다.'})

    try:
        alert_id = df_manager_data.get('alert_id', 'unknown')
        filename = toml_exporter.generate_filename(alert_id)
        
        # 임시 파일 경로를 세션에 저장
        tmp_path = str(Path(tempfile.gettempdir()) / filename)
        request.session['toml_temp_path'] = tmp_path

        # 데이터 수집 및 파일 저장
        collected_data = toml_collector.collect_all_data(df_manager_data)
        toml_exporter.save_to_file(collected_data, tmp_path)
        
        return JsonResponse({
            'success': True, 
            'message': 'TOML 데이터 준비 완료',
            'filename': filename, 
            'sections': list(collected_data.keys())
        })
            
    except Exception as e:
        logger.exception(f"Error preparing TOML data: {e}")
        return JsonResponse({'success': False, 'message': f'TOML 데이터 준비 실패: {str(e)}'})


@login_required
def download_toml(request):
    """준비된 TOML 파일 다운로드"""
    tmp_path_str = request.session.get('toml_temp_path')
    if not tmp_path_str:
        return HttpResponseBadRequest('TOML 파일 경로를 찾을 수 없습니다.')

    tmp_path = Path(tmp_path_str)
    if not tmp_path.exists():
        return HttpResponseNotFound('TOML 파일을 찾을 수 없습니다. 다시 시도해주세요.')
        
    try:
        return FileResponse(
            open(tmp_path, 'rb'), 
            as_attachment=True, 
            filename=tmp_path.name
        )
    except Exception as e:
        logger.exception(f"Error downloading TOML: {e}")
        return HttpResponse(
            f'TOML 다운로드 실패: {str(e)}', 
            status=500
        )


# ==================== 세션 관리 ====================

@require_POST
@login_required
def save_to_session(request):
    """데이터를 세션에 저장 (범용)"""
    try:
        data = json.loads(request.body)
        key = data.get('key')
        value = data.get('value')
        
        if not key:
            return JsonResponse({'success': False, 'message': 'Key is required'})
        
        request.session[key] = value
        return JsonResponse({'success': True, 'message': 'Saved to session'})
        
    except Exception as e:
        logger.exception(f"Error saving to session: {e}")
        return JsonResponse({'success': False, 'message': str(e)})