# str_dashboard/views.py

import logging
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse, FileResponse
from django.shortcuts import render
from django.views.decorators.http import require_POST
from datetime import datetime
from pathlib import Path
import tempfile

# 내부 모듈 import
from .utils.query_manager import QueryManager
from .utils.df_manager import DataFrameManager
from .utils.db import (
    OracleConnection,
    RedshiftConnection,
    get_default_config,
)
from .toml import toml_collector, toml_exporter

logger = logging.getLogger(__name__)

# ==================== 페이지 뷰 ====================

@login_required
def home(request):
    """홈 페이지"""
    return render(request, 'str_dashboard/home.html')

@login_required
def menu1_1(request):
    """ALERT ID 조회 페이지"""
    # 하드코딩된 기본 설정값 사용
    from .utils.db import DEFAULT_CONFIG
    
    context = {
        'active_top_menu': 'menu1',
        'active_sub_menu': 'menu1_1',
        'db_status': request.session.get('db_conn_status', 'need'),
        'rs_status': request.session.get('rs_conn_status', 'need'),
        # 하드코딩된 기본 연결 정보
        'default_host': DEFAULT_CONFIG['ORACLE']['HOST'],
        'default_port': DEFAULT_CONFIG['ORACLE']['PORT'],
        'default_service': DEFAULT_CONFIG['ORACLE']['SERVICE'],
        'default_username': DEFAULT_CONFIG['ORACLE']['USERNAME'],
        'default_rs_host': DEFAULT_CONFIG['REDSHIFT']['HOST'],
        'default_rs_port': DEFAULT_CONFIG['REDSHIFT']['PORT'],
        'default_rs_dbname': DEFAULT_CONFIG['REDSHIFT']['DBNAME'],
        'default_rs_username': DEFAULT_CONFIG['REDSHIFT']['USERNAME'],
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
            return JsonResponse({'success': False, 'message': '연결 테스트 실패'})
            
    except Exception as e:
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
            return JsonResponse({'success': False, 'message': '연결 테스트 실패'})
    except Exception as e:
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
        except Exception as e:
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
        except Exception as e:
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
    QueryManager를 통해 처리
    """
    alert_id = request.POST.get('alert_id', '').strip()
    if not alert_id:
        return JsonResponse({'success': False, 'message': 'ALERT ID를 입력하세요.'})
    
    db_info = request.session.get('db_conn')
    if not db_info or request.session.get('db_conn_status') != 'ok':
        return JsonResponse({'success': False, 'message': '먼저 Oracle 연결을 완료해 주세요.'})
    
    rs_info = request.session.get('rs_conn') if request.session.get('rs_conn_status') == 'ok' else None
    
    try:
        # QueryManager를 통해 모든 쿼리 실행
        query_manager = QueryManager(db_info, rs_info)
        result = query_manager.execute_all_queries(alert_id)
        
        if not result['success']:
            return JsonResponse(result)
        
        # 세션에 저장
        request.session['df_manager_data'] = result['df_manager_data']
        
        return JsonResponse({
            'success': True,
            'alert_id': alert_id,
            'summary': result['summary'],
            'message': f"데이터 조회 완료: {result['dataset_count']}개 데이터셋"
        })
        
    except Exception as e:
        logger.exception(f"Error in integrated query: {e}")
        return JsonResponse({'success': False, 'message': f'통합 조회 중 오류: {str(e)}'})


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
    import io
    
    dataset_name = request.GET.get('dataset', '').strip()
    if not dataset_name:
        return HttpResponse('Dataset name required', status=400)
    
    df_manager_data = request.session.get('df_manager_data')
    if not df_manager_data:
        return HttpResponse('No data found in session.', status=404)
    
    df_manager = DataFrameManager.from_dict(df_manager_data)
    df = df_manager.get_dataframe(dataset_name)
    
    if df is None:
        return HttpResponse(f'Dataset {dataset_name} not found', status=404)
    
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
        return HttpResponse('TOML 파일 경로를 찾을 수 없습니다.', status=400)
    
    tmp_path = Path(tmp_path_str)
    if not tmp_path.exists():
        return HttpResponse('TOML 파일을 찾을 수 없습니다. 다시 시도해주세요.', status=404)
    
    try:
        return FileResponse(
            open(tmp_path, 'rb'),
            as_attachment=True,
            filename=tmp_path.name
        )
    except Exception as e:
        logger.exception(f"Error downloading TOML: {e}")
        return HttpResponse(f'TOML 다운로드 실패: {str(e)}', status=500)


# ==================== 세션 관리 ====================

@require_POST
@login_required
def save_to_session(request):
    """데이터를 세션에 저장 (범용)"""
    import json
    
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