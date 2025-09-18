# str_dashboard/views.py

import logging
import json
import tempfile
from pathlib import Path
from datetime import datetime

from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse, FileResponse
from django.shortcuts import render
from django.views.decorators.http import require_POST

# 내부 모듈 import
from .utils.query_manager import QueryManager
from .utils.df_manager import DataFrameManager
from .utils.db import OracleConnection, RedshiftConnection, DEFAULT_CONFIG
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
    context = {
        'active_top_menu': 'menu1',
        'active_sub_menu': 'menu1_1',
        'db_status': request.session.get('db_conn_status', 'need'),
        'rs_status': request.session.get('rs_conn_status', 'need'),
        # 기본 연결 정보
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


# ==================== 데이터베이스 연결 API ====================

@require_POST
@login_required
def test_oracle_connection(request):
    """Oracle 데이터베이스 연결 테스트"""
    try:
        params = {
            'host': request.POST.get('host', '').strip(),
            'port': request.POST.get('port', '').strip(),
            'service_name': request.POST.get('service_name', '').strip(),
            'username': request.POST.get('username', '').strip(),
            'password': request.POST.get('password', '').strip()
        }
        
        if not all(params.values()):
            return JsonResponse({
                'success': False,
                'message': '모든 필드를 입력해주세요.'
            })

        conn_details = {
            'jdbc_url': OracleConnection.build_jdbc_url(
                params['host'], 
                params['port'], 
                params['service_name']
            ),
            'username': params['username'],
            'password': params['password']
        }
        
        oracle_conn = OracleConnection(**conn_details)
        
        if oracle_conn.test_connection():
            request.session['db_conn_status'] = 'ok'
            request.session['db_conn'] = conn_details
            logger.info("Oracle connection successful")
            return JsonResponse({
                'success': True,
                'message': 'Oracle 연결에 성공했습니다.'
            })
        else:
            return JsonResponse({
                'success': False,
                'message': 'Oracle 연결 테스트 실패'
            })
            
    except Exception as e:
        logger.error(f"Oracle connection test failed: {e}")
        request.session['db_conn_status'] = 'need'
        return JsonResponse({
            'success': False,
            'message': f'연결 오류: {str(e)}'
        })


@require_POST
@login_required
def test_redshift_connection(request):
    """Redshift 데이터베이스 연결 테스트"""
    try:
        params = {
            'host': request.POST.get('host', '').strip(),
            'port': request.POST.get('port', '').strip(),
            'dbname': request.POST.get('dbname', '').strip(),
            'username': request.POST.get('username', '').strip(),
            'password': request.POST.get('password', '').strip()
        }
        
        if not all(params.values()):
            return JsonResponse({
                'success': False,
                'message': '모든 필드를 입력해주세요.'
            })
        
        redshift_conn = RedshiftConnection(**params)
        
        if redshift_conn.test_connection():
            request.session['rs_conn_status'] = 'ok'
            request.session['rs_conn'] = params
            logger.info("Redshift connection successful")
            return JsonResponse({
                'success': True,
                'message': 'Redshift 연결에 성공했습니다.'
            })
        else:
            return JsonResponse({
                'success': False,
                'message': 'Redshift 연결 테스트 실패'
            })
            
    except Exception as e:
        logger.error(f"Redshift connection test failed: {e}")
        request.session['rs_conn_status'] = 'need'
        return JsonResponse({
            'success': False,
            'message': f'연결 오류: {str(e)}'
        })


@require_POST
@login_required
def connect_all_databases(request):
    """Oracle과 Redshift 동시 연결"""
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
    
    result = {
        'success': False,
        'oracle_status': 'fail',
        'redshift_status': 'fail'
    }
    
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
                logger.info("Oracle connected successfully")
            else:
                result['oracle_error'] = 'Connection test failed'
                
        except Exception as e:
            result['oracle_error'] = str(e)
            request.session['db_conn_status'] = 'need'
            logger.error(f"Oracle connection error: {e}")
    
    # Redshift 연결 시도
    if all(redshift_params.values()):
        try:
            redshift_conn = RedshiftConnection(**redshift_params)
            
            if redshift_conn.test_connection():
                request.session['rs_conn_status'] = 'ok'
                request.session['rs_conn'] = redshift_params
                result['redshift_status'] = 'ok'
                logger.info("Redshift connected successfully")
            else:
                result['redshift_error'] = 'Connection test failed'
                
        except Exception as e:
            result['redshift_error'] = str(e)
            request.session['rs_conn_status'] = 'need'
            logger.error(f"Redshift connection error: {e}")
    
    # 전체 성공 여부
    result['success'] = (
        result['oracle_status'] == 'ok' and 
        result['redshift_status'] == 'ok'
    )
    
    return JsonResponse(result)


# ==================== 통합 데이터 조회 API ====================

@require_POST
@login_required
def query_all_integrated(request):
    """
    ALERT ID 기반 통합 데이터 조회 (Stage 1-4)
    """
    alert_id = request.POST.get('alert_id', '').strip()
    
    if not alert_id:
        return JsonResponse({
            'success': False,
            'message': 'ALERT ID를 입력하세요.'
        })
    
    # Oracle 연결 확인
    db_info = request.session.get('db_conn')
    if not db_info or request.session.get('db_conn_status') != 'ok':
        return JsonResponse({
            'success': False,
            'message': 'Oracle 데이터베이스 연결이 필요합니다.'
        })
    
    # Redshift 연결 확인 (옵션)
    rs_info = None
    if request.session.get('rs_conn_status') == 'ok':
        rs_info = request.session.get('rs_conn')
    
    try:
        logger.info(f"Starting integrated query for ALERT ID: {alert_id}")
        
        # QueryManager를 통해 모든 Stage 실행
        query_manager = QueryManager(db_info, rs_info)
        result = query_manager.execute_all_queries(alert_id)
        
        if not result['success']:
            return JsonResponse(result)
        
        # 세션에 결과 저장
        request.session['df_manager_data'] = result['df_manager_data']
        request.session['last_alert_id'] = alert_id
        
        # 요약 정보 생성
        summary = result.get('summary', {})
        dataset_count = result.get('dataset_count', 0)
        
        return JsonResponse({
            'success': True,
            'alert_id': alert_id,
            'dataset_count': dataset_count,
            'summary': summary,
            'message': f"데이터 조회 완료: {dataset_count}개 데이터셋"
        })
        
    except Exception as e:
        logger.exception(f"Error in integrated query: {e}")
        return JsonResponse({
            'success': False,
            'message': f'통합 조회 중 오류 발생: {str(e)}'
        })


# ==================== DataFrame 관리 API ====================

@login_required
def df_manager_status(request):
    """DataFrame Manager 상태 조회"""
    df_manager_data = request.session.get('df_manager_data')
    
    if not df_manager_data:
        return JsonResponse({
            'success': False,
            'message': '조회된 데이터가 없습니다.'
        })
    
    try:
        df_manager = DataFrameManager.from_dict(df_manager_data)
        summary = df_manager.get_all_datasets_summary()
        
        return JsonResponse({
            'success': True,
            'summary': summary,
            'datasets_list': list(df_manager.datasets.keys()),
            'alert_id': df_manager.alert_id,
            'total_memory_mb': summary.get('total_memory_mb', 0)
        })
        
    except Exception as e:
        logger.error(f"Error getting DF manager status: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        })


@login_required
def export_dataframe_csv(request):
    """DataFrame을 CSV로 내보내기"""
    import io
    
    dataset_name = request.GET.get('dataset', '').strip()
    
    if not dataset_name:
        return HttpResponse('Dataset name is required', status=400)
    
    df_manager_data = request.session.get('df_manager_data')
    
    if not df_manager_data:
        return HttpResponse('No data found in session', status=404)
    
    try:
        df_manager = DataFrameManager.from_dict(df_manager_data)
        df = df_manager.get_dataframe(dataset_name)
        
        if df is None:
            return HttpResponse(f'Dataset "{dataset_name}" not found', status=404)
        
        # CSV 생성
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        
        # 파일명 생성
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{dataset_name}_{timestamp}.csv"
        
        # 응답 생성
        response = HttpResponse(
            output.getvalue(),
            content_type='text/csv; charset=utf-8-sig'
        )
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        
        return response
        
    except Exception as e:
        logger.error(f"Error exporting CSV: {e}")
        return HttpResponse(f'Export error: {str(e)}', status=500)


# ==================== TOML Export API ====================

@require_POST
@login_required
def prepare_toml_data(request):
    """TOML 데이터 준비"""
    df_manager_data = request.session.get('df_manager_data')
    
    if not df_manager_data:
        return JsonResponse({
            'success': False,
            'message': 'TOML로 내보낼 데이터가 없습니다.'
        })
    
    try:
        # 파일명 생성
        alert_id = df_manager_data.get('alert_id', 'unknown')
        filename = toml_exporter.generate_filename(alert_id)
        
        # 임시 파일 경로 생성
        tmp_path = Path(tempfile.gettempdir()) / filename
        request.session['toml_temp_path'] = str(tmp_path)
        
        # TOML 데이터 수집 및 저장
        collected_data = toml_collector.collect_all_data(df_manager_data)
        success = toml_exporter.save_to_file(collected_data, str(tmp_path))
        
        if not success:
            return JsonResponse({
                'success': False,
                'message': 'TOML 파일 생성 실패'
            })
        
        logger.info(f"TOML data prepared: {filename}")
        
        return JsonResponse({
            'success': True,
            'message': 'TOML 데이터 준비 완료',
            'filename': filename,
            'sections': list(collected_data.keys())
        })
        
    except Exception as e:
        logger.exception(f"Error preparing TOML data: {e}")
        return JsonResponse({
            'success': False,
            'message': f'TOML 데이터 준비 실패: {str(e)}'
        })


@login_required
def download_toml(request):
    """TOML 파일 다운로드"""
    tmp_path_str = request.session.get('toml_temp_path')
    
    if not tmp_path_str:
        return HttpResponse(
            'TOML 파일 경로를 찾을 수 없습니다.',
            status=400
        )
    
    tmp_path = Path(tmp_path_str)
    
    if not tmp_path.exists():
        return HttpResponse(
            'TOML 파일을 찾을 수 없습니다. 다시 생성해주세요.',
            status=404
        )
    
    try:
        # 파일 응답 생성
        response = FileResponse(
            open(tmp_path, 'rb'),
            as_attachment=True,
            filename=tmp_path.name
        )
        
        logger.info(f"TOML file downloaded: {tmp_path.name}")
        
        # 다운로드 후 임시 파일 삭제
        def cleanup():
            try:
                tmp_path.unlink()
                logger.info(f"Temporary TOML file deleted: {tmp_path}")
            except Exception as e:
                logger.warning(f"Could not delete temp file: {e}")
        
        # 응답 전송 후 정리
        response.close_callback = cleanup
        
        return response
        
    except Exception as e:
        logger.exception(f"Error downloading TOML: {e}")
        return HttpResponse(
            f'TOML 다운로드 실패: {str(e)}',
            status=500
        )


# ==================== 세션 관리 API ====================

@require_POST
@login_required
def save_to_session(request):
    """범용 세션 저장 API"""
    try:
        data = json.loads(request.body)
        key = data.get('key')
        value = data.get('value')
        
        if not key:
            return JsonResponse({
                'success': False,
                'message': 'Key is required'
            })
        
        request.session[key] = value
        
        return JsonResponse({
            'success': True,
            'message': f'Saved to session with key: {key}'
        })
        
    except json.JSONDecodeError:
        return JsonResponse({
            'success': False,
            'message': 'Invalid JSON data'
        })
    except Exception as e:
        logger.error(f"Error saving to session: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        })