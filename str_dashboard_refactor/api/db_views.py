"""
데이터베이스 연결 관련 API Views
"""
import logging
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponseBadRequest
from django.views.decorators.http import require_POST

from ..core.database import OracleConnection, RedshiftConnection, ConnectionConfig
from ..core.database.base import DatabaseConnectionError
from ..core.utils.session_manager import SessionManager

logger = logging.getLogger(__name__)


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
    
    try:
        # ConnectionConfig 생성
        config = ConnectionConfig(
            host=params['host'],
            port=params['port'],
            username=params['username'],
            password=params['password'],
            service_name=params['service_name']
        )
        
        oracle_conn = OracleConnection(config)
        
        if oracle_conn.test_connection():
            # 세션에 저장
            SessionManager.save_oracle_connection(request, config.to_dict())
            request.session.set_expiry(3600)
            
            logger.info(f"Oracle connection successful for user: {params['username']}")
            
            return JsonResponse({
                'success': True,
                'message': '연결에 성공했습니다.'
            })
        else:
            raise DatabaseConnectionError("연결 테스트 실패")
            
    except DatabaseConnectionError as e:
        SessionManager.save_data(request, SessionManager.Keys.DB_CONN_STATUS, 'need')
        logger.warning(f"Oracle connection failed: {e}")
        return JsonResponse({
            'success': False,
            'message': str(e)
        })
    except Exception as e:
        SessionManager.save_data(request, SessionManager.Keys.DB_CONN_STATUS, 'need')
        logger.exception(f"Unexpected error during connection test: {e}")
        return JsonResponse({
            'success': False,
            'message': f'연결 실패: {str(e)}'
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
        # ConnectionConfig 생성
        config = ConnectionConfig(
            host=params['host'],
            port=params['port'],
            username=params['username'],
            password=params['password'],
            database=params['dbname']
        )
        
        redshift_conn = RedshiftConnection(config)
        
        if redshift_conn.test_connection():
            # 세션에 저장
            SessionManager.save_redshift_connection(request, config.to_dict())
            
            logger.info(f"Redshift connection successful for user: {params['username']}")
            return JsonResponse({
                'success': True,
                'message': 'Redshift 연결에 성공했습니다.'
            })
        else:
            raise DatabaseConnectionError("연결 테스트 실패")
            
    except Exception as e:
        SessionManager.save_data(request, SessionManager.Keys.RS_CONN_STATUS, 'need')
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
        
        config = ConnectionConfig(
            host=oracle_params['host'],
            port=oracle_params['port'],
            username=oracle_params['username'],
            password=oracle_params['password'],
            service_name=oracle_params['service_name']
        )
        
        oracle_conn = OracleConnection(config)
        
        if oracle_conn.test_connection():
            SessionManager.save_oracle_connection(request, config.to_dict())
            oracle_status = 'ok'
            logger.info(f"Oracle connected: {oracle_params['username']}")
            
    except Exception as e:
        oracle_error = str(e)
        SessionManager.save_data(request, SessionManager.Keys.DB_CONN_STATUS, 'need')
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
        
        config = ConnectionConfig(
            host=redshift_params['host'],
            port=redshift_params['port'],
            username=redshift_params['username'],
            password=redshift_params['password'],
            database=redshift_params['dbname']
        )
        
        redshift_conn = RedshiftConnection(config)
        
        if redshift_conn.test_connection():
            SessionManager.save_redshift_connection(request, config.to_dict())
            redshift_status = 'ok'
            logger.info(f"Redshift connected: {redshift_params['username']}")
            
    except Exception as e:
        redshift_error = str(e)
        SessionManager.save_data(request, SessionManager.Keys.RS_CONN_STATUS, 'need')
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


@login_required
def check_db_status(request):
    """데이터베이스 연결 상태 확인 API"""
    db_info = SessionManager.get_db_connection_info(request)
    return JsonResponse(db_info)