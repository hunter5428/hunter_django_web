# str_dashboard/views.py (간소화 버전)

import json
import logging
from django.contrib.auth.decorators import login_required
from django.http import JsonResponse, HttpResponse, HttpResponseBadRequest, FileResponse
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
from .dataframe_manager import df_manager
from .toml import toml_collector, toml_exporter
from .orderbook_analyzer import OrderbookAnalyzer

# 데이터베이스 모듈
from .database import (
    OracleConnection,
    OracleConnectionError,
    RedshiftConnection,
    SQLQueryManager,
    execute_oracle_query_with_error_handling,
)

# 쿼리 모듈
from .queries.rule_historic_search import (
    fetch_df_result_0, 
    aggregate_by_rule_id_list,
)

logger = logging.getLogger(__name__)


# ==================== 세션 관리 헬퍼 ====================
class SessionManager:
    """세션 데이터 관리를 위한 헬퍼 클래스"""
    
    @staticmethod
    def save_data(request, key: str, data: Any, force_update: bool = True) -> None:
        """세션에 데이터 저장"""
        request.session[key] = data
        if force_update:
            request.session.modified = True
        logger.debug(f"Session saved: {key}")
    
    @staticmethod
    def save_multiple(request, data_dict: Dict[str, Any]) -> None:
        """여러 데이터를 한번에 세션에 저장"""
        for key, value in data_dict.items():
            request.session[key] = value
        request.session.modified = True
        logger.debug(f"Session saved: {len(data_dict)} items")
    
    @staticmethod
    def get_data(request, key: str, default: Any = None) -> Any:
        """세션에서 데이터 가져오기"""
        return request.session.get(key, default)


# ==================== 페이지 뷰 ====================

@login_required
def home(request):
    """홈 페이지"""
    return render(request, 'str_dashboard/home.html', {
        'active_top_menu': '',
        'active_sub_menu': ''
    })


@login_required
def menu1_1(request):
    """ALERT ID 조회 페이지"""
    db_info = SessionManager.get_data(request, 'db_conn')
    rs_info = SessionManager.get_data(request, 'rs_conn')
    
    # Oracle 기본 서비스명 추출
    default_service = 'PRDAMLKR.OCIAMLPRODDBA.OCIAMLPROD.ORACLEVCN.COM'
    if db_info and isinstance(db_info, dict):
        jdbc_url = db_info.get('jdbc_url', '')
        if jdbc_url.startswith('jdbc:oracle:thin:@//'):
            try:
                default_service = jdbc_url.split('/', 3)[-1]
            except Exception:
                pass
    
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
    }
    return render(request, 'str_dashboard/menu1_1/main.html', context)


# ==================== 데이터베이스 연결 ====================

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
            
            return JsonResponse({'success': True, 'message': '연결에 성공했습니다.'})
        else:
            raise OracleConnectionError("연결 테스트 실패")
            
    except Exception as e:
        SessionManager.save_data(request, 'db_conn_status', 'need')
        logger.warning(f"Oracle connection failed: {e}")
        return JsonResponse({'success': False, 'message': str(e)})


@login_required
@require_POST
def test_redshift_connection(request):
    """Redshift 데이터베이스 연결 테스트"""
    required_params = ['host', 'port', 'dbname', 'username', 'password']
    params = {}
    
    for param in required_params:
        value = request.POST.get(param, '').strip() if param != 'password' else request.POST.get(param, '')
        if not value:
            return JsonResponse({'success': False, 'message': f'{param}을(를) 입력해주세요.'})
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
            
            logger.info(f"Redshift connection successful")
            return JsonResponse({'success': True, 'message': 'Redshift 연결에 성공했습니다.'})
        else:
            raise Exception("연결 테스트 실패")
            
    except Exception as e:
        SessionManager.save_data(request, 'rs_conn_status', 'need')
        logger.exception(f"Redshift connection test failed: {e}")
        return JsonResponse({'success': False, 'message': str(e)})


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
                raise ValueError(f'Oracle {key} 누락')
        
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
            
    except Exception as e:
        oracle_error = str(e)
        SessionManager.save_data(request, 'db_conn_status', 'need')
    
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
                raise ValueError(f'Redshift {key} 누락')
        
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
            
    except Exception as e:
        redshift_error = str(e)
        SessionManager.save_data(request, 'rs_conn_status', 'need')
    
    request.session.set_expiry(3600)
    
    return JsonResponse({
        'success': oracle_status == 'ok' and redshift_status == 'ok',
        'message': '모든 데이터베이스 연결 성공' if oracle_status == 'ok' and redshift_status == 'ok' else '일부 데이터베이스 연결 실패',
        'oracle_status': oracle_status,
        'oracle_error': oracle_error,
        'redshift_status': redshift_status,
        'redshift_error': redshift_error
    })


# ==================== 통합 데이터 처리 (메인 API) ====================

@login_required
@require_POST
def query_all_data_integrated(request):
    """
    ALERT ID 기반 모든 데이터를 한번에 조회하는 통합 함수
    DataFrame Manager를 사용하여 중앙 관리
    """
    alert_id = request.POST.get('alert_id', '').strip()
    if not alert_id:
        return JsonResponse({'success': False, 'message': 'ALERT ID를 입력하세요.'})
    
    # DB 연결 확인
    db_info = SessionManager.get_data(request, 'db_conn')
    if not db_info or SessionManager.get_data(request, 'db_conn_status') != 'ok':
        return JsonResponse({'success': False, 'message': '먼저 Oracle 연결을 완료해 주세요.'})
    
    try:
        # DataFrame Manager 초기화
        df_manager.reset()
        df_manager.set_alert_id(alert_id)
        
        logger.info(f"Starting integrated query for ALERT ID: {alert_id}")
        
        # Oracle 연결
        oracle_conn = OracleConnection.from_session(db_info)
        
        # 1. ALERT 정보 조회
        alert_result = execute_oracle_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='alert_info_by_alert_id.sql',
            bind_params={':alert_id': '?'},
            query_params=[alert_id]
        )
        
        if not alert_result.get('success'):
            return JsonResponse(alert_result)
        
        # ALERT 데이터 처리
        alert_metadata = df_manager.process_alert_data(alert_result)
        
        if not alert_metadata.get('cust_id'):
            return JsonResponse({'success': False, 'message': 'ALERT에 연결된 고객 정보가 없습니다.'})
        
        # 2. 고객 정보 조회
        cust_id = alert_metadata['cust_id']
        customer_result = execute_oracle_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='customer_unified_info.sql',
            bind_params={':custId': '?'},
            query_params=[cust_id]
        )
        
        if customer_result.get('success'):
            customer_metadata = df_manager.process_customer_data(customer_result)
            customer_type = customer_metadata.get('customer_type')
            
            # 3. 추가 쿼리 실행
            _execute_additional_queries(request, oracle_conn, alert_metadata, customer_metadata, customer_type, cust_id)
        
        # 4. 전체 데이터 요약
        summary = df_manager.get_all_datasets_summary()
        
        # 5. 세션에 저장 (TOML 내보내기용)
        export_data = df_manager.export_to_dict()
        SessionManager.save_multiple(request, {
            'current_alert_id': alert_id,
            'current_alert_data': export_data,
            'df_manager_summary': summary
        })
        
        # DataFrame에서 필요한 데이터 추출하여 세션에 저장 (TOML 처리용)
        _save_toml_data_to_session(request)
        
        logger.info(f"Integrated query completed for ALERT ID: {alert_id}")
        logger.info(f"Total datasets: {len(df_manager.datasets)}, Total memory: {summary['total_memory_mb']} MB")
        
        return JsonResponse({
            'success': True,
            'alert_id': alert_id,
            'summary': summary,
            'message': f"데이터 조회 완료: {len(summary['datasets'])}개 데이터셋"
        })
        
    except Exception as e:
        logger.exception(f"Error in integrated query: {e}")
        return JsonResponse({'success': False, 'message': f'통합 조회 중 오류: {str(e)}'})


def _execute_additional_queries(request, oracle_conn, alert_metadata, customer_metadata, customer_type, cust_id):
    """추가 쿼리들 실행 (내부 함수)"""
    # Rule 히스토리
    if alert_metadata.get('canonical_ids'):
        rule_key = ','.join(sorted(alert_metadata['canonical_ids']))
        try:
            db_info = oracle_conn.__dict__
            df0 = fetch_df_result_0(
                jdbc_url=db_info['jdbc_url'],
                driver_class=db_info['driver_class'],
                driver_path=db_info['driver_path'],
                username=db_info['username'],
                password=db_info['password']
            )
            df1 = aggregate_by_rule_id_list(df0)
            matching_rows = df1[df1["STR_RULE_ID_LIST"] == rule_key]
            
            columns = list(matching_rows.columns) if not matching_rows.empty else []
            rows = matching_rows.values.tolist()
            df_manager.add_dataset('rule_history', columns, rows, rule_key=rule_key)
        except Exception as e:
            logger.error(f"Rule history query failed: {e}")
    
    # 법인/개인별 관련인
    if customer_type == '법인':
        result = execute_oracle_query_with_error_handling(
            oracle_conn=oracle_conn,
            sql_filename='corp_related_persons.sql',
            bind_params={':cust_id': '?'},
            query_params=[cust_id]
        )
        if result.get('success'):
            df_manager.add_dataset('corp_related', result.get('columns', []), result.get('rows', []))
    else:
        # 거래 기간 계산
        tran_start, tran_end = df_manager.calculate_transaction_period()
        if tran_start and tran_end:
            # 개인 관련인
            result = execute_oracle_query_with_error_handling(
                oracle_conn=oracle_conn,
                sql_filename='person_related_summary.sql',
                bind_params={':cust_id': '?', ':start_date': '?', ':end_date': '?'},
                query_params=[tran_start, tran_end, cust_id, cust_id, tran_start, tran_end]
            )
            if result.get('success'):
                df_manager.add_dataset('person_related', result.get('columns', []), result.get('rows', []))
            
            # IP 이력
            mid = customer_metadata.get('mid')
            if mid:
                result = execute_oracle_query_with_error_handling(
                    oracle_conn=oracle_conn,
                    sql_filename='query_ip_access_history.sql',
                    bind_params={':mem_id': '?', ':start_date': '?', ':end_date': '?'},
                    query_params=[mid, tran_start.split(' ')[0], tran_end.split(' ')[0]]
                )
                if result.get('success'):
                    df_manager.add_dataset('ip_history', result.get('columns', []), result.get('rows', []))
                
                # Redshift Orderbook
                if SessionManager.get_data(request, 'rs_conn_status') == 'ok':
                    _query_orderbook(request, mid, tran_start.split(' ')[0], tran_end.split(' ')[0])
    
    # 중복 회원
    customer_df = df_manager.get_dataframe('customer_info')
    if customer_df is not None and not customer_df.empty:
        dup_params = _extract_duplicate_params(customer_df)
        if dup_params:
            result = execute_oracle_query_with_error_handling(
                oracle_conn=oracle_conn,
                sql_filename='duplicate_unified.sql',
                bind_params={
                    ':current_cust_id': '?', ':address': '?', ':detail_address': '?',
                    ':workplace_name': '?', ':workplace_address': '?', 
                    ':workplace_detail_address': '?', ':phone_suffix': '?'
                },
                query_params=[
                    cust_id, dup_params.get('address'), dup_params.get('address'),
                    dup_params.get('detail_address'), cust_id,
                    dup_params.get('workplace_name'), dup_params.get('workplace_name'),
                    cust_id, dup_params.get('workplace_address'), dup_params.get('workplace_address'),
                    dup_params.get('workplace_detail_address'), dup_params.get('workplace_detail_address'),
                    dup_params.get('phone_suffix'), dup_params.get('phone_suffix')
                ]
            )
            if result.get('success'):
                df_manager.add_dataset('duplicate_persons', result.get('columns', []), result.get('rows', []))


def _query_orderbook(request, mem_id, start_date, end_date):
    """Redshift Orderbook 조회 (내부 함수)"""
    try:
        rs_info = SessionManager.get_data(request, 'rs_conn')
        if not rs_info:
            return
        
        redshift_conn = RedshiftConnection.from_session(rs_info)
        
        # 날짜 조정 (D+1)
        start_dt = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=1)
        end_dt = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        
        sql_path = Path(settings.BASE_DIR) / 'str_dashboard' / 'queries' / 'redshift_orderbook.sql'
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_query = f.read()
        
        cols, rows = redshift_conn.execute_query(
            sql_query,
            params=[
                start_dt.strftime('%Y-%m-%d 00:00:00'),
                end_dt.strftime('%Y-%m-%d 23:59:59'),
                mem_id
            ]
        )
        
        if cols and rows:
            df_manager.add_dataset('orderbook', cols, rows,
                                 user_id=mem_id,
                                 start_date=start_date,
                                 end_date=end_date)
            
            # Orderbook 분석
            df = pd.DataFrame(rows, columns=cols)
            analyzer = OrderbookAnalyzer(df)
            analyzer.analyze()
            
            analysis_result = {
                'text_summary': analyzer.generate_text_summary(),
                'patterns': analyzer.get_pattern_analysis(),
                'daily_summary': analyzer.get_daily_summary().to_dict('records') if not analyzer.get_daily_summary().empty else []
            }
            
            df_manager.add_dataset('orderbook_analysis',
                                  ['analysis_type', 'data'],
                                  [['summary', analysis_result]],
                                  **analysis_result)
            
    except Exception as e:
        logger.error(f"Orderbook query failed: {e}")


def _extract_duplicate_params(customer_df):
    """고객 정보에서 중복 검색 파라미터 추출 (내부 함수)"""
    if customer_df.empty:
        return {}
    
    row = customer_df.iloc[0]
    phone = row.get('연락처', '')
    phone_suffix = phone[-4:] if len(str(phone)) >= 4 else ''
    
    return {
        'full_email': row.get('이메일', ''),
        'phone_suffix': phone_suffix,
        'address': row.get('거주지주소', ''),
        'detail_address': row.get('거주지상세주소', ''),
        'workplace_name': row.get('직장명', ''),
        'workplace_address': row.get('직장주소', ''),
        'workplace_detail_address': row.get('직장상세주소', '')
    }


def _save_toml_data_to_session(request):
    """DataFrame 데이터를 TOML용 세션 데이터로 변환 저장"""
    # 고객 정보
    customer_df = df_manager.get_dataframe('customer_info')
    if customer_df is not None and not customer_df.empty:
        SessionManager.save_data(request, 'current_customer_data', {
            'columns': list(customer_df.columns),
            'rows': customer_df.values.tolist(),
            'customer_type': df_manager.metadata.get('customer_type'),
            'cust_id': df_manager.metadata.get('cust_id')
        })
    
    # 중복 회원
    dup_df = df_manager.get_dataframe('duplicate_persons')
    if dup_df is not None and not dup_df.empty:
        SessionManager.save_data(request, 'duplicate_persons_data', {
            'columns': list(dup_df.columns),
            'rows': dup_df.values.tolist()
        })


# ==================== DataFrame Manager APIs ====================

@login_required
def get_dataframe_manager_status(request):
    """DataFrame Manager 상태 조회 API"""
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
        return JsonResponse({'success': False, 'message': 'Dataset name required'})
    
    df = df_manager.get_dataframe(dataset_name)
    
    if df is None:
        return JsonResponse({'success': False, 'message': f'Dataset {dataset_name} not found'})
    
    try:
        output = io.StringIO()
        df.to_csv(output, index=False, encoding='utf-8-sig')
        
        response = HttpResponse(output.getvalue(), content_type='text/csv; charset=utf-8-sig')
        response['Content-Disposition'] = f'attachment; filename="{dataset_name}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
        
        return response
        
    except Exception as e:
        logger.error(f"CSV export failed: {e}")
        return JsonResponse({'success': False, 'message': str(e)})


# ==================== TOML Export ====================

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
            'duplicate_persons_data': SessionManager.get_data(request, 'duplicate_persons_data', {}),
        }
        
        alert_id = SessionManager.get_data(request, 'current_alert_id', 'unknown')
        filename = toml_exporter.generate_filename(alert_id)
        
        tmp_dir = tempfile.gettempdir()
        tmp_path = Path(tmp_dir) / filename
        
        success = toml_exporter.export_to_toml(session_data, str(tmp_path))
        
        if success:
            SessionManager.save_data(request, 'toml_temp_path', str(tmp_path))
            collected_data = toml_collector.collect_all_data(session_data)
            
            return JsonResponse({
                'success': True,
                'message': 'TOML 데이터 준비 완료',
                'data_count': len(collected_data),
                'sections': list(collected_data.keys()),
                'filename': filename
            })
        else:
            return JsonResponse({'success': False, 'message': 'TOML 데이터 준비 실패'})
            
    except Exception as e:
        logger.exception(f"Error preparing TOML data: {e}")
        return JsonResponse({'success': False, 'message': f'TOML 데이터 준비 실패: {str(e)}'})


@login_required
def download_toml(request):
    """준비된 TOML 파일 다운로드"""
    try:
        tmp_path = SessionManager.get_data(request, 'toml_temp_path')
        if not tmp_path or not Path(tmp_path).exists():
            return JsonResponse({'success': False, 'message': 'TOML 파일을 찾을 수 없습니다.'})
        
        alert_id = SessionManager.get_data(request, 'current_alert_id', 'unknown')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'str_data_{alert_id}_{timestamp}.toml'
        
        response = FileResponse(
            open(tmp_path, 'rb'),
            as_attachment=True,
            filename=filename
        )
        
        return response
        
    except Exception as e:
        logger.exception(f"Error downloading TOML: {e}")
        return JsonResponse({'success': False, 'message': f'TOML 다운로드 실패: {str(e)}'})


# ==================== 세션 관리 ====================

@login_required
@require_POST
def save_to_session(request):
    """JavaScript에서 세션에 데이터 저장"""
    key = request.POST.get('key', '').strip()
    data = request.POST.get('data', '').strip()
    
    if not key:
        return JsonResponse({'success': False, 'message': 'Key is required'})
    
    try:
        parsed_data = json.loads(data) if data else {}
        SessionManager.save_data(request, key, parsed_data)
        
        return JsonResponse({'success': True, 'key': key})
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decode error: {e}")
        return JsonResponse({'success': False, 'message': f'Invalid JSON data: {e}'})
    except Exception as e:
        logger.exception(f"Error saving to session: {e}")
        return JsonResponse({'success': False, 'message': f'Failed to save: {e}'})