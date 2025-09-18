# str_dashboard/urls.py

from django.urls import path
from django.contrib.auth import views as auth_views
from . import views

urlpatterns = [
    # ==================== 인증 ====================
    path(
        '',
        auth_views.LoginView.as_view(
            template_name='str_dashboard/login.html',
            redirect_authenticated_user=True
        ),
        name='login'
    ),
    path(
        'logout/',
        auth_views.LogoutView.as_view(),
        name='logout'
    ),

    # ==================== 페이지 ====================
    path('home/', views.home, name='home'),
    path('menu1/menu1_1/', views.menu1_1, name='menu1_1'),

    # ==================== 데이터베이스 연결 API ====================
    path(
        'api/test-oracle/',
        views.test_oracle_connection,
        name='test_oracle_connection'
    ),
    path(
        'api/test-redshift/',
        views.test_redshift_connection,
        name='test_redshift_connection'
    ),
    path(
        'api/connect-all/',
        views.connect_all_databases,
        name='connect_all_databases'
    ),
    
    # ==================== 통합 데이터 처리 API ====================
    path(
        'api/query-integrated/',
        views.query_all_integrated,
        name='query_all_integrated'
    ),
    
    # ==================== DataFrame 관리 API ====================
    path(
        'api/df-status/',
        views.df_manager_status,
        name='df_manager_status'
    ),
    path(
        'api/export-csv/',
        views.export_dataframe_csv,
        name='export_dataframe_csv'
    ),
    
    # ==================== TOML Export API ====================
    path(
        'api/prepare-toml/',
        views.prepare_toml_data,
        name='prepare_toml_data'
    ),
    path(
        'api/download-toml/',
        views.download_toml,
        name='download_toml'
    ),
    
    # ==================== 세션 관리 API ====================
    path(
        'api/save-session/',
        views.save_to_session,
        name='save_to_session'
    ),
]