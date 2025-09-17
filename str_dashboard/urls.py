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
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),

    # ==================== 페이지 ====================
    path('home/', views.home, name='home'),
    path('menu1/menu1_1/', views.menu1_1, name='menu1_1'),

    # ==================== 데이터베이스 연결 ====================
    path('api/test_oracle_connection/', views.test_oracle_connection, name='test_oracle_connection'),
    path('api/test_redshift_connection/', views.test_redshift_connection, name='test_redshift_connection'),
    path('api/connect_all_databases/', views.connect_all_databases, name='connect_all_databases'),
    
    # ==================== 통합 데이터 처리 ====================
    path('api/query_all_integrated/', views.query_all_data_integrated, name='query_all_integrated'),
    path('api/df_manager_status/', views.get_dataframe_manager_status, name='df_manager_status'),
    path('api/export_dataframe_csv/', views.export_dataframe_to_csv, name='export_dataframe_csv'),
    
    # ==================== TOML Export ====================
    path('api/prepare_toml_data/', views.prepare_toml_data, name='prepare_toml_data'),
    path('api/download_toml/', views.download_toml, name='download_toml'),
    
    # ==================== 세션 관리 ====================
    path('api/save_to_session/', views.save_to_session, name='save_to_session'),
]