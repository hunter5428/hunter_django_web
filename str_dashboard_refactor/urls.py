# str_dashboard/urls.py
from django.urls import path
from django.contrib.auth import views as auth_views
from .api import (
    auth_views as api_auth,
    db_views,
    alert_views,
    customer_views,
    orderbook_views,
    export_views
)

urlpatterns = [
    # 인증
    path(
        '',
        auth_views.LoginView.as_view(
            template_name='str_dashboard/login.html',
            redirect_authenticated_user=True
        ),
        name='login'
    ),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),

    # 페이지
    path('home/', api_auth.home, name='home'),
    path('menu1/menu1_1/', alert_views.menu1_1, name='menu1_1'),

    # Database Connection APIs
    path('api/test_oracle_connection/', db_views.test_oracle_connection, name='test_oracle_connection'),
    path('api/test_redshift_connection/', db_views.test_redshift_connection, name='test_redshift_connection'),
    path('api/connect_all_databases/', db_views.connect_all_databases, name='connect_all_databases'),
    path('api/check_db_status/', db_views.check_db_status, name='check_db_status'),
    
    # Alert APIs
    path('api/query_alert_info/', alert_views.query_alert_info, name='query_alert_info'),
    path('api/rule_history_search/', alert_views.rule_history_search, name='rule_history_search'),
    
    # Customer APIs
    path('api/query_customer_unified/', customer_views.query_customer_unified_info, name='query_customer_unified'),
    path('api/query_duplicate_unified/', customer_views.query_duplicate_unified, name='query_duplicate_unified'),
    path('api/query_corp_related_persons/', customer_views.query_corp_related_persons, name='query_corp_related_persons'),
    path('api/query_person_related_summary/', customer_views.query_person_related_summary, name='query_person_related_summary'),
    path('api/query_ip_access_history/', customer_views.query_ip_access_history, name='query_ip_access_history'),

    # Orderbook APIs
    path('api/query_redshift_orderbook/', orderbook_views.query_redshift_orderbook, name='query_redshift_orderbook'),
    path('api/get_cached_orderbook_info/', orderbook_views.get_cached_orderbook_info, name='get_cached_orderbook_info'),
    path('api/clear_orderbook_cache/', orderbook_views.clear_orderbook_cache, name='clear_orderbook_cache'),
    path('api/analyze_cached_orderbook/', orderbook_views.analyze_cached_orderbook, name='analyze_cached_orderbook'),
    path('api/get_orderbook_summary/', orderbook_views.get_orderbook_summary, name='get_orderbook_summary'),
    path('api/analyze_alert_orderbook/', orderbook_views.analyze_alert_orderbook, name='analyze_alert_orderbook'),
    path('api/analyze_stds_dtm_orderbook/', orderbook_views.analyze_stds_dtm_orderbook, name='analyze_stds_dtm_orderbook'),
    
    # Export APIs
    path('api/prepare_toml_data/', export_views.prepare_toml_data, name='prepare_toml_data'),
    path('api/download_toml/', export_views.download_toml, name='download_toml'),
    path('api/save_to_session/', export_views.save_to_session, name='save_to_session'),
]