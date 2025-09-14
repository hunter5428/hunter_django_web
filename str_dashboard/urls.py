# str_dashboard/urls.py
from django.urls import path
from django.contrib.auth import views as auth_views
from . import views

urlpatterns = [
    # 로그인
    path(
        '',
        auth_views.LoginView.as_view(
            template_name='str_dashboard/login.html',
            redirect_authenticated_user=True
        ),
        name='login'
    ),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),

    # 홈/메뉴
    path('home/', views.home, name='home'),
    path('menu1/menu1_1/', views.menu1_1, name='menu1_1'),

    # Database Connection APIs
    path('api/test_oracle_connection/', views.test_oracle_connection, name='test_oracle_connection'),
    path('api/test_redshift_connection/', views.test_redshift_connection, name='test_redshift_connection'),
    path('api/connect_all_databases/', views.connect_all_databases, name='connect_all_databases'),
    path('api/check_db_status/', views.check_db_status, name='check_db_status'),
    
    # Oracle Query APIs
    path('api/query_alert_info/', views.query_alert_info, name='query_alert_info'),
    path('api/query_customer_unified/', views.query_customer_unified_info, name='query_customer_unified'),
    path('api/rule_history_search/', views.rule_history_search, name='rule_history_search'),
    path('api/query_duplicate_unified/', views.query_duplicate_unified, name='query_duplicate_unified'),
    path('api/query_corp_related_persons/', views.query_corp_related_persons, name='query_corp_related_persons'),
    path('api/query_person_related_summary/', views.query_person_related_summary, name='query_person_related_summary'),
    path('api/query_ip_access_history/', views.query_ip_access_history, name='query_ip_access_history'),

    # Redshift Orderbook APIs
    path('api/query_redshift_orderbook/', views.query_redshift_orderbook, name='query_redshift_orderbook'),
    path('api/get_cached_orderbook_info/', views.get_cached_orderbook_info, name='get_cached_orderbook_info'),
    path('api/clear_orderbook_cache/', views.clear_orderbook_cache, name='clear_orderbook_cache'),
    # Orderbook Analysis APIs (기존 Orderbook APIs 섹션에 추가)
    path('api/analyze_cached_orderbook/', views.analyze_cached_orderbook, name='analyze_cached_orderbook'),
    path('api/get_orderbook_summary/', views.get_orderbook_summary, name='get_orderbook_summary'),
]