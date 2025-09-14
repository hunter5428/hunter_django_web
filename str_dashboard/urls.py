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

    # API
    path('api/test_oracle_connection/', views.test_oracle_connection, name='test_oracle_connection'),
    path('api/query_alert_info/', views.query_alert_info, name='query_alert_info'),
    path('api/query_customer_unified/', views.query_customer_unified_info, name='query_customer_unified'),
    path('api/rule_history_search/', views.rule_history_search, name='rule_history_search'),
    
    # 중복 회원 조회 API
    path('api/query_duplicate_unified/', views.query_duplicate_unified, name='query_duplicate_unified'),

    # 법인 관련인 조회 API
    path('api/query_corp_related_persons/', views.query_corp_related_persons, name='query_corp_related_persons'),
    
    # 개인 관련인(내부입출금) 조회 API
    path('api/query_person_related_summary/', views.query_person_related_summary, name='query_person_related_summary'),
    
    # IP 접속 내역 조회 API
    path('api/query_ip_access_history/', views.query_ip_access_history, name='query_ip_access_history'),

]