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
    path('api/query_person_info/', views.query_person_info, name='query_person_info'),
    path('api/rule_history_search/', views.rule_history_search, name='rule_history_search'),
    path('api/query_person_detail_info/', views.query_person_detail_info, name='query_person_detail_info'),
    
    # 중복 회원 조회 API (분리된 버전)
    #path('api/query_duplicate_by_email/', views.query_duplicate_by_email, name='query_duplicate_by_email'),
    #path('api/query_duplicate_by_address/', views.query_duplicate_by_address, name='query_duplicate_by_address'),
    #path('api/query_duplicate_by_workplace/', views.query_duplicate_by_workplace, name='query_duplicate_by_workplace'),
    #path('api/query_duplicate_by_workplace_address/', views.query_duplicate_by_workplace_address, name='query_duplicate_by_workplace_address'),
    path('api/query_duplicate_unified/', views.query_duplicate_unified, name='query_duplicate_unified'),

    # 법인 관련인 조회 API
    path('api/query_corp_related_persons/', views.query_corp_related_persons, name='query_corp_related_persons'),
]