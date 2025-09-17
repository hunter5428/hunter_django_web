# str_dashboard/urls.py
from django.urls import path
from django.contrib.auth import views as auth_views
from . import views

urlpatterns = [
    # 인증
    path('', auth_views.LoginView.as_view(
            template_name='str_dashboard/login.html',
            redirect_authenticated_user=True
        ), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),

    # 페이지
    path('home/', views.home, name='home'),
    path('menu1/menu1_1/', views.menu1_1, name='menu1_1'),

    # API
    path('api/test_oracle_connection/', views.test_oracle_connection, name='test_oracle_connection'),
    path('api/test_redshift_connection/', views.test_redshift_connection, name='test_redshift_connection'),
    path('api/query_alert_info/', views.query_alert_info, name='query_alert_info'),
]