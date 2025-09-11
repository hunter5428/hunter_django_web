# HUNTER_APP_DJANGO/HUNTER_APP_DJANGO/urls.py
from django.contrib import admin
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),

    # 앱 라우트
    path('', include('str_dashboard.urls')),
]
