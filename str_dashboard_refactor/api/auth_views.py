"""
인증 관련 API Views
"""
import logging
from django.contrib.auth.decorators import login_required
from django.shortcuts import render
from django.contrib.auth import views as auth_views

logger = logging.getLogger(__name__)


@login_required
def home(request):
    """홈 페이지"""
    context = {
        'active_top_menu': '',
        'active_sub_menu': ''
    }
    return render(request, 'str_dashboard/home.html', context)


# LoginView와 LogoutView는 urls.py에서 직접 사용