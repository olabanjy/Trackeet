from .views import *
from django.urls import path

app_name = 'home'

urlpatterns = [
   path('', homepage, name='homepage'),
   
]