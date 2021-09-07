from .views import *
from django.urls import path

app_name = 'tracker'

urlpatterns = [
   # path('', Dashboard.as_view(), name='dashboard'),
   path('', reload_data , name='reload-data'),
   path('custom-search/', custom_search , name='custom-search'),
   path('export_query/', export_query , name='export_query'),

   
]