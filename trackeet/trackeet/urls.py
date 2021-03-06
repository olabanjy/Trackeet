
from django.contrib import admin
from django.conf import settings
from django.conf.urls.static import static
from django.urls import path, include

urlpatterns = [
    path('admin/', admin.site.urls),
    path('trackx/',include('core.urls', namespace='core')),
    path('',include('home.urls', namespace='home')),
    path('yt/',include('tracker.urls', namespace='tracker')),

  
]


urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)