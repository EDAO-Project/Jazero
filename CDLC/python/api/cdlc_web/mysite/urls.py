from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path('cdlc/', include('cdlc.urls')),
    path('cdlc/result/', include('cdlc.urls')),
    path('admin/', admin.site.urls),
]