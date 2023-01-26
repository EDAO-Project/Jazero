from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path('jdlc/', include('jdlc.urls')),
    path('jdlc/result/', include('jdlc.urls')),
    path('admin/', admin.site.urls),
]