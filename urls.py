from django.urls import include, path

urlpatterns = [
    path('boto/', include('boto.urls')),
    # outras urls
]