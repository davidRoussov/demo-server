from django.urls import path
from .views import upload_file

urlpatterns = [
    path('uploads/', upload_file, name='upload_file'),
]
