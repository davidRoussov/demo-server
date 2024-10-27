from django.urls import path
from .views import upload_file
from .views import list_types

urlpatterns = [
    path('uploads/', upload_file, name='upload_file'),
    path('types/', list_types, name='list_types'),
]
