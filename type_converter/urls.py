from django.urls import path
from .views import infer_file
from .views import list_types

urlpatterns = [
    path('inferences/', infer_file, name='infer_file'),
    path('types/', list_types, name='list_types'),
]
