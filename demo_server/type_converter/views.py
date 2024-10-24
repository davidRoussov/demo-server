from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import Dataset

@csrf_exempt
def upload_file(request):
    if request.method != 'POST':
         return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    if 'file' not in request.FILES:
        return JsonResponse({ 'error': 'No file provided'}, status=400)

    file_obj = request.FILES['file']

    uploaded_dataset = Dataset(name=file_obj.name, content=file_obj.read())

    file_size = uploaded_dataset.size()

    return JsonResponse({
        'message': 'File uploaded successfully',
        'file_name': uploaded_dataset.name,
        'file_size': file_size
    }, status=200)
