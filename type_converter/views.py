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
    file_name = file_obj.name

    if not (file_name.endswith('.csv') or file_name.endswith('.xls') or file_name.endswith('.xlsx')):
        return JsonResponse({'error': 'Invalid file format. Only CSV and Excel files are allowed.'}, status=400)

    uploaded_dataset = Dataset(name=file_name, content=file_obj.read())

    file_size = uploaded_dataset.size()

    return JsonResponse({
        'message': 'File uploaded successfully',
        'file_name': file_name,
        'file_size': file_size
    }, status=200)
