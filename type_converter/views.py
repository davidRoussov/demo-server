from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import Dataset
import pandas

@csrf_exempt
def upload_file(request):
    if request.method != 'POST':
         return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    if 'file' not in request.FILES:
        return JsonResponse({ 'error': 'No file provided'}, status=400)

    file_obj = request.FILES['file']
    file_name = file_obj.name

    # Preliminary validation based on file extension
    if not (file_name.endswith('.csv') or file_name.endswith('.xls') or file_name.endswith('.xlsx')):
        return JsonResponse({'error': 'Invalid file format. Only CSV and Excel files are allowed.'}, status=400)

    # Comprehensive validation using pandas
    try:
        if file_name.endswith('.csv'):
            df = pandas.read_csv(file_obj)
        else:
            df = pandas.read_excel(file_obj)

        uploaded_dataset = Dataset(name=file_name, dataframe=df)
    except Exception as e:
        return JsonResponse({'error': f'Invalid file content. Could not process file: {str(e)}'}, status=400)

    row_count, column_count = uploaded_dataset.size()

    return JsonResponse({
        'message': 'File uploaded successfully',
        'file_name': file_name,
        'rows': row_count,
        'columns': column_count
    }, status=200)
