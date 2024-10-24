import logging
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .models import Dataset
import pandas

logger = logging.getLogger(__name__)

@csrf_exempt
def upload_file(request):
    logger.debug('In upload_file')
    logger.debug('Request method: %s', request.method)

    if request.method != 'POST':
        logger.warning('Non-POST request methods are not allowed')
        return JsonResponse({'error': 'Only POST requests are allowed'}, status=405)

    if 'file' not in request.FILES:
        logger.error('No file provided in request')
        return JsonResponse({ 'error': 'No file provided'}, status=400)

    file_obj = request.FILES['file']
    file_name = file_obj.name
    logger.info('Received file: %s', file_name)

    # Preliminary validation based on file extension
    if not (file_name.endswith('.csv') or file_name.endswith('.xls') or file_name.endswith('.xlsx')): 
        logger.error('Invalid file format for file: %s', file_name)
        return JsonResponse({'error': 'Invalid file format. Only CSV and Excel files are allowed.'}, status=400)

    # Comprehensive validation using pandas
    try:
        if file_name.endswith('.csv'):
            df = pandas.read_csv(file_obj)
        else:
            df = pandas.read_excel(file_obj)

        uploaded_dataset = Dataset(name=file_name, dataframe=df)
        logger.info('Successfully processed the file.')

    except Exception as e:
        logger.exception('An error occurred while processing the file: %s', str(e))
        return JsonResponse({'error': f'Invalid file content. Could not process file: {str(e)}'}, status=400)

    row_count, column_count = uploaded_dataset.size()
    logger.info('File contains %d rows and %d columns.', row_count, column_count)

    return JsonResponse({
        'message': 'File uploaded successfully',
        'file_name': file_name,
        'rows': row_count,
        'columns': column_count
    }, status=200)
