from flask import escape
import functions_framework
from google.cloud import storage
import logging

storage_client = storage.Client()

@functions_framework.http
def file_server(request):
    # Only allowing GET requests
    if request.method != 'GET':
        logging.error(f"Received unsupported method {request.method}")
        return ("Not Implemented", 501)

    filename = request.args.get('filename')

    if not filename:
        return ("File not specified", 400)

    bucket = storage_client.bucket('bu-ds561-bwong778-hw2-bucket')
    blob = bucket.blob(filename)

    # Check if the file exists
    if not blob.exists():
        logging.warning(f"File not found: {filename}")
        return ("Not Found", 404)

    file_content = blob.download_as_text()

    return file_content, 200
