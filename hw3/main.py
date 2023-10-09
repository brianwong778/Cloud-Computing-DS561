from google.cloud import storage
from flask import Flask, request, abort, send_file
import logging
import functions_framework
import os

app = Flask(__name__)

@functions_framework.http

def file_server(request):
    if request.method == 'GET':
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket('bu-ds561-bwong778-hw2-bucket')
            
            blob = bucket.blob(request)
            file_content = blob.download_as_text()
            return send_file(file_content, attachment_filename=request, as_attachment=True), 200
        
        except FileNotFoundError:
            logging.error(f"File not found: {request}")
            abort(404)
            
    else:
        logging.error(f"Unsupported method: {request.method} for file {request}")
        abort(501)
        
            
if __name__ =="__main__":
    app.run(host="0.0.0.0", port = int(os.environ.get("PORT",8080)))