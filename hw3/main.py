from google.cloud import storage
from flask import Flask, request, abort, send_file
import logging
import functions_framework
import os

app = Flask(__name__)

@functions_framework.http
def file_server(request):
    
    filename = request.path.lstrip('/')
    filename = filename.replace('bu-ds561-bwong778-hw2-bucket/', '')
    
    if request.method == 'GET':
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket('bu-ds561-bwong778-hw2-bucket')
            
            blob = bucket.blob(filename) 
            file_content = blob.download_as_text()
            
            return send_file(file_content, attachment_filename=filename, as_attachment=True), 200
        
        except Exception as e:
            logging.error(f"Error processing request: {e}")
            abort(500)  
            
    else:
        logging.error(f"Unsupported method: {request.method} for file {filename}")  
        abort(501)
        
            
if __name__ =="__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))


