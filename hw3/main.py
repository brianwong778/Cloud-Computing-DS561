from google.cloud import storage, pubsub_v1
from flask import Flask, request, abort, send_file
import logging
import functions_framework
import os
import requests

app = Flask(__name__)

PROJECT_ID = 'ds561-398719'
TOPIC_NAME = 'error-topic'
BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

@functions_framework.http
def file_server(request):
    
    filename = request.path.lstrip('/')
    filename = filename.replace('bu-ds561-bwong778-hw2-bucket/', '')
    
    country = request.headers.get('X-country')
    if country in BANNED_COUNTRIES:
        error_message = f"Access attempt from banned country: {country}"
        publish_error(error_message)

        abort(400)
    
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
        

def publish_error(error_message):
    data = error_message.encode("utf-8")
    try:
        publisher = pubsub_v1.PublisherClient()  
        publisher.publish(topic_path, data)
    except Exception as e:
        logging.error(f"Failed to publish error: {str(e)}")
    

if __name__ =="__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
        


