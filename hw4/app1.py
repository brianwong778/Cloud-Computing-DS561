from google.cloud import storage, pubsub_v1
from flask import Flask, request, abort, send_file
import logging
import os
import requests

app = Flask(__name__)

PROJECT_ID = 'ds561-398719'
TOPIC_NAME = 'error-topic'
BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE'])
def file_server(path):
    filename = path.lstrip('/')
    filename = filename.replace('bu-ds561-bwong778-hw2-bucket/', '')
    
    country = request.headers.get('X-country')
    if country in BANNED_COUNTRIES:
        logging.error('Banned Country:', 400)
        error_message = f"Access attempt from banned country: {country}"
        publish_error(error_message)

        return 'Banned country', 400
    
    if request.method == 'GET':
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket('bu-ds561-bwong778-hw2-bucket')
            
            blob = bucket.blob(filename) 
            if not blob.exists():
                logging.error('File not found:', 404)
                return 'File not found', 404
                
            file_content = blob.download_as_text()
            return file_content, 200
        
        except Exception as e:
            logging.error('Error', 500)
            return 'Internal Server Error', 500
    else:
        logging.error('Not Implemented:', 501)
        return 'Not Implemented', 501



def publish_error(error_message):
    data = error_message.encode("utf-8")
    try:
        publisher.publish(topic_path, data)
    except Exception as e:
        logging.error(f"Failed to publish error: {str(e)}")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
