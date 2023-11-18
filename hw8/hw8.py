from google.cloud import storage, pubsub_v1
from flask import Flask, request, abort, send_file, Response
import logging
import os
import requests

app = Flask(__name__)

PROJECT_ID = 'ds561-398719'
TOPIC_NAME = 'error-topic'
BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
def file_server(path):
    filename = path.lstrip('/')
    filename = filename.replace('bu-ds561-bwong778-hw2-bucket/', '')
    
    country = request.headers.get('X-country')
    if country in BANNED_COUNTRIES:
        error_message = f"Access attempt from banned country: {country}"
        publish_error(error_message)
        logging.error("From banned country")
        return "Banned country request: 400", 400
    
    if request.method == 'GET':
            storage_client = storage.Client()
            bucket = storage_client.bucket('bu-ds561-bwong778-hw2-bucket')       
           
            blob = bucket.blob(filename) 
            if(not blob.exists()):
                logging.error("File does not exist: ", 404)
                return 'file not found: 404', 404
            else:
                file_content = blob.download_as_text()
                zone = get_instance_zone()
                response = Response(file_content)
                response.headers['X-VM-Zone'] = zone
                return response
            
    else:
        logging.error(f"Unsupported method: {request.method} for file {filename}")  
        return "Unsupported method: 501", 501

def get_instance_zone():
    try:
        headers = {"Metadata-Flavor": "Google"}
        r = requests.get("http://metadata.google.internal/computeMetadata/v1/instance/zone", headers=headers)
        if r.status_code == 200:

            return r.text.split('/')[-1]
        return "Unknown"
    except Exception as e:
        logging.error(f"Failed to get zone: {str(e)}")
        return "Error"

def publish_error(error_message):
    data = error_message.encode("utf-8")
    try:
        publisher = pubsub_v1.PublisherClient()  
        publisher.publish(topic_path, data)
    except Exception as e:
        logging.error(f"Failed to publish error: {str(e)}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 80)))
