from flask import Flask, request, abort
from google.cloud import storage, pubsub_v1
import logging
import os


app = Flask(__name__)

logging.basicConfig(filename='app1.log', filemode='w', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

PROJECT_ID = 'ds561-398719'
TOPIC_NAME = 'error-topic'
BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

#PubSub and Storage
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)


@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE'])
def file_server(path):
    
    if request.method == 'GET':
        
        try:
        
            filename = path.lstrip('/')
            filename = filename.replace('bu-ds561-bwong778-hw2-bucket/', '')
            
            country = request.headers.get('X-country')
            is_banned = country in BANNED_COUNTRIES
            requested_file = filename

            if is_banned:
                logging.error("Access attempt from banned country: %s", country)
                error_message = f"Access attempt from banned country: {country}"
                publish_error(error_message)
                return 'Banned country', 400

            bucket = storage_client.bucket('bu-ds561-bwong778-hw2-bucket')         
            blob = bucket.blob(filename) 
            if not blob.exists():
                logging.error('File not found:', 404)
                return 'File not found', 404
                
            file_content = blob.download_as_text()
            return file_content, 200
        
        except Exception as e:
            logging.error("Internal Server Error: %s", str(e))
            return 'Internal Server Error', 500
    else:
        logging.error("Not Implemented: %s", requested_file)
        return 'Not Implemented', 501
    
def publish_error(error_message):
    data = error_message.encode("utf-8")
    try:
        publisher.publish(topic_path, data)
    except Exception as e:
        logging.error("Failed to publish error: %s", str(e))
        
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
