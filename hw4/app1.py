from flask import Flask, send_from_directory, abort
from google.cloud import storage, logging

app = Flask(__name__)

# Setup GCP storage
storage_client = storage.Client()
bucket = storage_client.get_bucket('YOUR_BUCKET_NAME')

# Setup GCP logging
logging_client = logging.Client()
logger = logging_client.logger('webserver_logs')

@app.route('/<path:filename>', methods=['GET'])
def get_file(filename):
    blob = storage.Blob(filename, bucket)
    if blob.exists():
        return blob.download_as_text(), 200
    else:
        logger.log_text(f'404 Error: {filename} not found')
        return "File not found", 404

@app.route('/', methods=['PUT', 'POST', 'DELETE', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE', 'PATCH'])
def not_implemented():
    logger.log_text(f'501 Error: Method not implemented')
    return "Not implemented", 501

if __name__ == '__main__':
    app.run(host='0.0.0.0')
