from flask import Flask, request, abort
from google.cloud import storage, pubsub_v1
from google.cloud.sql.connector import connector
import pymysql
import logging
import os
from dotenv import load_dotenv

storage_client = storage.Client()
load_dotenv()

app = Flask(__name__)

PROJECT_ID = 'ds561-398719'
TOPIC_NAME = 'error-topic'
BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

# Database connection function
def get_db_connection():
    conn = connector.connect(
        os.environ.get("DB_CONNECTION_STRING"),  # fetching the connection string from the environment variable
        "pymysql",
        user=os.environ.get("DB_USER"),          # fetching the user from the environment variable
        password=os.environ.get("DB_PASSWORD"),  # fetching the password from the environment variable
        db=os.environ.get("DB_NAME"),            # fetching the database name from the environment variable
    )
    return conn

# Function to insert request data into the database
def insert_request_data(country, client_ip, gender, age, income, is_banned, time_of_day, requested_file):
    connection = get_db_connection()
    cursor = connection.cursor()

    # Fetch country_id for given country
    cursor.execute("SELECT country_id FROM Countries WHERE country_name = %s", (country,))
    country_id = cursor.fetchone()
    if not country_id:
        cursor.execute("INSERT INTO Countries (country_name) VALUES (%s)", (country,))
        country_id = cursor.lastrowid
    else:
        country_id = country_id[0]

    # Check if client_ip and country_id pairing exists
    cursor.execute("SELECT 1 FROM ClientIPs WHERE client_ip = %s", (client_ip,))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO ClientIPs (client_ip, country_id) VALUES (%s, %s)", (client_ip, country_id))

    # Fetch ip_id for given client_ip
    cursor.execute("SELECT ip_id FROM ClientIPs WHERE client_ip = %s", (client_ip,))
    ip_id = cursor.fetchone()[0]

    # Insert into Requests
    cursor.execute("""
        INSERT INTO Requests (ip_id, gender, age, income, is_banned, time_of_day, requested_file) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (ip_id, gender, age, income, is_banned, time_of_day, requested_file))

    connection.commit()
    cursor.close()
    connection.close()

# Function to insert error data into the database
def insert_error_data(time_of_request, requested_file, error_code):
    connection = get_db_connection()
    cursor = connection.cursor()
    insert_query = """INSERT INTO Failed_Requests (time_of_request, requested_file, error_code)
                      VALUES (%s, %s, %s)"""
    cursor.execute(insert_query, (time_of_request, requested_file, error_code))
    connection.commit()
    cursor.close()
    connection.close()

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE'])
def file_server(path):
    filename = path.lstrip('/')
    filename = filename.replace('bu-ds561-bwong778-hw2-bucket/', '')
    
    country = request.headers.get('X-country')
    client_ip = request.headers.get('X-client-IP')  
    gender = request.headers.get('X-gender') 
    age = request.headers.get('X-age')
    income = request.headers.get('X-income')
    is_banned = country in BANNED_COUNTRIES
    time_of_day = request.headers.get('X-time')  
    requested_file = filename

    if is_banned:
        logging.error('Banned Country:', 400)
        error_message = f"Access attempt from banned country: {country}"
        publish_error(error_message)
        insert_error_data(time_of_day, requested_file, 400)
        return 'Banned country', 400

    if request.method == 'GET':
        try:
            #removed storage client initalization from here
            bucket = storage_client.bucket('bu-ds561-bwong778-hw2-bucket')         
            blob = bucket.blob(filename) 
            if not blob.exists():
                logging.error('File not found:', 404)
                insert_error_data(time_of_day, requested_file, 404)
                return 'File not found', 404
                
            file_content = blob.download_as_text()
            # Save request data to database
            insert_request_data(country, client_ip, gender, age, income, is_banned, time_of_day, requested_file)
            return file_content, 200
        
        except Exception as e:
            logging.error('Error', 500)
            insert_error_data(time_of_day, requested_file, 500)
            return 'Internal Server Error', 500
    else:
        logging.error('Not Implemented:', 501)
        insert_error_data(time_of_day, requested_file, 501)
        return 'Not Implemented', 501
    
def publish_error(error_message):
    data = error_message.encode("utf-8")
    try:
        publisher.publish(topic_path, data)
    except Exception as e:
        logging.error(f"Failed to publish error: {str(e)}")
    
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
