from flask import Flask, request, abort
from google.cloud import storage
from google.cloud.sql.connector import connector
import pymysql
import logging
import os
from dotenv import load_dotenv  # Importing python-dotenv module

load_dotenv()  # Loading environment variables from .env file

app = Flask(__name__)

PROJECT_ID = 'ds561-398719'
BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

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

    # Check if client_ip and country pairing exists
    cursor.execute("SELECT 1 FROM CountryIPs WHERE client_ip = %s", (client_ip,))
    if not cursor.fetchone():
        cursor.execute("INSERT INTO CountryIPs (client_ip, country) VALUES (%s, %s)", (client_ip, country))

    # Insert into Requests_Meta
    cursor.execute("""
        INSERT INTO hw5_Requests_Meta (client_ip, gender, age, income, is_banned, time_of_day) 
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (client_ip, gender, age, income, is_banned, time_of_day))
    
    # Retrieve the request_id
    request_id = cursor.lastrowid

    # Insert into Requests_Files
    cursor.execute("INSERT INTO hw5_Requests_Files (request_id, requested_file) VALUES (%s, %s)", (request_id, requested_file))

    connection.commit()
    cursor.close()
    connection.close()

# Function to insert error data into the database
def insert_error_data(time_of_request, requested_file, error_code):
    connection = get_db_connection()
    cursor = connection.cursor()
    insert_query = """INSERT INTO hw5_Failed_Requests (time_of_request, requested_file, error_code)
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
    
    # Extract information from request headers or request data
    country = request.headers.get('X-country')
    client_ip = request.headers.get('X-client-IP')  
    gender = request.headers.get('X-gender') 
    age = request.headers.get('X-age')
    income = request.headers.get('X-income')
    is_banned = country in BANNED_COUNTRIES
    time_of_day = request.headers.get('X-time')  
    requested_file = filename


    # Handle request and send response
    if is_banned:
        logging.error('Banned Country:', 400)
        insert_error_data(time_of_day, requested_file, 400)
        return 'Banned country', 400

    if request.method == 'GET':
        try:
            storage_client = storage.Client()
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
    
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
