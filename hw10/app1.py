from flask import Flask, request, abort
from google.cloud import storage, pubsub_v1
# from google.cloud.sql.connector import Connector  # Commented out SQL related import
# import pymysql  # Commented out SQL related import
# import sqlalchemy  # Commented out SQL related import
# from sqlalchemy import text  # Commented out SQL related import
import os
from dotenv import load_dotenv

app = Flask(__name__)

PROJECT_ID = 'ds561-398719'
TOPIC_NAME = 'hw10-topic'
BANNED_COUNTRIES = ["North Korea", "Iran", "Cuba", "Myanmar", "Iraq", "Libya", "Sudan", "Zimbabwe", "Syria"]

# PubSub and Storage
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)

# Load environment variables from .env file
load_dotenv()

'''

# Retrieve environment variables
try:
    # DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')  # Commented out SQL related environment variables
    # DB_USER = os.getenv('DB_USER')  # Commented out SQL related environment variables
    # DB_PASSWORD = os.getenv('DB_PASSWORD')  # Commented out SQL related environment variables
    # DB_NAME = os.getenv('DB_NAME')  # Commented out SQL related environment variables
except Exception as e:
    pass

'''

# connector = Connector()  # Commented out SQL related code

# function to return the database connection object
def getconn():
    try:
        # conn = connector.connect(
        #     DB_CONNECTION_STRING,
        #     "pymysql",
        #     user=DB_USER,
        #     password=DB_PASSWORD,
        #     db=DB_NAME
        # )
        return None  # Return None since SQL related code is commented out
    except Exception as e:
        pass

# create connection pool with 'creator' argument to our connection object function
try:
    # pool = sqlalchemy.create_engine(
    #     "mysql+pymysql://",
    #     creator=getconn,
    # )
    pass  # Commented out SQL related code
except Exception as e:
    pass

'''
def create_database_and_tables():
    connection = None
    try:
        # with pool.connect() as conn:
        #     conn.execute(text("CREATE DATABASE IF NOT EXISTS hw10db"))  # Commented out SQL related code
        #     conn.execute(text("USE hw10db"))  # Commented out SQL related code

        # Create tables
        # conn.execute(text("""
        #     CREATE TABLE IF NOT EXISTS Countries (
        #         country_id INT AUTO_INCREMENT PRIMARY KEY,
        #         country_name VARCHAR(255) NOT NULL UNIQUE
        #     )
        # """))  # Commented out SQL related code
        # conn.execute(text("""
        #     CREATE TABLE IF NOT EXISTS ClientIPs (
        #         ip_id INT AUTO_INCREMENT PRIMARY KEY,
        #         country_id INT,
        #         client_ip VARCHAR(45) NOT NULL UNIQUE,
        #         FOREIGN KEY (country_id) REFERENCES Countries(country_id)
        #     )
        # """))  # Commented out SQL related code
        # conn.execute(text("""
        #     CREATE TABLE IF NOT EXISTS Requests (
        #         request_id INT AUTO_INCREMENT PRIMARY KEY,
        #         ip_id INT,
        #         gender VARCHAR(10),
        #         age VARCHAR(255),
        #         income VARCHAR(255),
        #         is_banned BOOLEAN,
        #         time_of_day TIME,
        #         requested_file VARCHAR(255),
        #         FOREIGN KEY (ip_id) REFERENCES ClientIPs(ip_id)
        #     )
        # """))  # Commented out SQL related code
        # conn.execute(text("""
        #     CREATE TABLE IF NOT EXISTS Failed_Requests (
        #         error_id INT AUTO_INCREMENT PRIMARY KEY,
        #         time_of_request TIMESTAMP NOT NULL,
        #         requested_file VARCHAR(255),
        #         error_code INT NOT NULL
        #     )
        # """))  # Commented out SQL related code
        # conn.commit()  # Commented out SQL related code
    except Exception as e:
        pass

# create_database_and_tables()  # Commented out SQL related code
'''

'''
# Function to insert request data into the database
def insert_request_data(country, client_ip, gender, age, income, is_banned, time_of_day, requested_file):
    # with pool.connect() as db_conn:  # Commented out SQL related code
    try:
        # Fetch or create country_id for given country
        # country_id = db_conn.execute(
        #     sqlalchemy.text("SELECT country_id FROM Countries WHERE country_name = :country"),
        #     {"country": country}
        # ).scalar()  # Commented out SQL related code
        country_id = None  # Set to None since SQL related code is commented out
        if not country_id:
            # db_conn.execute(
            #     sqlalchemy.text("INSERT INTO Countries (country_name) VALUES (:country)"),
            #     {"country": country}
            # )  # Commented out SQL related code
            pass  # Commented out SQL related code
            # country_id = db_conn.execute(
            #     sqlalchemy.text("SELECT LAST_INSERT_ID()")
            # ).scalar()  # Commented out SQL related code

        # Check if client_ip and country_id pairing exists or insert
        # ip_id = db_conn.execute(
        #     sqlalchemy.text("SELECT ip_id FROM ClientIPs WHERE client_ip = :client_ip"),
        #     {"client_ip": client_ip}
        # ).scalar()  # Commented out SQL related code
        ip_id = None  # Set to None since SQL related code is commented out
        if not ip_id:
            # db_conn.execute(
            #     sqlalchemy.text("INSERT INTO ClientIPs (client_ip, country_id) VALUES (:client_ip, :country_id)"),
            #     {"client_ip": client_ip, "country_id": country_id}
            # )  # Commented out SQL related code
            pass  # Commented out SQL related code
            # ip_id = db_conn.execute(
            #     sqlalchemy.text("SELECT LAST_INSERT_ID()")
            # ).scalar()  # Commented out SQL related code

        # Insert into Requests
        # db_conn.execute(
        #     sqlalchemy.text(
        #         "INSERT INTO Requests (ip_id, gender, age, income, is_banned, time_of_day, requested_file) "
        #         "VALUES (:ip_id, :gender, :age, :income, :is_banned, :time_of_day, :requested_file)"
        #     ),
        #     {
        #         "ip_id": ip_id,
        #         "gender": gender,
        #         "age": age,
        #         "income": income,
        #         "is_banned": is_banned,
        #         "time_of_day": time_of_day,
        #         "requested_file": requested_file
        #     }
        # )  # Commented out SQL related code

        # Commit transactions
        # db_conn.commit()  # Commented out SQL related code
    except Exception as e:
        pass
'''

'''
# Function to insert error data into the database
def insert_error_data(time_of_request, requested_file, error_code):
    # with pool.connect() as db_conn:  # Commented out SQL related code
    try:
        # db_conn.execute(
        #     sqlalchemy.text(
        #         "INSERT INTO Failed_Requests (time_of_request, requested_file, error_code) "
        #         "VALUES (:time_of_request, :requested_file, :error_code)"
        #     ),
        #     {"time_of_request": time_of_request, "requested_file": requested_file, "error_code": error_code}
        # )  # Commented out SQL related code

        # Commit transactions
        # db_conn.commit()  # Commented out SQL related code
    except Exception as e:
        pass
'''

@app.route('/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE'])
@app.route('/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH', 'HEAD', 'CONNECT', 'OPTIONS', 'TRACE'])
def file_server(path):
    if request.method == 'GET':
        try:
            filename = path.lstrip('/')
            filename = filename.replace('hw10-bucket/', '')

            country = request.headers.get('X-country')
            is_banned = country in BANNED_COUNTRIES

            client_ip = request.headers.get('X-client-IP')
            gender = request.headers.get('X-gender')
            age = request.headers.get('X-age')
            income = request.headers.get('X-income')
            time_of_day = request.headers.get('X-time')
            requested_file = filename

            if is_banned:
                error_message = f"Access attempt from banned country: {country}"
                publish_error(error_message)
                # insert_error_data(time_of_day, requested_file, 400)  # Commented out SQL related code
                return 'Banned country', 400

            # removed storage client initialization from here
            bucket = storage_client.bucket('hw10-bucket')
            blob = bucket.blob(filename)
            if not blob.exists():
                # insert_error_data(time_of_day, requested_file, 404)  # Commented out SQL related code
                return 'File not found', 404

            file_content = blob.download_as_text()
            # Save request data to database
            # insert_request_data(country, client_ip, gender, age, income, is_banned, time_of_day, requested_file)  # Commented out SQL related code
            return file_content, 200

        except Exception as e:
            # insert_error_data(time_of_day, requested_file, 500)  # Commented out SQL related code
            return 'Internal Server Error', 500
    else:
        # insert_error_data(time_of_day, requested_file, 501)  # Commented out SQL related code
        return 'Not Implemented', 501

def publish_error(error_message):
    data = error_message.encode("utf-8")
    try:
        publisher.publish(topic_path, data)
    except Exception as e:
        pass

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
