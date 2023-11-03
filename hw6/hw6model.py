import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from google.cloud.sql.connector import Connector
import pymysql
import sqlalchemy
import logging
import os
from dotenv import load_dotenv


load_dotenv()

# Retrieve environment variables
try:
    DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_NAME = os.getenv('DB_NAME')
      

except Exception as e:
    logging.error("Error getting data from env: %s", str(e))
    #print('connector error')
    
connector = Connector()

# function to return the database connection object
def getconn():
    try:
        conn = connector.connect(
            DB_CONNECTION_STRING,
            "pymysql",
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME
        )
        return conn
    except Exception as e:
            logging.error("Error from connector: %s", str(e))

# create connection pool with 'creator' argument to our connection object function

    pool = sqlalchemy.create_engine(
        "mysql+pymysql://",
        creator=getconn,
    )

def train_model_ip_to_country(db_connector):
    with db_connector.pool.connect() as connection:
        # Join ClientIPs and Countries tables to get the IP and corresponding country name
        query = """
        SELECT cip.client_ip, cnt.country_name
        FROM ClientIPs cip
        JOIN Countries cnt ON cip.country_id = cnt.country_id
        """
        data = pd.read_sql(query, connection)
        # Convert IP address to a binary representation
        data['client_ip_binary'] = data['client_ip'].apply(
            lambda x: int(''.join([f'{int(octet):08b}' for octet in x.split('.')]), 2)
        )
        X = data[['client_ip_binary']]
        y = data['country_name']
        # Split data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Initialize and train the RandomForestClassifier
        model = RandomForestClassifier()
        model.fit(X_train, y_train)
        
        # Make predictions and calculate accuracy
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        print(f'Model accuracy (client IP to country): {accuracy * 100:.2f}%')
        return model
