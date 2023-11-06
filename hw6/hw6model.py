import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import accuracy_score, classification_report
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
    
    conn = connector.connect(
        DB_CONNECTION_STRING,
        "pymysql",
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME
    )
    return conn


pool = sqlalchemy.create_engine(
    "mysql+pymysql://",
    creator=getconn,
)

def train_country_classifier(db_interface):
    with db_interface.pool.connect() as conn:
        # SQL query to fetch client IP and corresponding country
        fetch_query = """
        SELECT ip_data.client_ip, country_data.country_name
        FROM ClientIPs ip_data
        INNER JOIN Countries country_data ON ip_data.country_id = country_data.country_id
        """
        client_country_data = pd.read_sql(fetch_query, conn)
        
        # Processing IP address to binary format
        client_country_data['ip_as_binary'] = client_country_data['client_ip'].apply(
            lambda ip: int(''.join([f'{int(byte):08b}' for byte in ip.split('.')]), 2)
        )
        
        # Features and Labels for the classifier
        features = client_country_data[['ip_as_binary']]
        labels = client_country_data['country_name']
        
        # Data partitioning
        features_training, features_testing, labels_training, labels_testing = train_test_split(
            features, labels, test_size=0.2, random_state=42
        )
        
        # Classifier setup and training
        ip_classifier = RandomForestClassifier()
        ip_classifier.fit(features_training, labels_training)
        
        # Evaluation of the classifier
        predictions = ip_classifier.predict(features_testing)
        classifier_accuracy = accuracy_score(labels_testing, predictions)
        print(f'Accuracy of IP to Country Classifier: {classifier_accuracy * 100:.2f}%')
        
        return ip_classifier


def train_income_classifier(db_interface):
    with db_interface.pool.connect() as conn:
        # SQL query to fetch fields for predicting income
        query = """
        SELECT req.gender, req.age, req.ip_id, req.is_banned, req.requested_file, req.income
        FROM Requests req
        """
        data = pd.read_sql(query, conn)

        # Convert categorical data to numeric codes and one-hot encode
        label_encoder = LabelEncoder()
        data['income'] = label_encoder.fit_transform(data['income'])
        data = pd.get_dummies(data, columns=['gender', 'age', 'is_banned', 'requested_file', 'ip_id'])

        # Prepare the feature matrix X and target vector y
        X = data.drop('income', axis=1)
        y = data['income']

        # Split the dataset
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
        
        # Train the model
        mlp = MLPClassifier(hidden_layer_sizes=(50,), activation='relu', solver='adam', alpha=0.0001, learning_rate='adaptive', max_iter=100)
        mlp.fit(X_train, y_train)
        
        # Make predictions
        predictions = mlp.predict(X_test)
        accuracy = accuracy_score(y_test, predictions)
        print(f'Accuracy of Income Classifier: {accuracy * 100:.2f}%')
        
        # Return the model and accuracy for further use
        return mlp, accuracy

        
if __name__ == "__main__":
    db_interface = type('DBInterface', (object,), {'pool': pool})
    country_classifier_model = train_country_classifier(db_interface)
    income_classifier, income_label_encoder = train_income_classifier(db_interface)