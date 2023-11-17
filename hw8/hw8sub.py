from google.cloud import pubsub_v1
import time

PROJECT_ID = 'ds561-398719'
SUBSCRIPTION_NAME = 'error-subscription'
TOPIC_NAME = 'error-topic'

def callback(message):
    print(f"Received error message: {message.data.decode('utf-8')}")
    message.ack()

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

print(f"Listening for messages on {subscription_path}...")
streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    streaming_pull_future.result()
except Exception as e:
    print(f"An exception occurred: {e}")
    streaming_pull_future.cancel()
