#APP 2

from google.cloud import pubsub_v1
import time

PROJECT_ID = 'ds561-398719'
SUBSCRIPTION_NAME = 'error-subscription'

def callback(message):
    print(f"Received error message: {message.data.decode('utf-8')}")
    message.ack()

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
print(f"Listening for messages on {subscription_path}...")

with subscriber:
    try:
        # Keep the main thread from exiting, so it continues to process messages indefinitely.
        streaming_pull_future.result()
    except Exception as e:
        streaming_pull_future.cancel()
        print(f"Subscription terminated due to {e}")

