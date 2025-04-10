import os
import time
from dotenv import load_dotenv
import json
from pika import BlockingConnection, ConnectionParameters, PlainCredentials, BasicProperties, exceptions

from news.schema.message_item import MessageItem

load_dotenv()

CONFIG = {
    'username': os.getenv('RABBITMQ_USERNAME'),
    'password': os.getenv('RABBITMQ_PASSWORD'),
    'host': os.getenv('RABBITMQ_HOST'),
    'port': int(os.getenv('RABBITMQ_PORT')),
    'queue_name': os.getenv('STORE_QUEUE'),
    'exchange_name': os.getenv('STORE_EXCHANGE'),
    'routing_key': os.getenv('STORE_KEY'),
}

CONTENT_TYPE = 'application/json'
MAX_RETRIES = 3

def get_connection_params():
    credentials = PlainCredentials(username=CONFIG['username'], password=CONFIG['password'])
    return ConnectionParameters(host=CONFIG['host'],
                                credentials=credentials,
                                heartbeat=600,
                                blocked_connection_timeout=300)


def send_message(message: MessageItem, retry_count=0):
    try:
        connection = BlockingConnection(get_connection_params())
        channel = connection.channel()
        channel.exchange_declare(
            exchange=CONFIG['exchange_name'],
            exchange_type='direct',
            durable=True
        )
        channel.confirm_delivery()
        channel.add_on_return_callback(lambda r: print(f"Message returned! Reply Code: {r.reply_code}"))
        channel.queue_declare(queue=CONFIG['queue_name'], durable=True)

        props = BasicProperties(
            content_type=CONTENT_TYPE,
            delivery_mode=2
        )
        serialized_message = json.dumps(message.__dict__)

        channel.basic_publish(exchange=CONFIG['exchange_name'],
                              routing_key=CONFIG['routing_key'],
                              body=serialized_message,
                              properties=props,
                              mandatory=True
        )
        connection.close()
    except (exceptions.AMQPConnectionError, exceptions.StreamLostError) as e:
        if retry_count < MAX_RETRIES:
            print(f"Retry count: {retry_count + 1}/{MAX_RETRIES}")
            time.sleep(5)
            send_message(message, retry_count + 1)
        else:
            print("Max Retries reached.")