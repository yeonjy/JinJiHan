import os
import time
from dotenv import load_dotenv
import json
import logging
from pika import BlockingConnection, ConnectionParameters, PlainCredentials, exceptions, BasicProperties

from news.crud.news_summarizer import summarize_news

load_dotenv()
logger = logging.getLogger(__name__)

credentials = PlainCredentials(username=os.getenv('RABBITMQ_USERNAME'), password=os.getenv('RABBITMQ_PASSWORD'))
connection = BlockingConnection(ConnectionParameters(host=os.getenv('RABBITMQ_HOST'),
                                                     port=int(os.getenv('RABBITMQ_PORT')),
                                                     credentials=credentials))
channel = connection.channel()
channel.exchange_declare(exchange=os.getenv('SUMMARY_EXCHANGE'),
                         exchange_type='direct',
                         durable=True)
channel.queue_declare(queue=os.getenv('SUMMARY_QUEUE'), durable=True)
channel.queue_bind(exchange=os.getenv('SUMMARY_EXCHANGE'),
                   queue=os.getenv('SUMMARY_QUEUE'), routing_key=os.getenv('SUMMARY_KEY'))
MAX_RETRIES = 3


def callback(ch, method, properties, body):
    logger.info(" [x] Queue Received ")
    received_data = body.decode()

    try:
        received_data_json = json.loads(received_data)
        if 'content' in received_data_json:
            summarize_news(news_id=received_data_json['id'], content=received_data_json['content'])
        else:
            logger.info("Error: 'content' key is missing in the received data.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
    except json.JSONDecodeError:
        logger.info("Error decoding JSON from the received data.")
        handle_retry(ch, method, body)


def handle_retry(ch, method, body):
    retry_count = method.headers.get('x-retry', 0)
    if retry_count < MAX_RETRIES:
        ch.basic_publish(
            exchange='',
            routing_key=method.routing_key,
            body=body,
            properties=BasicProperties(
                headers={'x-retry': retry_count + 1}
            )
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)
    else:
        ch.basic_publish(
            exchange=os.getenv('STORE_EXCHANGE'),
            routing_key=os.getenv('STORE_DLQ_KEY'),
            body=body
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=os.getenv('SUMMARY_QUEUE'), on_message_callback=callback, auto_ack=False)
try:
    channel.start_consuming()
except (exceptions.AMQPConnectionError, exceptions.StreamLostError) as e:
    logger.error("Connection lost or failed. Error: {}".format(e))
    time.sleep(10)