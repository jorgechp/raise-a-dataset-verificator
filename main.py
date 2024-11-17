from numbers import Number
from typing import List, Dict, Tuple

import pika
import json
import requests  # Importar requests para hacer peticiones HTTP
from pika.adapters.blocking_connection import BlockingChannel

from faircheck import process_response
from logging import error

VERIFICATION_QUEUE_NAME = 'verificationQueue'
RESPONSE_QUEUE_NAME = 'responseQueue'

INDICATORS_DICT = None

FAIR_ENOUGH_ENDPOINT = 'https://api.fair-enough.semanticscience.org/evaluations'


def execute_test(test_url: str) -> dict:
    """Ejecuta la prueba contra el nuevo endpoint"""
    body = {
        "subject": test_url,
        "collection": "fair-enough-data"
    }
    response = requests.post(FAIR_ENOUGH_ENDPOINT, json=body)
    if response.status_code in [200, 201]:
        return response.json()['contains']
    else:
        error_info = f"Error calling  uri: {test_url} Status code: {response.status_code}"
        error(error_info)
        raise EnvironmentError(error_info)


def callback(channel: BlockingChannel, method, properties, body):
    global INDICATORS_DICT
    request_received = json.loads(body)
    instance_id: str or None = request_received['instanceId'] if 'instanceId' in request_received else None

    test_url: str or None = request_received['uri'] if 'uri' in request_received else None
    if test_url is None:
        error_info = f"Error calling test url: {test_url}. Not exists"
        error(error_info)
        raise EnvironmentError(error_info)

    response_message = {
        'instanceUri': instance_id,
        'result': {}
    }

    response = execute_test(test_url)
    response_processed = process_response(response)

    for response_processed_key, response_processed_value in response_processed.items():
        key = INDICATORS_DICT[response_processed_key]
        if key not in response_message['result']:
            response_message['result'][key] = []

        response_message['result'][key].append({
            'indicator_id': response_processed_key,
            'comment_value': response_processed_value['comment'],
            'is_valid': response_processed_value['result']
        })

    if len(response_message['result']) > 0:
        channel.basic_publish(exchange='',
                              routing_key=RESPONSE_QUEUE_NAME,
                              body=json.dumps(response_message).encode())


def main():
    global INDICATORS_DICT
    with open('tests_relations.json') as f:
        INDICATORS_DICT = json.load(f)

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue=VERIFICATION_QUEUE_NAME, durable=True)
    channel.queue_declare(queue=RESPONSE_QUEUE_NAME, durable=True)

    channel.basic_consume(queue=VERIFICATION_QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    main()
