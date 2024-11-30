from numbers import Number
from typing import List, Dict, Tuple

import pika
import json
import requests  # Import requests to make HTTP requests
from pika.adapters.blocking_connection import BlockingChannel

from faircheck import process_response
from logging import error

VERIFICATION_QUEUE_NAME = 'RaiseDatasetVerificationQueue'
RESPONSE_QUEUE_NAME = 'RaiseDatasetResponseQueue'

INDICATORS_DICT = None

FAIR_ENOUGH_ENDPOINT = 'https://api.fair-enough.semanticscience.org/evaluations'


def execute_test(test_url: str) -> dict:
    """
    Executes a test by sending a POST request to the FAIR Enough API.

    Parameters:
    test_url (str): The URL of the dataset to be tested.

    Returns:
    dict: The response from the API containing the test results.

    Raises:
    EnvironmentError: If the API call fails.
    """
    body = {
        "subject": "https://doi.org/" + test_url,
        "collection": "fair-enough-data"
    }
    response = requests.post(FAIR_ENOUGH_ENDPOINT, json=body)
    if response.status_code in [200, 201]:
        return response.json()['contains']
    else:
        error_info = f"Error calling uri: {test_url}. Status code: {response.status_code}"
        error(error_info)
        raise EnvironmentError(error_info)


def callback(channel: BlockingChannel, method, properties, body):
    """
    Callback function to process messages from the verification queue.

    Parameters:
    channel (BlockingChannel): The channel object.
    method: Method frame.
    properties: Properties of the message.
    body: The message body containing the test request.
    """
    global INDICATORS_DICT
    request_received = json.loads(body)
    instance_id: str or None = request_received['instanceId'] if 'instanceId' in request_received else None

    test_url: str or None = request_received['uniqueIdentifier'] if 'uniqueIdentifier' in request_received else None
    if test_url is None:
        error_info = f"Error calling test url: {test_url}. Not exists"
        error(error_info)
        return

    response_message = {
        'instanceId': instance_id,
        'result': {}
    }

    response = execute_test(test_url)
    response_processed = process_response(response)

    for response_processed_key, response_processed_value in response_processed.items():
        key = INDICATORS_DICT[response_processed_key]
        if key not in response_message['result']:
            response_message['result'][key] = []

        response_message['result'][key].append({
            'indicatorId': response_processed_key,
            'commentValue': response_processed_value['comment'],
            'isValid': response_processed_value['result']
        })

    if len(response_message['result']) > 0:
        channel.basic_publish(exchange='',
                              routing_key=RESPONSE_QUEUE_NAME,
                              body=json.dumps(response_message).encode())


def main():
    """
    Main function to set up the RabbitMQ connection and start consuming messages.
    """
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
