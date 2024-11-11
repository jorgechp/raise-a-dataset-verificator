import pika
import requests
import json


def verify_indicator(data):
    api_url = "https://api.terceros.com/verify"
    response = requests.post(api_url, json=data)
    return response.json()


def update_database(result):
    # Lógica para actualizar la base de datos con el resultado de la verificación
    pass


def callback(ch, method, properties, body):
    print(f"Received {body}")
    data = json.loads(body)
    verification_result = verify_indicator(data)
    print(f"Verification result: {verification_result}")
    update_database(verification_result)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='verificationQueue')

    channel.basic_consume(queue='verificationQueue', on_message_callback=callback, auto_ack=True)

    print('Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()


if __name__ == '__main__':
    main()
