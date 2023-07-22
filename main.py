from typing import Callable

import pika
from decouple import config
import atexit


class DeliveryService:
    channel = None
    connection = None

    def __init__(self):
        self._connect()

    def _connect(self):
        # Connection parameters
        host = config('RABBITMQ_HOST', default=False, cast=str)
        connection_params = pika.ConnectionParameters(host=host)
        self.connection = pika.BlockingConnection(connection_params)
        self.channel = self.connection.channel()

    def _listen_queue(self, queue_name: str, callback: Callable):
        # Declare a queue named 'checkout_queue'
        self.channel.queue_declare(queue=queue_name)

        # Specify the callback function to be called when a message is received
        self.channel.basic_consume(queue=queue_name,
                                   on_message_callback=callback,
                                   auto_ack=True)
        print(' [*] Waiting for messages. To exit, press CTRL+C')
        self.channel.start_consuming()

    def _calculate_delivery(self, ch, method, properties, body):
        print('calculated')

    def exit_handler(self):
        self.connection.close()

    def start(self):
        self._connect()
        self._listen_queue("delivery_queue",
                           callback=self._calculate_delivery)


if __name__ == '__main__':
    checkout_service = DeliveryService()
    atexit.register(checkout_service.exit_handler)
    checkout_service.start()
