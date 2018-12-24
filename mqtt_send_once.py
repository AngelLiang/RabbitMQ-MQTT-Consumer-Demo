# coding=utf-8

import pika

HOST = '127.0.0.1'
exchange_name = 'amq.topic'
queue_name = 'hello'
body = 'Hello World!'


connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))
channel = connection.channel()


# channel.queue_declare(queue=queue_name)

channel.basic_publish(exchange=exchange_name, routing_key=queue_name, body=body)
print(" [x] Sent 'Hello World!'")
connection.close()
