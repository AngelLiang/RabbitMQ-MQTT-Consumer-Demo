# coding=utf-8

import sys
try:
    import pika
except Exception:
    print('Please install pika')
    print('pip install pika')
    exit(0)

HOST = '127.0.0.1'
exchange_name = 'amq.topic'
exchange_type = 'topic'
# 如果启动多个实例，则同时绑定这个队列，表示轮询给这些实例处理
queue_name = 'mqtt-comsumer'

binding_keys = sys.argv[1:]
if not binding_keys:
    print('Usage: %s [binding_key]...' % sys.argv[0])
    print("default binding_key='#'")
    binding_keys = ('#',)

connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))

channel = connection.channel()
channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)
channel.queue_declare(queue=queue_name)


# binding topic
for binding_key in binding_keys:
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)


def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body.decode()))


if __name__ == "__main__":
    print('queue_name:' + queue_name)
    print(' [*] Waiting for {}. To exit press CTRL+C'.format(exchange_name))
    channel.basic_consume(callback, queue=queue_name, no_ack=True)
    channel.start_consuming()
