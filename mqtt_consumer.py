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
# queue_name = 'hello'
binding_keys = sys.argv[1:] or ('#',)


connection = pika.BlockingConnection(pika.ConnectionParameters(host=HOST))

channel = connection.channel()
channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type, durable=True)

# result = channel.queue_declare(exclusive=True)
result = channel.queue_declare()
queue_name = result.method.queue
print(queue_name)

# 绑定topic
if not binding_keys:
    sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=binding_key)


def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body.decode()))


if __name__ == "__main__":
    print(' [*] Waiting for {}. To exit press CTRL+C'.format(exchange_name))
    channel.basic_consume(callback, queue=queue_name, no_ack=True)
    channel.start_consuming()
