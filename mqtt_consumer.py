# coding=utf-8

import sys
import datetime as dt
import pika

# 是否日志MongoDB入库
MONGDB_ENABLE = False

if MONGDB_ENABLE:
    from pymongo import MongoClient
    client = MongoClient('localhost', 27017)

    db = client['mqtt-log']
    mqtt_log = db['mqtt-log']

INFLUXDB_ENABLE = True
if INFLUXDB_ENABLE:
    from influxdb import InfluxDBClient
    client = InfluxDBClient('localhost', 8086, database='mqtt-log')
    # client.create_database('mqtt-log')

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
    topic = method.routing_key.replace('.', '/')
    if body:
        payload = body.decode()
    else:
        payload = ''
    # print(" [x] %r:%r" % (topic, payload))

    if MONGDB_ENABLE:
        json_body = {
            'topic': topic,
            'payload': payload,
            'create_datetime': dt.datetime.now()
        }
        mqtt_log.insert_one(json_body)

    if INFLUXDB_ENABLE:
        json_body = [{
            'measurement': 'mqtt-log',
            # time字段，主索引，数据库会自动生成
            # 'time': dt.datetime.utcnow(),

            # tags：有索引的字段
            "tags": {
                "topic": topic,
            },

            # fileds： 没有索引的字段
            "fields": {
                'payload': payload,
            }
        }]
        client.write_points(json_body)

        # 查询过去3s的数据
        # result = client.query("""SELECT * FROM "mqtt-log" WHERE time > now() - 3s AND "topic"='{topic}';""".format(topic=topic))
        # print("Result: {0}".format(result))


if __name__ == "__main__":
    print('queue_name:' + queue_name)
    print(' [*] Waiting for {}. To exit press CTRL+C'.format(exchange_name))
    channel.basic_consume(callback, queue=queue_name, no_ack=True)
    channel.start_consuming()
