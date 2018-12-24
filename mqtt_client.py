# coding=utf-8

try:
    import paho.mqtt.client as mqtt
except Exception:
    print('Please install paho-mqtt')
    print('pip install paho-mqtt')
    exit(0)


MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_CLIENT_ID = "python_mqtt_client"
MQTT_USERNAME = "guest"
MQTT_PASSWORD = "guest"
MQTT_KEEPALIVE = 120

MQTT_SUB_TOPIC = "hello"
MQTT_PUB_TOPIC = 'update'
MQTT_SUB_QOS = 1


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

    client.publish(MQTT_PUB_TOPIC, 'hello')
    client.subscribe(MQTT_SUB_TOPIC, MQTT_SUB_QOS)


def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    print('[topic]:' + topic)
    print("[payload]:" + str(payload))


def input_work():
    try:
        while 1:
            data = input()
            if data == 'q':
                exit(0)
            print('input:' + data)

            if data:
                mqttc.publish(MQTT_PUB_TOPIC, data)
                print('publish:' + data)
    except (KeyboardInterrupt, SystemExit):
        exit(0)


if __name__ == "__main__":
    print('To exit input q')
    mqttc = mqtt.Client(MQTT_CLIENT_ID)
    mqttc.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqttc.on_connect = on_connect
    mqttc.on_message = on_message

    mqttc.connect(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
    mqttc.loop_start()

    input_work()
