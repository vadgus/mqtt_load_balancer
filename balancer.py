import asyncio
import signal
import uvloop

from configparser import ConfigParser
from gmqtt import Client as MQTTClient

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

MQTT_STOP = asyncio.Event()
worker_index = 0


def on_connect(client, flags, rc, properties):
    client.subscribe(f'debug/balancer/+', qos=0)


def on_message(client, topic, payload, qos, properties):
    index = get_worker_index(topic)
    client.publish(f'debug/worker/{index}', payload)


def ask_exit(*args):
    MQTT_STOP.set()


def hashval(str, size):
    value = 0
    for x in str:
        value += (ord(x))
    return value % size


def get_worker_index(topic) -> int:
    global worker_index
    worker_index = hashval(topic, 5) + 1
    print(topic, worker_index)
    return worker_index


async def main(config: ConfigParser):
    client = MQTTClient('balancer')
    client.on_connect = on_connect
    client.on_message = on_message
    client.set_auth_credentials(config.get('FLESPI', 'TOKEN'), None)

    await client.connect(config.get('FLESPI', 'HOST'))

    await MQTT_STOP.wait()
    await client.disconnect()


if __name__ == '__main__':
    config = ConfigParser()
    config.read('.env')

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)
    loop.run_until_complete(main(config))
