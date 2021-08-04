import asyncio
import signal
import time

import uvloop

from configparser import ConfigParser
from gmqtt import Client as MQTTClient

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

MQTT_STOP = asyncio.Event()
worker_index = 0
worker_index_map = {}
worker_index_map_show = False


def on_connect(client, flags, rc, properties):
    client.subscribe(f'debug/balancer/+', qos=0)


def on_message(client, topic, payload, qos, properties):
    global worker_index_map_show

    index = get_worker_index(topic)

    if int(time.time()) % 5 == 0:
        if not worker_index_map_show:
            print(worker_index_map)
            worker_index_map_show = True
    else:
        worker_index_map_show = False

    client.publish(f'debug/worker/{index}', payload, qos)


def ask_exit(*args):
    MQTT_STOP.set()


def get_worker_index(topic: str) -> int:
    global worker_index, worker_index_map
    worker_index = 1 + hash(topic) % 5

    if not worker_index_map.get(worker_index):
        worker_index_map[worker_index] = 0
    worker_index_map[worker_index] += 1

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
