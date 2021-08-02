import asyncio
import sys
import signal
import uvloop

from configparser import ConfigParser
from gmqtt import Client as MQTTClient

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

MQTT_STOP = asyncio.Event()
worker_index = None


def on_connect(client, flags, rc, properties):
    client.subscribe(f'debug/worker/{worker_index}', qos=0)


def on_message(client, topic, payload, qos, properties):
    print(worker_index, topic, str(payload))


def ask_exit(*args):
    MQTT_STOP.set()


async def main(config: ConfigParser):
    global worker_index

    args = sys.argv[1:]
    for arg in args:
        if arg.startswith('--index='):
            worker_index = arg[8:]
    if not worker_index:
        ask_exit()
        raise Exception("process argument '--index=' invalid")

    client = MQTTClient(f'client_id_{worker_index}')
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
