import asyncio
import sys
import signal
import time
import uvloop

from configparser import ConfigParser

from gmqtt import Client as MQTTClient

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

STOP = asyncio.Event()


def on_connect(client, flags, rc, properties):
    print('Connected')
    client.subscribe('debug/#', qos=0)


def on_message(client, topic, payload, qos, properties):
    print('RECV MSG:', payload)


def on_disconnect(client, packet, exc=None):
    print('Disconnected')


def on_subscribe(client, mid, qos, properties):
    print('SUBSCRIBED')


def ask_exit(*args):
    STOP.set()


async def main():
    config = ConfigParser()
    config.read('.env')

    worker_index = ''
    args = sys.argv[1:]
    for arg in args:
        if arg.startswith('--index='):
            worker_index = arg[8:]
    if not worker_index:
        ask_exit()
        raise Exception("process argument '--index=' invalid")

    client_id = f'client_id_{worker_index}'
    client = MQTTClient(client_id)

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    client.on_subscribe = on_subscribe

    client.set_auth_credentials(config.get('FLESPI', 'TOKEN'), None)
    await client.connect(config.get('FLESPI', 'HOST'))

    client.publish(f'debug/{worker_index}', str(time.time()), qos=1)

    await STOP.wait()
    await client.disconnect()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()

    loop.add_signal_handler(signal.SIGINT, ask_exit)
    loop.add_signal_handler(signal.SIGTERM, ask_exit)

    loop.run_until_complete(main())
