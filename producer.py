import random
import time
import asyncio

from gmqtt import Client as MQTTClient
from configparser import ConfigParser


def pub(client, item_id):
    client.publish(f'debug/balancer/{item_id}', str(time.time()))


async def run(config):
    client_id = f'client_id_producer'
    client = MQTTClient(client_id)
    client.set_auth_credentials(config.get('FLESPI', 'TOKEN'), None)
    await client.connect(config.get('FLESPI', 'HOST'))

    messages_per_sec = int(config.get('PRODUCER', 'MESSAGES_PER_SEC') or 1)

    t = time.time()
    c = 0
    while True:
        if c >= messages_per_sec:
            if time.time() - t >= 1:
                t = time.time()
                c = 0
            continue

        item_id = random.randint(1, 5)
        pub(client, item_id)
        c += 1


config = ConfigParser()
config.read('.env')
loop = asyncio.get_event_loop()
future = asyncio.ensure_future(run(config))
loop.run_until_complete(future)
