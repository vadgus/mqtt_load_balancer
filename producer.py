import random
import time
import asyncio

from gmqtt import Client as MQTTClient
from configparser import ConfigParser


async def main():
    config = ConfigParser()
    config.read('.env')

    client = MQTTClient('producer')
    client.set_auth_credentials(config.get('FLESPI', 'TOKEN'), None)
    await client.connect(config.get('FLESPI', 'HOST'))

    c = 0
    t = time.time()
    messages_per_sec = int(config.get('PRODUCER', 'MESSAGES_PER_SEC') or 1)

    while True:
        if c >= messages_per_sec:
            if time.time() - t >= 1:
                t = time.time()
                c = 0
            continue

        item_id = random.randint(1, 2000000)
        client.publish(f'debug/balancer/{item_id}', str(time.time()))
        c += 1


loop = asyncio.get_event_loop()
future = asyncio.ensure_future(main())
loop.run_until_complete(future)
