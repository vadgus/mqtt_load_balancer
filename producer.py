from time import time
from random import randint
from configparser import ConfigParser
from asyncio import get_event_loop, ensure_future

from gmqtt import Client


async def main(config: ConfigParser):
    client = Client('producer')
    client.set_auth_credentials(config.get('FLESPI', 'TOKEN'), None)
    await client.connect(config.get('FLESPI', 'HOST'))

    c = 0
    t = time()
    messages_per_sec = int(config.get('PRODUCER', 'MESSAGES_PER_SEC')) or 1

    while True:
        if c >= messages_per_sec:
            if time() - t >= 1:
                t = time()
                c = 0
            continue

        item_id = randint(1, 2000000)
        client.publish(f'debug/balancer/{item_id}', str(time()))
        c += 1


if __name__ == '__main__':
    config = ConfigParser()
    config.read('.env')

    loop = get_event_loop()
    future = ensure_future(main(config))
    loop.run_until_complete(future)
