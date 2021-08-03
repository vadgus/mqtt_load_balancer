from time import time
from random import randint
from configparser import ConfigParser
from asyncio import get_event_loop, ensure_future

from gmqtt import Client


async def main(config: ConfigParser):
    client = Client('producer')
    client.set_auth_credentials(config.get('FLESPI', 'TOKEN'), None)
    await client.connect(config.get('FLESPI', 'HOST'))

    message_count = 0
    start_time = time()
    messages_per_sec = int(config.get('PRODUCER', 'MESSAGES_PER_SEC')) or 1

    while True:
        if message_count >= messages_per_sec:
            if time() - start_time >= 1:
                start_time = time()
                message_count = 0
            continue

        item_id = randint(1, 2000000)
        client.publish(f'debug/balancer/{item_id}', str(time()))
        message_count += 1


if __name__ == '__main__':
    config = ConfigParser()
    config.read('.env')

    loop = get_event_loop()
    future = ensure_future(main(config))
    loop.run_until_complete(future)
