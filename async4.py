#! /usr/bin/env python3

"""
class that talks to itself
"""

import time
import asyncio
import random

import patch


class Crazy:
    def __init__(self, loop):
        self.loop = loop
        self.queue = asyncio.Queue(loop=loop)
        self.count = 0

    def run(self):
        listen_task = asyncio.create_task(self.listen())
        tasks = [
            self.monitor(),
            self.talk('yo'),
            self.talk('hey'),

        ]
        x, y = self.loop.run_until_complete(
            asyncio.wait(tasks, timeout=20)
        )

        listen_task.cancel()
        self.loop.run_until_complete(listen_task)

        print("queue:", self.queue.qsize(), 'done:', len(x), 'pending:', len(y))

    async def post(self, msg):
        self.count += 1
        await self.queue.put((self.count, time.time(), msg))

    async def monitor(self):
        while True:
            await asyncio.sleep(1)

            print('count:', self.count, 'queue:', self.queue.qsize())
            if self.count > 19:
                break

    async def listen(self):
        try:
            while True:
                c, t, msg = await self.queue.get()

                print(f'{(time.time() - t) * 1000:.3f} ms {msg}')

                self.queue.task_done()
        except asyncio.CancelledError:
            print('listen cancelled')

    async def talk(self, msg):
        for count in range(10):
            delay = random.uniform(0, 2)
            await asyncio.sleep(delay)

            await self.post(f'{msg} {count}')

        print(msg, 'talk done')


def main():
    try:
        loop = asyncio.get_event_loop()
    except:
        loop = asyncio.new_event_loop()

    c = Crazy(loop)
    c.run()


if __name__ == '__main__':
    patch.patch()
    main()
