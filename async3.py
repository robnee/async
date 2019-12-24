#! /usr/bin/env python3

import time
import asyncio

# Borrowed from http://curio.readthedocs.org/en/latest/tutorial.html.


async def countdown(number, n):
    start = time.time()
    while n > 0:
        print(f'{start:.4f} {time.time():.4f} {time.time() - start:.4f} {number}  T-minus {n}')
        await asyncio.sleep(1)
        n -= 1


def run(futures, timeout=None):
    loop = asyncio.get_event_loop()

    done, pending = loop.run_until_complete(asyncio.wait(list(futures), timeout=timeout))

    for task in pending:
        task.cancel()


run([
    countdown("A", 3),
    countdown("B", 5),
], timeout=None)

print("done")
