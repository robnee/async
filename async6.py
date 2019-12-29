#! /usr/bin/env python3

"""
simple pubsub system
"""

import os
import re
import time
import logging
import asyncio
import aioredis
from collections import namedtuple
from sshtunnel import SSHTunnelForwarder

import patch


Message = namedtuple("Message", "source, key, value")

logger = logging.getLogger()


def ts():
    return time.time()


class RegexPattern:
    """ regex patterns """

    def __init__(self, pattern):
        self.pattern = pattern
        self.regex = re.compile(pattern)

    def match(self, subject):
        return self.regex.fullmatch(subject)

    def __str__(self):
        return self.pattern


class DotPattern:
    """ dot-separated path-style patterns """

    def __init__(self, pattern):
        self.pattern = pattern

    def match(self, subject):
        if self.pattern.upper() == subject.upper() or self.pattern in ("", "."):
            return True

        prefix = self.pattern + "" if self.pattern[-1] == "." else "."
        return subject.upper().startswith(prefix.upper())

    def __str__(self):
        return self.pattern


class MessageBus:
    """ MessageBus interface """

    def __init__(self):
        self._channels = {}

    def set_channel(self, key, value):
        self._channels[key] = value

    def get_channels(self):
        return self._channels.items()


class BasicMessageBus(MessageBus):
    """ Basic MessageBus implementation """

    def __init__(self):
        super().__init__()
        self.conn = None
        self.listeners = set()

    async def connect(self, address=None):
        self.conn = self
        logger.info(f"bus connected ({address})")

    async def send(self, message):
        if not self.conn:
            raise RuntimeError("bus not connected")

        if message.key.endswith("."):
            raise ValueError("trailing '.' in key")
            
        self.set_channel(message.key, message.value)
        for pattern, q in self.listeners:
            if pattern.match(message.key):
                await q.put(message)

    async def listen(self, pattern):
        if not self.conn:
            raise RuntimeError("bus not connected")

        try:
            p = DotPattern(pattern)
            q = asyncio.Queue()

            self.listeners.add((p, q))

            # yield current values
            for k, v in self.get_channels():
                if p.match(k):
                    yield Message("local", k, v)

            # yield the messages as they come through
            while True:
                msg = await q.get()
                if not msg:
                    break
                yield msg
        except:
            raise
        finally:
            self.listeners.remove((p, q))

    async def status(self):
        if self.conn:
            return {
                "status": "connected",
                "listeners": [str(p) for p, _ in self.listeners],
                "channels": list(self.get_channels()),
                "timestamp": ts(),
            }
        else:
            return {"status": "disconnected"}

    async def close(self):
        self.conn = None
        for p, q in self.listeners:
            await q.put(None)
        logger.info(f"connection closed")


class MessageBridge:
    """ bridge the local bus with an external bus such as redis """

    def __init__(self, pattern, bus):
        self.pattern = pattern
        self.bus = bus

    async def inbound(self, aredis):
        redis_pattern = self.pattern
        if redis_pattern.endswith('.'):
            redis_pattern += '*'

        try:
            chan, = await aredis.psubscribe(redis_pattern)
            while await chan.wait_message():
                k, v = await chan.get(encoding="utf-8")
                logger.info(f"bridge {self.pattern}: incoming message {k}: {v}")
                await self.bus.send(Message("redis", k.decode(), v))
        except asyncio.CancelledError:
            logger.info(f"bridge in {self.pattern}: cancelled")
        finally:
            await aredis.punsubscribe(redis_pattern)

    async def outbound(self, aredis):
        """ route local messages to redis """

        try:
            await aredis.publish('cat.log', f'outbound {self.pattern}')
            async for message in self.bus.listen(self.pattern):
                if message.source != "redis":
                    logger.info(f"bridge out {self.pattern}: {message}")
                    await aredis.publish(message.key, message.value)
        except asyncio.CancelledError:
            logger.info(f"bridge out{self.pattern}): cancelled")

    async def start(self):
        tunnel_config = {
            "ssh_address_or_host": ("robnee.com", 22),
            "remote_bind_address": ("127.0.0.1", 6379),
            "local_bind_address": ("127.0.0.1",),
            "ssh_username": "rnee",
            "ssh_pkey": os.path.expanduser(r"~/.ssh/id_rsa"),
        }

        with SSHTunnelForwarder(**tunnel_config) as tunnel:
            address = tunnel.local_bind_address
            aredis = await aioredis.create_redis_pool(address, encoding="utf-8")
            logger.info(f"redis connected: {aredis.address}")

            try:
                x = await asyncio.gather(
                    self.inbound(aredis),
                    self.outbound(aredis),
                )
            except asyncio.CancelledError:
                logger.info(f"bridge start {self.pattern}: cancelled")
            except Exception as e:
                logger.info(f'bridge start {self.pattern}: exception {e} {type(e)}')

            aredis.close()
            await aredis.wait_closed()


async def wait_graceafully(aws, timeout=None):
    """
    wait for tasks to complete issuing cancels to any still pending until done
    to ensure exceptions and results are always consumed
    """

    while True:
        done, pending = await asyncio.wait(aws, timeout=15)

        for t in done:
            if t.exception():
                print("exception:", patch.task_get_name(t), t.exception())
            elif t.result():
                print("result:", patch.task_get_name(t), t.result())

        if not pending:
            break

        for t in pending:
            t.cancel()

        aws = pending


async def main():
    """ main synchronous entry point """

    async def talk(bus, keys):
        print(f"talk({keys}): start")
        for v in range(5):
            for k in keys:
                await asyncio.sleep(0.35)
                await bus.send(Message("local", k, v))

    async def listen(bus, pattern):
        await asyncio.sleep(1.5)
        try:
            async for x in bus.listen(pattern):
                print(f"listen({pattern}):", x)
        except asyncio.CancelledError:
            pass

    async def mon():
        for n in range(6):
            await asyncio.sleep(2)
            print("mon status:", n, await ps.status())

    ps = BasicMessageBus()
    await ps.connect()

    bridge = MessageBridge("cat.", ps)

    aws = (
        talk(ps, ("cat.dog", "cat.pig", "cow.emu")),
        listen(ps, "."),
        listen(ps, "cat."),
        listen(ps, "cat.pig"),
        bridge.start(),
        mon(),
    )

    await wait_graceafully(aws, timeout=15)

    await ps.close()
    
    print("main: done")


if __name__ == "__main__":
    patch.patch()
    asyncio.run(main(), debug=True)
    print("all: done")
