"""
simple pubsub system
"""

import re
import sys
import asyncio
import datetime
import time
from collections import namedtuple
import logging


Message = namedtuple('Message', 'key, value')


def ts():
    return time.time()


class RefexPattern():
    def __init__(self, pattern):
        self.regex = re.compile(pattern)

    def match(self, subject):
        return self.regex.fullmatch(subject)

                    
class DotPattern():
    def __init__(self, pattern):
        self.pattern = pattern.upper()
        
    def match(self, subject):
        if self.pattern == subject.upper() or self.pattern in ('', '.'):
            return True

        prefix = self.pattern + '' if self.pattern[-1] == '.' else '.'
        return subject.upper().startswith(prefix)         

class MessageBus:
    def __init__(self):
        self.address = None
        self.channels = {}
        self.listeners = set()

    async def connect(self, address=None):
        self.address = address
        
        return self

    async def send(self, k, v):
        self.channels[k] = v
        for pattern, q in self.listeners:
            if pattern.match(k):
                result = await q.put(Message(k, v))

    async def listen(self, pattern):
        try: 
            p = DotPattern(pattern)
            q = asyncio.Queue()

            self.listeners.add((p, q))

            # yield current values
            for k, v in self.channels.items():
                if p.match(k):
                    yield Message(k, v)

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
        return {
            "iisteners": len(self.listeners),
            "channels": list(self.channels.keys()),
            "timestamp": ts(),
        }
        
    async def close(self):
        for p, q in self.listeners:
            await q.put(None)
    

def main():
    try:
        loop = asyncio.get_event_loop()
    except:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    ps = MessageBus()

    async def talk(keys):
        for n in range(5):
            for k in keys:
                await asyncio.sleep(1)
                await ps.send(k, n)
                
        await ps.close()
        print('talk: done')
        
    async def listen(k):
        await asyncio.sleep(1.5)
        async for x in ps.listen(k):
            print(f'listen({k}):', x)
        print(f'listen {k}: done')
        
    async def mon():
        await asyncio.sleep(5)
        s = await ps.status()
        print("mon status:", s)
        await asyncio.sleep(1)
        print('mon: done')
        
        return True
        
    aws = {
        talk(('cat.dog', 'cat.pig')),
        listen('.'),
        listen('cat.'),
        listen('cat.pig'),
        mon(),
    }
    done, pending = loop.run_until_complete(asyncio.wait(aws, timeout=15))

    print('run done:')
    for t in done:
        if t.exception():
            print('exception:', t, t.exception())
        else:
            print('result:', t.result())

    print('main: done')


def patch():
    version = sys.version_info.major * 10 + sys.version_info.minor
    if version < 37:
        asyncio.create_task = asyncio.ensure_future


def test():
    m = Message('a', 1, ts())
    print(m)
    
    
if __name__ == '__main__':
    patch()
    main()

