"""
simple pubsub system
"""

import re
import sys
import asyncio
import time
from collections import namedtuple
import logging
import warnings


Message = namedtuple('Message', 'key, value')


logger = logging.getLogger()

def ts():
    return time.time()


class RefexPattern():
    """ regex patterns """
    def __init__(self, pattern):
        self.regex = re.compile(pattern)

    def match(self, subject):
        return self.regex.fullmatch(subject)

                    
class DotPattern():
    """ dot-separated path-style patterns """
    
    def __init__(self, pattern):
        self.pattern = pattern.upper()
        
    def match(self, subject):
        if self.pattern == subject.upper() or self.pattern in ('', '.'):
            return True

        prefix = self.pattern + '' if self.pattern[-1] == '.' else '.'
        return subject.upper().startswith(prefix)         


class MessageBus:
    def __init__(self):
        self._channels = {}

    def set_channel(self, k, v):
        self._channels[k] = v

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
        logger.info(f'connected ({address})')

    async def send(self, k, v):
        if not self.conn:
            raise RuntimeError('not connected')

        if k.endswith('.'):
            raise ValueError("trailing '.' in key")
        self.set_channel(k, v)
        for pattern, q in self.listeners:
            if pattern.match(k):
                await q.put(Message(k, v))

    async def listen(self, pattern):
        if not self.conn:
            raise RuntimeError('not connected')
            
        try:
            p = DotPattern(pattern)
            q = asyncio.Queue()

            self.listeners.add((p, q))

            # yield current values
            for k, v in self.get_channels():
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
        if self.conn:
            return {
                "status": "connected",
                "iisteners": len(self.listeners),
                "channels": list(self.get_channels()),
                "timestamp": ts(),
            }
        else:
            return {
                "status": "disconnected",
            }

    async def close(self):
        self.conn = None
        for p, q in self.listeners:
            await q.put(None)
        logger.info(f'connection closed')
    

async def main():
    ps = BasicMessageBus()
    await ps.connect()

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
    done, pending = await asyncio.wait(aws, timeout=15)

    print('run done:')
    for t in done:
        if t.exception():
            print('exception:', t, t.exception())
        else:
            print('result:', t.result())

    print('main: done')


def patch():
    def run(task, debug=False):
        try:
            loop = asyncio.get_event_loop()
        except:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        if debug:
            loop.set_debug(True)
            logging.getLogger('asyncio').setLevel(logging.DEBUG)
            warnings.filterwarnings('always')
        else:
            loop.set_debug(False)
            logging.getLogger('asyncio').setLevel(logging.WARNING)
            warnings.filterwarnings('default')
            
        return loop.run_until_complete(task)
        
    version = sys.version_info.major * 10 + sys.version_info.minor
    if version < 37:
        asyncio.create_task = asyncio.ensure_future
        asyncio.run = run


def test():
    m = Message('a', 1, ts())
    print(m)
    
    
if __name__ == '__main__':
    patch()
    asyncio.run(main(), debug=True)
    
