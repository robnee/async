"""
simple pubsub system
"""

import re
import sys
import asyncio
import datetime
import time


def get_timestamp():
    return time.time()
    # .strftime("%Y%m%d %H:%M:%S")

    
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
            if pattern.fullmatch(k):
                result = await q.put((get_timestamp(), k, v))

    async def listen(self, pattern):
        try:
            p = re.compile(pattern)
            q = asyncio.Queue()

            self.listeners.add((p, q))

            # yield current values
            for k, v in self.channels.items():
                if p.fullmatch(k):
                    yield get_timestamp(), k, v
                
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
            "qlen": len(self.listeners),
            "channels": list(self.channels.keys()),
        }
        
    async def close(self):
        for p, q in self.listeners:
            print('closing:', p)
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
        
    aws = {
        talk(('junk', 'pig')),
        listen('junk'),
        listen('pig'),
        mon(),
    }
    done, pending = loop.run_until_complete(asyncio.wait(aws, timeout=15))

    print('done:')
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


if __name__ == '__main__':
    patch()
    main()

