"""
simple pubsub system
"""

import sys
import asyncio

    
class PubSub:
    def __init__(self):
        self.subs = {}

    async def publish(self, k, v):
        if k in self.subs:
            for q in self.subs[k]:
                await q.put((k, v))

    async def subscribe(self, k):
        try:
            q = asyncio.Queue()
            
            if k not in self.subs:
                self.subs[k] = set()
            self.subs[k].add(q)

            while True:
                msg = await q.get()
                if not msg:
                    break
                yield msg
        except:
            raise 
        finally:
            self.subs[k].remove(q)

        print(f'subscribe done: {k}')

    async def close(self):
        print('closing')
        for s in self.subs.values():
            for q in s:
                await q.put(None)
    

def main():
    try:
        loop = asyncio.get_event_loop()
    except:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    ps = PubSub()
    
    async def talk(keys):
        for n in range(5):
            for k in keys:
                await asyncio.sleep(1)
                await ps.publish(k, n)
                
        await ps.close()
        print('talk: done')
        
    async def listen(k):
        async for x in ps.subscribe(k):
            print(k, x)
        print(f'listen {k}: done')
        
    async def mon():
        await asyncio.sleep(10)
        print('mon: done')
        
    aws = {
        talk(('junk', 'pig')),
        listen('junk'),
        listen('pig'),
        mon(),
    }
    loop.run_until_complete(asyncio.wait(aws, timeout=15))

    print('main: done')


def patch():
    version = sys.version_info.major * 10 + sys.version_info.minor
    if version < 37:
        asyncio.create_task = asyncio.ensure_future


if __name__ == '__main__':
    patch()
    main()

