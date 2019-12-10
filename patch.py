import sys
import asyncio
import warnings
import logging


def patch():
    """ monkey patch some Python 3.7 stuff into earlier versions """

    def run(task, debug=False):
        try:
            loop = asyncio.get_event_loop()
        except:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if debug:
            loop.set_debug(True)
            logging.getLogger("asyncio").setLevel(logging.DEBUG)
            warnings.filterwarnings("always")
        else:
            loop.set_debug(False)
            logging.getLogger("asyncio").setLevel(logging.WARNING)
            warnings.filterwarnings("default")

        return loop.run_until_complete(task)

    version = sys.version_info.major * 10 + sys.version_info.minor
    if version < 37:
        asyncio.get_running_loop = asyncio.get_event_loop
        asyncio.create_task = asyncio.ensure_future
        asyncio.run = run
