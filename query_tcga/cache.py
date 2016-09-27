from __future__ import absolute_import
from .config import get_setting_value
import requests
import time
import errno
import logging

SESSION = requests.Session()

def RateLimited(maxPerSecond):
    minInterval = 1.0 / float(maxPerSecond)
    def decorate(func):
        lastTimeCalled = [0.0]
        def rateLimitedFunction(*args,**kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait>0:
                time.sleep(leftToWait)
            ret = func(*args,**kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate


def setup_cache():
    global SESSION
    if get_setting_value('USE_CACHE'):
        import requests_cache
        requests_cache.install_cache(cache_name='gdc_cache', backend='sqlite', expire_after=18000)
    #   import cachecontrol
    #   from cachecontrol.caches import FileCache
    #   SESSION = cachecontrol.CacheControl(requests.Session(), cache=FileCache('.web_cache', forever=True))
    #else:
    #    SESSION = requests.Session()


@RateLimited(1)
def requests_get(*args, **kwargs):
    global SESSION
    time.sleep(10)
    try:
        resp = SESSION.get(*args, timeout=5, **kwargs)
    except requests.ConnectionError as e:
        if e.errno != 54:
            raise # Not error we are looking for
        else:
            logging.warning('Warning - connection reset by peer. Trying request again.')
            time.sleep(12)
            resp = SESSION.get(*args, **kwargs)
    return resp


@RateLimited(1)
def requests_post(*args, **kwargs):
    time.sleep(1)
    try:
        resp = requests.post(*args, **kwargs)
    except requests.ConnectionError as e:
        if e.errno != errno.ECONNRESET:
            raise # Not error we are looking for
        else:
            logging.warning('Warning - connection reset by peer. Trying request again.')
            time.sleep(12)
            resp = requests.post(*args, **kwargs)
    return resp


if get_setting_value('USE_CACHE'):
    setup_cache()
