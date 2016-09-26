from __future__ import absolute_import
from .defaults import USE_CACHE
import requests
import time

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
    if USE_CACHE:
        import requests_cache
        requests_cache.install_cache(cache_name='github_cache', backend='sqlite', expire_after=180)


@RateLimited(1)
def requests_get(*args, **kwargs):
    time.sleep(2)
    resp = requests.get(*args, **kwargs)
    return resp
