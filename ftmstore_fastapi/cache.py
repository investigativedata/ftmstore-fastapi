from collections import Counter
from functools import cache
from typing import Any

import redis
from cachelib.serializers import RedisSerializer
from fastapi import Request
from followthemoney.util import make_entity_id

from . import settings
from .logging import get_logger

log = get_logger(__name__)

PREFIX = f"ftmstore_fastapi-{settings.VERSION}"


class Cache:
    def __init__(self):
        if settings.CACHE:
            con = redis.from_url(settings.REDIS_URL)
            con.ping()
            self.cache = con
        else:
            self.cache = None

        self.stats = Counter()

    def set(self, key: str, data: Any):
        if self.cache is not None:
            self.stats["set"] += 1
            key = self.get_key(key)
            self.cache.set(key, data)

    def get(self, key: str) -> Any:
        if self.cache is not None:
            self.log_stats()
            self.stats["get"] += 1
            key = self.get_key(key)
            res = self.cache.get(key)
            if res is not None:
                log.debug(f"Cache hit: `{key}`")
                self.stats["hits"] += 1
                return res
            self.stats["miss"] += 1

    def log_stats(self):
        if self.stats["get"] % 100 == 0:
            log.info("cache hits: %d" % self.stats["hits"])
            log.info("cache miss: %d" % self.stats["miss"])

    @staticmethod
    def get_key(key: str) -> str:
        return f"{PREFIX}:{key}"

    @staticmethod
    def make_key_from_request(request: Request) -> str:
        return make_entity_id(request.url)


@cache
def get_cache() -> Cache:
    return Cache()


serializer = RedisSerializer()


# decorator
def cache_view(func):
    cache = get_cache()

    def view(request: Request, *args, **kwargs):
        key = Cache.make_key_from_request(request)
        res = cache.get(key)
        if res is not None:
            return serializer.loads(res)
        res = func(request, *args, **kwargs)
        res = res.dict()
        cache.set(key, serializer.dumps(res))
        return res

    return view
