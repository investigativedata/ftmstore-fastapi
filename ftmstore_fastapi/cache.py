from collections import Counter
from functools import cache
from typing import Any

from cachelib import redis
from fastapi import Request
from followthemoney.util import make_entity_id
from furl import furl

from . import settings
from .logging import get_logger

log = get_logger(__name__)

PREFIX = f"ftmstore_fastapi-{settings.VERSION}"


class Cache:
    def __init__(self):
        if settings.CACHE:
            uri = furl(settings.REDIS_URL)
            uri.port = uri.port or 6379
            self.cache = redis.RedisCache(
                uri.host,
                uri.port,
                default_timeout=settings.CACHE_TIMEOUT,
                key_prefix=PREFIX,
            )
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


# decorator
def cache_view(func):
    cache = get_cache()

    def view(request: Request, *args, **kwargs):
        key = Cache.make_key_from_request(request)
        res = cache.get(key)
        if res is not None:
            return res
        res = func(request, *args, **kwargs)
        cache.set(key, res)
        return res

    return view
