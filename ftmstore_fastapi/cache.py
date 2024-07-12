from fastapi import Request
from normality import slugify

from ftmstore_fastapi import settings

PREFIX = f"ftmstore_fastapi:{settings.VERSION}:{slugify(settings.TITLE)}"


def get_cache_key(request: Request, *args, **kwargs) -> str | None:
    if not settings.CACHE:
        return None
    return f"{PREFIX}:{slugify(str(request.url))}"
