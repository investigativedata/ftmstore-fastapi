import os

from banal import as_bool
from nomenklatura.settings import DB_URL

from ftmstore_fastapi import __version__

VERSION = __version__

CATALOG = os.environ.get("CATALOG")
RESOLVER = os.environ.get("RESOLVER", os.environ.get("RESOLVER_PATH"))

FTM_STORE_URI = os.environ.get("FTM_STORE_URI", DB_URL)

DATASETS = os.environ.get("EXPOSE_DATASETS", "*")  # all by default
DATASETS_STATS = as_bool(os.environ.get("DATASETS_STATS", 1))

DEBUG = as_bool(os.environ.get("DEBUG", 0))
LOG_LEVEL = "DEBUG" if DEBUG else "INFO"
BUILD_API_KEY = os.environ.get("BUILD_API_KEY", "secret-key-for-build")

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "http://localhost:3000").split(",")
CACHE = as_bool(os.environ.get("CACHE", 0))
CACHE_TIMEOUT = int(os.environ.get("CACHE_TIMEOUT", 0))
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
DEFAULT_LIMIT = 100
LOG_JSON = as_bool(os.environ.get("LOG_JSON", 0))
INDEX_PROPERTIES = os.environ.get("INDEX_PROPERTIES", "").split(",")

# Api documentation render
TITLE = os.environ.get("TITLE", "FollowTheMoney Store API")
CONTACT = {
    "name": os.environ.get("CONTACT_AUTHOR", "Simon Wörpel"),
    "url": os.environ.get(
        "CONTACT_URL", "https://github.com/simonwoerpel/ftmstore-fastapi/"
    ),
    "email": os.environ.get("CONTACT_EMAIL", "simon@investigativedata.org"),
}
DESCRIPTION = """
This api exposes a
[FollowTheMoney-Store](https://github.com/alephdata/followthemoney-store) as a
read-only endpoint that allows granular data fetching and searching.

* [Available datasets in this api instance](/catalog)
* [More about the FollowTheMoney model](https://followthemoney.tech/explorer/)

This api works for all store implementations found in
[`nomenklatura.store`](https://github.com/opensanctions/nomenklatura/tree/main/nomenklatura/store)

There are four main api endpoints:

* Retrieve a single entity based on its id and dataset, optionally with inlined
  adjacent entities: `/{dataset}/entities/{entity_id}`
* Retrieve a list of entities based on filter criteria and sorting, with
  pagination: `/{dataset}/entities?{params}`
* Search for entities (by their name property types) via
  [Sqlite FTS](https://www.sqlite.org/fts5.html): `/{dataset}/search?q=<search term>`
* Aggregate (on store backend level) `sum`, `avg`, `max`, `min` for ftm properties

Two more endpoints for catalog / dataset metadata:

* Catalog overview: [`/catalog`](/catalog)
* Dataset metadata: `/{dataset}`
"""
