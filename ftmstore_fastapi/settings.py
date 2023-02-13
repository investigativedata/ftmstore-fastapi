import os
from pathlib import Path

from banal import as_bool

from ftmstore_fastapi import __version__

VERSION = __version__

CATALOG = os.environ.get("CATALOG")
if CATALOG is not None:
    CATALOG = Path(CATALOG)

DATASETS = os.environ.get("EXPOSE_DATASETS", "*")  # all by default

DEBUG = as_bool(os.environ.get("DEBUG", 0))
LOG_LEVEL = "DEBUG" if DEBUG else "INFO"
BUILD_API_KEY = os.environ.get("BUILD_API_KEY", "secret-key-for-build")

ALLOWED_ORIGIN = os.environ.get("ALLOWED_ORIGIN", "http://localhost:3000")
CACHE = as_bool(os.environ.get("CACHE", 0))
CACHE_TIMEOUT = int(os.environ.get("CACHE_TIMEOUT", 0))
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379")
DEFAULT_LIMIT = 100
LOG_JSON = as_bool(os.environ.get("LOG_JSON", 0))
IN_MEMORY = as_bool(os.environ.get("SQLITE_IN_MEMORY", 1))
PRELOAD_DATASETS = as_bool(os.environ.get("PRELOAD_DATASETS", 0))
INDEX_PROPERTIES = os.environ.get("INDEX_PROPERTIES", "").split(",")

# Api documentation render
TITLE = os.environ.get("TITLE", "FollowTheMoney Store API")
CONTACT = {
    "name": os.environ.get("CONTACT_AUTHOR", "Simon Wörpel"),
    "url": os.environ.get(
        "CONTACT_URL", "https://github.com/simonwoerpel/ftmstore-fastapi/"
    ),
    "email": os.environ.get("CONTACT_EMAIL", "simon.woerpel@medienrevolte.de"),
}
DESCRIPTION = """
This api exposes a
[FollowTheMoney-Store](https://github.com/alephdata/followthemoney-store) as a
read-only endpoint that allows granular data fetching and searching.

* [Available datasets in this api instance](/catalog)
* [More about the FollowTheMoney model](https://alephdata.github.io/followthemoney/)

This is suited for mid-scale entities collections that fit into an in-memory
sqlite database that is loaded on boot time feeded from the source database via
`FTM_STORE_URI`. For production use, a simple cache based on redis is
available.

There are three main api endpoints:

* Retrieve a single entity based on its id and dataset, optionally with inlined
  adjacent entities: `/{dataset}/entities/{entity_id}`
* Retrieve a list of entities based on filter criteria and sorting, with
  pagination: `/{dataset}/entities?{params}`
* Search for entities (by their name property types) via
  [Sqlite FTS](https://www.sqlite.org/fts5.html): `/{dataset}/search?q=<search term>`

Two more endpoints for catalog / dataset metadata:

* Catalog overview: [`/catalog`](/catalog)
* Dataset metadata: `/{dataset}`
"""
