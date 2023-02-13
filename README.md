# ftmstore-fastapi

Expose a [followthemoney-store](https://github.com/alephdata/followthemoney-store) to a readonly [FastAPI](https://fastapi.tiangolo.com/)

The api features filtering for entity `Schema` and `Property` values, sorting and a search endpoint to query against a [sqlite full-text index](https://www.sqlite.org/fts5.html) that contains the values for [name type](https://alephdata.github.io/followthemoney/explorer/types/name/) properties.

By default, the whole dataset(s) are loaded into an in-memory sqlite to provide very fast and cached responses.

For mid-scale datasets (up to 1GB json dump, didn't test bigger ones yet) the api is incredibly fast when using the in-memory sqlite.

There are three main api endpoints:

* Retrieve a single entity based on its id and dataset, optionally with inlined
  adjacent entities: `/{dataset}/entities/{entity_id}`
* Retrieve a list of entities based on filter criteria and sorting, with
  pagination: `/{dataset}/entities?{params}`
* Search for entities (by their name property types) via
  [Sqlite FTS](https://www.sqlite.org/fts5.html): `/{dataset}/search?q=<search term>`

Two more endpoints for catalog / dataset metadata:

* Catalog overview: `/catalog`
* Dataset metadata: `/{dataset}`

### entities endpoint

Retrieve a paginated list of entities for the given dataset based on filter
criteria.

Optionally inline (nest) adjacent entities.

Entities can be "dehytrated", that means only their featured properties are
returned. This is e.g. useful for static site builders to reduce the data
amount.

#### filter

`/{dataset}/entities?schema=Company?country=de`

Filtering works for all [FollowTheMoney](https://alephdata.github.io/followthemoney/explorer/)
properties as well as for arbitrary extra data stored within the entity
dict (referred to as context), example entity:

```json
{
    "id": "NK-A7z....",
    "schema": "LegalEntity",
    "properties": {
        "name": [ "John Doe" ]
    },
    "foo": "bar"
}
```

Could be queried like this:

`/{dataset}/entities?context.foo=bar`

#### sorting

For ftm properties:  `?order_by={prop}` (descending: `/?order_by=-{prop}`)

[Numeric](https://alephdata.github.io/followthemoney/explorer/types/number/#content)
property types are casted via sqlite `CAST(value AS NUMERIC)` (ignoring
errors, results in 0) before sorting, and the first property in the value
array is used as the sorting value. (The entity property dict remains
uncasted, aka all properties are multi values as string)

For arbitrary context data: `?order_by=context.foo`


## quickstart

    FTM_STORE_URI=postgres:///ftm gunicorn -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000 ftmstore_fastapi.api:app

This loads the datasets in the ftm store provided by `FTM_STORE_URI` into the in-memory sqlite (1 per worker) and exposes the api at http://localhost:8000

## settings

env vars and their defaults

```
FTM_STORE_URI=followthemoney.store
CATALOG=None  # optional specify catalog metadata file
EXPOSE_DATASETS="*"   # restrict exposed datasets to comma separated list
BUILD_API_KEY=secret-key-for-build  # an api key for static site builders to increase limits
ALLOWED_ORIGIN=http://localhost:3000  # cors
CACHE=0  # set 1 to use redis
CACHE_TIMEOUT=0  # infinite
REDIS_URL=redis://localhost:6379
DEFAULT_LIMIT=100  # results per page
SQLITE_IN_MEMORY=1  # 0 to disable
PRELOAD_DATASETS=0  # lazy loading of datasets, set 1 to load all at startup
INDEX_PROPERTIES=""  # comma-separated additional properties to add to the FTS index, e.g. : "keywords,notes"
# for api docs rendering:
TITLE=FollowTheMoney Store API"
CONTACT_AUTHOR
CONTACT_URL
CONTACT_EMAIL
```

## production deployment

To even improve performance, api responses can be cached in [redis](https://redis.io/). Per default, this is an infinite cache.

See the example `docker-compose.yml`

## developement

    pip install -e .

Spin up dev server and populate with fixtures data:

    make api

Run test & typing:

    make test
