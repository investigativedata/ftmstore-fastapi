# ftmstore-fastapi

Expose a [followthemoney-store](https://github.com/alephdata/followthemoney-store)
to a readonly [FastAPI](https://fastapi.tiangolo.com/)

An example instance is deployed here: https://api.investigraph.dev

The api features filtering for entity `Schema` and `Property` values, sorting
and a search endpoint to query against a [sqlite full-text index](https://www.sqlite.org/fts5.html)
that contains the values for [name type](https://alephdata.github.io/followthemoney/explorer/types/name/)
properties.

By default, the whole dataset(s) are loaded into an in-memory sqlite to provide
very fast and cached responses.

For mid-scale datasets (up to 1GB json dump, didn't test bigger ones yet) the
api is incredibly fast when using the in-memory sqlite.

There are two main api endpoints:

* Retrieve a single entity based on its id and dataset, optionally with inlined
  adjacent entities: `/{dataset}/entities/{entity_id}`
* Retrieve a list of entities based on filter criteria and sorting, with
  pagination: `/{dataset}/entities?{params}`
    * Search for entities (by their name property types) via
      [Sqlite FTS](https://www.sqlite.org/fts5.html): `/{dataset}/entities?q=<search term>`

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

Filtering works for all [FollowTheMoney](https://followthemoney.tech/explorer/)
properties

```json
{
    "id": "NK-A7z....",
    "schema": "LegalEntity",
    "properties": {
        "name": [ "Jane Doe" ]
    }
}
```

Could be queried like this:

`/{dataset}/entities?name__ilike=%jane%`

#### sorting

For ftm properties:  `?order_by={prop}` (descending: `/?order_by=-{prop}`)

[Numeric](https://alephdata.github.io/followthemoney/explorer/types/number/#content)
property types are casted via sqlite `CAST(value AS NUMERIC)` (ignoring
errors, results in 0) before sorting, and the first property in the value
array is used as the sorting value. (The entity property dict remains
uncasted, aka all properties are multi values as string)

#### searching

Add `q=<term>` GET parameter to the query.

Per default, names, countries, identifiers and *featured* entity properties are
indexed. See below for `INDEX_PROPERTIES` setting.

#### aggregation

Aggregations `sum`, `min`, `max`, `avg` are performed via sqlite, and work for
both properties and arbitrary extra data. The endpoint accepts all other
parameters from the entities endpoint as well (+ search term via `q`).

`/{dataset}/aggregate?schema=Payment&aggSum=amount&aggAvg=amount&aggMin=date&aggMax=date`

```json
{
  "total": 143598,
  "query": {
    "limit": 100,
    "page": 1,
    "schema": "Payment",
    "order_by": null,
    "prop": null,
    "value": null
  },
  "url": "...",
  "aggregations": {
    "amount": {
      "avg": 8909.264242607847,
      "sum": 1279352526.7100017
    },
    "date": {
      "min": "2007",
      "max": "2021"
    }
  }
}
```


## quickstart

    FTM_STORE_URI=postgres:///ftm gunicorn -w 4 -k uvicorn.workers.UvicornWorker -b 0.0.0.0:8000 ftmq_api.api:app

This loads the datasets in the ftm store provided by `FTM_STORE_URI` into the in-memory sqlite (1 per worker) and exposes the api at http://localhost:8000

## settings

env vars and their defaults

```
FTM_STORE_URI=followthemoney.store
CATALOG=None  # optional specify catalog metadata file
EXPOSE_DATASETS="*"   # restrict exposed datasets to comma separated list
BUILD_API_KEY=secret-key-for-build  # an api key for static site builders to increase limits
ALLOWED_ORIGIN=http://localhost:3000  # cors, comma-separated origins
CACHE=0  # set 1 to use redis
CACHE_TIMEOUT=0  # infinite
REDIS_URL=redis://localhost:6379
DEFAULT_LIMIT=100  # results per page
SQLITE_IN_MEMORY=1  # 0 to disable
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

## development

This package is using [poetry](https://python-poetry.org/) for packaging and dependencies management, so first [install it](https://python-poetry.org/docs/#installation).

Clone the repository to a local destination.

Within the root directory, run

    poetry install --with dev

This installs a few development dependencies, including [pre-commit](https://pre-commit.com/) which needs to be registered:

    poetry run pre-commit install

Before creating a commit, this checks for correct code formatting (isort, black) and some other useful stuff (see: `.pre-commit-config.yaml`)

Spin up dev server and populate with fixtures data:

    make api

Run test & typing:

    make test
    make typecheck

## supported by

Since March 2023, developing of this project is supported by
[Media Tech Lab Bayern batch #3](https://github.com/media-tech-lab)

<a href="https://www.media-lab.de/en/programs/media-tech-lab">
    <img src="https://raw.githubusercontent.com/media-tech-lab/.github/main/assets/mtl-powered-by.png" width="240" title="Media Tech Lab powered by logo">
</a>
