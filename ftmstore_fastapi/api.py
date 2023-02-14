import secrets
from typing import Literal

from fastapi import Depends, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from ftmstore.settings import DATABASE_URI

from . import settings, views
from .logging import get_logger
from .query import QueryParams
from .serialize import (
    AggregationResponse,
    DataCatalogResponse,
    DatasetResponse,
    EntitiesResponse,
    EntityResponse,
)
from .store import get_catalog

log = get_logger(__name__)

app = FastAPI(
    title=settings.TITLE,
    contact=settings.CONTACT,
    description=settings.DESCRIPTION,
    redoc_url="/",
    version=settings.VERSION,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.ALLOWED_ORIGIN, "http://localhost:3000"],
    allow_methods=["OPTIONS", "GET"],
)

log.info("Ftm store: %s" % DATABASE_URI)


@app.get("/catalog")
async def dataset_list() -> DataCatalogResponse:
    """
    Show metadata for catalog (as described in
    [nomenklatura.DataCatalog](https://github.com/opensanctions/nomenklatura))

    This is basically a list of the available dataset within this api instance.
    """
    return views.dataset_list()


# cache at boot time
catalog = get_catalog()
Datasets = Literal[tuple(catalog.names)]


@app.get("/{dataset}")
async def dataset_detail(dataset: Datasets) -> DatasetResponse:
    """
    Show metadata for given dataset (as described in
    [nomenklatura.Dataset](https://github.com/opensanctions/nomenklatura))
    """
    return views.dataset_detail(dataset)


def get_authenticated(
    api_key: str = Query(
        None,
        description="Secret api key to increase limit (useful for e.g. static site builders)",
    )
) -> bool:
    if not api_key:
        return False
    return secrets.compare_digest(api_key, settings.BUILD_API_KEY)


@app.get("/{dataset}/entities")
async def list_entities(
    request: Request,
    dataset: Datasets,
    params: QueryParams = Depends(QueryParams),
    retrieve_params: views.RetrieveParams = Depends(views.get_retrieve_params),
    authenticated: bool = Depends(get_authenticated),
) -> EntitiesResponse:
    """
    Retrieve a paginated list of entities for the given dataset based on filter
    criteria.

    Optionally inline (nest) adjacent entities.

    Entities can be "dehytrated", that means only their featured properties are
    returned. This is e.g. useful for static site builders to reduce the data
    amount.

    ## filter

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

    ## sorting

    For ftm properties:  `?order_by={prop}` (descending: `/?order_by=-{prop}`)

    [Numeric](https://alephdata.github.io/followthemoney/explorer/types/number/#content)
    property types are casted via sqlite `CAST(value AS NUMERIC)` (ignoring
    errors, results in 0) before sorting, and the first property in the value
    array is used as the sorting value. (The entity property dict remains
    uncasted, aka all properties are multi values as string)

    For arbitrary context data: `?order_by=context.foo`
    """
    return views.entity_list(
        request,
        dataset,
        retrieve_params,
        authenticated=authenticated,
    )


@app.get("/{dataset}/entities/{entity_id}")
async def detail_entity(
    request: Request,
    dataset: str,
    entity_id: str,
    retrieve_params: views.RetrieveParams = Depends(views.get_retrieve_params),
) -> EntityResponse:
    """
    Retrieve a single entity within the given dataset.

    Optionally inline (nest) adjacent entities.
    """
    return views.entity_detail(request, dataset, entity_id, retrieve_params)


@app.get("/{dataset}/search")
async def search(
    request: Request,
    dataset: str,
    q: str = Query(None, title="Search string"),
    params: QueryParams = Depends(QueryParams),
    retrieve_params: views.RetrieveParams = Depends(views.get_retrieve_params),
) -> EntitiesResponse:
    """
    Search entities in the [FTS5-Index](https://www.sqlite.org/fts5.html)

    All other parameters from the entities list view for optional filtering of
    the search result can be used here as well.
    """
    return views.search(request, dataset, retrieve_params, q)


@app.get("/{dataset}/aggregate")
async def aggregation(
    request: Request,
    dataset: str,
    q: str = Query(None, title="Search string"),
    params: QueryParams = Depends(QueryParams),
    aggregation_params: views.AggreagtionParams = Depends(views.get_aggregation_params),
) -> AggregationResponse:
    """
    Aggregate property values for given filter criteria (same as entities
    endpoint + search term)

    specify which props / context data should be aggregated like this:

        ?aggSum=amount&aggMin=amount

    multiple fields possible:

        ?aggMax=amount&aggMax=date
    """
    return views.aggregation(request, dataset, q, aggregation_params)
