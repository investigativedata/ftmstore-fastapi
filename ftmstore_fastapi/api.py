import secrets
from typing import Literal

from fastapi import Depends, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from ftmq.settings import DB_STORE_URI

from ftmstore_fastapi import settings, views
from ftmstore_fastapi.logging import get_logger
from ftmstore_fastapi.query import QueryParams
from ftmstore_fastapi.serialize import (  # AggregationResponse,
    CatalogResponse,
    DatasetResponse,
    EntitiesResponse,
    EntityResponse,
    ErrorResponse,
)
from ftmstore_fastapi.store import get_catalog

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
    allow_origins=[*settings.ALLOWED_ORIGIN, "http://localhost:3000"],
    allow_methods=["OPTIONS", "GET"],
)

log.info("Ftm store: %s" % DB_STORE_URI)


@app.get(
    "/catalog",
    response_model=CatalogResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def dataset_list(request: Request) -> CatalogResponse:
    """
    Show metadata for catalog (as described in
    [nomenklatura.DataCatalog](https://github.com/opensanctions/nomenklatura))

    This is basically a list of the available dataset within this api instance.
    """
    return views.dataset_list(request)


# cache at boot time
catalog = get_catalog()
Datasets = Literal[tuple(catalog.names)]


@app.get(
    "/{dataset}",
    response_model=DatasetResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def dataset_detail(request: Request, dataset: Datasets) -> DatasetResponse:
    """
    Show metadata for given dataset (as described in
    [nomenklatura.Dataset](https://github.com/opensanctions/nomenklatura))
    """
    return views.dataset_detail(request, dataset)


def get_authenticated(
    api_key: str = Query(
        None,
        description="Secret api key to increase limit (useful for e.g. static site builders)",
    )
) -> bool:
    if not api_key:
        return False
    return secrets.compare_digest(api_key, settings.BUILD_API_KEY)


@app.get(
    "/{dataset}/entities",
    response_model=EntitiesResponse,
    responses={
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def list_entities(
    request: Request,
    dataset: Datasets,
    q: str = Query(None, title="Search string"),
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

    Filtering works for all [FollowTheMoney](https://followthemoney.tech/explorer/)
    properties

    ```json
    {
        "id": "NK-A7z....",
        "schema": "LegalEntity",
        "properties": {
            "name": [ "Jane Doe" ]
        },
    }
    ```

    Could be queried like this:

    `/{dataset}/entities?name__ilike=%Jane%`

    ## sorting

    For ftm properties:  `?order_by={prop}` (descending: `/?order_by=-{prop}`)

    [Numeric](https://followthemoney.tech/explorer/types/number/)
    property types are casted via sql `CAST(value AS NUMERIC)` (ignoring
    errors, results in 0) before sorting, and the first property in the value
    array is used as the sorting value. (The entity property dict remains
    uncasted, aka all properties are multi values as string)

    ## searching

    Search entities in the [FTS5-Index](https://www.sqlite.org/fts5.html)

    Use optional `q` parameter for a search term.
    """
    return views.entity_list(
        request,
        dataset,
        retrieve_params,
        q=q,
        authenticated=authenticated,
    )


@app.get(
    "/{dataset}/entities/{entity_id}",
    response_model=EntityResponse,
    responses={
        307: {"description": "The entity was merged into another ID"},
        404: {"model": ErrorResponse, "description": "Entity not found"},
        500: {"model": ErrorResponse, "description": "Server error"},
    },
)
async def detail_entity(
    request: Request,
    dataset: Datasets,
    entity_id: str,
    retrieve_params: views.RetrieveParams = Depends(views.get_retrieve_params),
) -> EntityResponse | RedirectResponse | ErrorResponse:
    """
    Retrieve a single entity within the given dataset.

    Optionally inline (nest) adjacent entities.

    If the requested entity was merged into another entity, a redirect to the
    new api endpoint is returned with additional headers to allow client side
    logic:

        `x-entity-id` - the new entity id
        `x-entity-schema` - the new entity schema
    """
    return views.entity_detail(request, dataset, entity_id, retrieve_params)


# @app.get(
#     "/{dataset}/aggregate",
#     response_model=AggregationResponse,
#     responses={
#         500: {"model": ErrorResponse, "description": "Server error"},
#     },
# )
# async def aggregation(
#     request: Request,
#     dataset: Datasets,
#     q: str = Query(None, title="Search string"),
#     params: QueryParams = Depends(QueryParams),
#     aggregation_params: views.AggregationParams = Depends(views.get_aggregation_params),
# ) -> AggregationResponse:
#     """
#     Aggregate property values for given filter criteria (same as entities
#     endpoint + search term)

#     specify which props should be aggregated like this:

#         ?aggSum=amount&aggMin=amount

#     multiple fields possible:

#         ?aggMax=amount&aggMax=date
#     """
#     return views.aggregation(request, dataset, q, aggregation_params)
