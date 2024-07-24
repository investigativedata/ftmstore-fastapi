from collections.abc import Iterable

from anystore import anycache
from fastapi import Query as QueryField
from fastapi import Request
from fastapi.responses import RedirectResponse
from ftmq.model import Dataset
from ftmq.types import CE
from ftmq.util import get_dehydrated_proxy
from furl import furl

from ftmstore_fastapi.cache import get_cache_key
from ftmstore_fastapi.query import (
    AggregationParams,
    Query,
    RetrieveParams,
    ViewQueryParams,
)
from ftmstore_fastapi.serialize import (
    AggregationResponse,
    CatalogResponse,
    DatasetResponse,
    EntitiesResponse,
    EntityResponse,
)
from ftmstore_fastapi.store import get_catalog, get_dataset, get_view


def get_retrieve_params(
    nested: bool = QueryField(
        False, description="Inline adjacent entities instead of their ids"
    ),
    featured: bool = QueryField(
        False, description="Only include featured properties and caption"
    ),
    dehydrate: bool = QueryField(
        False, description="Only include id, schema and caption"
    ),
    dehydrate_nested: bool = QueryField(True, description="Dehydrate nested entities"),
) -> RetrieveParams:
    return RetrieveParams(
        nested=nested,
        featured=featured,
        dehydrate=dehydrate,
        dehydrate_nested=dehydrate_nested,
    )


def get_aggregation_params(
    aggSum: list[str] = QueryField([], description="Fields to aggregate for SUM"),
    aggMax: list[str] = QueryField([], description="Fields to aggregate for MAX"),
    aggMin: list[str] = QueryField([], description="Fields to aggregate for MIN"),
    aggAvg: list[str] = QueryField([], description="Fields to aggregate for AVG"),
) -> AggregationParams:
    return AggregationParams(aggSum=aggSum, aggMin=aggMin, aggMax=aggMax, aggAvg=aggAvg)


@anycache(key_func=get_cache_key, serialization_mode="pickle")
def dataset_list(request: Request) -> CatalogResponse:
    catalog = get_catalog()
    datasets: list[Dataset] = []
    for dataset in catalog.datasets:
        view = get_view(dataset.name)
        dataset.apply_stats(view.stats())
        datasets.append(dataset)
    catalog.datasets = datasets
    return CatalogResponse.from_catalog(request, catalog)


@anycache(key_func=get_cache_key, serialization_mode="pickle")
def dataset_detail(request: Request, name: str) -> DatasetResponse:
    view = get_view(name)
    dataset = get_dataset(name)
    dataset.apply_stats(view.stats())
    return DatasetResponse.from_dataset(request, dataset)


@anycache(key_func=get_cache_key, serialization_mode="pickle")
def entity_list(
    request: Request,
    retrieve_params: RetrieveParams,
    authenticated: bool | None = False,
) -> EntitiesResponse:
    view = get_view()
    params = ViewQueryParams.from_request(request, authenticated)
    query = Query.from_params(params)
    adjacents = []
    entities = [e for e in view.get_entities(query, retrieve_params)]
    if retrieve_params.nested:
        adjacents = view.get_adjacents(entities)
    return EntitiesResponse.from_view(
        request=request,
        entities=entities,
        adjacents=adjacents,
        stats=view.stats(query),
        authenticated=authenticated,
    )


@anycache(key_func=get_cache_key, serialization_mode="pickle")
def entity_detail(
    request: Request,
    entity_id: str,
    retrieve_params: RetrieveParams,
) -> EntityResponse | RedirectResponse:
    view = get_view()
    entity = view.get_entity(entity_id, retrieve_params)
    adjacents: Iterable[CE] = []
    if retrieve_params.nested:
        adjacents = [e[1] for e in view.view.get_adjacent(entity)]
        if retrieve_params.dehydrate_nested:
            adjacents = [get_dehydrated_proxy(e) for e in adjacents]
    if entity.id != entity_id:  # we have a redirect to a merged entity
        url = furl(request.url)
        url.path.segments[-1] = entity.id
        response = RedirectResponse(url)
        response.headers["X-Entity-ID"] = entity.id
        response.headers["X-Entity-Schema"] = entity.schema.name
        return response
    return EntityResponse.from_entity(entity, adjacents)


@anycache(key_func=get_cache_key, serialization_mode="pickle")
def aggregation(request: Request) -> AggregationResponse:
    view = get_view()
    params = ViewQueryParams.from_request(request)
    query = Query.from_params(params)
    return AggregationResponse.from_view(
        request=request,
        aggregations=view.aggregations(query),
        stats=view.stats(query),
    )
