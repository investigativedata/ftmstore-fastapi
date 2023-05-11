from functools import cache

from fastapi import Query as QueryField
from fastapi import Request
from fastapi.responses import RedirectResponse
from furl import furl
from pydantic import BaseModel

from .cache import cache_view
from .query import (
    AggregationParams,
    AggregationQuery,
    ExtraQueryParams,
    Query,
    SearchQuery,
)
from .serialize import (
    AggregationResponse,
    DataCatalogResponse,
    DatasetResponse,
    EntitiesResponse,
    EntityResponse,
)
from .store import get_catalog, get_dataset


class RetrieveParams(BaseModel):
    nested: bool
    featured: bool
    dehydrate: bool
    dehydrate_nested: bool


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


@cache
def dataset_list(request: Request) -> DataCatalogResponse:
    catalog = get_catalog()
    return DataCatalogResponse.from_catalog(request, catalog)


@cache
def dataset_detail(request: Request, name: str) -> DatasetResponse:
    dataset = get_dataset(name)
    return DatasetResponse.from_dataset(request, dataset)


@cache_view
def entity_list(
    request: Request,
    dataset: str,
    retrieve_params: RetrieveParams,
    q: str | None = None,
    authenticated: bool | None = False,
) -> EntitiesResponse:
    dataset = get_dataset(dataset)
    params = ExtraQueryParams.from_request(request, authenticated)
    if q:
        query = SearchQuery.from_params(dataset.name, params)
        query.term = q
    else:
        query = Query.from_params(dataset.name, params)
    return EntitiesResponse.from_view(
        request=request,
        entities=dataset.get_entities(query, **retrieve_params.dict()),
        total=dataset.get_count(query),
        schemata=dataset.get_schemata_groups(query),
        authenticated=authenticated,
    )


@cache_view
def entity_detail(
    request: Request, dataset: str, entity_id: str, retrieve_params: RetrieveParams
) -> EntityResponse | RedirectResponse:
    dataset = get_dataset(dataset)
    entity = dataset.get_entity(entity_id, **retrieve_params.dict())
    if entity.id != entity_id:  # we have a redirect to a merged entity
        url = furl(request.url)
        url.path.segments[-1] = entity.id
        response = RedirectResponse(url)
        response.headers["X-Entity-ID"] = entity.id
        response.headers["X-Entity-Schema"] = entity.schema.name
        return response
    return EntityResponse.from_entity(entity)


@cache_view
def aggregation(
    request: Request,
    dataset: str,
    q: str | None = None,
    aggregation_params: AggregationParams | None = None,
) -> AggregationResponse:
    dataset = get_dataset(dataset)
    params = ExtraQueryParams.from_request(request)
    query = AggregationQuery.from_params(dataset.name, params, aggregation_params)
    if q:
        query.term = q
    return AggregationResponse.from_view(
        request=request,
        aggregations=dataset.get_aggregations(query),
        total=dataset.get_count(query),
    )
