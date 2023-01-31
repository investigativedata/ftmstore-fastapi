from functools import cache

from fastapi import Query as QueryField
from fastapi import Request
from pydantic import BaseModel

from .cache import cache_view
from .query import ExtraQueryParams, Query, SearchQuery
from .serialize import (
    DataCatalogResponse,
    DatasetResponse,
    EntitiesResponse,
    EntityResponse,
)
from .store import get_catalog, get_dataset


class RetrieveParams(BaseModel):
    nested: bool
    dehydrate: bool
    dehydrate_nested: bool


def get_retrieve_params(
    nested: bool = QueryField(
        False, description="Inline adjacent entities instead of their ids"
    ),
    dehydrate: bool = QueryField(
        False, description="Only include featured properties and 1 name (caption)"
    ),
    dehydrate_nested: bool = QueryField(True, description="Dehydrate nested entities"),
) -> RetrieveParams:
    return RetrieveParams(
        nested=nested, dehydrate=dehydrate, dehydrate_nested=dehydrate_nested
    )


@cache
def dataset_list() -> DataCatalogResponse:
    catalog = get_catalog()
    return DataCatalogResponse.from_catalog(catalog)


@cache
def dataset_detail(name: str) -> DatasetResponse:
    dataset = get_dataset(name)
    return DatasetResponse.from_dataset(dataset)


@cache_view
def entity_list(
    request: Request,
    dataset: str,
    retrieve_params: RetrieveParams,
    authenticated: bool | None = False,
) -> EntitiesResponse:
    dataset = get_dataset(dataset)
    params = ExtraQueryParams.from_request(request, authenticated)
    query = Query.from_params(dataset.name, params)
    return EntitiesResponse.from_view(
        request=request,
        entities=dataset.get_entities(query, **retrieve_params.dict()),
        total=dataset.get_count(query),
        authenticated=authenticated,
    )


@cache_view
def entity_detail(
    request: Request, dataset: str, entity_id: str, retrieve_params: RetrieveParams
) -> EntityResponse:
    dataset = get_dataset(dataset)
    entity = dataset.get(entity_id, **retrieve_params.dict())
    return EntityResponse.from_entity(entity)


@cache_view
def search(
    request: Request,
    dataset: str,
    retrieve_params: RetrieveParams,
    q: str | None = None,
) -> EntitiesResponse:
    dataset = get_dataset(dataset)
    params = ExtraQueryParams.from_request(request)
    query = SearchQuery.from_params(dataset.name, params)
    if q:
        query.term = q
    return EntitiesResponse.from_view(
        request=request,
        entities=dataset.get_entities(query, **retrieve_params.dict()),
        total=dataset.get_count(query),
    )
