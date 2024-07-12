"""
serialization data models as seen in
https://github.com/opensanctions/yente/
"""

from collections import defaultdict
from collections.abc import Iterable
from typing import Any, Self, Union

from banal import clean_dict
from fastapi import Request
from followthemoney.types import registry
from ftmq.aggregations import AggregatorResult
from ftmq.model import Catalog, Dataset, DatasetStats
from ftmq.types import CE, CEGenerator
from furl import furl
from pydantic import BaseModel, ConfigDict, Field

from ftmstore_fastapi.query import ViewQueryParams

EntityProperties = dict[str, list[Union[str, "EntityResponse"]]]
Aggregations = dict[str, dict[str, Any]]


class ErrorResponse(BaseModel):
    detail: str = Field(..., example="Detailed error message")


class EntityResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(..., example="NK-A7z....")
    caption: str = Field(..., example="John Doe")
    schema_: str = Field(..., example="LegalEntity", alias="schema")
    properties: EntityProperties = Field(..., example={"name": ["John Doe"]})
    datasets: list[str] = Field([], example=["us_ofac_sdn"])
    referents: list[str] = Field([], example=["ofac-1234"])

    @classmethod
    def from_entity(
        cls, entity: CE, adjacents: Iterable[CE] | None = None
    ) -> "EntityResponse":
        properties = dict(entity.properties)
        if adjacents:
            adjacents = {e.id: EntityResponse.from_entity(e) for e in adjacents}
            for prop in entity.iterprops():
                if prop.type == registry.entity:
                    properties[prop.name] = [
                        adjacents.get(i, i) for i in entity.get(prop)
                    ]
        return cls(
            id=entity.id,
            caption=entity.caption,
            schema=entity.schema.name,
            properties=properties,
            datasets=list(entity.datasets),
            referents=list(entity.referents),
        )


EntityResponse.model_rebuild()


class EntitiesResponse(BaseModel):
    total: int
    items: int
    stats: DatasetStats
    query: ViewQueryParams
    url: str
    next_url: str | None = None
    prev_url: str | None = None
    entities: list[EntityResponse]

    @classmethod
    def from_view(
        cls,
        request: Request,
        entities: CEGenerator,
        stats: DatasetStats,
        adjacents: Iterable[CE] | None = None,
        authenticated: bool | None = False,
    ) -> "EntitiesResponse":
        query = ViewQueryParams.from_request(request, authenticated)
        url = furl(str(request.url))
        query_data = clean_dict(query.model_dump())
        query_data.pop("schema_", None)
        url.args.update(query_data)
        entities = [EntityResponse.from_entity(e, adjacents) for e in entities]
        response = cls(
            total=stats.entity_count,
            items=len(entities),
            query=query,
            entities=entities,
            stats=stats,
            url=str(url),
        )
        if query.page > 1:
            url.args["page"] = query.page - 1
            response.prev_url = str(url)
        if query.limit * query.page < stats.entity_count:
            url.args["page"] = query.page + 1
            response.next_url = str(url)
        return response


class AggregationResponse(BaseModel):
    total: int
    stats: DatasetStats
    query: ViewQueryParams
    url: str
    aggregations: Aggregations

    @classmethod
    def from_view(
        cls,
        request: Request,
        stats: DatasetStats,
        aggregations: AggregatorResult,
        authenticated: bool | None = False,
    ) -> Self:
        query = ViewQueryParams.from_request(request, authenticated)
        url = furl(str(request.url))
        query_data = clean_dict(query.model_dump())
        query_data.pop("schema_", None)
        url.args.update(query_data)

        # FIXME reverse aggregations ?
        agg_data = defaultdict(dict)
        for func, agg in aggregations.items():
            for field, value in agg.items():
                agg_data[field][func] = value

        return cls(
            total=stats.entity_count,
            query=query,
            stats=stats,
            aggregations=agg_data,
            url=str(url),
        )


class DatasetResponse(Dataset):
    entities_url: str | None = None

    @classmethod
    def from_dataset(cls, request: Request, dataset: Dataset) -> Self:
        return cls(
            **dataset.model_dump(),
            entities_url=f"{request.base_url}entities?dataset={dataset.name}",
        )


class CatalogResponse(Catalog):
    datasets: list[DatasetResponse]

    @classmethod
    def from_catalog(cls, request: Request, catalog: Catalog) -> Self:
        return cls(
            datasets=[
                DatasetResponse.from_dataset(request, d) for d in catalog.datasets
            ],
        )
