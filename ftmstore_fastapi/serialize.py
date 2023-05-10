"""
serialization data models as seen in
https://github.com/opensanctions/yente/
"""

from collections import defaultdict
from datetime import datetime
from typing import Any, Generator, Union

from banal import clean_dict, ensure_dict
from fastapi import Request
from followthemoney.model import registry
from furl import furl
from nomenklatura.entity import CE
from pydantic import BaseModel, Field

from .dataset import DataCatalog, Dataset, Entities, Things
from .query import ExtraQueryParams
from .util import get_proxy_caption

EntityProperties = dict[str, list[Union[str, "EntityResponse"]]]
Aggregations = dict[str, dict[str, Any]]


class ErrorResponse(BaseModel):
    detail: str = Field(..., example="Detailed error message")


class StatusResponse(BaseModel):
    status: str = "ok"


class EntityResponse(BaseModel):
    id: str = Field(..., example="NK-A7z....")
    caption: str = Field(..., example="John Doe")
    schema_: str = Field(..., example="LegalEntity", alias="schema")
    properties: EntityProperties = Field(..., example={"name": ["John Doe"]})
    datasets: list[str] = Field([], example=["us_ofac_sdn"])
    referents: list[str] = Field([], example=["ofac-1234"])
    context: dict[str, Any] = Field(
        {},
        example={"scope": "beneficiary"},
        description="Some arbitrary extra data for this entity",
    )

    class Config:
        allow_population_by_field_name = True

    @classmethod
    def from_entity(cls, entity: CE) -> "EntityResponse":
        properties = dict(entity.properties)
        adjacents = entity.context.pop("adjacents", None)
        if adjacents:
            adjacents = {k: EntityResponse.from_entity(v) for k, v in adjacents.items()}
            for prop in entity.iterprops():
                if prop.type == registry.entity:
                    properties[prop.name] = [
                        adjacents.get(i, i) for i in entity.get(prop)
                    ]
        return cls(
            id=entity.id,
            caption=get_proxy_caption(entity),
            schema=entity.schema.name,
            properties=properties,
            datasets=list(entity.datasets),
            referents=list(entity.referents),
            context={
                k: v
                for k, v in ensure_dict(entity.context).items()
                if k not in ("datasets", "referents")
            },
        )


EntityResponse.update_forward_refs()


class EntitiesResponse(BaseModel):
    total: int
    items: int
    schemata: dict[str, int]
    query: ExtraQueryParams
    url: str
    next_url: str | None = None
    prev_url: str | None = None
    entities: list[EntityResponse]

    @classmethod
    def from_view(
        cls,
        request: Request,
        entities: Entities,
        total: int,
        schemata: dict[str, int],
        authenticated: bool | None = False,
    ) -> "EntitiesResponse":
        query = ExtraQueryParams.from_request(request, authenticated)
        url = furl(request.url)
        query_data = clean_dict(query.dict())
        query_data.pop("schema_", None)
        url.args.update(query_data)
        entities = [EntityResponse.from_entity(e) for e in entities]
        response = cls(
            total=total,
            items=len(entities),
            query=query,
            entities=entities,
            schemata=schemata,
            url=str(url),
        )
        if query.page > 1:
            url.args["page"] = query.page - 1
            response.prev_url = str(url)
        if query.limit * query.page < total:
            url.args["page"] = query.page + 1
            response.next_url = str(url)
        return response


class AggregationResponse(BaseModel):
    total: int
    query: ExtraQueryParams
    url: str
    aggregations: Aggregations

    @classmethod
    def from_view(
        cls,
        request: Request,
        aggregations: Generator[tuple[str, str, str], None, None],
        total: int,
        authenticated: bool | None = False,
    ) -> "AggregationResponse":
        aggregations_data = defaultdict(dict)
        for field, func, value in aggregations:
            aggregations_data[field][func] = value
        query = ExtraQueryParams.from_request(request, authenticated)
        url = furl(request.url)
        query_data = clean_dict(query.dict())
        query_data.pop("schema_", None)
        url.args.update(query_data)
        return cls(
            total=total, query=query, url=str(url), aggregations=aggregations_data
        )


class Resource(BaseModel):
    name: str
    url: str
    mime_type: str
    mime_type_label: str


class Publisher(BaseModel):
    name: str | None = None
    url: str | None = None
    description: str | None = None
    official: bool | None = None


class DatasetResponse(BaseModel):
    name: str
    title: str
    summary: str | None = None
    url: str | None = None
    publisher: Publisher | None = None
    load: bool | None = False
    entities_url: str | None = None
    version: str | None = "1"
    updated_at: datetime | None = None
    category: str | None = None
    frequency: str | None = None
    resources: list[Resource]
    things: Things
    children: list[str]

    @classmethod
    def from_dataset(cls, request: Request, dataset: Dataset) -> "DatasetResponse":
        return cls(
            **dataset.to_dict(),
            entities_url=f"{request.base_url}{dataset.name}/entities",
        )


class DataCatalogResponse(BaseModel):
    datasets: list[DatasetResponse]
    updated_at: datetime | None = None

    @classmethod
    def from_catalog(
        cls, request: Request, catalog: DataCatalog
    ) -> "DataCatalogResponse":
        return cls(
            updated_at=catalog.updated_at,
            datasets=[
                DatasetResponse.from_dataset(request, c) for c in catalog.datasets
            ],
        )
