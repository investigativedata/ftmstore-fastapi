"""
serialization data models as seen in
https://github.com/opensanctions/yente/
"""

from typing import Any, Union

from banal import clean_dict, ensure_dict
from fastapi import Request
from followthemoney.model import registry
from furl import furl
from nomenklatura.entity import CE
from pydantic import BaseModel, Field

from .dataset import DataCatalog, Dataset, Entities
from .query import ExtraQueryParams

EntityProperties = dict[str, list[Union[str, "EntityResponse"]]]


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
            caption=entity.caption,
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
            url=str(url),
        )
        if query.page > 1:
            url.args["page"] = query.page - 1
            response.prev_url = str(url)
        if query.limit * query.page < total:
            url.args["page"] = query.page + 1
            response.next_url = str(url)
        return response


class DatasetResponse(BaseModel):
    name: str
    title: str
    summary: str | None = None
    url: str | None = None
    load: bool | None = False
    entities_url: str | None = None
    version: str | None = "1"
    children: list[str]

    @classmethod
    def from_dataset(cls, dataset: Dataset) -> "DatasetResponse":
        return cls(**dataset.to_dict())


class DataCatalogResponse(BaseModel):
    datasets: list[DatasetResponse]

    @classmethod
    def from_catalog(cls, catalog: DataCatalog) -> "DataCatalogResponse":
        return cls(datasets=[DatasetResponse.from_dataset(c) for c in catalog.datasets])
