from typing import Any

from banal import clean_dict
from fastapi import HTTPException, Request
from ftmq.aggregations import Aggregator
from ftmq.enums import Properties, Schemata
from ftmq.query import Query as _Query
from pydantic import BaseModel, Field, validator

from ftmstore_fastapi import settings


class RetrieveParams(BaseModel):
    nested: bool
    featured: bool
    dehydrate: bool
    dehydrate_nested: bool


class AggregationParams(BaseModel):
    aggSum: list[str] | None = []
    aggMin: list[str] | None = []
    aggMax: list[str] | None = []
    aggAvg: list[str] | None = []


class QueryParams(BaseModel):
    limit: int | None = settings.DEFAULT_LIMIT
    page: int | None = 1
    schema_: Schemata | None = Field(
        None,
        example="LegalEntity",
        alias="schema",
    )
    order_by: str | None = Field(None, example="-date")
    prop: str | None = Field(None, example="country")
    value: str | None = Field(None, example="de")

    class Config:
        allow_population_by_field_name = True

    @validator("prop")
    def validate_prop(cls, prop: str | None) -> bool:
        if prop is not None:
            if prop == "reverse":
                return prop
            if prop not in Properties:
                raise HTTPException(400, detail=[f"Invalid ftm property: `{prop}`"])
        return prop

    @validator("schema_")
    def validate_schema(cls, value: str | None) -> bool:
        if value is not None and value not in Schemata:
            raise HTTPException(400, detail=[f"Invalid ftm schema: `{value}`"])
        return value

    def to_where_lookup_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.dict().items() if v and k not in META_FIELDS}


META_FIELDS = (
    set(AggregationParams.__fields__)
    | set(RetrieveParams.__fields__)  # noqa: W503
    | set(QueryParams.__fields__) - {"prop", "value", "operator"}  # noqa: W503
)


class ViewQueryParams(QueryParams):
    class Config:
        extra = "allow"
        allow_population_by_field_name = True

    def __init__(self, **data):
        data.pop("api_key", None)
        super().__init__(**data)

    @classmethod
    def from_request(
        cls, request: Request, authenticated: bool | None = False
    ) -> "ViewQueryParams":
        params = cls(**request.query_params)
        if not authenticated and params.limit > settings.DEFAULT_LIMIT:
            params.limit = settings.DEFAULT_LIMIT
        return params

    def to_aggregator(self) -> Aggregator:
        data = clean_dict(
            {
                k[3:].lower(): getattr(self, k, None)
                for k in AggregationParams.__fields__
            }
        )
        return Aggregator.from_dict(data)


class Query(_Query):
    @classmethod
    def from_params(
        cls: "Query", params: ViewQueryParams, dataset: str | None = None
    ) -> "Query":
        q = cls()[(params.page - 1) * params.limit : params.page * params.limit]
        if dataset is not None:
            q = q.where(dataset=dataset)
        if params.order_by:
            ascending = True
            if params.order_by.startswith("-"):
                ascending = False
                params.order_by = params.order_by.lstrip("-")
            q = q.order_by(params.order_by, ascending=ascending)
        if params.schema_:
            q = q.where(schema=params.schema_)
        q = q.where(**params.to_where_lookup_dict())
        aggregator = params.to_aggregator()
        q.aggregations = aggregator.aggregations
        return q


class SearchQuery(_Query):
    pass
