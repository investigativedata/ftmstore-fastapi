from collections import defaultdict
from collections.abc import Generator
from typing import Any

from fastapi import HTTPException, Request
from ftmq.enums import Properties, Schemata
from ftmq.query import Query as _Query
from pydantic import BaseModel, Field, validator

from ftmstore_fastapi import settings


class AggregationParams(BaseModel):
    aggSum: list[str] | None = []
    aggMin: list[str] | None = []
    aggMax: list[str] | None = []
    aggAvg: list[str] | None = []

    def inverse(self) -> dict[str, set[str]]:
        aggregations = defaultdict(set)
        for agg_key, fields in self:
            for field in fields:
                aggregations[field].add(agg_key[3:].lower())
        return aggregations


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
        extra = "allow"

    def __init__(self, **data):
        data.pop("api_key", None)
        data = {k: v for k, v in data.items() if k not in AggregationParams.__fields__}
        super().__init__(**data)

    @classmethod
    def from_request(
        cls, request: Request, authenticated: bool | None = False
    ) -> "QueryParams":
        params = cls(**request.query_params)
        if not authenticated and params.limit > settings.DEFAULT_LIMIT:
            params.limit = settings.DEFAULT_LIMIT
        return params

    @validator("prop")
    def validate_prop(cls, prop: str | None) -> bool:
        if prop is not None:
            if prop == "reverse":
                return prop
            if prop.startswith("context."):
                return prop
            if prop not in Properties:
                raise HTTPException(400, detail=[f"Invalid ftm property: `{prop}`"])
        return prop

    @validator("schema_")
    def validate_schema(cls, value: str | None) -> bool:
        if value is not None and value not in Schemata:
            raise HTTPException(400, detail=[f"Invalid ftm schema: `{value}`"])
        return value

    def to_lookup_dict(self) -> dict[str, Any]:
        return {
            k: v
            for k, v in self.dict().items()
            if k not in ("schema_", "order_by", "limit", "page") and v
        }


class Query(_Query):
    @classmethod
    def from_params(cls: "Query", params: QueryParams) -> "Query":
        q = cls()[(params.page - 1) * params.limit : params.page * params.limit]
        if params.order_by:
            ascending = True
            if params.order_by.startswith("-"):
                ascending = False
                params.order_by = params.order_by.lstrip("-")
            q = q.order_by(params.order_by, ascending=ascending)
        if params.schema_:
            q = q.where(schema=params.schema_)
        q = q.where(**params.to_lookup_dict())

        return q

    def agg(self, *args) -> None:
        raise NotImplementedError


class AggregationQuery(Query):
    def __init__(self, *args, aggregations: AggregationParams | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.aggregations = aggregations

    @classmethod
    def from_params(
        cls, params: QueryParams, aggregations=AggregationParams
    ) -> "AggregationQuery":
        q = super().from_params(params)
        q.aggregations = aggregations
        return q

    def get_queries(self) -> Generator[Query, None, None]:
        if self.aggregations is None:
            return

        self.limit = None
        self.offset = None
        self.order_by = None
        # FIXME this is not performance efficient as we could group multiple
        # aggregations for one field together into 1 query
        aggregations = self.aggregations.inverse()
        for field, funcs in aggregations.items():
            for func in funcs:
                yield self.agg(field, func)


class SearchQuery(_Query):
    pass
