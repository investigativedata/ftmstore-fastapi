from typing import Annotated, Any, Self

from banal import clean_dict
from fastapi import Query as FastQuery
from fastapi import Request
from ftmq.aggregations import Aggregator
from ftmq.query import Query as _Query
from ftmq.types import Schemata
from pydantic import BaseModel, ConfigDict, Field

from ftmstore_fastapi import settings
from ftmstore_fastapi.store import Datasets


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
    aggCount: list[str] | None = []
    aggGroups: list[str] | None = []


class BaseQueryParams(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    q: Annotated[
        str | None,
        FastQuery(
            description="""Optional search query for name based search
            (on regular endpoints) or full text search
            (via ftmq-search) on /search endpoint"""
        ),
    ] = None
    dataset: Annotated[
        list[Datasets] | None,
        FastQuery(description="One or more dataset names to limit scope to"),
    ] = []
    limit: int | None = settings.DEFAULT_LIMIT
    page: int | None = 1
    schema_: Schemata | None = Field(
        None,
        example="LegalEntity",
        alias="schema",
    )

    def to_where_lookup_dict(self) -> dict[str, Any]:
        return {
            k: v for k, v in self.model_dump().items() if v and k not in META_FIELDS
        }


class QueryParams(BaseQueryParams):
    order_by: str | None = Field(None, example="-date")
    reverse: str | None = Field(None, example="eu-id-1234")


META_FIELDS = (
    set(AggregationParams.model_fields)
    | set(RetrieveParams.model_fields)  # noqa: W503
    | set(QueryParams.model_fields)  # noqa: W503
)

LISTISH_PARAMS = ["dataset", *AggregationParams.model_fields.keys()]


class ViewQueryParams(QueryParams):
    model_config = ConfigDict(populate_by_name=True, extra="allow")

    def __init__(self, **data):
        data.pop("api_key", None)
        super().__init__(**data)

    @classmethod
    def from_request(cls, request: Request, authenticated: bool | None = False) -> Self:
        params = dict(request.query_params)
        # listish params
        for p in LISTISH_PARAMS:
            listish = request.query_params.getlist(p)
            if listish:
                params[p] = listish
        params = cls(**params)
        if not authenticated and params.limit > settings.DEFAULT_LIMIT:
            params.limit = settings.DEFAULT_LIMIT
        return params

    def to_aggregator(self) -> Aggregator:
        data = clean_dict(
            {
                k[3:].lower(): getattr(self, k, None)
                for k in AggregationParams.model_fields
            }
        )
        return Aggregator.from_dict(data)


class Query(_Query):
    @classmethod
    def from_params(cls, params: ViewQueryParams) -> Self:
        q = cls()[(params.page - 1) * params.limit : params.page * params.limit]
        if params.dataset:
            q = q.where(dataset__in=params.dataset)
        if params.schema_:
            q = q.where(schema=params.schema_)
        if params.order_by:
            ascending = True
            if params.order_by.startswith("-"):
                ascending = False
                params.order_by = params.order_by.lstrip("-")
            q = q.order_by(params.order_by, ascending=ascending)
        if params.reverse:
            q = q.where(reverse=params.reverse)
        q = q.where(**params.to_where_lookup_dict())
        if params.q:
            q = q.search(params.q)
        aggregator = params.to_aggregator()
        q.aggregations = aggregator.aggregations
        return q


class SearchQueryParams(BaseQueryParams):
    model_config = ConfigDict(populate_by_name=True)

    country: Annotated[
        list[str] | None,
        FastQuery(description="One or more country codes to limit results to"),
    ] = []

    def __init__(self, **data):
        data.pop("api_key", None)
        super().__init__(**data)

    @classmethod
    def from_request(cls, request: Request, authenticated: bool | None = False) -> Self:
        params = dict(request.query_params)
        # listish params
        for p in ("dataset", "country"):
            listish = request.query_params.getlist(p)
            if listish:
                params[p] = listish
        params = cls(**params)
        if not authenticated and params.limit > settings.DEFAULT_LIMIT:
            params.limit = settings.DEFAULT_LIMIT
        return params


class SearchQuery(_Query):
    @classmethod
    def from_params(cls, params: SearchQueryParams) -> Self:
        q = cls()[(params.page - 1) * params.limit : params.page * params.limit]
        if params.dataset:
            q = q.where(dataset__in=params.dataset)
        if params.schema_:
            q = q.where(schema=params.schema_)
        if params.country:
            q = q.where(country__in=params.country)
        if params.q:
            q = q.search(params.q)
        return q
