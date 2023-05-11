from collections import defaultdict
from functools import cached_property
from typing import Any, Generator, Iterable, Literal

from banal import as_bool, clean_dict, ensure_dict, ensure_list, is_listish
from fastapi import HTTPException, Request
from followthemoney.model import registry
from followthemoney.util import join_text
from normality import collapse_spaces, normalize, slugify
from pydantic import BaseModel, Field, validator

from . import constants, settings


class QueryParams(BaseModel):
    limit: int | None = settings.DEFAULT_LIMIT
    page: int | None = 1
    schema_: Literal[tuple(constants.SCHEMATA)] | None = Field(
        None,
        example="LegalEntity",
        alias="schema",
    )
    order_by: str | None = Field(None, example="-date")
    prop: str | None = Field(None, example="country")
    value: str | None = Field(None, example="de")

    @validator("prop")
    def validate_prop(cls, prop: str | None) -> bool:
        if prop is not None:
            if prop == "reverse":
                return prop
            if prop.startswith("context."):
                return prop
            if prop not in constants.PROPERTIES:
                raise HTTPException(400, detail=[f"Invalid ftm property: `{prop}`"])
        return prop

    @validator("schema_")
    def validate_schema(cls, value: str | None) -> bool:
        if value is not None and value not in constants.SCHEMATA:
            raise HTTPException(400, detail=[f"Invalid ftm schema: `{value}`"])
        return value


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


class ExtraQueryParams(QueryParams):
    class Config:
        extra = "allow"

    def __init__(self, **data):
        data.pop("api_key", None)
        data = {k: v for k, v in data.items() if k not in AggregationParams.__fields__}
        super().__init__(**data)

    @classmethod
    def from_request(
        cls, request: Request, authenticated: bool | None = False
    ) -> "ExtraQueryParams":
        params = QueryParams(**request.query_params)
        if not authenticated and params.limit > settings.DEFAULT_LIMIT:
            params.limit = settings.DEFAULT_LIMIT
        return cls(**{**request.query_params, **params.dict()})


class Query:
    """
    An attempt for a simple ftm-dsl that allows filtering by entity properties.
    Props are extracted on query time via sqlite json implementation, are type
    casted (for numeric property types) and the resulting queries are always
    parameterized.

    This class initializes a query that can be chained:

    Example:
        q = Query("ftm_collection")
        q = q.where(country="en").order_by("-name")

    This results in:
        SELECT id, schema, entity,
            json_extract(entity, '$.properties.country') AS country,
            json_extract(entity, '$.properties.name') AS name
        FROM ftm_collection
        WHERE EXISTS (SELECT 1 FROM json_each(name) WHERE value = ?)
        ORDER BY name DESC
    """

    META_FIELDS = {"id", "schema", "entity", "datasets", "referents", "reverse"}
    OPERATORS = {
        "like": "LIKE",
        "ilike": "LIKE",  # FIXME sqlite COLLATE NOCASE
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
        "in": "IN",
        "null": "IS",
        "not": "<>",
    }
    SELECT_FIELDS = ["t.id", "t.schema", "t.entity"]

    def __init__(
        self,
        table: str,
        order_by_fields: Iterable[str] | None = None,
        order_direction: str | None = "ASC",
        limit: int | None = None,
        offset: int | None = None,
        where_lookup: dict | None = None,
    ):
        self.table = table
        self.order_by_fields = order_by_fields
        self.order_direction = order_direction
        self.limit = limit
        self.offset = offset
        self.where_lookup = where_lookup

    def __str__(self) -> str:
        return self.to_str()

    def _chain(self, **kwargs):
        # merge current state
        new_kwargs = self.__dict__.copy()
        new_kwargs.pop("lookups", None)  # cached prop
        new_kwargs.pop("search_lookups", None)  # cached prop
        for key, new_value in kwargs.items():
            old_value = new_kwargs[key]
            if old_value is None:
                new_kwargs[key] = new_value
            # "remove" old value:
            elif new_value is None:
                new_kwargs[key] = None
            # overwrite order by
            elif key == "order_by_fields":
                new_kwargs[key] = new_value
            # combine iterables and dicts
            elif is_listish(old_value):
                new_kwargs[key] = sorted(set(old_value) | set(new_value))
            elif isinstance(old_value, dict):
                new_kwargs[key] = {**old_value, **new_value}
            else:  # replace
                new_kwargs[key] = new_value
        return self.__class__(**new_kwargs)

    def where(self, **filters) -> "Query":
        return self._chain(where_lookup=filters)

    def order_by(self, *fields, ascending=True) -> "Query":
        return self._chain(
            order_by_fields=fields, order_direction="ASC" if ascending else "DESC"
        )

    # for slicing
    def __getitem__(self, value) -> "Query":
        if isinstance(value, int):
            if value < 0:
                raise HTTPException(
                    400, detail=["Invalid slicing: slice must not be negative."]
                )
            return self._chain(limit=1, offset=value)
        if isinstance(value, slice):
            if value.step is not None:
                raise HTTPException(400, detail=["Invalid slicing: steps not allowed."])
            offset = value.start or 0
            if value.stop is not None:
                return self._chain(limit=value.stop - offset, offset=offset)
            return self._chain(offset=offset, limit=-1)
        raise NotImplementedError

    @cached_property
    def lookups(self) -> set[tuple[str, Any]]:
        """
        WHERE lookups for ftm properties (name="foo") will be rewritten as

        SELECT ...
        json_extract(entity, '$.properties.name') AS name
        ...
        WHERE EXISTS (SELECT 1 FROM json_each(name) WHERE value = 'foo')
        """
        parts = set()
        lookup = clean_dict(self.where_lookup)
        if not lookup:
            return parts

        def _get_part(key: str, p_value: str, operator: str | None = None) -> str:
            operator = operator or "="
            if key == "reverse":
                return f"EXISTS (SELECT 1 FROM json_each(r.value) WHERE value {operator} {p_value})"
            if key in constants.PROPERTIES or key.endswith("[]"):
                return f"EXISTS (SELECT 1 FROM json_each({self.slugify(key)}) WHERE {self.cast('value', key)} {operator} {p_value})"
            key = self.slugify(key)
            if key in self.META_FIELDS:
                key = f"t.{key}"
            return f"{key} {operator} {p_value}"

        for field, value in lookup.items():
            field, *operator = field.split("__")

            if not field.startswith("context."):
                if field.rstrip("[]") not in self.META_FIELDS | constants.PROPERTIES:
                    if field not in constants.PROPERTIES:
                        raise HTTPException(
                            400, detail=[f"Lookup `{field}`: Invalid FtM property."]
                        )
                    raise HTTPException(
                        400, detail=[f"Lookup `{field}` not any of {self.META_FIELDS}"]
                    )

            p_value = "?"
            sql_operator = "="

            if operator:
                if len(operator) > 1:
                    raise HTTPException(400, detail=[f"Invalid operator: {operator}"])
                operator = operator[0]
                if operator not in self.OPERATORS:
                    raise HTTPException(400, detail=[f"Invalid operator: {operator}"])

                if operator == "in":
                    # q.where(field__in=["a", "b"])
                    value = tuple(ensure_list(value))
                    p_value = ", ".join("?" * len(value))
                    p_value = f"({p_value})"

                if operator == "null":
                    # q.where(field__null=True|False)
                    p_value = "NULL" if as_bool(value) else "NOT NULL"
                    value = None

                sql_operator = self.OPERATORS[operator]

            part = _get_part(field, p_value, sql_operator), value
            parts.add(part)
        return parts

    def get_json_lookup_field(self, field: str) -> tuple[str, str] | tuple[None, None]:
        # return selector, alias
        field = field.split("__", 1)[0]
        alias = self.slugify(field)
        if field not in self.META_FIELDS:
            if field not in constants.PROPERTIES:
                field = field.rstrip("[]")
                if field.startswith("context."):
                    field = field[8:]
                return f"json_extract(t.entity, '$.{field}')", alias
            return f"json_extract(t.entity, '$.properties.{field}')", alias
        return None, None

    def get_json_select_fields(self) -> Generator[str, None, None]:
        for field in ensure_dict(self.where_lookup):
            field, alias = self.get_json_lookup_field(field)
            if field is not None:
                yield f"{field} AS {alias}"
        for field in ensure_list(self.order_by_fields):
            field, alias = self.get_json_lookup_field(field)
            if field is not None:
                yield f"{field} AS {alias}"

    @property
    def select_part(self) -> str:
        return join_text(
            *self.SELECT_FIELDS, *sorted(set(self.get_json_select_fields())), sep=", "
        )

    @property
    def from_part(self) -> str:
        if "reverse" in ensure_dict(self.where_lookup):
            return f"{self.table} t, json_each(t.entity, '$.properties') r"
        return f"{self.table} t"

    @property
    def where_part(self) -> str:
        if not self.where_lookup:
            return
        return "WHERE " + " AND ".join(f"({p[0]})" for p in sorted(self.lookups))

    @property
    def order_part(self) -> str:
        if self.order_by_fields is None:
            return
        fields = (self.cast(self.slugify(f), f) for f in self.order_by_fields)
        return "ORDER BY " + ", ".join(fields) + " " + self.order_direction

    @property
    def limit_part(self) -> str:
        if self.limit is None:
            return
        offset = self.offset or 0
        if self.limit < -1:
            raise HTTPException(
                400, detail=[f"Limit {self.limit} must not be negative"]
            )
        return f"LIMIT {self.limit} OFFSET {offset}"

    @property
    def parameters(self):
        # get sorted values to replace in parameterized query
        for _, value in sorted(self.lookups):
            if is_listish(value):
                yield from value
            elif value is not None:
                yield value

    def to_str(self) -> str:
        rest = join_text(
            self.where_part,
            self.order_part,
            self.limit_part,
        )
        q = f"SELECT {self.select_part} FROM {self.from_part}"
        if rest is not None:
            q = q + " " + rest
        return q

    @property
    def count(self) -> str:
        q = self._chain(limit=None, offset=None, order_by_fields=None)
        return f"SELECT COUNT(*) FROM ({q.to_str()})"

    @classmethod
    def from_params(cls, table: str, params: ExtraQueryParams) -> "Query":
        q = cls(table)[(params.page - 1) * params.limit : params.page * params.limit]
        if params.order_by:
            q = q.order_by(params.order_by)
            if params.order_by.startswith("-"):
                params.order_by = params.order_by.lstrip("-")
                q = q.order_by(params.order_by, ascending=False)
        if params.schema_:
            q = q.where(schema=params.schema_)
        if params.prop and params.value:
            q = q.where(**{params.prop: params.value})
        extra = clean_dict({k: v for k, v in params if k not in params.__fields__})
        extra = {
            k: v
            for k, v in extra.items()
            if k not in ("api_key", "nested", "dehydrate", "dehydrate_nested", "q")
        }
        if extra:
            q = q.where(**extra)

        return q

    @staticmethod
    def slugify(field: str) -> str:
        return slugify(field, sep="_")

    @staticmethod
    def cast(field: str, prop: str) -> str:
        # FIXME cast array elements!?
        if constants.PROPERTY_TYPES.get(prop) == registry.number:
            if field == "value":
                return f"CAST({field} AS NUMERIC)"
            return f"CAST(json_extract({field}, '$[0]') AS NUMERIC)"
        return field


class SearchQuery(Query):
    """
    Look up search term in fts table and join with original query
    """

    def __init__(self, *args, term: str | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.term = term

    @property
    def parameters(self):
        yield from super().parameters
        if self.term:
            yield self.clean_term(self.term)

    def to_str(self) -> str:
        if not self.term:
            return super().to_str()

        lookup = "s.text MATCH ?"
        if self.where_part:
            where_part = self.where_part + f" AND {lookup}"
        else:
            where_part = f"WHERE {lookup}"

        q = f"""SELECT {self.select_part} FROM {self.table}__fts s
        LEFT JOIN {self.table} t ON s.id = t.id
        {where_part}
        {self.order_part or 'ORDER BY s.rank'}
        {self.limit_part or ''}
        """
        return collapse_spaces(q)

    @staticmethod
    def clean_term(value: str) -> str:
        # FIXME sqlite FTS Match errors on special characters like . - '
        return normalize(value, lowercase=False)


class AggregationQuery(SearchQuery):
    def __init__(self, *args, aggregations: AggregationParams | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.aggregations = aggregations

    @classmethod
    def from_params(
        cls, table: str, params: ExtraQueryParams, aggregations=AggregationParams
    ) -> "AggregationQuery":
        q = super(AggregationQuery, cls).from_params(table, params)
        q.aggregations = aggregations
        return q

    def get_agg_queries(self) -> Generator[tuple[str, str, str], None, None]:
        if self.aggregations is None:
            return

        self.limit = None
        self.offset = None
        self.order_by = None
        # FIXME this is not performance efficient as we could group multiple
        # aggregations for one field together into 1 query
        inner = super().to_str()
        aggregations = self.aggregations.inverse()
        for field, funcs in aggregations.items():
            json_lookup, _ = self.get_json_lookup_field(field)
            for func in funcs:
                select_part = f"{func.upper()}(value) AS agg{func.title()}"
                yield field, func, f"SELECT {select_part} FROM ({inner}) t, json_each({json_lookup})"
