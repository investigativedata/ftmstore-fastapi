import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from ftmstore_fastapi.query import Query, QueryParams


def test_query():
    params = QueryParams()
    q = Query.from_params(params)
    assert q.to_dict() == {"limit": 100, "offset": 0}

    params = QueryParams(schema="Event")
    q = Query.from_params(params)
    assert q.to_dict() == {"limit": 100, "offset": 0, "schema": "Event"}

    params = QueryParams(schema="Event", prop="date", value=2023)
    q = Query.from_params(params)
    assert q.to_dict() == {
        "limit": 100,
        "offset": 0,
        "schema": "Event",
        "prop": "date",
        "value": "2023",
    }

    params = QueryParams(
        schema="Event", prop="date", value=2023, order_by="location", page=2, limit=10
    )
    q = Query.from_params(params)
    assert q.to_dict() == {
        "limit": 20,
        "offset": 10,
        "schema": "Event",
        "prop": "date",
        "value": "2023",
        "order_by": ["location"],
    }

    params = QueryParams(
        schema="Event", date__gte=2023, order_by="-location", page=2, limit=10
    )
    q = Query.from_params(params)
    assert q.to_dict() == {
        "limit": 20,
        "offset": 10,
        "schema": "Event",
        "prop": "date",
        "value": {"gte": "2023"},
        "order_by": ["-location"],
    }

    # invalid lookups
    with pytest.raises(ValidationError):
        QueryParams(schema="foo")
    with pytest.raises(HTTPException):
        QueryParams(prop="foo")
