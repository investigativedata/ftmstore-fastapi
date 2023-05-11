import sqlite3
import threading
from typing import Any, Generator, TypeVar

import orjson
from fastapi import HTTPException
from followthemoney import model
from followthemoney.model import registry
from followthemoney.util import join_text
from ftmstore import get_dataset
from nomenklatura.dataset import DataCatalog
from nomenklatura.dataset import Dataset as NKDataset
from nomenklatura.entity import CE
from pydantic import BaseModel

from .logging import get_logger
from .query import AggregationQuery, Query
from .settings import DATASETS_STATS, IN_MEMORY, INDEX_PROPERTIES
from .util import (
    get_country_name,
    get_dehydrated_proxy,
    get_featured_proxy,
    get_proxy,
    get_proxy_caption,
    uplevel,
)

DS = TypeVar("DS", bound="Dataset")
Entities = Generator[CE, None, None]


class Country(BaseModel):
    code: str
    count: int
    label: str | None

    def __init__(self, **data):
        data["label"] = get_country_name(data["code"])
        super().__init__(**data)


class Schema(BaseModel):
    name: str
    count: int
    label: str
    plural: str

    def __init__(self, **data):
        schema = model.get(data["name"])
        data["label"] = schema.label
        data["plural"] = schema.plural
        super().__init__(**data)


class Things(BaseModel):
    total: int | None = None
    schemata: list[Schema] | None = []
    countries: list[Country] | None = []


class Dataset(NKDataset):
    def __init__(
        self,
        catalog: DataCatalog[DS] | None,
        data: dict[str, Any],
    ) -> None:
        data["title"] = data.get("title", data["name"].title())
        super().__init__(catalog, data)
        self.log = get_logger(__name__, dataset=str(self))
        self._connections = {}
        self._lock = threading.RLock()
        self._things = Things()
        self.load()
        if DATASETS_STATS:
            self._things = self.get_stats()

    def get_entity(
        self,
        entity_id: str,
        nested: bool | None = False,
        featured: bool | None = False,
        dehydrate: bool = False,
        dehydrate_nested: bool = True,
    ) -> CE | None:
        """
        retrieve a single entity from the store based on its ID
        """
        q = Query(self.name).where(id=entity_id)
        for proxy in self.get_entities(
            q,
            nested=nested,
            featured=featured,
            dehydrate=dehydrate,
            dehydrate_nested=dehydrate_nested,
        ):
            return proxy
        # look up merged entities
        q = Query(self.name).where(**{"context.referents[]": entity_id})
        for proxy in self.get_entities(q):
            return proxy
        raise HTTPException(404, detail=[f"Entity with ID `{entity_id}` not found"])

    def get_entities(
        self,
        query: Query,
        nested: bool | None = False,
        featured: bool | None = False,
        dehydrate: bool = False,
        dehydrate_nested: bool = True,
    ) -> Entities:
        """
        retrieve multiple entities from the store based on filter criteria
        """
        for id_, schema, entity, *rest in self.connection.execute(
            str(query), tuple(query.parameters)
        ):
            data = orjson.loads(entity)
            data["id"] = id_
            if dehydrate:
                proxy = get_dehydrated_proxy(data)
            elif featured:
                proxy = get_featured_proxy(data)
            else:
                proxy = get_proxy(data)
            if nested:
                proxy = self.nest(proxy, dehydrate_nested)
            yield proxy

    def get_aggregations(
        self, query: AggregationQuery
    ) -> Generator[tuple[str, str, str], None, None]:
        for field, func, agg_query in query.get_agg_queries():
            for res in self.connection.execute(agg_query, tuple(query.parameters)):
                yield field, func, res[0]

    def get_count(self, query: Query) -> int:
        for i, *rest in self.connection.execute(query.count, tuple(query.parameters)):
            return i

    def get_schemata_groups(self, query: Query) -> dict[str, int]:
        res = self.connection.execute(query.schemata, tuple(query.parameters))
        return dict([x for x in res])

    def nest(self, proxy: CE, dehydrate: bool = False) -> CE:
        q = Query(self.name).where(id__in=proxy.get_type_values(registry.entity))
        proxy.context["adjacents"] = {
            p.id: p for p in self.get_entities(q, dehydrate=dehydrate)
        }
        return proxy

    def __str__(self) -> str:
        return self.name

    def to_dict(self) -> dict[str, Any]:
        data = super().to_dict()
        data["things"] = self._things.dict()
        return data

    @property
    def connection(self) -> sqlite3.Connection:
        with self._lock:
            tid = threading.get_ident()
            if tid not in self._connections:
                self._connections[tid] = self.get_connection()
            return self._connections[tid]

    def get_connection(self) -> sqlite3.Connection:
        if IN_MEMORY:
            return sqlite3.connect("file::memory:?cache=shared", uri=True)
        else:
            fp = f"./ftmstore_fastapi.{self.name}.db"
            return sqlite3.connect(fp)

    def load(self):
        """
        load complete dataset into sqlite memory table
        (can be deactivated with settings.IN_MEMORY=false)
        """
        if IN_MEMORY:
            self.log.info("Loading dataset table to memory ...")
        else:
            fp = f"./ftmstore_fastapi.{self.name}.db"
            self.log.info("Generating dataset table ...", fp=fp)

        self.connection.execute(
            f"CREATE TABLE {self.name} (id TEXT, schema TEXT, entity JSON)"
        )
        self.connection.execute(f"CREATE INDEX {self.name}__ix ON {self.name}(id)")
        self.connection.execute(f"CREATE INDEX {self.name}__sx ON {self.name}(schema)")
        self.connection.execute("PRAGMA mmap_size = 30000000000")
        self.connection.executemany(
            f"INSERT INTO {self.name} VALUES (?, ?, ?)", self.load_proxies()
        )
        self.connection.execute(
            f"CREATE VIRTUAL TABLE {self.name}__fts USING fts5(id UNINDEXED, text)"
        )
        self.connection.executemany(
            f"INSERT INTO {self.name}__fts VALUES (?, ?)", self.load_search()
        )
        self.connection.commit()
        self.log.info("Done.")

    def load_proxies(self) -> Entities:
        self.log.info("Loading entities ...")
        store = get_dataset(self.name)
        ix = 0
        for ix, proxy in enumerate(store.iterate()):
            proxy = uplevel(proxy)
            yield (
                proxy.id,
                proxy.schema.name,
                orjson.dumps(proxy.to_dict()).decode(),
            )
            if ix and ix % 10_000 == 0:
                self.log.info("Loading entity %d ..." % ix)
        self.log.info("Loading entity %d ..." % ix)

    def load_search(self) -> Generator[tuple[str, str, str], None, None]:
        self.log.info("Loading search data ...")
        store = get_dataset(self.name)
        ix = 0
        for ix, proxy in enumerate(store.iterate()):
            txt = set([get_proxy_caption(proxy), *proxy.names])
            props = []
            for prop in proxy.schema.featured + INDEX_PROPERTIES:
                if proxy.has(prop, quiet=True):
                    props.extend(proxy.get(prop))
            search = join_text(
                *txt,
                *props,
                *proxy.countries,
                *proxy.get_type_values(registry.identifier),
            )
            yield proxy.id, search
            if ix and ix % 10_000 == 0:
                self.log.info("Loading entity %d ..." % ix)
        self.log.info("Loading entity %d ..." % ix)

    def get_stats(self) -> Things:
        self.log.info("Generating statistics ...")
        things = Things()
        for res in self.connection.execute(f"SELECT COUNT(*) FROM {self.name}"):
            things.total = res[0]
        for name, count in self.connection.execute(
            f"SELECT schema, COUNT(id) FROM {self.name} GROUP BY schema"
        ):
            things.schemata.append(Schema(name=name, count=count))
        # FIXME this only works for sqlite:
        for code, count in self.connection.execute(
            f"""SELECT value, COUNT(DISTINCT {self.name}.id)
            FROM {self.name}, json_each(json_extract(entity, '$.properties.country'))
            GROUP BY value"""
        ):
            things.countries.append(Country(code=code, count=count))
        return things
