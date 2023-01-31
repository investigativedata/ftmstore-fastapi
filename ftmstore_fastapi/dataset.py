import sqlite3
from typing import Any, Generator, TypeVar

import orjson
from fastapi import HTTPException
from followthemoney.model import registry
from followthemoney.util import join_text
from ftmstore import get_dataset
from nomenklatura.dataset import DataCatalog
from nomenklatura.dataset import Dataset as NKDataset
from nomenklatura.entity import CE

from .logging import get_logger
from .query import Query
from .settings import IN_MEMORY, PRELOAD_DATASETS
from .util import get_dehydrated_proxy, get_proxy, uplevel

DS = TypeVar("DS", bound="Dataset")
Entities = Generator[CE, None, None]


class Dataset(NKDataset):
    def __init__(
        self,
        catalog: DataCatalog[DS] | None,
        data: dict[str, Any],
    ) -> None:
        data["title"] = data.get("title", data["name"].title())
        super().__init__(catalog, data)
        self.log = get_logger(__name__, dataset=str(self))
        self._connection = None
        self._loaded = False
        if PRELOAD_DATASETS:
            _ = self.connection

    def get(
        self,
        entity_id: str,
        nested: bool | None = False,
        dehydrate: bool = False,
        dehydrate_nested: bool = True,
    ) -> CE | None:
        q = Query(self.name).where(id=entity_id)
        for proxy in self.get_entities(
            q, nested=nested, dehydrate=dehydrate, dehydrate_nested=dehydrate_nested
        ):
            return proxy
        raise HTTPException(404, detail=f"Entity with ID `{entity_id}` not found")

    def get_entities(
        self,
        query: Query,
        nested: bool | None = False,
        dehydrate: bool = False,
        dehydrate_nested: bool = True,
    ) -> Entities:
        for id_, schema, entity, *rest in self.connection.execute(
            str(query), tuple(query.parameters)
        ):
            data = orjson.loads(entity)
            data["id"] = id_
            if dehydrate:
                proxy = get_dehydrated_proxy(data)
            else:
                proxy = get_proxy(data)
            if nested:
                proxy = self.nested(proxy, dehydrate_nested)
            yield proxy

    def get_count(self, query: Query) -> int:
        for i, *rest in self.connection.execute(query.count, tuple(query.parameters)):
            return i

    def nested(self, proxy: CE, dehydrate: bool = False) -> CE:
        q = Query(self.name).where(id__in=proxy.get_type_values(registry.entity))
        proxy.context["adjacents"] = {
            p.id: p for p in self.get_entities(q, dehydrate=dehydrate)
        }
        return proxy

    def __str__(self) -> str:
        return self.name

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is not None:
            if self._loaded:
                return self._connection
        connection = self.load()
        self._loaded = True
        self._connection = connection
        return connection

    def load(self) -> sqlite3.Connection:
        """
        load complete dataset into sqlite memory table
        (can be deactivated with settings.IN_MEMORY=false)
        """
        if IN_MEMORY:
            self.log.info("Loading dataset table to memory ...")
            connection = sqlite3.connect("file::memory:?cache=shared", uri=True)
        else:
            fp = f"./ftmstore_fastapi.{self.name}.db"
            self.log.info("Generating dataset table ...", fp=fp)
            connection = sqlite3.connect(fp)

        connection.execute(
            f"CREATE TABLE {self.name} (id TEXT, schema TEXT, entity JSON)"
        )
        connection.execute(f"CREATE INDEX {self.name}__ix ON {self.name}(id)")
        connection.execute(f"CREATE INDEX {self.name}__sx ON {self.name}(schema)")
        connection.execute("PRAGMA mmap_size = 30000000000")
        connection.executemany(
            f"INSERT INTO {self.name} VALUES (?, ?, ?)", self.load_proxies()
        )
        connection.execute(
            f"CREATE VIRTUAL TABLE {self.name}__fts USING fts5(id UNINDEXED, text)"
        )
        connection.executemany(
            f"INSERT INTO {self.name}__fts VALUES (?, ?)", self.load_search()
        )
        connection.commit()
        self.log.info("Done.")
        return connection

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
            if proxy.schema.is_a("Thing"):
                search = join_text(
                    *proxy.names,
                    *proxy.countries,
                    *proxy.get_type_values(registry.identifier),
                )
                yield proxy.id, search
            if ix and ix % 10_000 == 0:
                self.log.info("Loading entity %d ..." % ix)
        self.log.info("Loading entity %d ..." % ix)
