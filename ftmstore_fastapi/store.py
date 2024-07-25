from collections.abc import AsyncGenerator
from functools import cache, lru_cache
from typing import TYPE_CHECKING, Literal

from fastapi import HTTPException
from ftmq.dedupe import get_resolver
from ftmq.model import Catalog, Dataset, DatasetStats
from ftmq.query import Q
from ftmq.store import Store
from ftmq.store import get_store as _get_store
from ftmq.store.sql import AggregatorResult
from ftmq.types import CE
from ftmq.util import get_dehydrated_proxy, get_featured_proxy

from ftmstore_fastapi.logging import get_logger
from ftmstore_fastapi.settings import CATALOG, FTM_STORE_URI, RESOLVER

if TYPE_CHECKING:
    from ftmstore_fastapi.views import RetrieveParams

log = get_logger(__name__)


@cache
def get_catalog(uri: str | None = CATALOG) -> Catalog:
    uri = uri or CATALOG
    if uri is not None:
        return Catalog._from_uri(uri)
    return Catalog()


@cache
def get_dataset(name: str, catalog: Catalog | None = None) -> Dataset:
    catalog = catalog or get_catalog()
    dataset = catalog.get(name)
    if dataset is None:
        raise HTTPException(404, detail=[f"Dataset `{name}` not found."])
    return dataset


@cache
def get_store(
    dataset: str | None = None,
    catalog_uri: str | None = None,
    resolver_uri: str | None = None,
) -> Store:
    catalog = get_catalog(catalog_uri or CATALOG)
    resolver = get_resolver(resolver_uri or RESOLVER)
    if dataset is not None:
        dataset = get_dataset(dataset, catalog)
        store = _get_store(
            catalog=catalog, dataset=dataset, uri=FTM_STORE_URI, linker=resolver
        )
    else:
        store = _get_store(catalog=catalog, uri=FTM_STORE_URI, linker=resolver)
    return store


class View:
    def __init__(
        self,
        dataset: str | None = None,
        catalog_uri: str | None = None,
        resolver_uri: str | None = None,
    ) -> None:
        self.store = get_store(dataset, catalog_uri, resolver_uri)
        self.dataset = dataset
        self.query = self.store.query()
        self.view = self.store.default_view()

    async def stats(self, *args, **kwargs) -> DatasetStats:
        return self.query.stats(*args, **kwargs)

    async def aggregations(self, *args, **kwargs) -> AggregatorResult:
        return self.query.aggregations(*args, **kwargs)

    async def get_adjacents(self, *args, **kwargs) -> set[CE]:
        return self.query.get_adjacents(*args, **kwargs)

    async def get_adjacent(self, *args, **kwargs) -> AsyncGenerator:
        for res in self.view.get_adjacent(*args, **kwargs):
            yield res

    async def get_entity(self, entity_id: str, params: "RetrieveParams") -> CE | None:
        canonical = self.store.linker.get_canonical(entity_id)
        proxy = get_cached_entity(self.view, canonical)
        if proxy is None:
            raise HTTPException(404, detail=[f"Entity `{entity_id}` not found."])
        if params.dehydrate:
            return get_dehydrated_proxy(proxy)
        if params.featured:
            return get_featured_proxy(proxy)
        return proxy

    async def get_entities(
        self, query: Q, params: "RetrieveParams"
    ) -> AsyncGenerator[CE, None]:
        for proxy in self.query.entities(query):
            if params.dehydrate:
                proxy = get_dehydrated_proxy(proxy)
            elif params.featured:
                proxy = get_featured_proxy(proxy)
            yield proxy


@cache
def get_view(
    dataset: str | None = None,
    catalog_uri: str | None = None,
    resolver_uri: str | None = None,
) -> View:
    return View(dataset, catalog_uri, resolver_uri)


@lru_cache(10_000)
def get_cached_entity(view: View, entity_id: str) -> CE:
    return view.get_entity(entity_id)


# cache at boot time
catalog = get_catalog()
Datasets = Literal[tuple(catalog.names)]
