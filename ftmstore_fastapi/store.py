from functools import cache
from typing import TYPE_CHECKING, Literal

from fastapi import HTTPException
from ftmq.model import Catalog, Dataset
from ftmq.query import Q
from ftmq.store import Store
from ftmq.store import get_store as _get_store
from ftmq.types import CE, CEGenerator

from ftmstore_fastapi.logging import get_logger
from ftmstore_fastapi.settings import CATALOG, FTM_STORE_URI
from ftmstore_fastapi.util import get_dehydrated_proxy, get_featured_proxy

if TYPE_CHECKING:
    from ftmstore_fastapi.views import RetrieveParams

log = get_logger(__name__)


@cache
def get_catalog(uri: str | None = CATALOG) -> Catalog:
    uri = uri or CATALOG
    if uri is not None:
        return Catalog.from_uri(uri)
    return Catalog()


@cache
def get_dataset(name: str, catalog: Catalog | None = None) -> Dataset:
    catalog = catalog or get_catalog()
    dataset = catalog.get(name)
    if dataset is None:
        raise HTTPException(404, detail=[f"Dataset `{name}` not found."])
    return dataset


@cache
def get_store(dataset: str | None = None, catalog_uri: str | None = None) -> Store:
    catalog = get_catalog(catalog_uri)
    if dataset is not None:
        dataset = get_dataset(dataset, catalog)
        return _get_store(catalog=catalog, dataset=dataset, uri=FTM_STORE_URI)
    return _get_store(catalog=catalog, uri=FTM_STORE_URI)


class View:
    def __init__(
        self, dataset: str | None = None, catalog_uri: str | None = None
    ) -> None:
        store = get_store(dataset, catalog_uri)
        self.dataset = dataset
        self.query = store.query()
        self.view = store.default_view()

        self.coverage = self.query.coverage
        self.aggregations = self.query.aggregations
        self.get_adjacents = self.query.get_adjacents

    def get_entity(self, entity_id: str, params: "RetrieveParams") -> CE | None:
        proxy = self.view.get_entity(entity_id)
        if params.dehydrate:
            return get_dehydrated_proxy(proxy)
        if params.featured:
            return get_featured_proxy(proxy)
        return proxy

    def get_entities(self, query: Q, params: "RetrieveParams") -> CEGenerator:
        for proxy in self.query.entities(query):
            if params.dehydrate:
                proxy = get_dehydrated_proxy(proxy)
            elif params.featured:
                proxy = get_featured_proxy(proxy)
            yield proxy


@cache
def get_view(dataset: str | None = None, catalog_uri: str | None = None) -> View:
    return View(dataset, catalog_uri)


# cache at boot time
catalog = get_catalog()
Datasets = Literal[tuple(catalog.names)]
