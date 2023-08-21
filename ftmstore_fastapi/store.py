from functools import cache

import orjson
from fastapi import HTTPException
from ftmstore import Store as FtmStore
from ftmstore import settings
from nomenklatura.dataset import DataCatalog

from .dataset import Dataset
from .logging import get_logger
from .settings import CATALOG

log = get_logger(__name__)


class Store(FtmStore):
    def get_catalog(self):
        datasets = [
            {"name": d.name, "title": d.name.replace("_", " ").title()}
            for d in self.all()
        ]
        return DataCatalog(Dataset, {"datasets": datasets})


@cache
def get_store(
    database_uri: str | None = None, prefix: str | None = None, **config
) -> Store:
    return Store(
        database_uri or settings.DATABASE_URI,
        prefix or settings.DATABASE_PREFIX,
        **config,
    )


@cache
def get_catalog() -> DataCatalog:
    if CATALOG is not None:
        with open(CATALOG) as f:
            data = orjson.loads(f.read())
        return DataCatalog(Dataset, data)
    store = get_store()
    return store.get_catalog()


@cache
def get_dataset(name: str) -> Dataset:
    catalog = get_catalog()
    dataset = catalog.get(name)
    if dataset is None:
        raise HTTPException(404, detail=[f"Dataset `{name}` not found."])
    return dataset
