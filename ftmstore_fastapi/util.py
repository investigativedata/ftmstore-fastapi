from functools import cache
from typing import Any

import pycountry
from followthemoney import model
from followthemoney.proxy import E
from nomenklatura.entity import CE, CompositeEntity


def uplevel(proxy: E) -> CE:
    return CompositeEntity.from_dict(model, proxy.to_dict())


def get_proxy(data: dict[str, Any]) -> CE:
    return CompositeEntity.from_dict(model, data)


def get_proxy_caption(proxy: CE) -> str:
    # FIXME
    if proxy.caption != proxy.schema.label:
        return proxy.caption
    for prop in proxy.schema.caption:
        for value in proxy.get(prop):
            return value
    return proxy.schema.label


def get_dehydrated_proxy(
    data: dict[str, Any] | E | CE, include_context: bool = True
) -> CE:
    """
    reduce proxy payload to only include 1 name (caption)
    and optionally context
    """
    proxy = get_proxy(data)
    caption = get_proxy_caption(proxy)
    dehydrated = get_proxy(
        {"id": proxy.id, "schema": proxy.schema.name, "caption": caption}
    )
    if include_context:
        dehydrated.datasets = proxy.datasets
        dehydrated.referents = proxy.referents
        dehydrated.context = proxy.context
    return dehydrated


def get_featured_proxy(
    data: dict[str, Any] | E | CE, include_context: bool = True
) -> CE:
    """
    reduce proxy payload to only include featured properties and optionally context
    """
    proxy = get_proxy(data)
    featured = get_dehydrated_proxy(data)
    if include_context:
        featured.datasets = proxy.datasets
        featured.referents = proxy.referents
        featured.context = proxy.context
    for prop in proxy.schema.featured:
        featured.add(prop, proxy.get(prop))
    return featured


@cache
def get_country_name(alpha2: str) -> str:
    try:
        country = pycountry.countries.get(alpha_2=alpha2.lower())
        return country.name
    except (LookupError, AttributeError):
        return
