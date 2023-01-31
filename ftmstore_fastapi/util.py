from typing import Any

from followthemoney import model
from followthemoney.model import registry
from followthemoney.proxy import E
from nomenklatura.entity import CE, CompositeEntity


def uplevel(proxy: E) -> CE:
    return CompositeEntity.from_dict(model, proxy.to_dict())


def get_proxy(data: dict[str, Any]) -> CE:
    return CompositeEntity.from_dict(model, data)


def get_dehydrated_proxy(
    data: dict[str, Any] | E | CE, include_context: bool = True
) -> CE:
    """
    reduce proxy payload to only include 1 name (caption)
    and featured properties
    """
    proxy = get_proxy(data)
    dehydrated = get_proxy({"id": proxy.id, "schema": proxy.schema.name})
    if include_context:
        dehydrated.datasets = proxy.datasets
        dehydrated.referents = proxy.referents
        dehydrated.context = proxy.context
    for prop in proxy.schema.featured:
        prop = proxy.schema.properties[prop]
        if prop.type == registry.name:
            dehydrated.add("name", proxy.caption)
        else:
            dehydrated.add(prop, proxy.get(prop))
    return dehydrated
