from ftmq.types import CE
from ftmq.util import make_proxy


def get_proxy_caption_property(proxy: CE) -> dict[str, str]:
    for prop in proxy.schema.caption:
        for value in proxy.get(prop):
            return {prop: value}
    return {}


def get_dehydrated_proxy(proxy: CE) -> CE:
    """
    reduce proxy payload to only include caption property
    """
    return make_proxy(
        {
            "id": proxy.id,
            "schema": proxy.schema.name,
            "properties": get_proxy_caption_property(proxy),
            "datasets": proxy.datasets,
        }
    )


def get_featured_proxy(proxy: CE) -> CE:
    """
    reduce proxy payload to only include featured properties
    """
    featured = get_dehydrated_proxy(proxy)
    for prop in proxy.schema.featured:
        featured.add(prop, proxy.get(prop))
    return featured
