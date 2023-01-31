# from enum import Enum
# FIXME make real enum

from followthemoney import model

SCHEMATA = set(model.schemata.keys())
PROPERTIES = set([p.name for p in model.properties])
PROPERTY_TYPES = {p.name: p.type for p in model.properties}
