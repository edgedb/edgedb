
from .data_ops import *
from typing import *


def is_nominal_subtype_in_schema(subtype: str, supertype: str, dbschema: DBSchema):
    # TODO: properly implement
    return subtype == supertype
