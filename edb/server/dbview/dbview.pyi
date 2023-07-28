from typing import *

from edb.schema.extensions import Extension
from edb.schema.schema import FlatSchema
from edb.server.config.ops import SettingValue

if TYPE_CHECKING:
    SettingsMap = Mapping[str, SettingValue]

class Database:
    name: str
    dbver: int
    db_config: SettingsMap
    user_schema: FlatSchema
    reflection_cache: Mapping[Any, Any]
    backend_ids: list[Mapping[Literal['id'], str]]
    extensions: Mapping[str, Extension]
