##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os
import postgresql.python.os

from semantix.utils.config import ConfigurableMeta, cvalue, ConfigError

from . import ast
from . import codegen
from . import common


class Config(metaclass=ConfigurableMeta):
    pg_config_path = cvalue(default=None, type=str, doc='Path to PostgreSQL pg_config executable.')
    pg_config_path_env = cvalue(default='PGINSTALLATION', type=str,
                                doc='The name of the environment variable holding pg_config path.')

    @classmethod
    def get_pg_config_path(cls):
        pg_config_path = os.environ.get(cls.pg_config_path_env)
        if not pg_config_path:
            pg_config_path = cls.pg_config_path
        if not pg_config_path:
            pg_config_path = postgresql.python.os.find_executable('pg_config')
        if not pg_config_path:
            hint = 'Set "{env}" environment variable or "{conf}" configuration variable to pg_config path'\
                   .format(env=cls.pg_config_path_env,
                           conf=cls.__module__ + '.' + cls.__name__ + '.pg_config_path')
            raise ConfigError('could not find PostgreSQL installation', hint=hint)
        return pg_config_path

