##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.config import configurable, cvalue

from . import ast
from . import codegen
from . import common


@configurable
class Config:
    pg_install_path = cvalue('/usr/share/postgresql-%(version)s', type=str,
                             doc='Path to PostgreSQL installation')
