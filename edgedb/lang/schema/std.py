##
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import os.path

from edgedb.lang import caosql

from . import ddl as s_ddl
from . import schema as s_schema


def load_std_schema():
    schema = s_schema.ProtoSchema()

    std_eql_f = os.path.join(os.path.dirname(__file__), '_std.eql')
    with open(std_eql_f) as f:
        std_eql = f.read()
    std_d = s_ddl.delta_from_ddl(caosql.parse_block(std_eql))
    std_d.apply(schema)

    return schema
