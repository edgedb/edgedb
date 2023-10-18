##
# Copyright (c) 2015-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

from __future__ import annotations
import pathlib

from . import start as start  # noqa

def get_spec_filepath():
    "Returns an absolute path to the serialized grammar spec file"

    edgeql_dir = pathlib.Path(__file__).parent.parent.parent
    return str(edgeql_dir / 'grammar.bc')
