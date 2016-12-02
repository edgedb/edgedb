##
# Copyright (c) 2013-present MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##

import uuid

from edgedb.lang.common.persistent_hash import persistent_hash


def test_common_persistent_hash_1():
    assert persistent_hash(1) == persistent_hash(1)
    assert persistent_hash((1, '2')) == persistent_hash((1, '2'))

    u = uuid.uuid4()
    assert persistent_hash(u) != persistent_hash(uuid.uuid4())
    assert persistent_hash(u) != persistent_hash(u.hex)
    assert persistent_hash(u) == persistent_hash(u)
