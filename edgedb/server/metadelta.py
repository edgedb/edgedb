##
# Copyright (c) 2008-2010 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix import caos
from semantix.caos import proto


def metadelta(oldmeta, newmeta):
    result = []

    for type in ('atom', 'concept', 'link'):
        for obj in newmeta(type, include_builtin=True, include_automatic=True):
            result.append((None, obj))

    return result
