##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.value import value
from .base import HEAD, ConfigRootNode
from .tree import *


__all__ = 'cvalue',


class cvalue(value):
    __slots__ = ('_owner',)

    def __init__(self, *args, abstract=False, inherit=False, **kwargs):
        super().__init__(*args, **kwargs)
        self._owner = None

    def _set_name(self, name):
        self._name = name
        CvalueRootNode._set_value(CVALUES, name, self, None)

    def _get_value(self):
        if self._name is None:
            raise ValueError('Unable to get value on uninitialized cvalue: ' \
                             'no name set {!r}'.format(self))

        conf = HEAD.get()
        while conf:
            try:
                value = ConfigRootNode._get_value(conf.node, self._name)
            except AttributeError:
                conf = conf.parent
            else:
                return value.value

        return super()._get_value()


class CvalueContainer(TreeValue):
    __slots__ = ()


class CvalueNode(TreeNode):
    __slots__ = ()


class CvalueRootNode(CvalueNode, TreeRootNode):
    __slots__ = ()

    node_cls = CvalueNode
    value_cls = CvalueContainer


CVALUES = CvalueRootNode('CVALUES')
