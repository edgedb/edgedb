##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.slots import SlotsMeta


__all__ = 'TreeValue', 'TreeNode', 'TreeRootNode'


class TreeValue(metaclass=SlotsMeta):
    __slots__ = 'name', 'value', 'context'

    def __init__(self, name, value, context=None):
        self.name = name
        self.value = value
        self.context = context


class TreeNode(metaclass=SlotsMeta):
    __slots__ = '__node_name__', '__node_children__'

    def __init__(self, name):
        self.__node_name__ = name
        self.__node_children__ = None

    def __setattr__(self, name, value):
        if name.startswith('__node_'):
            return object.__setattr__(self, name, value)

        if self.__node_children__ is None:
            self.__node_children__ = {}

        self.__node_children__[name] = value

    def __getattribute__(self, name):
        if name.startswith('__node_') or name in ('__dict__', '__bases__', '__class__', '__enter__', '__exit__'):
            return object.__getattribute__(self, name)

        return_container = False
        if name[0] == '~':
            name = name[1:]
            return_container = True

        if self.__node_children__ is None:
            raise AttributeError(name)

        try:
            obj = self.__node_children__[name]
        except KeyError:
            raise AttributeError(name)

        if isinstance(obj, TreeValue):
            if return_container:
                return obj
            else:
                return obj.value
        else:
            return obj


class TreeRootNode(TreeNode):
    __slots__ = ()

    node_cls = TreeNode
    value_cls = TreeValue

    @classmethod
    def _set_value(cls, rootnode, fullname, value, context):
        assert isinstance(rootnode, cls)

        parts = fullname.split('.')
        value_name = parts[-1]
        node = rootnode

        for part in parts[:-1]:
            try:
                node = getattr(node, part)
            except AttributeError:
                new = cls.node_cls(node.__node_name__ + '.' + part)
                setattr(node, part, new)
                node = new
            else:
                if isinstance(node, cls.value_cls):
                    raise ValueError('Overlapping configs: {}.{}'.format(node.__node_name__, part))

        try:
            getattr(node, value_name)
        except AttributeError:
            setattr(node, value_name, cls.value_cls(value_name, value, context))
        else:
            raise ValueError('Overlapping config values: {}'.format(fullname))

    @classmethod
    def _get_value(cls, rootnode, fullname):
        assert isinstance(rootnode, cls)

        node = rootnode
        *parts, value_name = fullname.split('.')

        for part in parts:
            node = getattr(node, part)

        value = getattr(node, '~' + value_name)
        if not isinstance(value, cls.value_cls):
            raise ValueError('Overlapping config value: {}'.format(fullname))

        return value
