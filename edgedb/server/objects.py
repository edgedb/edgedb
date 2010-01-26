class MetaError(Exception):
    pass


class MetaMismatchError(Exception):
    pass


class MetaObject(object):
    @classmethod
    def get_canonical_class(cls):
        return cls


class GraphObjectMetaData(object):
    def __init__(self, name=None):
        self.name = name


class GraphObject(MetaObject):
    pass


class Node(GraphObject):
    pass


class Atom(Node):
    pass


class Concept(Node):
    pass


class Link(GraphObject):
    pass


def get_safe_attrname(name, reserved):
    name = str(name)
    while name in reserved:
        name += '_'
    return name
