##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


from semantix.utils.datastructures.typed import TypedDict, TypedList
from semantix.utils.json import Encoder as JsonEncoder, dumps as json_dumps, dumpb as json_dumpb
from ..elements.base import Markup


class Encoder(JsonEncoder):
    """Serializes markup to JSON.

    Format: to minimize the encoded JSON string we pack markup objects in
    a special way.  ``Markup`` objects serialize to lists, with the first
    element set to number ``0``, second to a markup class id, third to the
    class mro (short names of parent Markup classes, except Markup), and,
    fourth to markup's fields names. When a markup object is serializing, Encoder
    looks in its ``fields_map`` to get ``class id`` and ``fields`` names
    sequence; if nothing found, it creates a unique ``class id`` and a footprint
    of the markup class' fields.  ``Lists`` serialize to regular lists, but
    with the first element set to number ``1``.  ``Dicts`` serialize as is.
    Finally, encoder serializes to a JSON list its ``fields_map`` and
    serialized markup object: ``[fields_map, markup_sequence]``
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.types = {}
        self.types_cnt = 0;

        self.fields_map = {}

    def default(self, obj):
        if isinstance(obj, Markup):
            cls = obj.__class__
            cls_name = cls._markup_name

            try:
                cls_id, mro, fields = self.fields_map[cls_name]
            except KeyError:
                mro = []
                for parent in cls.__mro__[1:]:
                    if parent is not Markup and issubclass(parent, Markup):
                        mro.append(parent._markup_name)

                desc = self.fields_map[cls_name] = (len(self.fields_map),
                                                    mro,
                                                    tuple(cls.get_fields().keys()))
                cls_id, mro, fields = desc

            result = [0, cls_id]

            for field_name in fields:
                field = getattr(obj, field_name)
                result.append(field)

            return result

        elif isinstance(obj, TypedList):
            if not len(obj):
                return None

            result = list(obj)
            result.insert(0, 1)
            return result

        elif isinstance(obj, TypedDict):
            if not len(obj):
                return None

            return dict(obj)

        return super().default(obj)

    def dumps(self, markup, as_bytes):
        try:
            if as_bytes:
                encoded = super().dumpb(markup)
                table = json_dumpb(self.fields_map)
                return b'[' + table + b',' + encoded + b']'
            else:
                encoded = super().dumps(markup)
                table = json_dumps(self.fields_map)
                return '[' + table + ',' + encoded + ']'
        finally:
            self.fields_map = {}


class Renderer:
    encoder = Encoder

    def _render(self, markup, as_bytes):
        return type(self).encoder().dumps(markup, as_bytes)

    @classmethod
    def render(cls, markup, *, as_bytes=False):
        return cls()._render(markup, as_bytes)


render = Renderer.render
