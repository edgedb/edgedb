##
# Copyright (c) 2011 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import linecache

from semantix.utils.datastructures import Field
from semantix.utils.datastructures import typed
from . import base


class LangMarkup(base.Markup, ns='lang'):
    pass


class Number(LangMarkup):
    num = Field(str, default=None, coerce=True)


class String(LangMarkup):
    str = Field(str, default=None, coerce=True)


class Ref(LangMarkup):
    ref = Field(int, coerce=True)
    refname = Field(str, default=None)

    def __repr__(self):
        return '<{} {} {}>'.format('Ref', self.refname, self.ref)


class BaseObject(LangMarkup):
    """Base language object with ``id``, but without ``attributes``"""
    id = Field(int, default=None, coerce=True)


class Object(BaseObject):
    class_module = Field(str)
    class_name = Field(str)
    repr = Field(str, default=None)
    attributes = Field(base.MarkupMapping, default=None, coerce=True)


class List(BaseObject):
    items = Field(base.MarkupList, default=base.MarkupList, coerce=True)
    trimmed = Field(bool, default=False)


class Dict(BaseObject):
    items = Field(base.MarkupMapping, default=base.MarkupMapping, coerce=True)
    trimmed = Field(bool, default=False)


class TreeNodeChild(BaseObject):
    label = Field(str, default=None)
    node = Field(base.Markup)

class TreeNodeChildrenList(typed.TypedList, type=TreeNodeChild):
    pass

class TreeNode(BaseObject):
    name = Field(str)
    children = Field(TreeNodeChildrenList, default=TreeNodeChildrenList, coerce=True)

    def add_child(self, *, label=None, node):
        self.children.append(TreeNodeChild(label=label, node=node))


class NoneConstantType(LangMarkup):
    pass

class TrueConstantType(LangMarkup):
    pass

class FalseConstantType(LangMarkup):
    pass

class Constants:
    none = NoneConstantType()
    true = TrueConstantType()
    false = FalseConstantType()


class TracebackPoint(BaseObject):
    name = Field(str)
    filename = Field(str)
    lineno = Field(int)
    colno = Field(int, default=None)

    lines = Field(typed.StrList, default=None, coerce=True)
    line_numbers = Field(typed.IntList, default=None, coerce=True)

    locals = Field(Dict, default=None)

    def load_source(self, window=3, lines=None):
        self.lines = self.line_numbers = None

        if self.lineno and \
                        ((self.filename and not self.filename.startswith('<') \
                                                and not self.filename.endswith('>')) \
                         or lines):

            lineno = self.lineno

            sourcelines = lines or linecache.getlines(self.filename, globals())

            lines = []
            line_numbers = []

            start = max(1, lineno - window)
            end = min(len(sourcelines), lineno + window) + 1
            for i in range(start, end):
                lines.append(sourcelines[i - 1].rstrip())
                line_numbers.append(i)

            if lines:
                self.lines = typed.StrList(lines)
                self.line_numbers = typed.IntList(line_numbers)


class TracebackPointList(typed.TypedList, type=TracebackPoint):
    pass


class Traceback(BaseObject):
    items = Field(TracebackPointList, default=TracebackPointList, coerce=True)


class ExceptionContext(BaseObject):
    title = Field(str, default='Context')
    body = Field(base.MarkupList, coerce=True)


class ExceptionContextList(typed.TypedList, type=ExceptionContext):
    pass

class _Exception(Object):
    pass

class Exception(_Exception):
    msg = Field(str)

    # NB: Traceback is just an exception context
    #
    contexts = Field(ExceptionContextList, default=None, coerce=True)

    context = Field(_Exception, None)
    cause = Field(_Exception, None)
