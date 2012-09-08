##
# Copyright (c) 2008-2012 Sprymix Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import yaml
import importlib
import collections
import copy
from string import Template

from semantix.utils.lang import meta as lang_base
from semantix.utils.lang import context as lang_context
from semantix.utils.lang.import_ import module as module_types


class Composer(yaml.composer.Composer):
    def compose_document(self):
        start_document = self.get_event()

        node = self.compose_node(None, None)

        schema = getattr(start_document, 'schema', None)
        if schema:
            module, obj = schema.rsplit('.', 1)
            module = importlib.import_module(module)
            schema = getattr(module, obj)
            node = schema().check(node)
            node.import_context = schema().get_import_context_class()

            if node.import_context and \
                    not isinstance(self.document_context.import_context, node.import_context):
                self.document_context.import_context = \
                            node.import_context.from_parent(self.document_context.import_context,
                                                            self.document_context.import_context)

        node.schema = schema
        node.document_name = getattr(start_document, 'document_name', None)
        node.imports = getattr(start_document, 'imports', None)
        if self.document_context is not None:
            self.document_context.document_name = node.document_name

        self.get_event()
        self.anchors = {}
        return node


class Constructor(yaml.constructor.Constructor):
    def __init__(self, context=None):
        super().__init__()
        self.document_context = context
        self.obj_data = {}

    def _get_class_from_tag(self, clsname, node, intent='class'):
        if not clsname:
            raise yaml.constructor.ConstructorError("while constructing a Python %s" % intent, node.start_mark,
                                                    "expected non-empty class name appended to the tag", node.start_mark)

        module_name, class_name = clsname.rsplit('.', 1)

        try:
            module = importlib.import_module(module_name)
            result = getattr(module, class_name)
        except (ImportError, AttributeError) as exc:
            raise yaml.constructor.ConstructorError("while constructing a Python %s" % intent, node.start_mark,
                                                    "could not find %r (%s)" % (clsname, exc), node.start_mark)

        return result

    def _get_source_context(self, node, document_context):
        start = lang_context.SourcePoint(node.start_mark.line, node.start_mark.column,
                                               node.start_mark.pointer)
        end = lang_context.SourcePoint(node.end_mark.line, node.end_mark.column,
                                               node.end_mark.pointer)

        context = lang_context.SourceContext(node.start_mark.name, node.start_mark.buffer,
                                                   start, end, document_context)
        return context

    def construct_document(self, node):
        if node.imports:
            for module_name, alias in node.imports.items():
                try:
                    if node.import_context:
                        parent_context = self.document_context.import_context
                        module_name = node.import_context.from_parent(module_name, parent=parent_context)
                    mod = importlib.import_module(module_name)
                except ImportError as e:
                    raise yaml.constructor.ConstructorError(None, None, '%r' % e, node.start_mark) from e

                if not alias:
                    alias = mod.__name__
                self.document_context.imports[alias] = module_types.ModuleInfo(mod)

        return super().construct_document(node)

    def construct_python_class(self, parent, node):
        cls = self._get_class_from_tag(parent, node, 'class')
        name = getattr(node, 'document_name', cls.__name__ + '_' + str(id(node)))

        # Call correct class constructor with __prepare__ method
        #

        try:
            import_context = self.document_context.import_context
        except AttributeError:
            cls_module = cls.__module__
        else:
            cls_module = import_context


        result = type(cls)(name, (cls,), type(cls).__prepare__(name, (cls,)))

        result.__module__ = cls_module

        nodecopy = copy.copy(node)
        nodecopy.tags = copy.copy(nodecopy.tags)
        nodecopy.tag = nodecopy.tags.pop()

        data = self.construct_object(nodecopy)

        context = self._get_source_context(node, self.document_context)
        result.prepare_class(context, data)

        yield result

    def construct_python_object(self, classname, node):
        cls = self._get_class_from_tag(classname, node, 'object')
        if not issubclass(cls, lang_base.Object):
            raise yaml.constructor.ConstructorError(
                    "while constructing a Python object", node.start_mark,
                    "expected %s to be a subclass of semantix.utils.lang.meta.Object" % classname, node.start_mark)

        context = self._get_source_context(node, self.document_context)

        nodecopy = copy.copy(node)
        nodecopy.tags = copy.copy(nodecopy.tags)
        nodecopy.tag = nodecopy.tags.pop()

        data = self.construct_object(nodecopy, True)

        newargs = ()
        newkwargs = {}

        try:
            getnewargs = cls.__sx_getnewargs__
        except AttributeError:
            pass
        else:
            newargs = getnewargs(context, data)
            if not isinstance(newargs, tuple):
                newargs, newkwargs = newargs['args'], newargs['kwargs']

        result = cls.__new__(cls, *newargs, **newkwargs)
        lang_context.SourceContext.register_object(result, context)

        yield result

        try:
            constructor = type(result).__sx_setstate__
        except AttributeError:
            pass
        else:
            constructor(result, data)

    def construct_ordered_mapping(self, node, deep=False):
        if isinstance(node, yaml.nodes.MappingNode):
            self.flatten_mapping(node)

        if not isinstance(node, yaml.nodes.MappingNode):
            raise yaml.constructor.ConstructorError(None, None,
                    "expected a mapping node, but found %s" % node.id,
                    node.start_mark)

        mapping = []
        for key_node, value_node in node.value:
            key = self.construct_object(key_node, deep=deep)
            if not isinstance(key, collections.Hashable):
                raise yaml.constructor.ConstructorError("while constructing a mapping",
                                                        node.start_mark,
                                                        "found unhashable key", key_node.start_mark)
            value = self.construct_object(value_node, deep=deep)
            mapping.append((key, value))

        return mapping

    def construct_ordered_map(self, node):
        data = collections.OrderedDict()
        yield data
        value = self.construct_ordered_mapping(node)
        data.update(value)

    def construct_mapseq(self, node):
        data = []
        yield data
        value = self.construct_ordered_mapping(node)
        data.extend(value)


Constructor.add_multi_constructor(
    'tag:semantix.sprymix.com,2009/semantix/class/derive:',
    Constructor.construct_python_class
)

Constructor.add_multi_constructor(
    'tag:semantix.sprymix.com,2009/semantix/object/create:',
    Constructor.construct_python_object
)

Constructor.add_constructor(
    'tag:semantix.sprymix.com,2009/semantix/orderedmap',
    Constructor.construct_ordered_map
)

Constructor.add_constructor(
    'tag:semantix.sprymix.com,2009/semantix/mapseq',
    Constructor.construct_mapseq
)

Constructor.add_constructor(
    '!tpl',
    lambda loader, node: Template(node.value)
)
