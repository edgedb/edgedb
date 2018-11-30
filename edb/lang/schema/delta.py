#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2008-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import collections.abc

from edb.lang.common import adapter
from edb.lang import edgeql
from edb.lang.edgeql import ast as qlast

from edb.lang.common import markup, ordered, struct, typed

from . import expr as s_expr
from . import objects as so
from . import ordering as s_ordering
from . import utils


def delta_schemas(schema1, schema2):
    from . import modules, database
    from . import derivable

    result = database.AlterDatabase()

    my_modules = set(schema1.modules)
    other_modules = set(schema2.modules)

    added_modules = my_modules - other_modules
    dropped_modules = other_modules - my_modules

    for added_module in added_modules:
        create = modules.CreateModule(classname=added_module)
        create.add(AlterObjectProperty(property='name', old_value=None,
                                       new_value=added_module))
        result.add(create)

    global_adds_mods = []
    global_dels = []

    for type in s_ordering.get_global_dep_order():
        new = s_ordering.sort_objects(
            schema1, schema1.get_objects(type=type))

        old = s_ordering.sort_objects(
            schema2, schema2.get_objects(type=type))

        if issubclass(type, derivable.DerivableObject):
            new = filter(lambda i: i.generic(schema1), new)
            old = filter(lambda i: i.generic(schema2), old)

        adds_mods, dels = so.Object._delta_sets(
            old, new, old_schema=schema2, new_schema=schema1)

        global_adds_mods.append(adds_mods)
        global_dels.append(dels)

    for add_mod in global_adds_mods:
        result.update(add_mod)

    for dels in reversed(global_dels):
        result.update(dels)

    for dropped_module in dropped_modules:
        result.add(modules.DeleteModule(classname=dropped_module))

    return result


def delta_module(schema1, schema2, modname):
    from . import database
    from . import derivable

    result = database.AlterDatabase()

    module2 = schema2.get(modname, None)

    global_adds_mods = []
    global_dels = []

    for type in s_ordering.get_global_dep_order():
        new = s_ordering.sort_objects(
            schema1, schema1.get_objects(modules=[modname], type=type))

        if module2 is not None:
            old = s_ordering.sort_objects(
                schema2, schema2.get_objects(modules=[modname], type=type))
        else:
            old = set()

        if issubclass(type, derivable.DerivableObject):
            new = filter(lambda i: i.generic(schema1), new)
            old = filter(lambda i: i.generic(schema2), old)

        adds_mods, dels = so.Object._delta_sets(
            old, new, old_schema=schema2, new_schema=schema1)

        global_adds_mods.append(adds_mods)
        global_dels.append(dels)

    for add_mod in global_adds_mods:
        result.update(add_mod)

    for dels in reversed(global_dels):
        result.update(dels)

    return result


class CommandMeta(adapter.Adapter, struct.MixedStructMeta,
                  markup.MarkupCapableMeta):

    _astnode_map = {}

    def __new__(mcls, name, bases, dct, *, context_class=None, **kwargs):
        cls = super().__new__(mcls, name, bases, dct, **kwargs)

        if context_class is not None:
            cls._context_class = context_class

        return cls

    def __init__(cls, name, bases, clsdict, *, adapts=None, **kwargs):
        adapter.Adapter.__init__(cls, name, bases, clsdict, adapts=adapts)
        struct.MixedStructMeta.__init__(cls, name, bases, clsdict)
        astnodes = clsdict.get('astnode')
        if astnodes and not isinstance(astnodes, (list, tuple)):
            astnodes = [astnodes]
        if astnodes:
            cls.register_astnodes(astnodes)

    def register_astnodes(cls, astnodes):
        mapping = type(cls)._astnode_map

        for astnode in astnodes:
            existing = mapping.get(astnode)
            if existing:
                msg = ('duplicate EdgeQL AST node to command mapping: ' +
                       '{!r} is already declared for {!r}')
                raise TypeError(msg.format(astnode, existing))

            mapping[astnode] = cls


_void = object()


class Command(struct.MixedStruct, metaclass=CommandMeta):
    """Abstract base class for all delta commands."""

    source_context = struct.Field(object, default=None)

    _context_class = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ops = ordered.OrderedSet()
        self.before_ops = ordered.OrderedSet()

    def copy(self):
        result = super().copy()
        result.ops = ordered.OrderedSet(
            op.copy() for op in self.ops)
        result.before_ops = ordered.OrderedSet(
            op.copy() for op in self.before_ops)
        return result

    @classmethod
    def adapt(cls, obj):
        result = obj.copy_with_class(cls)
        for op in obj:
            result.ops.add(type(cls).adapt(op))
        return result

    def _resolve_type_ref(self, ref, schema):
        return utils.resolve_typeref(ref, schema)

    def _resolve_attr_value(self, value, fname, field, schema):
        ftype = field.type

        if isinstance(ftype, so.ObjectMeta):
            value = self._resolve_type_ref(value, schema)

        elif issubclass(ftype, typed.AbstractTypedMapping):
            if issubclass(ftype.valuetype, so.Object):
                vals = {}

                for k, val in value.items():
                    vals[k] = self._resolve_type_ref(val, schema)

                value = ftype(vals)

        elif issubclass(ftype, (typed.AbstractTypedSequence,
                                typed.AbstractTypedSet)):
            if issubclass(ftype.type, so.Object):
                vals = []

                for val in value:
                    vals.append(self._resolve_type_ref(val, schema))

                value = ftype(vals)

        else:
            value = field.coerce_value(schema, value)

        return value

    def get_struct_properties(self, schema):
        result = {}
        metaclass = self.get_schema_metaclass()

        for op in self.get_subcommands(type=AlterObjectProperty):
            try:
                field = metaclass.get_field(op.property)
            except KeyError:
                continue

            result[op.property] = self._resolve_attr_value(
                op.new_value, op.property, field, schema)

        return result

    def get_attribute_value(self, attr_name):
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                return op.new_value
        else:
            return None

    def set_attribute_value(self, attr_name, value):
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                op.new_value = value
                break
        else:
            self.add(AlterObjectProperty(property=attr_name, new_value=value))

    def discard_attribute(self, attr_name):
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                self.discard(op)
                return

    def __iter__(self):
        for op in self.ops:
            yield from op.before_ops
            yield op

    def get_subcommands(self, *, type=None):
        if type is not None:
            return filter(lambda i: isinstance(i, type), self)
        else:
            return list(self)

    def has_subcommands(self):
        return bool(self.ops)

    def after(self, command):
        if isinstance(command, CommandGroup):
            for op in command:
                self.before_ops.add(command)
        else:
            self.before_ops.add(command)

    def prepend(self, command):
        if isinstance(command, CommandGroup):
            for op in reversed(command):
                self.ops.add(op, last=False)
        else:
            self.ops.add(command, last=False)

    def add(self, command):
        if isinstance(command, CommandGroup):
            self.update(command)
        else:
            self.ops.add(command)

    def update(self, commands):
        for command in commands:
            self.add(command)

    def discard(self, command):
        self.ops.discard(command)

    def sort_subcommands_by_type(self):
        def _key(c):
            if isinstance(c, CreateObject):
                return 0
            else:
                return 2

        self.ops = ordered.OrderedSet(sorted(self.ops, key=_key))

    def apply(self, schema, context):
        return schema, None

    def get_ast(self, schema, context):
        if context is None:
            context = CommandContext()

        with self.new_context(schema, context):
            return self._get_ast(schema, context)

    @classmethod
    def command_for_ast_node(cls, astnode, schema, context):
        cmdcls = type(cls)._astnode_map.get(type(astnode))
        if hasattr(cmdcls, '_command_for_ast_node'):
            # Delegate the choice of command class to the specific command.
            cmdcls = cmdcls._command_for_ast_node(astnode, schema, context)

        return cmdcls

    @classmethod
    def from_ast(cls, schema, astnode, *, context=None):
        if context is None:
            context = CommandContext()

        cmdcls = cls.command_for_ast_node(
            astnode, schema=schema, context=context)

        if cmdcls is None:
            msg = 'cannot find command for ast node {!r}'.format(astnode)
            raise TypeError(msg)

        return cmdcls._cmd_tree_from_ast(schema, astnode, context)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        cmd = cls._cmd_from_ast(schema, astnode, context)
        cmd.source_context = astnode.context

        if getattr(astnode, 'commands', None):
            context_class = cls.get_context_class()

            if context_class is not None:
                with context(context_class(schema, cmd, scls=None)):
                    for subastnode in astnode.commands:
                        subcmd = Command.from_ast(
                            schema, subastnode, context=context)
                        if subcmd is not None:
                            cmd.add(subcmd)
            else:
                for subastnode in astnode.commands:
                    subcmd = Command.from_ast(
                        schema, subastnode, context=context)
                    if subcmd is not None:
                        cmd.add(subcmd)

        return cmd

    @classmethod
    def _cmd_from_ast(cls, schema, astnode, context):
        return cls()

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=str(self))

        for dd in self:
            if isinstance(dd, AlterObjectProperty):
                diff = markup.elements.doc.ValueDiff(
                    before=repr(dd.old_value), after=repr(dd.new_value))

                node.add_child(label=dd.property, node=diff)
            else:
                node.add_child(node=markup.serialize(dd, ctx=ctx))

        return node

    @classmethod
    def get_context_class(cls):
        return cls._context_class

    def new_context(self, schema, context, scls=_void):
        if context is None:
            context = CommandContext()

        if scls is _void:
            scls = getattr(self, 'scls', None)

        context_class = self.get_context_class()
        return context(context_class(schema, self, scls))

    def __str__(self):
        return struct.MixedStruct.__str__(self)

    def __repr__(self):
        flds = struct.MixedStruct.__repr__(self)
        return '<{}.{}{}>'.format(self.__class__.__module__,
                                  self.__class__.__name__,
                                  (' ' + flds) if flds else '')


class CommandList(typed.TypedList, type=Command):
    pass


class CommandGroup(Command):
    def apply(self, schema, context=None):
        for op in self:
            schema, _ = op.apply(schema, context)
        return schema, None


class CommandContextToken:
    def __init__(self, op):
        self.op = op
        self.unresolved_refs = {}


class CommandContextWrapper:
    def __init__(self, context, token):
        self.context = context
        self.token = token

    def __enter__(self):
        self.context.push(self.token)
        return self.token

    def __exit__(self, exc_type, exc_value, traceback):
        self.context.pop()


class CommandContext:
    def __init__(self, *, declarative=False):
        self.stack = []
        self._cache = {}
        self.declarative = declarative
        self.schema = None
        self.modaliases = {}
        self.stdmode = False
        self.testmode = False

    def push(self, token):
        self.stack.append(token)

    def pop(self):
        return self.stack.pop()

    def get(self, cls):
        if issubclass(cls, Command):
            cls = cls.get_context_class()

        for item in reversed(self.stack):
            if isinstance(item, cls):
                return item

    def top(self):
        if self.stack:
            return self.stack[0]
        else:
            return None

    def current(self):
        if self.stack:
            return self.stack[-1]
        else:
            return None

    def parent(self):
        if len(self.stack) > 1:
            return self.stack[-2]
        else:
            return None

    def copy(self):
        ctx = CommandContext()
        ctx.stack = self.stack[:]
        return ctx

    def at_top(self):
        ctx = CommandContext()
        ctx.stack = ctx.stack[:1]
        return ctx

    def cache_value(self, key, value):
        self._cache[key] = value

    def get_cached(self, key):
        return self._cache.get(key)

    def __call__(self, token):
        return CommandContextWrapper(self, token)


class ObjectCommandMeta(type(Command)):
    _transparent_adapter_subclass = True
    _schema_metaclasses = {}

    def __new__(mcls, name, bases, dct, *, schema_metaclass=None, **kwargs):
        cls = super().__new__(mcls, name, bases, dct, **kwargs)
        if cls.get_adaptee() is not None:
            # This is a command adapter rather than the actual
            # command, so skip the registrations.
            return cls

        if (schema_metaclass is not None or
                not hasattr(cls, '_schema_metaclass')):
            cls._schema_metaclass = schema_metaclass

        delta_action = getattr(cls, '_delta_action', None)
        if cls._schema_metaclass is not None and delta_action is not None:
            key = delta_action, cls._schema_metaclass
            cmdcls = mcls._schema_metaclasses.get(key)
            if cmdcls is not None:
                raise TypeError(
                    f'Action {cls._delta_action!r} for '
                    f'{cls._schema_metaclass} is already claimed by {cmdcls}'
                )
            mcls._schema_metaclasses[key] = cls

        return cls

    @classmethod
    def get_command_class(mcls, cmdtype, schema_metaclass):
        return mcls._schema_metaclasses.get(
            (cmdtype._delta_action, schema_metaclass))

    @classmethod
    def get_command_class_or_die(mcls, cmdtype, schema_metaclass):
        cmdcls = mcls.get_command_class(cmdtype, schema_metaclass)
        if cmdcls is None:
            raise TypeError(f'missing {cmdtype.__name__} implementation '
                            f'for {schema_metaclass.__name__}')
        return cmdcls


class ObjectCommand(Command, metaclass=ObjectCommandMeta):
    """Base class for all Object-related commands."""

    @classmethod
    def get_schema_metaclass(cls):
        if cls._schema_metaclass is None:
            raise TypeError(f'schema metaclass not set for {cls}')
        return cls._schema_metaclass

    def get_subcommands(self, *, type=None, metaclass=None):
        if metaclass is not None:
            return filter(
                lambda i: (isinstance(i, ObjectCommand) and
                           issubclass(i.get_schema_metaclass(), metaclass)),
                self)
        else:
            return super().get_subcommands(type=type)


class ObjectCommandContext(CommandContextToken):
    def __init__(self, schema, op, scls):
        super().__init__(op)
        self.scls = scls
        self.original_schema = schema


class CreateObject(ObjectCommand):
    _delta_action = 'create'

    def _create_begin(self, schema, context):
        schema, props = self._make_constructor_args(schema, context)

        metaclass = self.get_schema_metaclass()
        schema, self.scls = metaclass.create_in_schema(schema, **props)

        if not props.get('id'):
            # Record the generated ID.
            self.add(AlterObjectProperty(
                property='id', new_value=self.scls.id))

        return schema

    def _make_constructor_args(self, schema, context):
        return schema, self.get_struct_properties(schema)

    def _create_innards(self, schema, context):
        return schema

    def _create_finalize(self, schema, context):
        return self.scls.finalize(schema, dctx=context)

    def apply(self, schema, context):
        schema = self._create_begin(schema, context)
        with self.new_context(schema, context):
            schema = self._create_innards(schema, context)
            schema = self._create_finalize(schema, context)
        return schema, self.scls


class AlterObject(ObjectCommand):
    _delta_action = 'alter'


class DeleteObject(ObjectCommand):
    _delta_action = 'delete'


class AlterSpecialObjectProperty(Command):
    astnode = qlast.SetSpecialField

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        return AlterObjectProperty(
            property=astnode.name,
            new_value=astnode.value
        )


class AlterObjectProperty(Command):
    property = struct.Field(str)
    old_value = struct.Field(object, None)
    new_value = struct.Field(object, None)
    source = struct.Field(str, None)

    @classmethod
    def _cmd_tree_from_ast(cls, schema, astnode, context):
        from edb.lang.edgeql import compiler as qlcompiler

        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '::' + propname

        assert '::' not in propname

        if isinstance(astnode, qlast.DropAttributeValue):
            parent_ctx = context.get(CommandContextToken)
            parent_cls = parent_ctx.op.get_schema_metaclass()

            field = parent_cls._fields.get(propname)
            if (field is not None and
                    issubclass(field.type, typed.AbstractTypedCollection)):
                value = field.type()
            else:
                value = None

            return cls(property=propname, new_value=value)

        if astnode.as_expr:
            new_value = s_expr.ExpressionText(
                edgeql.generate_source(astnode.value, pretty=False))
        else:
            if isinstance(astnode.value, qlast.BaseConstant):
                new_value = qlcompiler.evaluate_ast_to_python_val(
                    astnode.value, schema=schema)

            elif isinstance(astnode.value, qlast.Tuple):
                new_value = tuple(
                    qlcompiler.evaluate_ast_to_python_val(
                        el.value, schema=schema)
                    for el in astnode.value.elements
                )

            elif isinstance(astnode.value, qlast.ObjectRef):

                new_value = utils.ast_objref_to_objref(
                    astnode.value, modaliases=context.modaliases,
                    schema=schema)

            else:
                raise ValueError(
                    f'unexpected value in attribute: {astnode.value!r}')

        return cls(property=propname, new_value=new_value)

    def _get_ast(self, schema, context):
        value = self.new_value

        new_value_empty = \
            (value is None or
                (isinstance(value, collections.abc.Container) and not value))

        old_value_empty = \
            (self.old_value is None or
                (isinstance(self.old_value, collections.abc.Container) and
                    not self.old_value))

        if new_value_empty and not old_value_empty:
            op = qlast.DropAttributeValue(
                name=qlast.ObjectRef(module='', name=self.property))
            return op

        if new_value_empty and old_value_empty:
            return

        if isinstance(value, s_expr.ExpressionText):
            value = edgeql.parse(str(value))
        elif utils.is_nontrivial_container(value):
            value = qlast.Tuple(elements=[
                qlast.BaseConstant.from_python(el) for el in value
            ])
        else:
            value = qlast.BaseConstant.from_python(value)

        as_expr = isinstance(value, qlast.ExpressionText)
        op = qlast.CreateAttributeValue(
            name=qlast.ObjectRef(module='', name=self.property),
            value=value, as_expr=as_expr)
        return op

    def __repr__(self):
        return '<%s.%s "%s":"%s"->"%s">' % (
            self.__class__.__module__, self.__class__.__name__,
            self.property, self.old_value, self.new_value)
