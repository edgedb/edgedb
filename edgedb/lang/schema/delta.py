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


import collections
import re

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common import exceptions as base_err
from edgedb.lang.common import debug as edgedb_debug
from edgedb.lang.common import adapter
from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast

from edgedb.lang.common import markup, nlang, ordered, struct, typed
from edgedb.lang.common.persistent_hash import persistent_hash

from . import objects as so
from . import expr as s_expr
from . import utils


def delta_schemas(schema1, schema2, *, include_derived=False):
    from . import modules, objects as so, database

    result = database.AlterDatabase()

    my_modules = set(schema1.modules)
    other_modules = set(schema2.modules)

    added_modules = my_modules - other_modules
    dropped_modules = other_modules - my_modules
    common_modules = my_modules & other_modules

    for added_module in added_modules:
        my_module = schema1.get_module(added_module)

        create = modules.CreateModule(classname=added_module)

        create.add(AlterObjectProperty(property='name', old_value=None,
                                       new_value=added_module))

        create.add(AlterObjectProperty(property='imports', old_value=None,
                                       new_value=tuple(my_module.imports)))
        result.add(create)

    for common_module in common_modules:
        my_module = schema1.get_module(common_module)
        other_module = schema2.get_module(common_module)

        if my_module.imports != other_module.imports:
            alter = modules.AlterModule(classname=common_module)

            alter.add(AlterObjectProperty(
                property='imports',
                old_value=tuple(other_module.imports),
                new_value=tuple(my_module.imports)))
            result.add(alter)

    global_adds_mods = []
    global_dels = []

    for type in schema1.global_dep_order:
        o1 = schema1.get_objects(type=type, include_derived=include_derived)
        new = ordered.OrderedIndex(o1, key=lambda o: o.persistent_hash())
        o2 = schema2.get_objects(type=type, include_derived=include_derived)
        old = ordered.OrderedIndex(o2, key=lambda o: o.persistent_hash())

        if type in ('link', 'link_property', 'constraint'):
            new = filter(lambda i: i.generic(), new)
            old = filter(lambda i: i.generic(), old)

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
    from . import modules, objects as so, database

    result = database.AlterDatabase()

    try:
        module2 = schema2.get_module(modname)
    except KeyError:
        module2 = None
    module1 = schema1.get_module(modname)

    global_adds_mods = []
    global_dels = []

    if module2 is None:
        create = modules.CreateModule(classname=modname)

        create.add(AlterObjectProperty(property='name', old_value=None,
                                       new_value=modname))

        create.add(AlterObjectProperty(property='imports', old_value=None,
                                       new_value=tuple(module1.imports)))
        result.add(create)

    for type in schema1.global_dep_order:
        new = ordered.OrderedIndex(module1.get_objects(type=type),
                                   key=lambda o: o.persistent_hash())
        if module2 is not None:
            old = ordered.OrderedIndex(module2.get_objects(type=type),
                                       key=lambda o: o.persistent_hash())
        else:
            old = ordered.OrderedIndex(key=lambda o: o.persistent_hash())

        if type in ('link', 'link_property', 'constraint'):
            new = filter(lambda i: i.generic(), new)
            old = filter(lambda i: i.generic(), old)

        adds_mods, dels = so.Object._delta_sets(
            old, new, old_schema=schema2, new_schema=schema1)

        global_adds_mods.append(adds_mods)
        global_dels.append(dels)

    for add_mod in global_adds_mods:
        result.update(add_mod)

    for dels in reversed(global_dels):
        result.update(dels)

    return result


class DeltaExceptionSchemaContext(markup.MarkupExceptionContext):
    title = 'Protoschema Diff'

    def __init__(self, schema1, schema2,
                 schema1_title=None, schema2_title=None,
                 schema1_checksums=None, schema2_checksums=None):
        super().__init__()
        self.schema1 = schema1
        self.schema2 = schema2
        self.schema1_title = schema1_title or 'schema 1'
        self.schema2_title = schema2_title or 'schema 2'
        self.schema1_checksums = schema1_checksums
        self.schema2_checksums = schema2_checksums

    @classmethod
    def as_markup(cls, self, *, ctx):
        me = markup.elements
        body = []

        delta = delta_schemas(self.schema2, self.schema1)
        title = (f'Schema Delta Diff ({self.schema1_title} -> '
                 f'{self.schema2_title}):')
        body.append(me.doc.Section(
            title=title, body=[markup.serialize(delta, ctx=ctx)]))

        diff = edgedb_debug.get_schema_hash_diff(self.schema1, self.schema2,
                                                 a_title=self.schema1_title,
                                                 b_title=self.schema2_title)

        body.append(me.doc.Section(title='Schema Hash Diff', body=[diff]))

        diff = edgedb_debug.get_list_diff(
            self.schema1_checksums or [],
            self.schema2_checksums or [],
            a_title=self.schema1_title,
            b_title=self.schema2_title)

        body.append(me.doc.Section(title='Schema Object Hashes Diff',
                                   body=[diff]))

        diff = edgedb_debug.get_schema_text_diff(self.schema1, self.schema2,
                                                 a_title=self.schema1_title,
                                                 b_title=self.schema2_title)

        body.append(me.doc.Section(title='Schema Text Diff', body=[diff]))

        return markup.elements.lang.ExceptionContext(title=self.title,
                                                     body=body)


class DeltaExceptionContext(markup.MarkupExceptionContext):
    title = 'EdgeDB Delta Context'

    def __init__(self, delta):
        super().__init__
        self.delta = delta

    @classmethod
    def as_markup(cls, self, *, ctx):
        title = '{} {:032x}'.format(self.title, self.delta.id)
        body = [markup.serialize(delta, ctx=ctx)
                for delta in self.delta.deltas]
        return markup.elements.lang.ExceptionContext(title=title, body=body)


class DeltaError(EdgeDBError):
    def __init__(self, msg=None, *, hint=None, details=None, delta=None):
        super().__init__(msg, hint=hint, details=details)
        self.delta = delta
        if self.delta is not None:
            base_err.replace_context(
                self, DeltaExceptionContext(delta=self.delta))


class DeltaChecksumError(DeltaError):
    def __init__(self, msg=None, *, hint=None, details=None,
                 schema1=None, schema2=None,
                 schema1_title=None, schema2_title=None,
                 checksums1=None, checksums2=None):
        super().__init__(msg, hint=hint, details=details)
        if schema1 and schema2:
            err_ctx = DeltaExceptionSchemaContext(schema1, schema2,
                                                  schema1_title=schema1_title,
                                                  schema2_title=schema2_title,
                                                  schema1_checksums=checksums1,
                                                  schema2_checksums=checksums2)
            base_err.replace_context(self, err_ctx)


class DeltaHookError(DeltaError):
    pass


class DeltaHookNotFoundError(DeltaHookError):
    pass


class DeltaMeta(struct.MixedStructMeta):
    pass


class DeltaRefError(DeltaError):
    pass


class DeltaRef:
    patterns = {
        'offset': re.compile(
            r'(?:(?P<repo>\w+):)?(?P<ref>\w+)(?:(?P<off1>~+)(?P<off2>\d*))?'),
    }

    def __init__(self, ref, offset, repo=None):
        self.ref = ref
        self.offset = offset
        self.repo = repo

    @classmethod
    def parse(cls, spec):
        match = cls.patterns['offset'].match(str(spec))
        if match:
            offset = 0
            off1 = match.group('off1')
            if off1:
                offset = len(off1)
            off2 = match.group('off2')
            if off2:
                offset += int(off2) - 1
            return cls(match.group('ref'), offset, match.group('repo'))

    def __str__(self):
        if self.offset:
            result = '%s~%s' % (self.ref, self.offset)
        else:
            result = '%s' % self.ref
        if self.repo:
            result = self.repo + ':' + result
        return result


class Delta(struct.MixedStruct, metaclass=DeltaMeta):
    CURRENT_FORMAT_VERSION = 14

    id = struct.Field(int)
    parent_id = struct.Field(int, None)
    comment = struct.Field(str, None)
    checksum = struct.Field(int)
    checksum_details = struct.Field(list, list)
    deltas = struct.Field(list, None)
    script = struct.Field(str, None)
    snapshot = struct.Field(object, None)
    formatver = struct.Field(int, None)
    preprocess = struct.Field(str, None)
    postprocess = struct.Field(str, None)

    def __init__(self, **kwargs):
        hash_items = (kwargs['parent_id'], kwargs['checksum'],
                      kwargs['comment'])
        kwargs['id'] = persistent_hash('%s%s%s' % hash_items)
        super().__init__(**kwargs)

    def apply(self, schema):
        if self.snapshot is not None:
            try:
                self.snapshot.apply(schema)
            except Exception as e:
                msg = 'failed to apply delta {:032x}'.format(self.id)
                raise DeltaError(msg, delta=self) from e
        elif self.deltas:
            for d in self.deltas:
                try:
                    d.apply(schema)
                except Exception as e:
                    msg = 'failed to apply delta {:032x}'.format(self.id)
                    raise DeltaError(msg, delta=self) from e

    def call_hook(self, session, stage, hook):
        stage_code = getattr(self, stage, None)

        if stage_code is not None:
            with session.transaction():
                def _call_hook():
                    locals = {}

                    exec(stage_code, {}, locals)

                    try:
                        hook_func = locals[hook]
                    except KeyError as e:
                        msg = f'{stage} code does not define {hook}() callable'
                        raise DeltaHookNotFoundError(msg) from e
                    else:
                        hook_func(session)

                _call_hook()

    def upgrade(self, context, schema):
        for d in self.deltas:
            d.upgrade(context, self.formatver, schema)

        if self.formatver < 14:
            if self.deltas[0].preprocess:
                self.preprocess = \
                    s_expr.ExpressionText(self.deltas[0].preprocess)
            if self.deltas[0].postprocess:
                self.postprocess = \
                    s_expr.ExpressionText(self.deltas[0].postprocess)

        self.formatver = context.new_format_ver


class DeltaSet:
    def __init__(self, deltas):
        self.deltas = ordered.OrderedSet(deltas)

    def apply(self, schema):
        for d in self.deltas:
            d.apply(schema)

    def add(self, delta):
        self.deltas.add(delta)

    def __iter__(self):
        return iter(self.deltas)

    def __bool__(self):
        return bool(self.deltas)


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
        ftype = field.type[0]

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
            value = self.adapt_value(field, value)

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

    def discard_attribute(self, attr_name):
        for op in self.get_subcommands(type=AlterObjectProperty):
            if op.property == attr_name:
                self.discard(op)
                return

    def adapt_value(self, field, value):
        if value is not None and not isinstance(value, field.type):
            value = field.adapt(value)
        return value

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
        pass

    def upgrade(self, context, format_ver, schema):
        if context is None:
            context = CommandContext()

        with self.new_context(context):
            for op in self.ops:
                op.upgrade(context, format_ver, schema)

    def get_ast(self, context=None):
        if context is None:
            context = CommandContext()

        with self.new_context(context):
            return self._get_ast(context)

    @classmethod
    def command_for_ast_node(cls, astnode, schema, context):
        cmdcls = type(cls)._astnode_map.get(type(astnode))
        if hasattr(cmdcls, '_command_for_ast_node'):
            # Delegate the choice of command class to the specific command.
            cmdcls = cmdcls._command_for_ast_node(astnode, schema, context)

        return cmdcls

    @classmethod
    def from_ast(cls, astnode, *, context=None, schema):
        if context is None:
            context = CommandContext()

        cmdcls = cls.command_for_ast_node(
            astnode, schema=schema, context=context)

        if cmdcls is None:
            msg = 'cannot find command for ast node {!r}'.format(astnode)
            raise TypeError(msg)

        return cmdcls._cmd_tree_from_ast(
            astnode, context=context, schema=schema)

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        cmd = cls._cmd_from_ast(astnode, context, schema)
        cmd.source_context = astnode.context

        if getattr(astnode, 'commands', None):
            context_class = cls.get_context_class()

            if context_class is not None:
                with context(context_class(cmd)):
                    for subastnode in astnode.commands:
                        subcmd = Command.from_ast(
                            subastnode, context=context, schema=schema)
                        if subcmd is not None:
                            cmd.add(subcmd)
            else:
                for subastnode in astnode.commands:
                    subcmd = Command.from_ast(
                        subastnode, context=context, schema=schema)
                    if subcmd is not None:
                        cmd.add(subcmd)

        return cmd

    @classmethod
    def _cmd_from_ast(cls, astnode, context, schema):
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

    def new_context(self, context, scls=_void):
        if context is None:
            context = CommandContext()

        if scls is _void:
            scls = getattr(self, 'scls', None)

        context_class = self.get_context_class()
        return context(context_class(self, scls))

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
            op.apply(schema, context)


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


class DeltaUpgradeContext(CommandContext):
    def __init__(self, new_format_ver):
        super().__init__()
        self.new_format_ver = new_format_ver


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


class Ghost(ObjectCommand):
    """A special class to represent deleted delta commands."""

    def upgrade(self, context, format_ver, schema):
        """Remove self on during any upgrade."""
        super().upgrade(context, format_ver, schema)

        parent_ctx = context.get(CommandContextToken)
        if parent_ctx:
            parent_ctx.op.discard(self)

    def apply(self, schema, context):
        pass


class ObjectCommandContext(CommandContextToken):
    def __init__(self, op, scls=None):
        super().__init__(op)
        self.scls = scls
        if scls is not None:
            self.original_class = scls.get_canonical_class().copy(scls)
        else:
            self.original_class = None


class CreateObject(ObjectCommand):
    _delta_action = 'create'

    def _create_begin(self, schema, context):
        props = self.get_struct_properties(schema)

        metaclass = self.get_schema_metaclass()
        self.scls = metaclass(
            **props, _setdefaults_=False, _relaxrequired_=True)

    def _create_innards(self, schema, context):
        pass

    def _create_finalize(self, schema, context):
        self.scls.finalize(schema, dctx=context)

    def apply(self, schema, context):
        self._create_begin(schema, context)
        with self.new_context(context):
            self._create_innards(schema, context)
            self._create_finalize(schema, context)
        return self.scls


class AlterObject(ObjectCommand):
    _delta_action = 'alter'


class DeleteObject(ObjectCommand):
    _delta_action = 'delete'


class AlterSpecialObjectProperty(Command):
    astnode = qlast.SetSpecialField

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context, schema):
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
    def _cmd_tree_from_ast(cls, astnode, context, schema):
        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '::' + propname

        assert '::' not in propname

        if isinstance(astnode, qlast.DropAttributeValue):
            parent_ctx = context.get(CommandContextToken)
            parent_cls = parent_ctx.op.get_schema_metaclass()

            field = parent_cls._fields.get(propname)
            if (field is not None and
                    issubclass(field.type[0], typed.AbstractTypedCollection)):
                value = field.type[0]()
            else:
                value = None

            return cls(property=propname, new_value=value)

        if astnode.as_expr:
            new_value = s_expr.ExpressionText(
                edgeql.generate_source(astnode.value, pretty=False))
        else:
            if isinstance(astnode.value, qlast.Constant):
                new_value = astnode.value.value
            elif isinstance(astnode.value, qlast.Tuple):
                new_value = tuple(el.value for el in astnode.value.elements)
            else:
                raise ValueError(
                    f'unexpected value in attribute: {astnode.value!r}')

        return cls(property=propname, new_value=new_value)

    def _get_ast(self, context):
        value = self.new_value

        new_value_empty = \
            (value is None or
                (isinstance(value, collections.Container) and not value))

        old_value_empty = \
            (self.old_value is None or
                (isinstance(self.old_value, collections.Container) and
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
                qlast.Constant(value=el) for el in value
            ])
        elif isinstance(value, nlang.WordCombination):
            forms = value.as_dict()
            if len(forms) > 1:
                items = []
                for k, v in forms.items():
                    items.append((
                        qlast.Constant(value=k),
                        qlast.Constant(value=v)
                    ))
                value = qlast.Array(elements=[
                    qlast.Tuple(elements=[k, v]) for k, v in items
                ])
            else:
                value = qlast.Constant(value=str(value))
        else:
            value = qlast.Constant(value=value)

        as_expr = isinstance(value, qlast.ExpressionText)
        op = qlast.CreateAttributeValue(
            name=qlast.ObjectRef(module='', name=self.property),
            value=value, as_expr=as_expr)
        return op

    def __repr__(self):
        return '<%s.%s "%s":"%s"->"%s">' % (
            self.__class__.__module__, self.__class__.__name__,
            self.property, self.old_value, self.new_value)
