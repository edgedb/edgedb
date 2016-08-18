##
# Copyright (c) 2008-2011 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import collections
import re

from edgedb.lang.common.exceptions import EdgeDBError
from edgedb.lang.common import exceptions as base_err
from edgedb.lang.common import debug as edgedb_debug
from edgedb.lang import edgeql
from edgedb.lang.edgeql import ast as qlast

from edgedb.lang.common import datastructures, functional, markup
from edgedb.lang.common.datastructures import Field
from edgedb.lang.common.datastructures import typed
from edgedb.lang.common.datastructures import OrderedIndex
from edgedb.lang.common.nlang import morphology
from edgedb.lang.common.algos.persistent_hash import persistent_hash

from . import objects as so
from . import expr as s_expr
from . import name as s_name
from . import utils


def delta_schemas(schema1, schema2):
    from . import modules, objects as so, database

    result = database.AlterDatabase()

    my_modules = set(schema1.modules)
    other_modules = set(schema2.modules)

    added_modules = my_modules - other_modules
    dropped_modules = other_modules - my_modules
    common_modules = my_modules & other_modules

    for added_module in added_modules:
        my_module = schema1.get_module(added_module)
        create = modules.CreateModule(
                    prototype_name=added_module,
                    prototype_class=modules.ProtoModule)
        create.add(AlterPrototypeProperty(
                    property='name', old_value=None,
                    new_value=added_module))
        create.add(AlterPrototypeProperty(
                    property='imports', old_value=None,
                    new_value=tuple(my_module.imports)))
        result.add(create)

    for common_module in common_modules:
        my_module = schema1.get_module(common_module)
        other_module = schema2.get_module(common_module)

        if my_module.imports != other_module.imports:
            alter = modules.AlterModule(
                        prototype_name=common_module,
                        prototype_class=modules.ProtoModule)
            alter.add(AlterPrototypeProperty(
                        property='imports',
                        old_value=tuple(other_module.imports),
                        new_value=tuple(my_module.imports)))
            result.add(alter)

    global_adds_mods = []
    global_dels = []

    for type in schema1.global_dep_order:
        new = OrderedIndex(schema1(type), key=lambda o: o.persistent_hash())
        old = OrderedIndex(schema2(type), key=lambda o: o.persistent_hash())

        if type in ('link', 'link_property', 'constraint'):
            new = filter(lambda i: i.generic(), new)
            old = filter(lambda i: i.generic(), old)

        adds_mods, dels = so.BasePrototype._delta_sets(
            old, new, old_schema=schema2, new_schema=schema1)

        global_adds_mods.append(adds_mods)
        global_dels.append(dels)

    for add_mod in global_adds_mods:
        result.update(add_mod)

    for dels in reversed(global_dels):
        result.update(dels)

    for dropped_module in dropped_modules:
        result.add(modules.DeleteModule(prototype_name=dropped_module,
                                        prototype_class=modules.ProtoModule))

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
        title = 'Schema Delta Diff ({} -> {}):'.format(
                    self.schema1_title, self.schema2_title)
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


class DeltaMeta(datastructures.MixedStructMeta):
    pass


class DeltaRefError(DeltaError):
    pass


class DeltaRef:
    patterns = {
        'offset': re.compile(
            '(?:(?P<repo>\w+):)?(?P<ref>\w+)(?:(?P<off1>~+)(?P<off2>\d*))?'),
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


class DeltaDriver:
    def __init__(self, *, create=None, alter=None, rebase=None,
                          rename=None, delete=None):
        self.create = create
        self.alter = alter
        self.rebase = rebase
        self.rename = rename
        self.delete = delete


class Delta(datastructures.MixedStruct, metaclass=DeltaMeta):
    CURRENT_FORMAT_VERSION = 14

    id = datastructures.Field(int)
    parent_id = datastructures.Field(int, None)
    comment = datastructures.Field(str, None)
    checksum = datastructures.Field(int)
    checksum_details = datastructures.Field(list, list)
    deltas = datastructures.Field(list, None)
    script = datastructures.Field(str, None)
    snapshot = datastructures.Field(object, None)
    formatver = datastructures.Field(int, None)
    preprocess = Field(str, None)
    postprocess = Field(str, None)

    def __init__(self, **kwargs):
        hash_items = (kwargs['parent_id'], kwargs['checksum'],
                      kwargs['comment'])
        kwargs['id'] = persistent_hash('%s%s%s' % hash_items)
        super().__init__(**kwargs)

    def apply(self, schema):
        ''

        """LINE [delta.progress] APPLYING
        '{:032x}'.format(self.id)
        """

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
                        msg = '{} code does not define {}() callable' \
                                .format(stage, hook)
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
        self.deltas = datastructures.OrderedSet(deltas)

    def apply(self, schema):
        for d in self.deltas:
            d.apply(schema)

    def add(self, delta):
        self.deltas.add(delta)

    def __iter__(self):
        return iter(self.deltas)

    def __bool__(self):
        return bool(self.deltas)


class CommandMeta(functional.Adapter, datastructures.MixedStructMeta):
    _astnode_map = {}

    def __init__(cls, name, bases, clsdict, *, adapts=None):
        functional.Adapter.__init__(cls, name, bases, clsdict, adapts=adapts)
        datastructures.MixedStructMeta.__init__(cls, name, bases, clsdict)
        astnodes = clsdict.get('astnode')
        if astnodes:
            if not isinstance(astnodes, (list, tuple)):
                astnodes = [astnodes]

            mapping = type(cls)._astnode_map

            for astnode in astnodes:
                existing = mapping.get(astnode)
                if existing:
                    msg = ('duplicate EdgeQL AST node to command mapping: ' +
                           '{!r} is already declared for {!r}')
                    raise TypeError(msg.format(astnode, existing))

                mapping[astnode] = cls


@markup.serializer.serializer(method='as_markup')
class Command(datastructures.MixedStruct, metaclass=CommandMeta):
    preprocess = Field(str, None)
    postprocess = Field(str, None)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ops = datastructures.OrderedSet()

    @classmethod
    def adapt(cls, obj):
        result = super().copy(obj)
        for op in obj.ops:
            result.ops.add(type(cls).adapt(op))
        return result

    def _resolve_ref(self, ref, schema, allow_unresolved_refs=False,
                     resolve=True):
        try:
            proto_name = ref.prototype_name
        except AttributeError:
            # Not a ref
            return ref, None
        else:
            obj = None

            if allow_unresolved_refs:
                obj = schema.get(proto_name, default=None)
            else:
                obj = schema.get(proto_name)

            return obj, ref

    def _resolve_attr_value(self, value, fname, field, schema, unresolved,
                            allow_unresolved_refs=False):
        ftype = field.type[0]

        if isinstance(ftype, so.PrototypeClass):

            if isinstance(value, so.PrototypeRef):
                value, ref = self._resolve_ref(
                    value, schema, allow_unresolved_refs)
                if value is None:
                    unresolved[fname] = ref

            elif (isinstance(value, so.Collection)
                    and any(isinstance(st, so.PrototypeRef)
                            for st in value.get_subtypes())):
                subtypes = []
                for st in value.get_subtypes():
                    eltype, ref = self._resolve_ref(
                        st, schema, allow_unresolved_refs)
                    if eltype is None:
                        unresolved[fname] = ref
                        subtypes.append(st)
                    else:
                        subtypes.append(eltype)

                value = value.__class__.from_subtypes(subtypes)

        elif issubclass(ftype, typed.AbstractTypedMapping):
            if issubclass(ftype.valuetype, so.ProtoObject):
                vals = {}

                for k, val in value.items():
                    val, ref = self._resolve_ref(
                        val, schema, allow_unresolved_refs)
                    if val is None:
                        unresolved[fname] = value
                        continue

                    vals[k] = val

                if fname in unresolved:
                    ctype = so.PrototypeDict
                else:
                    ctype = ftype

                value = ctype(vals)

        elif issubclass(ftype, (typed.AbstractTypedSequence,
                                typed.AbstractTypedSet)):
            if issubclass(ftype.type, so.ProtoObject):
                vals = []

                for val in value:
                    val, ref = self._resolve_ref(
                        val, schema, allow_unresolved_refs)
                    if val is None:
                        unresolved[fname] = value
                        continue

                    vals.append(val)

                if fname in unresolved:
                    if issubclass(ftype, typed.AbstractTypedSet):
                        ctype = so.PrototypeSet
                    else:
                        ctype = so.PrototypeList
                else:
                    ctype = ftype

                value = ctype(vals)
        else:
            value = self.adapt_value(field, value)

        return value

    def get_struct_properties(self, schema,
                              include_old_value=False,
                              allow_unresolved_refs=False):
        result = {}
        unresolved = {}

        for op in self(AlterPrototypeProperty):
            try:
                field = self.prototype_class.get_field(op.property)
            except KeyError:
                continue

            value = self._resolve_attr_value(
                op.new_value, op.property, field, schema, unresolved,
                allow_unresolved_refs=allow_unresolved_refs)

            if include_old_value:
                if op.old_value is not None:
                    old_value = self._resolve_attr_value(
                        op.old_value, op.property, field, schema, unresolved,
                        allow_unresolved_refs=True)
                else:
                    old_value = None
                value = (old_value, value)

            result[op.property] = value

        if allow_unresolved_refs:
            return result, unresolved
        else:
            return result

    def adapt_value(self, field, value):
        if value is not None and not isinstance(value, field.type):
            value = field.adapt(value)
        return value

    def __iter__(self):
        return iter(self.ops)

    def __call__(self, typ):
        return filter(lambda i: isinstance(i, typ), self.ops)

    def add(self, command):
        self.ops.add(command)

    def update(self, commands):
        self.ops.update(commands)

    def discard(self, command):
        self.ops.discard(command)

    def apply(self, schema, context):
        pass

    def upgrade(self, context, format_ver, schema):
        try:
            context_class = self.__class__.context_class
        except AttributeError:
            for op in self.ops:
                op.upgrade(context, format_ver, schema)
        else:
            with context(context_class(self)):
                for op in self.ops:
                    op.upgrade(context, format_ver, schema)

    def get_ast(self, context=None):
        if context is None:
            context = CommandContext()

        context_class = self.__class__.context_class
        with context(context_class(self)):
            return self._get_ast(context)

    @classmethod
    def command_for_ast_node(cls, astnode):
        return type(cls)._astnode_map.get(astnode)

    @classmethod
    def from_ast(cls, astnode, context=None):
        if context is None:
            context = CommandContext()

        cmdcls = cls.command_for_ast_node(type(astnode))
        if cmdcls is None:
            msg = 'cannot find command for ast node {!r}'.format(astnode)
            raise TypeError(msg)

        return cmdcls._cmd_tree_from_ast(astnode, context=context)

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        cmd = cls._cmd_from_ast(astnode, context)

        if getattr(astnode, 'commands', None):
            context_class = getattr(cls, 'context_class', None)

            if context_class is not None:
                with context(context_class(cmd)):
                    for subastnode in astnode.commands:
                        subcmd = Command.from_ast(subastnode, context=context)
                        if subcmd is not None:
                            cmd.add(subcmd)
            else:
                for subastnode in astnode.commands:
                    subcmd = Command.from_ast(subastnode, context=context)
                    if subcmd is not None:
                        cmd.add(subcmd)

        return cmd

    @classmethod
    def _cmd_from_ast(cls, astnode, context):
        return cls()

    @classmethod
    def as_markup(cls, self, *, ctx):
        node = markup.elements.lang.TreeNode(name=str(self))

        if self.ops:
            for dd in self:
                if isinstance(dd, AlterPrototypeProperty):
                    diff = markup.elements.doc.ValueDiff(
                        before=repr(dd.old_value), after=repr(dd.new_value))

                    node.add_child(label=dd.property, node=diff)
                else:
                    node.add_child(node=markup.serialize(dd, ctx=ctx))

        return node

    def __str__(self):
        return '{} ({})'.format(self.__class__.__name__,
                                datastructures.MixedStruct.__str__(self))

    def __repr__(self):
        flds = datastructures.MixedStruct.__repr__(self)
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
    def __init__(self):
        self.stack = []

    def push(self, token):
        self.stack.append(token)

    def pop(self):
        return self.stack.pop()

    def get(self, cls):
        for item in reversed(self.stack):
            if isinstance(item, cls):
                return item

    def get_top(self):
        return self.stack[0]

    def copy(self):
        ctx = CommandContext()
        ctx.stack = self.stack[:]
        return ctx

    def __call__(self, token):
        return CommandContextWrapper(self, token)


class DeltaUpgradeContext(CommandContext):
    def __init__(self, new_format_ver):
        super().__init__()
        self.new_format_ver = new_format_ver


class PrototypeCommand(Command):
    prototype_class = Field(so.PrototypeClass, str_formatter=None)

    def _register_unresolved_refs(self, schema, context, unresolved):
        top_context = context.get_top()

        for prop, ref in unresolved.items():
            if isinstance(ref, (typed.AbstractTypedSequence,
                                typed.AbstractTypedSet)):
                keys = tuple(r.prototype_name for r in ref)
            else:
                keys = (ref.prototype_name,)

            for key in keys:
                try:
                    referrers = top_context.unresolved_refs[key]
                except KeyError:
                    referrers = top_context.unresolved_refs[key] = []

                referrers.append((self, context.copy(), self.prototype, prop))

    def _resolve_refs(self, schema, context, prototype):
        top_context = context.get_top()

        key = prototype.name

        try:
            unresolved = top_context.unresolved_refs.pop(key)
        except KeyError:
            pass
        else:
            for ref_command, ref_context, ref_proto, proto_attr in unresolved:
                ref_command.ref_appears(schema, ref_context, ref_proto,
                                        proto_attr, prototype)

    def ref_appears(self, schema, context, ref_proto, proto_attr, prototype):
        field = ref_proto.__class__.get_field(proto_attr)

        if issubclass(field.type[0], typed.AbstractTypedSequence):
            refs = getattr(ref_proto, proto_attr)
            for i, ref in enumerate(refs):
                if getattr(ref, 'prototype_name', None) == prototype.name:
                    refs[i] = prototype
                    break
            else:
                refs.append(prototype)
        elif issubclass(field.type[0], typed.AbstractTypedSet):
            refs = getattr(ref_proto, proto_attr)
            for ref in list(refs):
                if getattr(ref, 'prototype_name', None) == prototype.name:
                    refs.discard(ref)
                    refs.add(prototype)
                    break
            else:
                refs.add(prototype)
        else:
            setattr(ref_proto, proto_attr, prototype)


class Ghost(PrototypeCommand):
    """A special class to represent deleted delta commands."""

    def upgrade(self, context, format_ver, schema):
        """Remove self on during any upgrade."""
        super().upgrade(context, format_ver, schema)

        parent_ctx = context.get(CommandContextToken)
        if parent_ctx:
            parent_ctx.op.discard(self)

    def apply(self, schema, context):
        pass


class PrototypeCommandContext(CommandContextToken):
    def __init__(self, op, proto=None):
        super().__init__(op)
        self.proto = proto
        if proto is not None:
            self.original_proto = proto.get_canonical_class().copy(proto)
        else:
            self.original_proto = None


class CreatePrototype(PrototypeCommand):
    def apply(self, schema, context):
        props, unresolved = self.get_struct_properties(
            schema, allow_unresolved_refs=True)
        self.prototype = self.prototype_class(**props)

        if unresolved:
            self._register_unresolved_refs(schema, context, unresolved)

        return self.prototype


class CreateSimplePrototype(CreatePrototype):
    prototype_data = Field(object, None)

    def apply(self, schema, context=None):
        self.prototype = self.prototype_class.get_canonical_class()(
            self.prototype_data)
        return self.prototype


class AlterSpecialPrototypeProperty(Command):
    astnode = qlast.SetSpecialFieldNode

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        return AlterPrototypeProperty(
            property=astnode.name,
            new_value=astnode.value
        )


class AlterPrototypeProperty(Command):
    property = Field(str)
    old_value = Field(object, None)
    new_value = Field(object, None)

    @classmethod
    def _cmd_tree_from_ast(cls, astnode, context):
        propname = astnode.name.name
        if astnode.name.module:
            propname = astnode.name.module + '::' + propname

        assert '::' not in propname

        if isinstance(astnode, qlast.DropAttributeValueNode):
            parent_ctx = context.get(CommandContextToken)
            parent_cls = parent_ctx.op.prototype_class

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
            if isinstance(astnode.value, qlast.ConstantNode):
                new_value = astnode.value.value
            elif isinstance(astnode.value, qlast.SequenceNode):
                new_value = tuple(el.value for el in astnode.value.elements)
            elif isinstance(astnode.value, qlast.MappingNode):
                m = {}
                for k, v in astnode.value.items:
                    k = k.value
                    if isinstance(v, qlast.ConstantNode):
                        v = v.value
                    elif isinstance(v, qlast.SequenceNode):
                        v = tuple(el.value for el in v.elements)
                    elif (isinstance(v, qlast.FunctionCallNode) and
                            v.func == 'typeref'):
                        if len(v.args) > 1:
                            # collection
                            ct = so.Collection.get_class(v.args[0].value)
                            subtypes = []
                            for st in v.args[1:]:
                                stname = s_name.Name(v.args[1].value)
                                subtypes.append(so.PrototypeRef(
                                    prototype_name=stname))

                            v = ct.from_subtypes(subtypes)
                        else:
                            v = so.PrototypeRef(
                                prototype_name=s_name.Name(v.args[0].value))
                    elif isinstance(v, qlast.TypeCastNode):
                        v = v.expr.value
                    elif isinstance(v, qlast.UnaryOpNode):
                        v = edgeql.generate_source(v)
                        # Remove the space between the operator and the operand
                        v = ''.join(v.split(' ', maxsplit=1))
                    else:
                        msg = 'unexpected value in attribute {!r}: {!r}'\
                                    .format(propname, v)
                        raise ValueError(msg)
                    m[k] = v

                new_value = m
            else:
                msg = 'unexpected value in attribute: {!r}' \
                            .format(astnode.value)
                raise ValueError(msg)

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
            op = qlast.DropAttributeValueNode(
                name=qlast.PrototypeRefNode(module='', name=self.property))
            return op

        if new_value_empty and old_value_empty:
            return

        if isinstance(value, s_expr.ExpressionText):
            value = qlast.ExpressionTextNode(expr=str(value))
        elif utils.is_nontrivial_container(value):
            value = qlast.SequenceNode(elements=[
                qlast.ConstantNode(value=el) for el in value
            ])
        elif isinstance(value, morphology.WordCombination):
            forms = value.as_dict()
            if len(forms) > 1:
                items = []
                for k, v in forms.items():
                    items.append((
                        qlast.ConstantNode(value=k),
                        qlast.ConstantNode(value=v)
                    ))
                value = qlast.MappingNode(items=items)
            else:
                value = qlast.ConstantNode(value=str(value))
        else:
            value = qlast.ConstantNode(value=value)

        as_expr = isinstance(value, qlast.ExpressionTextNode)
        op = qlast.CreateAttributeValueNode(
                name=qlast.PrototypeRefNode(module='', name=self.property),
                value=value, as_expr=as_expr)
        return op

    def __repr__(self):
        return '<%s.%s "%s":"%s"->"%s">' % (
                    self.__class__.__module__, self.__class__.__name__,
                    self.property, self.old_value, self.new_value)
