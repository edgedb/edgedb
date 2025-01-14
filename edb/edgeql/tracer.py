#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2015-present MagicStack Inc. and the EdgeDB authors.
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


from __future__ import annotations

# Import specific things to avoid name clashes
from typing import (Dict, FrozenSet, Generator, List, Mapping, Optional,
                    Union, Set, Tuple, Iterable, Generic, TypeVar, Sequence,
                    AbstractSet)

import functools

from contextlib import contextmanager
from edb import errors
from edb.common import parsing
from edb.schema import links as s_links
from edb.schema import name as sn
from edb.schema import objects as so
from edb.schema import pointers as s_pointers
from edb.schema import schema as s_schema
from edb.schema import sources as s_sources
from edb.schema import types as s_types

from edb.edgeql import ast as qlast


NamedObject_T = TypeVar("NamedObject_T", bound="NamedObject")


class NamedObject:
    '''Generic tracing object with an explicit name.'''

    def __init__(self, name: sn.QualName) -> None:
        self.name = name

    def get_name(self, schema: s_schema.Schema) -> sn.QualName:
        return self.name

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return cls.__name__.lower()


SentinelObject = NamedObject(
    name=sn.QualName(module='__unknown__', name='__unknown__'),
)


ObjectLike = Union[NamedObject, so.Object]


class Function(NamedObject):
    pass


class Constraint(NamedObject):
    pass


class ConcreteConstraint(NamedObject):
    pass


class Annotation(NamedObject):
    pass


class AnnotationValue(NamedObject):
    pass


class Global(NamedObject):
    pass


class Index(NamedObject):
    pass


class ConcreteIndex(NamedObject):
    pass


class Field(NamedObject):
    pass


class Type(NamedObject):
    def is_scalar(self) -> bool:
        return False

    def is_object_type(self) -> bool:
        return False


class ScalarType(Type):
    def is_scalar(self) -> bool:
        return True


TypeLike = Union[Type, s_types.Type]


T = TypeVar('T')


class UnqualObjectIndex(Generic[T]):

    def __init__(self, items: Mapping[sn.UnqualName, T]) -> None:
        self._items = items

    def items(
        self,
        schema: s_schema.Schema,
    ) -> Iterable[Tuple[sn.UnqualName, T]]:
        return self._items.items()


class Source(NamedObject):

    pointers: Dict[sn.UnqualName, Union[s_pointers.Pointer, Pointer]]

    '''Abstract type that mocks the s_sources.Source for tracing purposes.'''

    def __init__(self, name: sn.QualName) -> None:
        super().__init__(name)
        self.pointers = {}

    def maybe_get_ptr(
        self,
        schema: s_schema.Schema,
        name: sn.UnqualName,
    ) -> Optional[Union[s_pointers.Pointer, Pointer]]:
        return self.pointers.get(name)

    def getptr(
        self,
        schema: s_schema.Schema,
        name: sn.UnqualName,
    ) -> Union[s_pointers.Pointer, Pointer]:
        ptr = self.maybe_get_ptr(schema, name)
        if ptr is None:
            raise AssertionError(f'{self.name} has no link or property {name}')
        return ptr

    def get_pointers(
        self,
        schema: s_schema.Schema,
    ) -> UnqualObjectIndex[Union[s_pointers.Pointer, Pointer]]:
        return UnqualObjectIndex(self.pointers)


Source_T = TypeVar("Source_T", bound="Source")
SourceLike = Union[Source, s_sources.Source]
SourceLike_T = TypeVar("SourceLike_T", bound="SourceLike")


class ObjectType(Type, Source):

    def is_pointer(self) -> bool:
        return False

    def is_scalar(self) -> bool:
        return False

    def is_object_type(self) -> bool:
        return True


class Alias(ObjectType):
    pass


class UnionType(Type):

    def __init__(self, types: List[Union[Type, UnionType, so.Object]]) -> None:
        self.types = types

    def get_name(self, schema: s_schema.Schema) -> sn.QualName:
        component_ids = sorted(str(t.get_name(schema)) for t in self.types)
        nqname = f"({' | '.join(component_ids)})"
        return sn.QualName(name=nqname, module='__derived__')

    def is_object_type(self) -> bool:
        return True


class Pointer(Source):

    def __init__(
        self,
        name: sn.QualName,
        *,
        source: Optional[SourceLike] = None,
        target: Optional[TypeLike] = None,
        target_expr: Optional[qlast.Expr] = None,
    ) -> None:
        super().__init__(name)
        self.source = source
        self.target = target
        self.target_expr = target_expr

    def is_pointer(self) -> bool:
        return True

    def is_property(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        raise NotImplementedError

    def maybe_get_ptr(
        self,
        schema: s_schema.Schema,
        name: sn.UnqualName,
    ) -> Optional[Union[s_pointers.Pointer, Pointer]]:
        if (not (res := super().maybe_get_ptr(schema, name))
                and isinstance(self.target, (Source, s_sources.Source))):
            res = self.target.maybe_get_ptr(schema, name)
        return res

    def get_target(
        self,
        schema: s_schema.Schema,
    ) -> Optional[TypeLike]:
        return self.target

    def get_source(
        self,
        schema: s_schema.Schema,
    ) -> Optional[SourceLike]:
        return self.source


class Property(Pointer):
    def is_property(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        return True


class Link(Pointer):
    def is_property(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        return False


class UnknownPointer(Pointer):
    def is_property(
        self,
        schema: s_schema.Schema,
    ) -> bool:
        return False

    @classmethod
    def get_schema_class_displayname(cls) -> str:
        return 'link or property'


class AccessPolicy(NamedObject):

    def __init__(
        self,
        name: sn.QualName,
        *,
        source: Optional[SourceLike] = None,
    ) -> None:
        super().__init__(name)
        self.source = source

    def get_source(
        self,
        schema: s_schema.Schema,
    ) -> Optional[SourceLike]:
        return self.source


class Trigger(NamedObject):

    def __init__(
        self,
        name: sn.QualName,
        *,
        source: Optional[SourceLike] = None,
    ) -> None:
        super().__init__(name)
        self.source = source

    def get_source(
        self,
        schema: s_schema.Schema,
    ) -> Optional[SourceLike]:
        return self.source


class Rewrite(NamedObject):

    def __init__(
        self,
        name: sn.QualName,
        *,
        source: Optional[SourceLike] = None,
    ) -> None:
        super().__init__(name)
        self.source = source

    def get_source(
        self,
        schema: s_schema.Schema,
    ) -> Optional[SourceLike]:
        return self.source


def qualify_name(name: sn.QualName, qual: str) -> sn.QualName:
    return sn.QualName(name.module, f'{name.name}@{qual}')


def trace_refs(
    qltree: qlast.Base,
    *,
    schema: s_schema.Schema,
    anchors: Optional[Mapping[str, sn.QualName]] = None,
    path_prefix: Optional[sn.QualName] = None,
    module: str,
    objects: Dict[sn.QualName, Optional[ObjectLike]],
    pointers: Mapping[sn.UnqualName, Set[sn.QualName]],
    params: Mapping[str, sn.QualName],
    local_modules: AbstractSet[str]
) -> Tuple[FrozenSet[sn.QualName], FrozenSet[sn.QualName]]:

    """Return a list of schema item names used in an expression.

    First set is strong deps, second is weak.
    """

    ctx = TracerContext(
        schema=schema,
        module=module,
        objects=objects,
        pointers=pointers,
        anchors=anchors or {},
        path_prefix=path_prefix,
        modaliases={},
        params=params,
        visited=set(),
        local_modules=local_modules,
    )
    trace(qltree, ctx=ctx)
    return frozenset(ctx.refs), frozenset(ctx.weak_refs)


def resolve_name(
    ref: qlast.ObjectRef,
    *,
    current_module: str,
    schema: s_schema.Schema,
    objects: Dict[sn.QualName, Optional[ObjectLike]],
    modaliases: Optional[Dict[Optional[str], str]],
    local_modules: AbstractSet[str],
    declaration: bool=False,
) -> sn.QualName:
    """Resolve a name into a fully-qualified one.

    This takes into account the current module and modaliases.

    This function mostly mirrors schema.FlatSchema._search_with_getter
    except:
    - If no module and no default module was set, try the current module
    - When searching in std, ensure module is not a local module
    - If no result found, return a name with the best modname available
    """

    def exists(name: sn.QualName) -> bool:
        return (
            objects.get(name) is not None
            or schema.get(name, default=None, type=so.Object) is not None
        )

    module = ref.module
    orig_module = module

    # Apply module aliases
    is_current, module = s_schema.apply_module_aliases(
        module, modaliases, current_module,
    )
    no_std = declaration or is_current

    # Check if something matches the name
    if module is not None:
        fqname = sn.QualName(module=module, name=ref.name)
        if exists(fqname):
            return fqname

    elif orig_module is None:
        # Look for name in current module
        fqname = sn.QualName(module=current_module, name=ref.name)
        if exists(fqname):
            return fqname

    # Try something in std if __current__ was not specified
    if not no_std:
        # If module == None, look in std
        if orig_module is None:
            mod_name = 'std'
            fqname = sn.QualName(mod_name, ref.name)
            if exists(fqname):
                return fqname

        # Ensure module is not a local module.
        # Then try the module as part of std.
        if module and module not in local_modules:
            mod_name = f'std::{module}'
            fqname = sn.QualName(mod_name, ref.name)
            if exists(fqname):
                return fqname

    # Just pick the best module name available
    return sn.QualName(
        module=module or orig_module or current_module,
        name=ref.name,
    )


class TracerContext:
    def __init__(
        self,
        *,
        schema: s_schema.Schema,
        module: str,
        objects: Dict[sn.QualName, Optional[ObjectLike]],
        pointers: Mapping[sn.UnqualName, Set[sn.QualName]],
        anchors: Mapping[str, sn.QualName],
        path_prefix: Optional[sn.QualName],
        modaliases: Dict[Optional[str], str],
        params: Mapping[str, sn.QualName],
        visited: Set[Union[s_pointers.Pointer, Pointer]],
        local_modules: AbstractSet[str],
    ) -> None:
        self.schema = schema
        self.refs: Set[sn.QualName] = set()
        self.weak_refs: Set[sn.QualName] = set()
        self.module = module
        self.objects = objects
        self.pointers = pointers
        self.anchors = anchors
        self.path_prefix = path_prefix
        self.modaliases = modaliases
        self.params = params
        self.local_modules = local_modules
        self.visited = visited

    def get_ref_name(self, ref: qlast.BaseObjectRef) -> sn.QualName:
        # We don't actually expect to handle anything other than
        # ObjectRef here.
        assert isinstance(ref, qlast.ObjectRef)

        if not ref.module and ref.name in self.params:
            return self.params[ref.name]

        return resolve_name(
            ref,
            current_module=self.module,
            schema=self.schema,
            objects=self.objects,
            modaliases=self.modaliases,
            local_modules=self.local_modules,
        )

    def get_ref_name_startswith(self, ref: qlast.ObjectRef) -> Set[sn.QualName]:
        refs = set()
        prefixes = set()

        if ref.module:
            # replace the module alias with the real name
            module = self.modaliases.get(ref.module, ref.module)
            prefixes.add(f'{module}::{ref.name}')
            prefixes.add(f'std::{module}::{ref.name}')
        else:
            prefixes.add(f'{self.module}::{ref.name}')
            prefixes.add(f'std::{ref.name}')

        for objname in self.objects.keys():
            short_name = str(objname).split('@@', 1)[0]
            if short_name in prefixes:
                refs.add(objname)

        return refs


def _fork_context(ctx: TracerContext) -> TracerContext:
    nctx = TracerContext(
        schema=ctx.schema,
        module=ctx.module,
        objects=dict(ctx.objects),
        pointers=ctx.pointers,
        anchors=ctx.anchors,
        path_prefix=ctx.path_prefix,
        modaliases=dict(ctx.modaliases),
        params=ctx.params,
        visited=ctx.visited,
        local_modules=ctx.local_modules,
    )
    nctx.refs = ctx.refs
    nctx.weak_refs = ctx.weak_refs

    return nctx


@contextmanager
def alias_context(
    ctx: TracerContext,
    aliases: Optional[
        Sequence[qlast.Alias]],
) -> Generator[TracerContext, None, None]:
    ctx = _fork_context(ctx)

    for alias in (aliases or ()):
        # module and modalias in ctx needs to be amended
        if isinstance(alias, qlast.ModuleAliasDecl):
            if alias.alias:
                ctx.modaliases[alias.alias] = alias.module
            else:
                # default module
                ctx.module = alias.module

        elif isinstance(alias, qlast.AliasedExpr):
            obj = trace(alias.expr, ctx=ctx)
            # Regardless of whether tracing the expression produces an
            # object, record the alias.
            ctx.objects[sn.QualName('__alias__', alias.alias)] = obj

    try:
        yield ctx
    finally:
        # refs are already updated
        pass


@contextmanager
def result_alias_context(
    ctx: TracerContext,
    node: Union[qlast.ReturningQuery, qlast.SubjectQuery],
    obj: Optional[ObjectLike],
) -> Generator[TracerContext, None, None]:

    alias: Optional[str] = None
    if isinstance(node, qlast.SelectQuery):
        alias = node.result_alias
    elif isinstance(node, qlast.GroupQuery):
        alias = node.subject_alias

    # potentially SELECT uses an alias for the main result
    if obj is not None and alias:
        nctx = TracerContext(
            schema=ctx.schema,
            module=ctx.module,
            objects=dict(ctx.objects),
            pointers=ctx.pointers,
            anchors=ctx.anchors,
            path_prefix=ctx.path_prefix,
            modaliases=ctx.modaliases,
            params=ctx.params,
            visited=ctx.visited,
            local_modules=ctx.local_modules,
        )
        # use the same refs set
        nctx.refs = ctx.refs
        nctx.objects[sn.QualName('__alias__', alias)] = obj
    else:
        nctx = ctx

    try:
        yield nctx
    finally:
        # refs are already updated
        pass


@functools.singledispatch
def trace(
    node: Optional[qlast.Base],
    *,
    ctx: TracerContext,
) -> Optional[ObjectLike]:
    raise NotImplementedError(f"do not know how to trace {node!r}")


@trace.register
def trace_none(node: None, *, ctx: TracerContext) -> None:
    pass


@trace.register
def trace_Constant(node: qlast.BaseConstant, *, ctx: TracerContext) -> None:
    pass


@trace.register
def trace_Parameter(node: qlast.Parameter, *, ctx: TracerContext) -> None:
    raise errors.SchemaError(
        'query parameters are not allowed in schemas',
        span=node.span,
    )


@trace.register
def trace_Array(node: qlast.Array, *, ctx: TracerContext) -> None:
    for el in node.elements:
        trace(el, ctx=ctx)


@trace.register
def trace_StrInterpFragment(
    node: qlast.StrInterpFragment, *, ctx: TracerContext
) -> None:
    trace(node.expr, ctx=ctx)


@trace.register
def trace_StrInterp(node: qlast.StrInterp, *, ctx: TracerContext) -> None:
    for el in node.interpolations:
        trace(el, ctx=ctx)


@trace.register
def trace_Set(node: qlast.Set, *, ctx: TracerContext) -> None:
    for el in node.elements:
        trace(el, ctx=ctx)


@trace.register
def trace_Tuple(node: qlast.Tuple, *, ctx: TracerContext) -> None:
    for el in node.elements:
        trace(el, ctx=ctx)


@trace.register
def trace_NamedTuple(node: qlast.NamedTuple, *, ctx: TracerContext) -> None:
    for el in node.elements:
        trace(el.val, ctx=ctx)


@trace.register
def trace_BinOp(node: qlast.BinOp, *, ctx: TracerContext) -> None:
    trace(node.left, ctx=ctx)
    trace(node.right, ctx=ctx)


@trace.register
def trace_UnaryOp(node: qlast.UnaryOp, *, ctx: TracerContext) -> None:
    trace(node.operand, ctx=ctx)


@trace.register
def trace_Detached(
    node: qlast.DetachedExpr, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    # DETACHED works with partial paths same as its inner expression.
    return trace(node.expr, ctx=ctx)


@trace.register
def trace_Global(
    node: qlast.GlobalExpr, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    refname = ctx.get_ref_name(node.name)
    if refname in ctx.objects:
        ctx.refs.add(refname)
        tip = ctx.objects[refname]
    else:
        tip = ctx.schema.get(refname, sourcectx=node.span)
    return tip


def check_type_exists(
    typename: sn.QualName,
    ctx: TracerContext,
    span: Optional[parsing.Span],
    *,
    hint: Optional[str] = None,
) -> None:
    if typename in ctx.objects:
        return

    try:
        # Check if the typename is already in the schema
        ctx.schema.get(typename, type=s_types.Type, sourcectx=span)
    except errors.InvalidReferenceError as e:
        if hint and not e.hint:
            e.set_hint_and_details(hint, e.details)
        raise e


@trace.register
def trace_TypeCast(node: qlast.TypeCast, *, ctx: TracerContext) -> None:
    trace(node.expr, ctx=ctx)
    if isinstance(node.type, qlast.TypeName):
        if not node.type.subtypes:
            typename: sn.QualName = ctx.get_ref_name(node.type.maintype)
            check_type_exists(typename, ctx, node.type.span)
            ctx.refs.add(typename)


@trace.register
def trace_IsOp(node: qlast.IsOp, *, ctx: TracerContext) -> None:
    trace(node.left, ctx=ctx)
    if isinstance(node.right, qlast.TypeName):
        if not node.right.subtypes:
            typename: sn.QualName = ctx.get_ref_name(node.right.maintype)

            hint: Optional[str] = None
            if typename.name.lower() in ['null', 'none']:
                hint = (
                    'Did you mean to use `exists` to check if a set is empty?'
                )
            check_type_exists(typename, ctx, node.right.span, hint=hint)

            ctx.refs.add(typename)


@trace.register
def trace_Introspect(node: qlast.Introspect, *, ctx: TracerContext) -> None:
    if isinstance(node.type, qlast.TypeName):
        if not node.type.subtypes:
            typename: sn.QualName = ctx.get_ref_name(node.type.maintype)
            check_type_exists(typename, ctx, node.type.span)
            ctx.refs.add(typename)


@trace.register
def trace_FunctionCall(node: qlast.FunctionCall, *, ctx: TracerContext) -> None:

    if isinstance(node.func, tuple):
        fname = qlast.ObjectRef(module=node.func[0], name=node.func[1])
    else:
        fname = qlast.ObjectRef(name=node.func)
    # The function call is dependent on the function actually being
    # present, so we add all variations of that function name to the
    # dependency list.

    names = ctx.get_ref_name_startswith(fname)
    ctx.refs.update(names)

    for arg in node.args:
        trace(arg, ctx=ctx)
    for arg in node.kwargs.values():
        trace(arg, ctx=ctx)


@trace.register
def trace_Indirection(node: qlast.Indirection, *, ctx: TracerContext) -> None:
    for indirection in node.indirection:
        trace(indirection, ctx=ctx)
    trace(node.arg, ctx=ctx)


@trace.register
def trace_Index(node: qlast.Index, *, ctx: TracerContext) -> None:
    trace(node.index, ctx=ctx)


@trace.register
def trace_Slice(node: qlast.Slice, *, ctx: TracerContext) -> None:
    trace(node.start, ctx=ctx)
    trace(node.stop, ctx=ctx)


@trace.register
def trace_Path(
    node: qlast.Path,
    *,
    ctx: TracerContext,
) -> Optional[ObjectLike]:
    tip: Optional[ObjectLike] = None
    ptr: Optional[Union[Pointer, s_pointers.Pointer]] = None
    plen = len(node.steps)

    # HACK: This isn't very smart, and can't properly track types
    # through arbitrary expressions. To try to mitigate the damage
    # from this, when we have a pointer step but don't know the type,
    # we track *weak* references to all pointers with that name.
    # This won't always work (if there is a tangle of cyclic weak deps),
    # but it works pretty well.

    for i, step in enumerate(node.steps):
        if isinstance(step, qlast.ObjectRef):
            # the ObjectRef without a module may be referring to an
            # aliased expression
            aname = sn.QualName('__alias__', step.name)
            if not step.module and aname in ctx.objects:
                tip = ctx.objects[aname]
            else:
                refname = ctx.get_ref_name(step)
                if refname in ctx.objects:
                    ctx.refs.add(refname)
                    tip = ctx.objects[refname]
                else:
                    tip = ctx.schema.get(refname, sourcectx=step.span)

        elif isinstance(step, qlast.Ptr):
            pname = sn.UnqualName(step.name)

            if i == 0:
                # Abbreviated path.
                if ctx.path_prefix in ctx.objects:
                    tip = ctx.objects[ctx.path_prefix]
                    if isinstance(tip, Pointer):
                        ptr = tip
                else:
                    # We can't reason about this path.
                    # Do a weak dependency on anything with the same name.
                    ctx.weak_refs.update(ctx.pointers.get(pname, ()))

            if step.type == 'property':
                if ptr is None:
                    # This is either a computable def  or unknown link, bail.
                    # Do a weak dependency on anything with the same name.
                    ctx.weak_refs.update(ctx.pointers.get(pname, ()))
                    tip = None

                elif isinstance(ptr, (s_links.Link, Pointer)):
                    lprop = ptr.maybe_get_ptr(
                        ctx.schema,
                        pname,
                    )
                    if lprop is None:
                        # Invalid link property reference, bail.
                        return None

                    if (isinstance(lprop, Pointer) and
                            lprop.source is not None):
                        src = lprop.source
                        src_name = src.get_name(ctx.schema)
                        if (isinstance(src, Pointer) and
                                src.source is not None):
                            src_src_name = src.source.get_name(ctx.schema)
                            source_name = qualify_name(
                                src_src_name, src_name.name)
                        else:
                            source_name = src_name
                        ctx.refs.add(qualify_name(source_name, step.name))
            else:
                if step.direction == '<':
                    if plen > i + 1 and isinstance(node.steps[i + 1],
                                                   qlast.TypeIntersection):
                        # A reverse link traversal with a type intersection,
                        # process it on the next step.
                        pass
                    else:
                        # No type intersection, so the only type that
                        # it can be is "Object", which is trivial.
                        # However, we need to make it dependent on
                        # every link of the same name now.
                        for fqname in ctx.pointers.get(pname, ()):
                            obj = ctx.objects.get(fqname)

                            # Ignore what appears to not be a link
                            # with the right name.
                            if (isinstance(obj, (s_pointers.Pointer,
                                                 Pointer)) and
                                fqname.name.split('@', 1)[1] ==
                                    step.name):

                                target = obj.get_target(ctx.schema)
                                # Ignore scalars, but include other
                                # computables to produce better error
                                # messages.
                                if (target is None or
                                        not target.is_scalar()):
                                    # Record link with matching short
                                    # name.
                                    ctx.refs.add(fqname)

                        tip = ptr = None
                else:
                    if isinstance(tip, (Source, s_sources.Source)):
                        ptr = tip.maybe_get_ptr(
                            ctx.schema, sn.UnqualName(step.name)
                        )
                        if ptr is None:
                            # Invalid pointer reference, bail.
                            return None
                        else:
                            ptr_source = ptr.get_source(ctx.schema)

                        if ptr_source is not None:
                            sname = ptr_source.get_name(ctx.schema)
                            assert isinstance(sname, sn.QualName)
                            ctx.refs.add(qualify_name(sname, step.name))
                            tip = ptr.get_target(ctx.schema)

                            if tip is None:
                                if ptr in ctx.visited:
                                    # Possibly recursive definition, bail out.
                                    return None

                                # This can only be Pointer that didn't
                                # infer the target type yet.
                                assert isinstance(ptr, Pointer)
                                # We haven't computed the target yet,
                                # so try computing it now.
                                ctx.visited.add(ptr)

                                target_ctx = _fork_context(ctx)
                                target_ctx.path_prefix = sname
                                ptr_target = trace(
                                    ptr.target_expr, ctx=target_ctx
                                )

                                if isinstance(ptr_target, (Type,
                                                           s_types.Type)):
                                    tip = ptr.target = ptr_target

                        else:
                            # Can't figure out the new tip, so we bail.
                            return None

                    else:
                        # We can't reason about this path.
                        # Do a weak dependency on anything with the same name.
                        ctx.weak_refs.update(ctx.pointers.get(pname, ()))
                        tip = ptr = None

        elif isinstance(step, qlast.TypeIntersection):
            # This tip is determined from the type in the type
            # intersection, which is valid in the general case, but
            # there's a special case that needs to be potentially
            # handled for backward links.
            tip = _resolve_type_expr(step.type, ctx=ctx)
            prev_step = node.steps[i - 1]
            if isinstance(prev_step, qlast.Ptr):
                if prev_step.direction == '<':
                    if isinstance(tip, (s_sources.Source, ObjectType)):
                        ptr = tip.maybe_get_ptr(
                            ctx.schema, sn.UnqualName(prev_step.name)
                        )
                        if ptr is None:
                            # Invalid pointer reference, bail.
                            return None

                        if isinstance(tip, Type):
                            tip_name = tip.get_name(ctx.schema)
                            ctx.refs.add(qualify_name(tip_name, prev_step.name))

        elif isinstance(step, qlast.Splat):
            if step.type is not None:
                _resolve_type_expr(step.type, ctx=ctx)
            if step.intersection is not None:
                _resolve_type_expr(step.intersection.type, ctx=ctx)

        else:
            tr = trace(step, ctx=ctx)
            tip = ptr = None
            if tr is not None:
                tip = tr
                if isinstance(tip, Pointer):
                    ptr = tip

    return tip


@trace.register
def trace_Anchor(
    node: qlast.Anchor, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    if name := ctx.anchors.get(node.name):
        return ctx.objects[name]
    return None


def _resolve_type_expr(
    texpr: qlast.TypeExpr,
    *,
    ctx: TracerContext,
) -> TypeLike:

    if isinstance(texpr, qlast.TypeName):
        if texpr.subtypes and isinstance(texpr.maintype, qlast.ObjectRef):
            return Type(
                name=sn.QualName(
                    module='__coll__',
                    name=texpr.maintype.name,
                ),
            )
        else:
            refname = ctx.get_ref_name(texpr.maintype)
            local_obj = ctx.objects.get(refname)
            obj: TypeLike
            if local_obj is None:
                obj = ctx.schema.get(
                    refname, type=s_types.Type, sourcectx=texpr.span)
            else:
                assert isinstance(local_obj, Type)
                obj = local_obj
                ctx.refs.add(refname)

            return obj

    elif isinstance(texpr, qlast.TypeOp):

        if texpr.op == '|':
            return UnionType([
                _resolve_type_expr(texpr.left, ctx=ctx),
                _resolve_type_expr(texpr.right, ctx=ctx),
            ])

        else:
            raise NotImplementedError(
                f'unsupported type operation: {texpr.op}')

    else:
        raise NotImplementedError(
            f'unsupported type expression: {texpr!r}'
        )


@trace.register
def trace_TypeIntersection(
    node: qlast.TypeIntersection, *, ctx: TracerContext
) -> None:
    trace(node.type, ctx=ctx)


@trace.register
def trace_TypeOf(node: qlast.TypeOf, *, ctx: TracerContext) -> None:
    trace(node.expr, ctx=ctx)


@trace.register
def trace_TypeName(node: qlast.TypeName, *, ctx: TracerContext) -> None:
    if node.subtypes:
        for st in node.subtypes:
            trace(st, ctx=ctx)
    elif isinstance(node.maintype, qlast.ObjectRef):
        tref = node.maintype
        if tref.module:
            fq_name = sn.QualName(module=tref.module, name=tref.name)
        else:
            fq_name = sn.QualName(module=ctx.module, name=tref.name)
            if fq_name not in ctx.objects:
                std_name = sn.QualName(module="std", name=tref.name)
                if ctx.schema.get(std_name, default=None) is not None:
                    fq_name = std_name
        ctx.refs.add(fq_name)


@trace.register
def trace_TypeOp(node: qlast.TypeOp, *, ctx: TracerContext) -> None:
    trace(node.left, ctx=ctx)
    trace(node.right, ctx=ctx)


@trace.register
def trace_IfElse(node: qlast.IfElse, *, ctx: TracerContext) -> None:
    trace(node.if_expr, ctx=ctx)
    trace(node.else_expr, ctx=ctx)
    trace(node.condition, ctx=ctx)


@trace.register
def trace_Shape(
    node: qlast.Shape, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    tip = trace(node.expr, ctx=ctx)
    if isinstance(node.expr, qlast.Path):
        orig_prefix = ctx.path_prefix
        if tip is not None:
            tip_name = tip.get_name(ctx.schema)
            assert isinstance(tip_name, sn.QualName)
            ctx.path_prefix = tip_name
        else:
            ctx.path_prefix = None

    for element in node.elements:
        trace(element, ctx=ctx)

    if isinstance(node.expr, qlast.Path):
        ctx.path_prefix = orig_prefix

    return tip


@trace.register
def trace_ShapeElement(node: qlast.ShapeElement, *, ctx: TracerContext) -> None:
    trace(node.expr, ctx=ctx)
    if node.elements:
        for element in node.elements:
            trace(element, ctx=ctx)
    trace(node.where, ctx=ctx)
    if node.orderby:
        for sortexpr in node.orderby:
            trace(sortexpr, ctx=ctx)
    trace(node.offset, ctx=ctx)
    trace(node.limit, ctx=ctx)
    trace(node.compexpr, ctx=ctx)


def _update_path_prefix(tip: Optional[ObjectLike], ctx: TracerContext) -> None:
    if tip is not None:
        tip_name = tip.get_name(ctx.schema)
        assert isinstance(tip_name, sn.QualName)
        ctx.path_prefix = tip_name
    else:
        ctx.path_prefix = None


@trace.register
def trace_Select(
    node: qlast.SelectQuery, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    with alias_context(ctx, node.aliases) as ctx:
        tip = trace(node.result, ctx=ctx)
        _update_path_prefix(tip, ctx=ctx)

        # potentially SELECT uses an alias for the main result
        with result_alias_context(ctx, node, tip) as nctx:
            if node.where is not None:
                trace(node.where, ctx=nctx)
            if node.orderby:
                for expr in node.orderby:
                    trace(expr, ctx=nctx)
            if node.offset is not None:
                trace(node.offset, ctx=nctx)
            if node.limit is not None:
                trace(node.limit, ctx=nctx)

        return tip


def trace_GroupingAtom(node: qlast.GroupingAtom, *, ctx: TracerContext) -> None:
    if isinstance(node, qlast.ObjectRef):
        trace(qlast.Path(steps=[node]), ctx=ctx)
    elif isinstance(node, qlast.Path):
        trace(node, ctx=ctx)
    else:
        assert isinstance(node, qlast.GroupingIdentList)
        for el in node.elements:
            trace_GroupingAtom(el, ctx=ctx)


@trace.register
def trace_GroupingSimple(
    node: qlast.GroupingSimple, *, ctx: TracerContext
) -> None:
    trace_GroupingAtom(node.element, ctx=ctx)


@trace.register
def trace_GroupingSets(node: qlast.GroupingSets, *, ctx: TracerContext) -> None:
    for s in node.sets:
        trace(s, ctx=ctx)


@trace.register
def trace_GroupingOperation(
    node: qlast.GroupingOperation, *, ctx: TracerContext
) -> None:
    for s in node.elements:
        trace(s, ctx=ctx)


@trace.register
def trace_Group(
    node: qlast.GroupQuery, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    return _trace_GroupQuery(node, ctx=ctx)


@trace.register
def trace_InternalGroupQuery(
    node: qlast.InternalGroupQuery, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    return _trace_GroupQuery(node, ctx=ctx)


def _trace_GroupQuery(
    node: qlast.GroupQuery | qlast.InternalGroupQuery, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    with alias_context(ctx, node.aliases) as ctx:
        tip = trace(node.subject, ctx=ctx)
        if tip is not None:
            tip_name = tip.get_name(ctx.schema)
            assert isinstance(tip_name, sn.QualName)
            ctx.path_prefix = tip_name

        # potentially GROUP uses an alias for the main result
        with result_alias_context(ctx, node, tip) as nctx:
            with alias_context(nctx, node.using) as byctx:
                for by_el in node.by:
                    trace(by_el, ctx=byctx)

        if isinstance(node, qlast.InternalGroupQuery):
            with alias_context(nctx, node.using) as byctx:
                ctx.objects[sn.QualName('__alias__', node.group_alias)] = (
                    SentinelObject)
                if node.grouping_alias:
                    ctx.objects[
                        sn.QualName('__alias__', node.grouping_alias)] = (
                            SentinelObject)
                trace(node.result, ctx=byctx)

        return tip


@trace.register
def trace_SortExpr(node: qlast.SortExpr, *, ctx: TracerContext) -> None:
    trace(node.path, ctx=ctx)


@trace.register
def trace_InsertQuery(node: qlast.InsertQuery, *, ctx: TracerContext) -> None:
    with alias_context(ctx, node.aliases) as ctx:
        if node.unless_conflict:
            trace(node.unless_conflict[0], ctx=ctx)
            trace(node.unless_conflict[1], ctx=ctx)

        tip = trace(qlast.Path(steps=[node.subject]), ctx=ctx)
        _update_path_prefix(tip, ctx=ctx)

        for element in node.shape:
            trace(element, ctx=ctx)


@trace.register
def trace_UpdateQuery(
    node: qlast.UpdateQuery, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    with alias_context(ctx, node.aliases) as ctx:
        tip = trace(node.subject, ctx=ctx)
        _update_path_prefix(tip, ctx=ctx)

        # potentially UPDATE uses an alias for the main result
        with result_alias_context(ctx, node, tip) as nctx:
            for element in node.shape:
                trace(element, ctx=nctx)

            trace(node.where, ctx=nctx)

        return tip


@trace.register
def trace_DeleteQuery(
    node: qlast.DeleteQuery, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    with alias_context(ctx, node.aliases) as ctx:
        tip = trace(node.subject, ctx=ctx)
        _update_path_prefix(tip, ctx=ctx)

        # potentially DELETE uses an alias for the main result
        with result_alias_context(ctx, node, tip) as nctx:
            if node.where is not None:
                trace(node.where, ctx=nctx)
            if node.orderby:
                for expr in node.orderby:
                    trace(expr, ctx=nctx)
            if node.offset is not None:
                trace(node.offset, ctx=nctx)
            if node.limit is not None:
                trace(node.limit, ctx=nctx)

        return tip


@trace.register
def trace_For(
    node: qlast.ForQuery, *, ctx: TracerContext
) -> Optional[ObjectLike]:
    with alias_context(ctx, node.aliases) as ctx:
        obj = trace(node.iterator, ctx=ctx)
        if obj is None:
            obj = SentinelObject
        ctx.objects[sn.QualName('__alias__', node.iterator_alias)] = obj
        tip = trace(node.result, ctx=ctx)

        return tip


@trace.register
def trace_DescribeStmt(
    node: qlast.DescribeStmt,
    *,
    ctx: TracerContext,
) -> None:

    if isinstance(node.object, qlast.ObjectRef):
        fq_name = ctx.get_ref_name(node.object)
        ctx.refs.add(fq_name)


@trace.register
def trace_ExplainStmt(
    node: qlast.ExplainStmt,
    *,
    ctx: TracerContext,
) -> None:
    pass


@trace.register
def trace_AdministerStmt(
    node: qlast.AdministerStmt,
    *,
    ctx: TracerContext,
) -> None:
    pass


@trace.register
def trace_Placeholder(
    node: qlast.Placeholder,
    *,
    ctx: TracerContext,
) -> None:
    pass
