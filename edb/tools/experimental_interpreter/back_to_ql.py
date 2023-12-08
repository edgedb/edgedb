
from typing import Any, List, Optional, Sequence, cast, Dict

from edb.edgeql import ast as qlast
from edb.schema.pointers import PointerDirection

# from .basis.built_ins import all_builtin_ops
from .data.data_ops import (ArrExpr, BackLinkExpr, BoolVal,
                            DetachedExpr, Expr, FilterOrderExpr, ForExpr,
                            FreeVarExpr, FunAppExpr, InsertExpr,
                            IntVal, Label, LinkPropLabel,
                            LinkPropProjExpr, MultiSetExpr, NamedTupleExpr,
                            ObjectProjExpr, ObjectVal,
                            OffsetLimitExpr, OptionalForExpr, OrderAscending,
                            OrderDescending, OrderLabelSep, RefVal,
                            ShapedExprExpr, ShapeExpr, StrLabel,  StrVal,
                            SubqueryExpr, Tp, TpIntersectExpr, TypeCastExpr,
                            UnionExpr, UnnamedTupleExpr, UpdateExpr, Val,
                            WithExpr, )
from .data import data_ops as e
from .data.expr_ops import (abstract_over_expr, instantiate_expr)
from .elaboration import DEFAULT_HEAD_NAME


def reverse_elab_error(msg: str, expr: Val | Expr | Sequence[Val]) -> Any:
    raise ValueError("Reverse Elab Error", msg, expr)

def reverse_elab_raw_name(name: e.RawName) -> qlast.ObjectRef:
    match name:
        case e.QualifiedName(names=names):
            return qlast.ObjectRef(name=names[-1], module="::".join(names[:-1]))
        case e.UnqualifiedName(name=n):
            return qlast.ObjectRef(name=n)
        case _:
            raise ValueError(name)

def show_raw_name(name: e.RawName) -> str:
    match name:
        case e.QualifiedName(names=names):
            return "::".join(names)
        case e.UnqualifiedName(name=n):
            return n
        case _:
            raise ValueError(name)

def reverse_elab_label(lbl: Label) -> qlast.Path:
    match lbl:
        case StrLabel(l):
            return qlast.Path(
                steps=[qlast.Ptr(
                    name=l,
                    direction=PointerDirection.Outbound)])
        case LinkPropLabel(l):
            return qlast.Path(
                steps=[qlast.Ptr(
                    name=l,
                    type='property')])
        case _:
            raise ValueError(lbl)


def reverse_elab_shape(expr: ShapeExpr) -> List[qlast.ShapeElement]:
    return [qlast.ShapeElement(expr=reverse_elab_label(lbl),
                               compexpr=reverse_elab(instantiate_expr(
                                   FreeVarExpr(DEFAULT_HEAD_NAME), val)),
                               operation=qlast.ShapeOperation(
                                   op=qlast.ShapeOp.ASSIGN)
                               )
            for (lbl, val) in expr.shape.items()
            ]


def reverse_elab_type_name(tp: Tp) -> qlast.TypeName:
    match tp:
        case (e.ScalarTp(qname) | e.UncheckedTypeName(qname)):
            if isinstance(qname, e.QualifiedName):
                if len(qname.names) == 2:
                    return qlast.TypeName(maintype=qlast.ObjectRef(name=qname.names[0]), module="::".join(qname.names[:-1]))
                elif len(qname.names) == 1:
                    return qlast.TypeName(maintype=qlast.ObjectRef(name=qname.names[0]))
                else:
                    raise ValueError("Unimplemented")
            elif isinstance(qname, e.UnqualifiedName):
                return qlast.TypeName(maintype=qlast.ObjectRef(name=qname.name))
            else:
                raise ValueError("Unimplemented")
    raise ValueError("Unimplemented")


def reverse_elab_order(order: Dict[str, Expr]) -> Optional[List[qlast.SortExpr]]:
    # if isinstance(order, ObjectExpr):
        keys = sorted([(idx, spec, k) for k in order.keys()
                      for [idx, spec] in [k.split(OrderLabelSep)]])
        if len(keys) == 0:
            return None
        return [qlast.SortExpr(path=reverse_elab(order[k]),
                               direction=(
            qlast.SortOrder.Asc if spec == OrderAscending else
            qlast.SortOrder.Desc if spec == OrderDescending else
            reverse_elab_error("unknown direction " + spec, order[k])
        ))
            for (idx, spec, k) in keys
        ]
    # else:
    #     return [qlast.SortExpr(path=reverse_elab(order))]


def reverse_elab_object_val(val: ObjectVal) -> qlast.Expr:
    return qlast.Shape(
        expr=None,
        elements=reverse_elab_shape(
            ShapeExpr(
                shape={lbl: abstract_over_expr(
                    MultiSetExpr([e for e in mv.vals]))
                    for (lbl, (u, mv)) in val.val.items()})))


def append_path_element(
        subject: qlast.Expr, to_add: qlast.PathElement) -> qlast.Path:
    match subject:
        case qlast.Path(steps=steps, partial=partial):
            return qlast.Path(steps=[*steps,
                                     to_add
                                     ], partial=partial)
        case rsub:
            return qlast.Path(steps=[rsub,
                                     to_add
                                     ], partial=False)


def reverse_elab(ir_expr: Expr) -> qlast.Expr:
    expr: Expr
    match ir_expr:
        case StrVal(s):
            return qlast.StringConstant(value=s)
        case IntVal(i):
            return qlast.IntegerConstant(
                value=str(abs(i)),
                is_negative=(i < 0))
        case BoolVal(b):
            return qlast.BooleanConstant(value=str(b))
        case RefVal(_):
            return qlast.StringConstant(
                value=str("<REFVAL, TODO: UUID_CASTING>"))
        # case LinkPropVal(_):
        #     return qlast.StringConstant(value=str("<TODO: LINK_PROP_VAL>"))
        # case FreeVal(val=val):
        #     return reverse_elab_object_val(val)
        # case ObjectExpr(val=_):
        #     return qlast.Shape(expr=None,
        #                        elements=reverse_elab_shape(
        #                            object_to_shape(ir_expr)))
        case InsertExpr(name=tname, new=arg):
            return qlast.InsertQuery(subject=reverse_elab_raw_name(tname),
                                     shape=reverse_elab_shape(
                                         e.ShapeExpr(shape={StrLabel(k):
                                                            abstract_over_expr(v) for (k,v) in arg.items()})))
        case FilterOrderExpr(subject=subject, filter=filter, order=order):
            result_name = filter.var
            return qlast.SelectQuery(
                result=reverse_elab(subject),
                result_alias=result_name,
                where=reverse_elab(
                    instantiate_expr(FreeVarExpr(result_name),
                                     filter)),
                orderby=reverse_elab_order(
                        {l : instantiate_expr(
                            FreeVarExpr(result_name),
                            o) for (l,o) in order.items()}))
        case OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
            return qlast.SelectQuery(
                result=reverse_elab(subject),
                offset=reverse_elab(offset),
                limit=reverse_elab(limit)
            )
        case ShapedExprExpr(expr=subject, shape=shape):
            return qlast.Shape(
                expr=reverse_elab(subject),
                elements=reverse_elab_shape(shape)
            )
        case FreeVarExpr(var=name):
            if name == DEFAULT_HEAD_NAME:
                return qlast.Path(steps=[], partial=True)
            else:
                return qlast.Path(steps=[qlast.ObjectRef(name=name)])
        case e.QualifiedName(names=names):
            return reverse_elab_raw_name(ir_expr)
        case e.UnqualifiedName(name=name):
            return reverse_elab_raw_name(ir_expr)
        case FunAppExpr(fun=fname, args=args, overloading_index=_):
            # if (isinstance(fname, e.QualifiedName) 
            #     and fname.names[0] == "std" 
            #     and fname.names[1] in all_builtin_ops.keys() and len(args) == 2):
            #     return qlast.BinOp(
            #         op=show_raw_name(fname), left=reverse_elab(args[0]),
            #         right=reverse_elab(args[1]))
            # else:
            return qlast.FunctionCall(func=show_raw_name(fname),
                                        args=[reverse_elab(arg)
                                            for arg in args])
        case e.ConditionalDedupExpr(expr=inner):
            return qlast.FunctionCall(func="cond_dedup",
                                        args=[reverse_elab(inner)])
        case e.FreeObjectExpr():
            return qlast.Path(steps=[qlast.ObjectRef(name="std::FreeObject")])
        case ObjectProjExpr(subject=subject, label=label):
            label_path_component = qlast.Ptr(
                name=label,
                direction=PointerDirection.Outbound, type=None)
            return append_path_element(
                reverse_elab(subject),
                label_path_component)
        case BackLinkExpr(subject=subject, label=label):
            label_path_component = qlast.Ptr(
                name=label, direction=PointerDirection.Inbound, type=None)
            return append_path_element(
                reverse_elab(subject),
                label_path_component)
        case LinkPropProjExpr(subject=subject, linkprop=label):
            label_path_component = qlast.Ptr(
                name=label,
                direction=PointerDirection.Outbound, type="property")
            return append_path_element(
                reverse_elab(subject),
                label_path_component)
        case TpIntersectExpr(subject=subject, tp=tp_name):
            tp_path_component = qlast.TypeIntersection(
                type=qlast.TypeName(maintype=reverse_elab_raw_name(tp_name)))
            return append_path_element(
                reverse_elab(subject),
                tp_path_component)
        case TypeCastExpr(tp=tp, arg=arg):
            return qlast.TypeCast(
                type=reverse_elab_type_name(tp),
                expr=reverse_elab(arg))
        case (UnnamedTupleExpr(val=tuples) | e.UnnamedTupleVal(val=tuples)):
            return qlast.Tuple(elements=[reverse_elab(e) for e in tuples])
        case NamedTupleExpr(val=tuples):
            return qlast.NamedTuple(
                elements=[qlast.TupleElement(
                    name=qlast.Ptr(name=k),
                    val=reverse_elab(v))
                    for (k, v) in tuples.items()])
        case UnionExpr(left=l, right=r):
            return qlast.BinOp(
                op="UNION", left=reverse_elab(l),
                right=reverse_elab(r))
        case ArrExpr(elems=elems):
            return qlast.Array(elements=[reverse_elab(e) for e in elems])
        case UpdateExpr(subject=subject, shape=shape):
            return qlast.UpdateQuery(
                subject=reverse_elab(subject),
                shape=reverse_elab_shape(shape))
        case e.DeleteExpr(subject=subject):
            return qlast.DeleteQuery(subject=reverse_elab(subject))
        case MultiSetExpr(expr=elems):
            return qlast.Set(elements=[reverse_elab(e) for e in elems])
        case WithExpr(bound=bound, next=next):
            name = next.var
            body = reverse_elab(instantiate_expr(FreeVarExpr(name), next))
            if isinstance(
                    body, qlast.SelectQuery) or isinstance(
                    body, qlast.InsertQuery) or isinstance(
                    body, qlast.UpdateQuery) or isinstance(
                    body, qlast.ForQuery):
                if body.aliases is None:
                    body.aliases = []
                body.aliases = [qlast.AliasedExpr(
                    alias=name, expr=reverse_elab(bound)), *body.aliases]
                return body
            else:
                return qlast.SelectQuery(
                    result=body,
                    aliases=[qlast.AliasedExpr(
                        alias=name, expr=reverse_elab(bound))])
                # raise ValueError("Expression does not suppor alias", body)
        case ForExpr(bound=bound, next=next):
            name = next.var
            bound_v = reverse_elab(bound)
            body = reverse_elab(instantiate_expr(FreeVarExpr(name), next))
            return qlast.ForQuery(
                iterator=bound_v,
                iterator_alias=name,
                result=body)
        case OptionalForExpr(bound=bound, next=next):
            name = next.var
            bound_v = reverse_elab(bound)
            body = reverse_elab(instantiate_expr(FreeVarExpr(name), next))
            return qlast.ForQuery(
                iterator=bound_v,
                iterator_alias=name,
                optional=True,
                result=body)
        case DetachedExpr(expr=expr):
            return qlast.DetachedExpr(expr=reverse_elab(expr))
        case SubqueryExpr(expr=expr):
            return reverse_elab(expr)
        case e.IfElseExpr(then_branch=then_branch,
                          condition=condition,
                          else_branch=else_branch):
            return qlast.IfElse(
                if_expr=reverse_elab(then_branch),
                condition=reverse_elab(condition),
                else_expr=reverse_elab(else_branch))
        case _:
            raise ValueError("Unimplemented", ir_expr)
