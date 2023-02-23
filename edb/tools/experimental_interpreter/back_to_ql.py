
from .data.data_ops import *
from .data.expr_ops import *
from .elaboration import DEFAULT_HEAD_NAME
from edb.schema.pointers import PointerDirection

from edb.edgeql import ast as qlast
from .basis.built_ins import all_builtin_ops

def reverse_elab_error(msg : str, expr : Val | Expr | Sequence[Val]) -> Any:
    raise ValueError("Reverse Elab Error", msg, expr)


def reverse_elab_label(lbl : Label) -> qlast.Path :
    match lbl:
        case StrLabel(l):
            return qlast.Path(steps=[qlast.Ptr(ptr=qlast.ObjectRef(name=l), direction=PointerDirection.Outbound)])
        case LinkPropLabel(l):
            return qlast.Path(steps=[qlast.Ptr(ptr=qlast.ObjectRef(name=l), type='property')])
        case _:
            raise ValueError(lbl)

def reverse_elab_shape(expr : ShapeExpr) -> Sequence[qlast.ShapeElement] :
    return [qlast.ShapeElement(expr=reverse_elab_label(lbl), 
                               compexpr=reverse_elab(instantiate_expr(FreeVarExpr(DEFAULT_HEAD_NAME), val)),
                               operation=qlast.ShapeOperation(op=qlast.ShapeOp.ASSIGN)
                               )
        for (lbl, val) in expr.shape.items()
    ]

def reverse_elab_type_name(tp : Tp) -> qlast.TypeName:
    match tp:
        case StrTp():
            return qlast.TypeName(maintype=qlast.ObjectRef(name="str"))
        case JsonTp():
            return qlast.TypeName(maintype=qlast.ObjectRef(name="json"))
        case DateTimeTp():
            return qlast.TypeName(maintype=qlast.ObjectRef(name="datetime"))
    raise ValueError("Unimplemented")

def reverse_elab_order(order : ObjectExpr) -> Sequence[qlast.SortExpr]:
    keys = sorted([(idx, spec, k)  for k in order.val.keys() for [idx, spec] in [k.label.split(OrderLabelSep)]])
    return [qlast.SortExpr(path=reverse_elab(order.val[k]), 
        direction = (
                qlast.SortOrder.Asc if spec == OrderAscending else
                qlast.SortOrder.Desc if spec == OrderDescending else
                reverse_elab_error("unknown direction", order)
                ))
        for (idx, spec, k) in keys
    ]


def append_path_element(subject : qlast.Base, to_add : qlast.PathElement) -> qlast.Path:
    match subject:
        case qlast.Path(steps=steps, partial=partial):
            return qlast.Path(steps=[*steps, 
                    to_add
                ], partial=partial)
        case rsub:
            return qlast.Path(steps=[rsub, 
                    to_add
                ], partial=False)



def reverse_elab(ir_expr : Expr) -> qlast.Base:
    expr : Expr
    match ir_expr:
        case StrVal(s):
            return qlast.StringConstant(value=s)
        case IntVal(i):
            return qlast.IntegerConstant(value=str(abs(i)), is_negative=(i < 0))
        case BoolVal(b):
            return qlast.BooleanConstant(value=str(b))
        case ObjectExpr(val=dic):
            return qlast.Shape(expr=None, elements=reverse_elab_shape(object_to_shape(ir_expr)))
        case InsertExpr(name=tname, new=arg):
            return qlast.InsertQuery(subject=qlast.ObjectRef(name=tname), 
                shape=reverse_elab_shape(object_to_shape(cast(ObjectExpr, arg)))
            )
        case FilterOrderExpr(subject=subject, filter=filter, order=order):
            result_name = next_name()
            return qlast.SelectQuery(
                result=reverse_elab(subject),
                result_alias=result_name,
                where=reverse_elab(instantiate_expr(FreeVarExpr(result_name), filter)),
                orderby=reverse_elab_order(cast(ObjectExpr, instantiate_expr(FreeVarExpr(result_name), order)))
            )
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
        case FunAppExpr(fun=fname, args=args, overloading_index=idx):
            if fname in all_builtin_ops.keys() and len(args) == 2:
                return qlast.BinOp(op=fname, left=reverse_elab(args[0]), right=reverse_elab(args[1]))
            else:
                return qlast.FunctionCall(func=fname, args=[reverse_elab(arg) for arg in args])
                # raise ValueError ("Unimplemented")
        case ObjectProjExpr(subject=subject, label=label):
            label_path_component = qlast.Ptr(ptr=qlast.ObjectRef(name=label), direction=PointerDirection.Outbound, type=None)
            return append_path_element(reverse_elab(subject), label_path_component)
        case BackLinkExpr(subject=subject, label=label):
            label_path_component = qlast.Ptr(ptr=qlast.ObjectRef(name=label), direction=PointerDirection.Inbound, type=None)
            return append_path_element(reverse_elab(subject), label_path_component)
        case LinkPropProjExpr(subject=subject, linkprop=label):
            label_path_component = qlast.Ptr(ptr=qlast.ObjectRef(name=label), direction=PointerDirection.Outbound, type="property")
            return append_path_element(reverse_elab(subject), label_path_component)
        case TpIntersectExpr(subject=subject, tp=tp_name):
            tp_path_component = qlast.TypeIntersection(type=qlast.TypeName(maintype=qlast.ObjectRef(name=tp_name)))
            return append_path_element(reverse_elab(subject), tp_path_component)
        case TypeCastExpr(tp=tp, arg=arg):
            return qlast.TypeCast(type=reverse_elab_type_name(tp), expr=reverse_elab(arg))
        case UnnamedTupleExpr(val=tuples):
            return qlast.Tuple(elements=[reverse_elab(e) for e in tuples])
        case NamedTupleExpr(val=tuples):
            return qlast.NamedTuple(elements=[qlast.TupleElement
                (name=qlast.ObjectRef(name=k), val=reverse_elab(v)) 
                for (k, v) in tuples.items()
            ])
        case UnionExpr(left=l, right=r):
            return qlast.BinOp(op="UNION", left=reverse_elab(l), right=reverse_elab(r))
        case ArrExpr(elems=elems):
            return qlast.Array(elements=[reverse_elab(e) for e in elems])
        case UpdateExpr(subject=subject, shape=shape):
            return qlast.UpdateQuery(subject=reverse_elab(subject), shape=reverse_elab_shape(shape))
        case MultiSetExpr(expr=elems):
            return qlast.Set(elements=[reverse_elab(e) for e in elems])
        case WithExpr(bound=bound, next=next):
            name = next_name()
            body = reverse_elab(instantiate_expr(FreeVarExpr(name), next))
            if isinstance(body, qlast.SelectQuery) or isinstance(body, qlast.InsertQuery) or isinstance(body, qlast.UpdateQuery) or isinstance(body, qlast.ForQuery):
                if body.aliases is None:
                    body.aliases = []
                body.aliases = [*body.aliases, qlast.AliasedExpr(alias=name, expr=reverse_elab(bound))]
                return body
            else:
                return qlast.SelectQuery(result=body, aliases=[qlast.AliasedExpr(alias=name, expr=reverse_elab(bound))])
                # raise ValueError("Expression does not suppor alias", body)
        case ForExpr(bound=bound, next=next):
            name = next_name()
            bound_v = reverse_elab(bound)
            body = reverse_elab(instantiate_expr(FreeVarExpr(name), next))
            return qlast.ForQuery(iterator=bound_v, iterator_alias=name, result=body)
        case DetachedExpr(expr=expr):
            return qlast.DetachedExpr(expr=reverse_elab(expr))
            
        case _:
            raise ValueError("Unimplemented", ir_expr)
