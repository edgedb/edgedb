

from . import data_ops as e
from . import expr_ops as eops

def show_card(card: e.Cardinal) -> str:
    match card:
        case e.FiniteCardinal(value=val):
            return str(val)
        case e.InfiniteCardinal():
            return "∞"
        case _:
            raise ValueError('Unimplemented', card)


def show_cmmode(mode: e.CMMode) -> str:
    return "[" + show_card(mode.lower) + "," + show_card(mode.upper) + "]"


def show_tp(tp: e.Tp) -> str:
    match tp:
        case e.ObjectTp(val=tp_val):
            return ('{' + ', '.join(lbl + ": " + show_tp(md_tp.tp)
                                    + show_cmmode(md_tp.mode)
                    for lbl, md_tp in tp_val.items()) + '}')
        case e.IntTp():
            return 'int'
        case e.IntInfTp():
            return 'intinf'
        case e.BoolTp():
            return 'bool'
        case e.StrTp():
            return 'str'
        case e.ArrTp(tp=tp):
            return f'arr({show_tp(tp)})'
        case e.NamedTupleTp(val=tp_val):
            return ('prod(' + ', '.join(
                f'{lbl}: {show_tp(md_tp)}'
                for lbl, md_tp in tp_val.items()) + ')')
        case e.UnnamedTupleTp(val=tp_val):
            return ('prod(' + ', '.join(
                f'{show_tp(md_tp)}'
                for md_tp in tp_val) + ')')
        case e.SomeTp(index=index):
            return f'some_{{{index}}}'
        case e.AnyTp():
            return 'any'
        case e.VarTp(name=name):
            return f'{name}'
        case e.UnifiableTp(id=id, resolution=resolution):
            return (f'unifiable_{{{id}}}_as_' +
                    (show_tp(resolution) if resolution else 'None'))
        case e.LinkPropTp(subject=s_tp, linkprop=lp_tp):
            return f'{show_tp(s_tp)}@{show_tp(lp_tp)}'
        case e.UnionTp(left=left, right=right):
            return f'{show_tp(left)} | {show_tp(right)}'
        case e.IntersectTp(left=left, right=right):
            return f'{show_tp(left)} & {show_tp(right)}'
        case e.UncheckedComputableTp(expr=expr):
            return 'comp_unck' + show_expr(expr)
        case e.ComputableTp(expr=expr, tp=tp):
            return 'comp(' + show_tp(tp) + "," + show_expr(expr) + ")"
        case _:
            raise ValueError('Unimplemented', tp)


def show_result_tp(tp: e.ResultTp) -> str:
    return show_tp(tp.tp) + show_cmmode(tp.mode)


def show_label(lbl: e.Label) -> str:
    match lbl:
        case e.StrLabel(label=s_label):
            return s_label
        case e.LinkPropLabel(label=l_label):
            return "@" + l_label
        case _:
            raise ValueError('Unimplemented', lbl)


def show_expr(expr: e.Expr) -> str:
    match expr:
        case e.IntVal(val=val):
            return str(val)
        case e.BoolVal(val=val):
            return str(val)
        case e.StrVal(val=val):
            return val
        case e.BindingExpr(var=var, body=_):
            return "λ" + var + ". " + show_expr(
                eops.instantiate_expr(e.FreeVarExpr(var), expr))
        case e.TypeCastExpr(tp=tp, arg=arg):
            return "<" + show_tp(tp) + ">" + show_expr(arg)
        case e.MultiSetExpr(expr=arr):
            return "{" + ", ".join(show_expr(el) for el in arr) + "}"
        case e.ObjectExpr(val=elems):
            return "{" + ", ".join(f'{show_label(lbl)} := {show_expr(el)}'
                                   for lbl, el in elems.items()) + "}"
        case e.ShapeExpr(shape=shape):
            return "{" + ", ".join(show_label(lbl) + " := " + show_expr(el)
                                   for lbl, el in shape.items()) + "}"
        case e.UnionExpr(left=left, right=right):
            return show_expr(left) + " `UNION` " + show_expr(right)
        case e.FunAppExpr(fun=fname, args=args, overloading_index=_):
            return (fname + "(" + ", ".join(show_expr(el) for el in args) +
                    ")")
        case e.FreeVarExpr(var=var):
            return var
        case e.ObjectProjExpr(subject=subject, label=label):
            return show_expr(subject) + "." + label
        case e.LinkPropProjExpr(subject=subject, linkprop=linkprop):
            return show_expr(subject) + "@" + linkprop
        case e.WithExpr(bound=bound, next=next):
            return "with " + show_expr(bound) + ", " + show_expr(next)
        case e.DetachedExpr(expr=subject):
            return "detached " + show_expr(subject)
        case e.BackLinkExpr(subject=subject, label=label):
            return show_expr(subject) + ".<" + label
        case e.TpIntersectExpr(subject=subject, tp=tp):
            return show_expr(subject) + " [is " + tp + "]"
        case e.SubqueryExpr(expr=subject):
            return "select " + show_expr(subject)
        case e.FilterOrderExpr(subject=subject, filter=filter, order=order):
            return ("(" + show_expr(subject) + " filter " + show_expr(filter) +
                    " order by " + show_expr(order) + ")")
        case e.OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
            return ("(" + show_expr(subject) + " offset " + show_expr(offset) +
                    " limit " + show_expr(limit) + ")")
        case e.InsertExpr(name=name, new=new):
            return ("insert " + name + " " + show_expr(new))
        case e.UpdateExpr(subject=subject, shape=shape):
            return ("update " + show_expr(subject) + " " + show_expr(shape))
        case e.ForExpr(bound=bound, next=next):
            return ("for " + show_expr(bound) + " union " + show_expr(next))
        case e.OptionalForExpr(bound=bound, next=next):
            return ("for optional " + show_expr(bound)
                    + " union " + show_expr(next))
        case e.ShapedExprExpr(expr=subject, shape=shape):
            return show_expr(subject) + " " + show_expr(shape)
        case e.UnnamedTupleExpr(val=elems):
            return "(" + ", ".join(show_expr(el) for el in elems) + ")"
        case e.NamedTupleExpr(val=elems):
            return "(" + ", ".join(f'{lbl} := {show_expr(el)}'
                                   for lbl, el in elems.items()) + ")"
        case e.ArrExpr(elems=arr):
            return "[" + ", ".join(show_expr(el) for el in arr) + "]"
        case e.IfElseExpr(
                then_branch=then_branch,
                condition=condition,
                else_branch=else_branch):
            return (show_expr(then_branch) + " if " + show_expr(condition) +
                    " else " + show_expr(else_branch))
        case _:
            raise ValueError('Unimplemented', expr)


def show_schema(dbschema: e.DBSchema) -> str:
    return ("\n".join(name + " := " + show_tp(tp) for name, tp in
                      dbschema.val.items()))
