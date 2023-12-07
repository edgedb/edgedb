

from . import data_ops as e
from . import expr_ops as eops
from typing import *

def show_card(card: e.Cardinal) -> str:
    match card:
        case e.ZeroCardinal():
            return "0"
        case e.OneCardinal():
            return "1"
        case e.InfiniteCardinal():
            return "∞"
        case _:
            raise ValueError('Unimplemented', card)


def show_cmmode(mode: e.CMMode) -> str:
    return "[" + show_card(mode.lower) + "," + show_card(mode.upper) + "]"

def show_qname(name: e.QualifiedName) -> str:
    return "::".join(name.names)

def show_raw_name(name: e.QualifiedName | e.UnqualifiedName) -> str:
    if isinstance(name, e.QualifiedName):
        return show_qname(name)
    else:
        return name.name


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
        case e.CompositeTp(kind=kind, tps=tps):
            return f'{kind}<{",".join(show_tp(tp) for tp in tps)}>'
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
        # case e.VarTp(name=name):
        #     return f'{name}'
        # case e.UnifiableTp(id=id, resolution=resolution):
        #     return (f'unifiable_{{{id}}}_as_' +
        #             (show_tp(resolution) if resolution else 'None'))
        case e.NamedNominalLinkTp(name=name, linkprop=lp_tp):
            return f'{show_raw_name(name)}@{show_tp(lp_tp)}'
        # case e.UncheckedNamedNominalLinkTp(name=name, linkprop=lp_tp):
        #     return f'{name}@{show_tp(lp_tp)}'
        case e.NominalLinkTp(name=name, subject=s_tp, linkprop=lp_tp):
            return f'{show_tp(s_tp)}_{name}@{show_tp(lp_tp)}'
        case e.UnionTp(left=left, right=right):
            return f'{show_tp(left)} | {show_tp(right)}'
        case e.IntersectTp(left=left, right=right):
            return f'{show_tp(left)} & {show_tp(right)}'
        case e.UncheckedComputableTp(expr=expr):
            return 'comp_unck(' + show_expr(expr) + ')'
        case e.ComputableTp(expr=expr, tp=tp):
            return 'comp(' + show_tp(tp) + "," + show_expr(expr) + ")"
        case e.DefaultTp(expr=expr, tp=tp):
            return 'default(' + show_tp(tp) + "," + show_expr(expr) + ")"
        case _:
            raise ValueError('Unimplemented', tp)

def show_func_tps(tp: e.FunArgRetType) -> str:
    return (", ".join(show_tp(arg_tp) for arg_tp in tp.args_tp) +
            " -> " + show_result_tp(tp.ret_tp))

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
        # case e.ObjectExpr(val=elems):
        #     return "{" + ", ".join(f'{show_label(lbl)} := {show_expr(el)}'
        #                            for lbl, el in elems.items()) + "}"
        case e.ShapeExpr(shape=shape):
            return "{" + ", ".join(show_label(lbl) + " := " + show_expr(el)
                                   for lbl, el in shape.items()) + "}"
        case e.UnionExpr(left=left, right=right):
            return show_expr(left) + " `UNION` " + show_expr(right)
        case e.FunAppExpr(fun=fname, args=args, overloading_index=_):
            return (show_raw_name(fname) + "(" + ", ".join(show_expr(el) for el in args) +
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
            return show_expr(subject) + " [is " + show_raw_name(tp) + "]"
        case e.SubqueryExpr(expr=subject):
            return "select " + show_expr(subject)
        case e.FilterOrderExpr(subject=subject, filter=filter, order=order):
            return ("(" + show_expr(subject) + " filter " + show_expr(filter) +
                    " order by {" +  ", ".join([l + " => " + show_expr(o) for (l,o) in order.items()]) + "})")
        case e.OffsetLimitExpr(subject=subject, offset=offset, limit=limit):
            return ("(" + show_expr(subject) + " offset " + show_expr(offset) +
                    " limit " + show_expr(limit) + ")")
        case e.InsertExpr(name=name, new=new):
            return ("insert " + show_raw_name(name) + " {" + 
                    ", ".join([k + " := " + show_expr(n) for (k,n) in new.items()]) + "}")
        case e.UpdateExpr(subject=subject, shape=shape):
            return ("update " + show_expr(subject) + " " + show_expr(shape))
        case e.DeleteExpr(subject=subject):
            return ("delete " + show_expr(subject))
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
        case e.ConditionalDedupExpr(expr=inner):
            return "cond_dedup(" + show_expr(inner) + ")"
        case e.FreeObjectExpr():
            return "{<free>}"
        case _:
            raise ValueError('Unimplemented', expr)


def show_me(me: e.ModuleEntity) -> str:
    match me:
        case e.ModuleEntityTypeDef(typedef=typedef):
            return show_tp(typedef)
        case e.ModuleEntityFuncDef(funcdef=funcdef):
            return "<func>"
        case _:
            raise ValueError('Unimplemented', me)

def show_module(dbschema: e.DBModule) -> str:
    return ("\n".join(name + " := " + show_me(me) for name, me in
                      dbschema.defs.items()))

def show_module_name(name: Tuple[str, ...]) -> str:
    return "::".join(name)

def show_schema(dbschema: e.DBSchema) -> str:
    return ("\n".join(show_module_name(name) + " := " + show_module(module) 
                      for name, module in dbschema.modules.items())
            + "\n".join(show_module_name(name) + " := " + show_module(module)
                        for name, module in dbschema.unchecked_modules.items()))


def show_tcctx(tcctx: e.TcCtx) -> str:
    return (show_schema(tcctx.schema) + "\n" +
            ("\n".join(name + " := " + show_result_tp(r_tp)
                       for name, r_tp in tcctx.varctx.items())))

def show_visibility_marker(maker: e.Marker) -> str:
    match maker:
        case e.Visible():
            return "v"
        case e.Invisible():
            return "i"
        case _:
            raise ValueError('Unimplemented', maker)

def show_val(val: e.Val | e.ObjectVal | e.MultiSetVal) -> str:
    match val:
        case e.IntVal(val=v):
            return str(v)
        case e.BoolVal(val=v):
            return str(v)
        case e.StrVal(val=v):
            return v
        case e.ObjectVal(val=elems):
            return "{" + ", ".join(f'{show_label(lbl)} ({show_visibility_marker(m)}): {show_val(el)}'
                                   for (lbl, (m, el)) in elems.items()) + "}"
        case e.RefVal(refid=id, val=v):
            return f"ref({id})" + show_val(v)
        case e.UnnamedTupleVal(val=elems):
            return "(" + ", ".join(show_val(el) for el in elems) + ")"
        case e.NamedTupleVal(val=elems):
            return "(" + ", ".join(f'{lbl} := {show_val(el)}'
                                   for lbl, el in elems.items()) + ")"
        case e.ArrVal(val=arr):
            return "[" + ", ".join(show_val(el) for el in arr) + "]"
        case e.MultiSetVal(vals=arr):
            return "{" + ", ".join(show_val(el) for el in arr) + "}"
        case _:
            raise ValueError('Unimplemented', val)