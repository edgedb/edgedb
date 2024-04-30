from typing import Tuple, Dict

from ..data import data_ops as e
from ..data import module_ops as mops
from ..data import path_factor as path_factor
from . import typechecking as tck
from . import module_check_tools as mck


def object_tp_comp_name_resolve(
    root_ctx: e.TcCtx,
    tp_comp: e.Tp,
) -> e.Tp:
    match tp_comp:
        case e.UncheckedTypeName(name):
            return tck.check_type_valid(root_ctx, tp_comp)
        case e.NamedNominalLinkTp(name=name, linkprop=l_prop):
            if isinstance(name, e.UnqualifiedName):
                name_ck = mops.resolve_simple_name(root_ctx, name)
            else:
                name_ck = name
            resolved_tp = mops.try_resolve_type_name(root_ctx, name_ck)
            if not isinstance(resolved_tp, e.ObjectTp):
                raise ValueError(
                    "Scalar type cannot carry link props", tp_comp
                )

            linkprop_ck: Dict[str, e.ResultTp] = {}
            for lbl, (t_comp_tp, t_comp_card) in l_prop.val.items():
                linkprop_ck[lbl] = e.ResultTp(
                    object_tp_comp_name_resolve(
                        root_ctx=root_ctx,
                        tp_comp=t_comp_tp,
                    ),
                    t_comp_card,
                )

            return e.NamedNominalLinkTp(
                name=name_ck, linkprop=e.ObjectTp(linkprop_ck)
            )
        case e.NominalLinkTp(subject=_, name=name, linkprop=l_prop):
            raise ValueError(
                "No nominal link tp should appear in name resolution", tp_comp
            )
        case e.UncheckedComputableTp(expr=c_expr):
            return tp_comp
        case e.ComputableTp(expr=c_expr, tp=c_tp):
            return e.ComputableTp(
                expr=c_expr, tp=object_tp_comp_name_resolve(root_ctx, c_tp)
            )
        case e.DefaultTp(expr=c_expr, tp=c_tp):
            return e.DefaultTp(
                expr=c_expr, tp=object_tp_comp_name_resolve(root_ctx, c_tp)
            )
        case e.OverloadedTargetTp(linkprop=linkprop):
            assert linkprop is not None
            return e.OverloadedTargetTp(
                linkprop=e.ObjectTp(
                    {
                        lbl: e.ResultTp(
                            object_tp_comp_name_resolve(root_ctx, t_comp_tp),
                            t_comp_card,
                        )
                        for lbl, (
                            t_comp_tp,
                            t_comp_card,
                        ) in linkprop.val.items()
                    }
                )
            )
        case e.UnionTp(l, r):
            return e.UnionTp(
                object_tp_comp_name_resolve(root_ctx, l),
                object_tp_comp_name_resolve(root_ctx, r),
            )
        case e.CompositeTp(kind=kind, tps=tps, labels=labels):
            return e.CompositeTp(
                kind=kind,
                tps=[object_tp_comp_name_resolve(root_ctx, t) for t in tps],
                labels=labels,
            )
        case e.SomeTp(_):
            return tp_comp
        case e.AnyTp(_):
            return tp_comp
        case _:
            raise ValueError("Not Implemented", tp_comp)


def fun_arg_ret_type_name_resolve(
    root_ctx: e.TcCtx,
    tp: e.FunArgRetType,
) -> e.FunArgRetType:
    return e.FunArgRetType(
        args_tp=[object_tp_comp_name_resolve(root_ctx, t) for t in tp.args_tp],
        args_mod=tp.args_mod,
        args_label=tp.args_label,
        ret_tp=e.ResultTp(
            object_tp_comp_name_resolve(root_ctx, tp.ret_tp.tp), tp.ret_tp.mode
        ),
    )


def func_def_name_resolve(
    root_ctx: e.TcCtx,
    func_def: e.FuncDef,
) -> e.FuncDef:
    match func_def:
        case e.DefinedFuncDef(tp=tp, impl=impl, defaults=defaults):
            return e.DefinedFuncDef(
                tp=fun_arg_ret_type_name_resolve(root_ctx, tp),
                impl=impl,
                defaults=defaults,
            )
        case e.BuiltinFuncDef(tp=tp, impl=impl, defaults=defaults):
            # do not check validity for builtin funcs
            return e.BuiltinFuncDef(tp=tp, impl=impl, defaults=defaults)
        case _:
            raise ValueError("Not Implemented", func_def)


def module_name_resolve(
    dbschema: e.DBSchema, module_name: Tuple[str, ...]
) -> None:
    """
    Modifies the db schema after checking
    """

    def f(
        root_ctx: e.TcCtx,
        subject_tp: e.Tp,
        tp_comp: e.Tp,
        tp_comp_card: e.CMMode,
    ) -> e.Tp:
        return object_tp_comp_name_resolve(root_ctx, tp_comp)

    mck.unchecked_module_map(dbschema, module_name, f, func_def_name_resolve)


def checked_module_name_resolve(
    dbschema: e.DBSchema, module_name: Tuple[str, ...]
) -> None:
    """
    Modifies the db schema after checking
    """
    assert module_name not in dbschema.unchecked_modules
    dbschema.unchecked_modules[module_name] = dbschema.modules[module_name]
    del dbschema.modules[module_name]
    module_name_resolve(dbschema, module_name)
    assert module_name not in dbschema.modules
    dbschema.modules[module_name] = dbschema.unchecked_modules[module_name]
    del dbschema.unchecked_modules[module_name]
