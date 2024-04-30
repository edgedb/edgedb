from typing import Tuple, Dict, Sequence, List

from ..data import data_ops as e
from ..data import expr_ops as eops
from ..data import module_ops as mops
from ..data import path_factor as path_factor
from ..data import expr_to_str as pp


def merge_result_tp(ctx: e.TcCtx, l: e.ResultTp, r: e.ResultTp) -> e.ResultTp:
    if l.mode != r.mode:
        raise ValueError("Cardinality mismatch", l, r)
    match l.tp, r.tp:
        case e.NamedNominalLinkTp(
            name=l_name, linkprop=l_linkprop
        ), e.NamedNominalLinkTp(name=r_name, linkprop=r_linkprop):
            if l_name != r_name:
                raise ValueError("Named nominal link tp name mismatch", l, r)
            new_link_prop: Dict[str, e.ResultTp] = {}
            for lbl, (l_comp_tp, l_comp_card) in l_linkprop.val.items():
                new_link_prop[lbl] = e.ResultTp(l_comp_tp, l_comp_card)
            for lbl, (r_comp_tp, r_comp_card) in r_linkprop.val.items():
                if lbl not in new_link_prop:
                    new_link_prop[lbl] = e.ResultTp(r_comp_tp, r_comp_card)
                else:
                    new_link_prop[lbl] = merge_result_tp(
                        ctx,
                        new_link_prop[lbl],
                        e.ResultTp(r_comp_tp, r_comp_card),
                    )
            return e.ResultTp(
                e.NamedNominalLinkTp(
                    name=l_name, linkprop=e.ObjectTp(new_link_prop)
                ),
                l.mode,
            )
        case e.NamedNominalLinkTp(
            name=l_name, linkprop=l_linkprop
        ), e.OverloadedTargetTp(linkprop=r_linkprop):
            assert r_linkprop is not None
            return merge_result_tp(
                ctx,
                l,
                e.ResultTp(
                    e.NamedNominalLinkTp(name=l_name, linkprop=r_linkprop),
                    r.mode,
                ),
            )

        case _:
            if l.tp != r.tp:
                raise ValueError(
                    "Type mismatch", pp.show_result_tp(l), pp.show_result_tp(r)
                )
            return l


def copy_construct_inheritance(
    ctx: e.TcCtx,
    typedef: e.ObjectTp,
    super_types: List[e.QualifiedName],
    constraints: Sequence[e.Constraint],
    indexes: Sequence[Sequence[str]],
) -> Tuple[e.ObjectTp, Sequence[e.Constraint], Sequence[Sequence[str]]]:

    definitions = [
        mops.resolve_type_def(ctx, super_type) for super_type in super_types
    ]
    final_tp_dict: Dict[str, e.ResultTp] = {}
    final_constraints: List[e.Constraint] = [*constraints]
    final_indexes: List[Sequence[str]] = [*indexes]
    for i, mdef in enumerate(definitions):
        definition = mdef.typedef
        super_constraint = mdef.constraints
        super_indexes = mdef.indexes
        assert isinstance(definition, e.ObjectTp)
        def_dep = ctx.schema.subtyping_relations[super_types[i]]
        definition_ck, constraints_ck, indexes_ck = copy_construct_inheritance(
            ctx, definition, def_dep, super_constraint, super_indexes
        )

        for lbl, (t_comp_tp, t_comp_card) in definition_ck.val.items():
            if lbl not in final_tp_dict:
                final_tp_dict[lbl] = e.ResultTp(t_comp_tp, t_comp_card)
            else:
                final_tp_dict[lbl] = merge_result_tp(
                    ctx, final_tp_dict[lbl], e.ResultTp(t_comp_tp, t_comp_card)
                )
        final_constraints = [
            *final_constraints,
            *(
                c
                for c in constraints_ck
                if isinstance(c, e.ExclusiveConstraint) and c.delegated
            ),
        ]
        final_indexes = [*final_indexes, *indexes_ck]

    for lbl, (t_comp_tp, t_comp_card) in typedef.val.items():
        if lbl not in final_tp_dict:
            final_tp_dict[lbl] = e.ResultTp(t_comp_tp, t_comp_card)
        else:
            final_tp_dict[lbl] = merge_result_tp(
                ctx, final_tp_dict[lbl], e.ResultTp(t_comp_tp, t_comp_card)
            )
    return e.ObjectTp(final_tp_dict), final_constraints, final_indexes


def module_inheritance_populate(
    dbschema: e.DBSchema, module_name: Tuple[str, ...]
) -> None:
    """
    Modifies the db schema after checking
    """
    result_vals: Dict[str, e.ModuleEntity] = {}
    dbmodule = dbschema.unchecked_modules[module_name]
    for t_name, t_me in dbmodule.defs.items():
        root_ctx = eops.emtpy_tcctx_from_dbschema(dbschema, module_name)
        match t_me:
            case e.ModuleEntityTypeDef(
                typedef=typedef,
                is_abstract=is_abstract,
                constraints=constraints,
                indexes=indexes,
            ):
                if isinstance(typedef, e.ObjectTp):
                    if (
                        e.QualifiedName([*module_name, t_name])
                        in dbschema.subtyping_relations
                    ):
                        new_typedef, new_constraints, new_indexes = (
                            copy_construct_inheritance(
                                root_ctx,
                                typedef,
                                dbschema.subtyping_relations[
                                    e.QualifiedName([*module_name, t_name])
                                ],
                                constraints,
                                indexes,
                            )
                        )

                        result_vals = {
                            **result_vals,
                            t_name: e.ModuleEntityTypeDef(
                                typedef=new_typedef,
                                is_abstract=is_abstract,
                                constraints=new_constraints,
                                indexes=new_indexes,
                            ),
                        }
                    else:
                        result_vals = {**result_vals, t_name: t_me}
                elif isinstance(typedef, e.ScalarTp):
                    # insert assignment casts
                    assert isinstance(
                        typedef.name, e.QualifiedName
                    ), "Name resolution should have been done"
                    assert typedef.name == e.QualifiedName(
                        [*module_name, t_name]
                    )
                    assert (
                        typedef.name
                        not in dbschema.unchecked_subtyping_relations
                    )
                    for parent_name in dbschema.subtyping_relations[
                        typedef.name
                    ]:

                        def default_cast_fun(v):
                            return v

                        cast_key = (
                            e.ScalarTp(parent_name),
                            e.ScalarTp(typedef.name),
                        )
                        assert cast_key not in dbschema.casts
                        dbschema.casts[cast_key] = e.TpCast(
                            e.TpCastKind.Assignment, default_cast_fun
                        )
                    result_vals = {**result_vals, t_name: t_me}
                else:
                    raise ValueError("Not Implemented", typedef)
            case e.ModuleEntityFuncDef(funcdefs=funcdefs):
                result_vals = {
                    **result_vals,
                    t_name: e.ModuleEntityFuncDef(funcdefs=funcdefs),
                }
            case _:
                raise ValueError("Unimplemented", t_me)
    dbschema.unchecked_modules[module_name] = e.DBModule(result_vals)


def module_subtyping_resolve(dbschema: e.DBSchema) -> None:
    for qname, rname_list in dbschema.unchecked_subtyping_relations.items():
        if qname in dbschema.subtyping_relations:
            raise ValueError("Duplicate subtyping relation", qname)
        rname_ck_list = []
        for cur_module, rname in rname_list:
            resolved_name, _ = mops.resolve_raw_name_and_type_def(
                e.TcCtx(dbschema, cur_module, {}), rname
            )
            rname_ck_list.append(resolved_name)
        dbschema.subtyping_relations[qname] = rname_ck_list

    # remove everything from unchecked_subtyping_relations
    dbschema.unchecked_subtyping_relations.clear()
