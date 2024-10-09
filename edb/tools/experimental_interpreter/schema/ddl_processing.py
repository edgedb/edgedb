from ..data import data_ops as e
from typing import List

from edb.edgeql import ast as qlast
from edb.common import debug
from .. import elaboration as elab
from ..basis.server_funcs import get_default_func_impl_for_cast
from edb.edgeql import qltypes as qltypes
from .. import elab_schema as elab_schema
from ..type_checking_tools import typechecking as tck
from ..interpreter_logging import print_warning

from .function_elaboration import process_builtin_fun_def


def process_ddl(schema: e.DBSchema, ddl: qlast.DDLOperation) -> None:
    """
    Process a single DDL operation.
    """
    # debug.dump_edgeql(ddl)
    match ddl:
        case qlast.CreateModule(
            name=qlast.ObjectRef(name=module_name), commands=[]
        ):
            schema.modules[(module_name,)] = e.DBModule({})
        case (
            qlast.CreatePseudoType()
            | qlast.CreateAnnotation()
            | qlast.AlterAnnotation()
        ):
            print_warning("WARNING: not supported yet", ddl)
        case qlast.CreateScalarType(
            name=qlast.ObjectRef(module=module_name, name=type_name),
            commands=[],
            bases=bases,
            abstract=is_abstract,
        ):
            assert (
                module_name is not None
            ), "Scalar types cannot be created in top level"
            schema.modules[(module_name,)].defs[type_name] = (
                e.ModuleEntityTypeDef(
                    e.ScalarTp(e.QualifiedName([module_name, type_name])),
                    constraints=[],
                    is_abstract=is_abstract,
                    indexes=[],
                )
            )
            # We require DDL to contain fully qualified names
            schema.subtyping_relations[
                e.QualifiedName([module_name, type_name])
            ] = []
            for base_tp in bases:
                base_elabed = elab.elab_TypeName(base_tp)
                match base_elabed:
                    # for bare ddl, we assume qualified type name
                    # is actually checked
                    case e.UncheckedTypeName(name=e.QualifiedName(_)):
                        assert isinstance(base_elabed.name, e.QualifiedName)
                        schema.subtyping_relations[
                            e.QualifiedName([module_name, type_name])
                        ].append(base_elabed.name)
                    case e.AnyTp(spec):
                        # choice: make anytype live in std
                        schema.subtyping_relations[
                            e.QualifiedName([module_name, type_name])
                        ].append(
                            e.QualifiedName(["std", "any" + (spec or "")])
                        )
                    case e.CompositeTp(kind=e.CompositeTpKind.Enum, tps=_):
                        print_warning(
                            "WARNING: behavior of extending"
                            " enum types undefined",
                            base_elabed,
                        )
                    case _:
                        raise ValueError(
                            "Must inherit from single name", base_elabed
                        )
        case qlast.CreateOperator(
            kind=_,
            params=params,
            name=name,
            returning=ret_tp,
            returning_typemod=ret_typemod,
        ):
            process_builtin_fun_def(schema, name, params, ret_tp, ret_typemod)
        case qlast.CreateFunction(
            commands=commands,
            params=params,
            name=name,
            returning=ret_tp,
            returning_typemod=ret_typemod,
        ):
            process_builtin_fun_def(schema, name, params, ret_tp, ret_typemod)

        case qlast.CreateCast(
            from_type=from_type,
            to_type=to_type,
            commands=commands,
            allow_implicit=allow_implicit,
            allow_assignment=allow_assignment,
            code=cast_code,
        ):
            from_tp = elab.elab_TypeName(from_type)
            to_tp = elab.elab_TypeName(to_type)
            from_tp_ck = tck.check_type_valid(schema, from_tp)
            to_tp_ck = tck.check_type_valid(schema, to_tp)
            assert (
                from_tp_ck,
                to_tp_ck,
            ) not in schema.casts, "duplicate casts"
            match cast_code:
                case qlast.CastCode(from_expr=from_expr, from_cast=from_cast):
                    match from_expr, from_cast:
                        case False, True:
                            cast_impl = get_default_func_impl_for_cast(
                                from_tp_ck, to_tp_ck
                            )
                        case True, False:
                            cast_impl = get_default_func_impl_for_cast(
                                from_tp_ck, to_tp_ck
                            )
                        case False, False:
                            cast_impl = get_default_func_impl_for_cast(
                                from_tp_ck, to_tp_ck
                            )
                        case _:
                            raise ValueError(
                                "TODO", cast_code, from_tp_ck, to_tp_ck
                            )
                case _:
                    raise ValueError("TODO", cast_code)
            schema.casts[(from_tp_ck, to_tp_ck)] = e.TpCast(
                (
                    e.TpCastKind.Implicit
                    if allow_implicit
                    else (
                        e.TpCastKind.Assignment
                        if allow_assignment
                        else e.TpCastKind.Explicit
                    )
                ),
                cast_impl,
            )  # TODO implicit cast
        case qlast.CreateConstraint():
            print_warning("WARNING: not supported yet", ddl)
        case qlast.CreateProperty():
            print_warning("WARNING: not supported yet", ddl)
        case qlast.CreateLink():
            print_warning("WARNING: not supported yet", ddl)

        case qlast.CreateObjectType(
            bases=bases,
            commands=commands,
            name=qlast.ObjectRef(name=name, module=module_name),
            abstract=abstract,
        ):
            assert (
                module_name is not None
            ), "Object types cannot be created in top level"
            obj_tp, constraints, indexes = elab_schema.elab_create_object_tp(
                commands
            )
            elab_schema.add_bases_for_name(schema, (module_name,), name, bases)
            schema.modules[(module_name,)].defs[name] = e.ModuleEntityTypeDef(
                obj_tp,
                is_abstract=abstract,
                constraints=constraints,
                indexes=indexes,
            )
        case qlast.AlterObjectType():
            print_warning("WARNING: not supported yet", ddl)
        case _:
            debug.dump(ddl)
            raise ValueError("DDL not yet supported", ddl)


def process_ddls(schema: e.DBSchema, ddls: List[qlast.DDLOperation]) -> None:
    for ddl in ddls:
        process_ddl(schema, ddl)
