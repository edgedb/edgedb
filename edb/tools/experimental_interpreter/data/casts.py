from . import data_ops as e
from . import val_to_json as v2j


def type_cast(tp: e.Tp, arg: e.Val) -> e.Val:
    match (tp, arg):
        case e.ScalarTp(qname), e.ScalarVal(_, v):
            match qname.names:
                case ["std", "str"]:
                    return e.StrVal(str(v))
                case ["std", "datetime"]:
                    assert isinstance(v, str)
                    return e.ScalarVal(tp=e.ScalarTp(qname), val=v)
                case ["std", "int64"]:
                    return e.IntVal(int(v))
                case _:
                    raise ValueError("cannot cast", tp, arg)
        case _:
            raise ValueError("cannot cast", tp, arg)


def get_json_cast(source_tp: e.Tp, schema: e.DBSchema) -> e.TpCast:

    def cast_to_json(arg: e.Val) -> e.Val:
        return e.ScalarVal(
            tp=e.ScalarTp(e.QualifiedName(["std", "json"])),
            val=v2j.typed_val_to_json_like(arg, source_tp, schema),
        )

    return e.TpCast(e.TpCastKind.Explicit, cast_to_json)
