from . import data_ops as e


def type_cast(tp: e.Tp, arg: e.Val) -> e.Val:
    match (tp, arg):
        # case (DateTimeTp(), StrVal(datestr)):
        #     return DateTimeVal(datestr)
        # case (JsonTp(), StrVal(datestr)):
        #     return JsonVal(datestr)  # TODO!
        # case (StrTp(), IntVal(i)):
        #     return StrVal(str(i))  # TODO!
        case e.ScalarTp(qname), e.ScalarVal(_, v):
            match qname.names:
                case ["std", "str"]:
                    return e.StrVal(str(v))
                case _:
                    raise ValueError("cannot cast", tp, arg)
        case _:
            raise ValueError("cannot cast", tp, arg)
