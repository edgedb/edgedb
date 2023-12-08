from .data_ops import (DateTimeVal, IntVal, StrVal,
                       Tp, Val, JsonVal)


def type_cast(tp: Tp, arg: Val) -> Val:
    match (tp, arg):
        case (DateTimeTp(), StrVal(datestr)):
            return DateTimeVal(datestr)
        case (JsonTp(), StrVal(datestr)):
            return JsonVal(datestr)  # TODO!
        case (StrTp(), IntVal(i)):
            return StrVal(str(i))  # TODO!

    raise ValueError("cannot cast", tp, arg)
