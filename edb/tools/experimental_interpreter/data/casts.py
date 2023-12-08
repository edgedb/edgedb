from .data_ops import ( IntVal, StrVal,
                       Tp, Val, )


def type_cast(tp: Tp, arg: Val) -> Val:
    match (tp, arg):
        # case (DateTimeTp(), StrVal(datestr)):
        #     return DateTimeVal(datestr)
        # case (JsonTp(), StrVal(datestr)):
        #     return JsonVal(datestr)  # TODO!
        # case (StrTp(), IntVal(i)):
        #     return StrVal(str(i))  # TODO!

        case _:
            raise ValueError("cannot cast", tp, arg)
