from .data_ops import *

def type_cast(tp : Tp, arg : Val) -> Val:
    match (tp, arg):
        case (DateTimeTp(), StrVal(datestr)):
            return DateTimeVal(datestr)

    raise ValueError("cannot cast")
    

