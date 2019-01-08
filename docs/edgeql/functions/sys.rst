.. _ref_eql_functions_sys:


======
System
======

.. eql:function:: sys::sleep(duration: float64) -> bool
                  sys::sleep(duration: timedelta) -> bool

    :index: sleep delay

    Make the current session sleep for *duration* time.

    *duration* can either be a number of seconds specified
    as a floating point number, or a :eql:type:`std::timedelta`.

    .. code-block:: edgeql-repl

        db> SELECT sys::sleep(1);
        {True}

        db> SELECT sys::sleep(<timedelta>'5 seconds');
        {True}
