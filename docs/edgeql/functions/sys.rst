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

    The function always returns ``true``.

    .. code-block:: edgeql-repl

        db> SELECT sys::sleep(1);
        {True}

        db> SELECT sys::sleep(<timedelta>'5 seconds');
        {True}


.. eql:function:: sys::advisory_lock(key: int64) -> bool

    Obtain an exclusive session-level advisory lock.

    Advisory locks behave like semaphores, so multiple calls
    for the same *key* stack.  If another session already holds
    a lock for the same *key*, this function will wait until
    the lock is released.

    *key* must be a non-negative integer.

    The function always returns ``true``.


.. eql:function:: sys::advisory_unlock(key: int64) -> bool

    Release an exclusive session-level advisory lock.

    The function returns ``true`` if the lock was successfully
    released, and ``false`` if the lock was not held.

    *key* must be a non-negative integer.


.. eql:function:: sys::advisory_unlock_all() -> bool

    Release all session-level advisory locks held by the current session.

    The function returns ``true`` if the lock was successfully
    released, and ``false`` if the lock was not held.

    The function always returns ``true``.
