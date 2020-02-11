.. _ref_eql_functions_sys:


======
System
======

:edb-alt-title: System Functions


.. list-table::
    :class: funcoptable

    * - :eql:func:`sys::sleep`
      - :eql:func-desc:`sys::sleep`

    * - :eql:func:`sys::advisory_lock`
      - :eql:func-desc:`sys::advisory_lock`

    * - :eql:func:`sys::advisory_unlock`
      - :eql:func-desc:`sys::advisory_unlock`

    * - :eql:func:`sys::advisory_unlock_all`
      - :eql:func-desc:`sys::advisory_unlock_all`

    * - :eql:func:`sys::get_version`
      - :eql:func-desc:`sys::get_version`

    * - :eql:func:`sys::get_version_as_str`
      - :eql:func-desc:`sys::get_version_as_str`

    * - :eql:func:`sys::get_current_database`
      - :eql:func-desc:`sys::get_current_database`


-----------


.. eql:function:: sys::sleep(duration: float64) -> bool
                  sys::sleep(duration: duration) -> bool

    :index: sleep delay

    Make the current session sleep for *duration* time.

    *duration* can either be a number of seconds specified
    as a floating point number, or a :eql:type:`duration`.

    The function always returns ``true``.

    .. code-block:: edgeql-repl

        db> SELECT sys::sleep(1);
        {true}

        db> SELECT sys::sleep(<duration>'5 seconds');
        {true}


----------


.. eql:function:: sys::advisory_lock(key: int64) -> bool

    Obtain an exclusive session-level advisory lock.

    Advisory locks behave like semaphores, so multiple calls
    for the same *key* stack.  If another session already holds
    a lock for the same *key*, this function will wait until
    the lock is released.

    *key* must be a non-negative integer.

    The function always returns ``true``.


----------


.. eql:function:: sys::advisory_unlock(key: int64) -> bool

    Release an exclusive session-level advisory lock.

    The function returns ``true`` if the lock was successfully
    released, and ``false`` if the lock was not held.

    *key* must be a non-negative integer.


----------


.. eql:function:: sys::advisory_unlock_all() -> bool

    Release all session-level advisory locks held by the current session.

    The function returns ``true`` if the lock was successfully
    released, and ``false`` if the lock was not held.

    The function always returns ``true``.


----------


.. eql:function:: sys::get_version() -> tuple<major: int64, \
                                              minor: int64, \
                                              stage: sys::version_stage, \
                                              stage_no: int64, \
                                              local: array<str>>

    Return the server version as a tuple.

    The ``major`` and ``minor`` elements contain the major and the minor
    components of the version; ``stage`` is an enumeration value containing
    one of ``'dev'``, ``'alpha'``, ``'beta'``, ``'rc'`` or ``'final'``;
    ``stage_no`` is the stage sequence number (e.g. ``2`` in an alpha 2
    release); and ``local`` contains an arbitrary array of local version
    identifiers.

    .. code-block:: edgeql-repl

        db> SELECT sys::get_version();
        {(major := 1, minor := 0, stage := <sys::version_stage>'alpha',
          stage_no := 1, local := [])}


----------


.. eql:function:: sys::get_version_as_str() -> str

    Return the server version as a string.

    .. code-block:: edgeql-repl

        db> SELECT sys::get_version_as_str();
        {'1.0-alpha.1'}


----------


.. eql:function:: sys::get_transaction_isolation() -> \
                        sys::TransactionIsolation

    Return the isolation level of the current transaction.

    Possible return values are given by
    :eql:type:`sys::TransactionIsolation`.

    .. code-block:: edgeql-repl

        db> SELECT sys::get_transaction_isolation();
        {<enum>'REPEATABLE READ'}


----------


.. eql:function:: sys::get_current_database() -> str

    Return the name of the current database as a string.

    .. code-block:: edgeql-repl

        db> SELECT sys::get_current_database();
        {'my_database'}


-----------


.. eql:type:: sys::TransactionIsolation

    :index: enum transaction isolation

    :eql:type:`Enum <enum>` indicating the possible transaction
    isolation modes.

    This enum takes the following values: ``REPEATABLE READ``,
    ``SERIALIZABLE``.
