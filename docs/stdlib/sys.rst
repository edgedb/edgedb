.. _ref_std_sys:


======
System
======

:edb-alt-title: System Functions


.. list-table::
    :class: funcoptable

    * - :eql:func:`sys::get_version`
      - :eql:func-desc:`sys::get_version`

    * - :eql:func:`sys::get_version_as_str`
      - :eql:func-desc:`sys::get_version_as_str`

    * - :eql:func:`sys::get_current_database`
      - :eql:func-desc:`sys::get_current_database`


----------


.. eql:function:: sys::get_version() -> tuple<major: int64, \
                                              minor: int64, \
                                              stage: sys::VersionStage, \
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
        {(major := 1, minor := 0, stage := <sys::VersionStage>'alpha',
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
        {sys::TransactionIsolation.RepeatableRead}


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

    This enum takes the following values: ``RepeatableRead``,
    ``Serializable``.
