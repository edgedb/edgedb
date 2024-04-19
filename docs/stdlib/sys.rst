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

    * - :eql:func:`sys::get_current_branch`
      - :eql:func-desc:`sys::get_current_branch`


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

        db> select sys::get_version();
        {(major := 1, minor := 0, stage := <sys::VersionStage>'alpha',
          stage_no := 1, local := [])}


----------


.. eql:function:: sys::get_version_as_str() -> str

    Return the server version as a string.

    .. code-block:: edgeql-repl

        db> select sys::get_version_as_str();
        {'1.0-alpha.1'}


----------


.. eql:function:: sys::get_transaction_isolation() -> \
                        sys::TransactionIsolation

    Return the isolation level of the current transaction.

    Possible return values are given by
    :eql:type:`sys::TransactionIsolation`.

    .. code-block:: edgeql-repl

        db> select sys::get_transaction_isolation();
        {sys::TransactionIsolation.Serializable}


----------


.. eql:function:: sys::get_current_database() -> str

    Return the name of the current database as a string.

    .. code-block:: edgeql-repl

        db> select sys::get_current_database();
        {'my_database'}

    .. versionadded:: 5.0

        In EdgeDB 5.0+, this function will return the name of the current
        database branch.


----------


.. eql:function:: sys::get_current_branch() -> str

    .. versionadded:: 5.0

    Return the name of the current database branch as a string.

    .. code-block:: edgeql-repl

        db> select sys::get_current_branch();
        {'my_branch'}


-----------


.. eql:type:: sys::TransactionIsolation

    :index: enum transaction isolation

    :eql:type:`Enum <enum>` indicating the possible transaction
    isolation modes.

    This enum only accepts a value of ``Serializable``.
