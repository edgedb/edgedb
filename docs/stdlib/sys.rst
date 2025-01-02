.. _ref_std_sys:


======
System
======

:edb-alt-title: System Functions and Types


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

    * - :eql:type:`sys::Branch`
      - A read-only type representing all database branches.

    * - :eql:type:`sys::QueryStats`
      - A read-only type representing query performance statistics.

    * - :eql:func:`sys::reset_query_stats`
      - :eql:func-desc:`sys::reset_query_stats`


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


-----------


.. eql:type:: sys::Branch

    .. versionadded:: 6.0

    A read-only type representing all database branches.

    :eql:synopsis:`name -> str`
        The name of the branch.

    :eql:synopsis:`last_migration -> str`
        The name of the last migration applied to the branch.


-----------


.. eql:type:: sys::QueryStats

    .. versionadded:: 6.0

    A read-only type representing query performance statistics.

    :eql:synopsis:`branch -> sys::Branch`
        The :eql:type:`branch <sys::Branch>` this statistics entry was
        collected in.

    :eql:synopsis:`query -> str`
        Text string of a representative query.

    :eql:synopsis:`query_type -> sys::QueryType`
        The :eql:type:`type <sys::QueryType>` of the query.

    :eql:synopsis:`tag -> str`
        Query tag, commonly specifies the origin of the query, e.g 'gel/cli'
        for queries originating from the CLI.  Clients can specify a tag for
        easier query identification.

    :eql:synopsis:`stats_since -> datetime`
        Time at which statistics gathering started for this query.

    :eql:synopsis:`minmax_stats_since -> datetime`
        Time at which min/max statistics gathering started for this query
        (fields ``min_plan_time``, ``max_plan_time``, ``min_exec_time`` and
        ``max_exec_time``).

    All queries have to be planned by the backend before execution. The planned
    statements are cached (managed by the EdgeDB server) and reused if the same
    query is executed multiple times.

    :eql:synopsis:`plans -> int64`
        Number of times the query was planned in the backend.

    :eql:synopsis:`total_plan_time -> duration`
        Total time spent planning the query in the backend.

    :eql:synopsis:`min_plan_time -> duration`
        Minimum time spent planning the query in the backend. This field will
        be zero if the counter has been reset using the
        :eql:func:`sys::reset_query_stats` function with the ``minmax_only``
        parameter set to ``true`` and never been planned since.

    :eql:synopsis:`max_plan_time -> duration`
        Maximum time spent planning the query in the backend. This field will
        be zero if the counter has been reset using the
        :eql:func:`sys::reset_query_stats` function with the ``minmax_only``
        parameter set to ``true`` and never been planned since.

    :eql:synopsis:`mean_plan_time -> duration`
        Mean time spent planning the query in the backend.

    :eql:synopsis:`stddev_plan_time -> duration`
        Population standard deviation of time spent planning the query in the
        backend.

    After planning, the query is usually executed in the backend, with the
    result being forwarded to the client.

    :eql:synopsis:`calls -> int64`
        Number of times the query was executed.

    :eql:synopsis:`total_exec_time -> duration`
        Total time spent executing the query in the backend.

    :eql:synopsis:`min_exec_time -> duration`
        Minimum time spent executing the query in the backend. This field will
        be zero until this query is executed first time after reset performed
        by the :eql:func:`sys::reset_query_stats` function with the
        ``minmax_only`` parameter set to ``true``.

    :eql:synopsis:`max_exec_time -> duration`
        Maximum time spent executing the query in the backend. This field will
        be zero until this query is executed first time after reset performed
        by the :eql:func:`sys::reset_query_stats` function with the
        ``minmax_only`` parameter set to ``true``.

    :eql:synopsis:`mean_exec_time -> duration`
        Mean time spent executing the query in the backend.

    :eql:synopsis:`stddev_exec_time -> duration`
        Population standard deviation of time spent executing the query in the
        backend.

    :eql:synopsis:`rows -> int64`
        Total number of rows retrieved or affected by the query.

    The following properties are used to identify a unique statistics entry
    (together with the query text above):

    :eql:synopsis:`compilation_config -> std::json`
        The config used to compile the query.

    :eql:synopsis:`protocol_version -> tuple<major: int16, minor: int16>`
        The version of the binary protocol receiving the query.

    :eql:synopsis:`default_namespace -> str`
        The default module/schema used to compile the query.

    :eql:synopsis:`namespace_aliases -> json`
        The module aliases used to compile the query.

    :eql:synopsis:`output_format -> sys::OutputFormat`
        The :eql:type:`OutputFormat <sys::OutputFormat>` indicated in the
        binary protocol receiving the query.

    :eql:synopsis:`expect_one -> bool`
        Whether the query is expected to return exactly one row.

    :eql:synopsis:`implicit_limit -> int64`
        The implicit limit set for the query.

    :eql:synopsis:`inline_typeids -> bool`
        Whether type IDs are inlined in the query result.

    :eql:synopsis:`inline_typenames -> bool`
        Whether type names are inlined in the query result.

    :eql:synopsis:`inline_objectids -> bool`
        Whether object IDs are inlined in the query result.


-----------


.. eql:type:: sys::QueryType

    .. versionadded:: 6.0

    :eql:type:`Enum <enum>` indicating the possible query types.

    Possible values are:

    * ``EdgeQL``
    * ``SQL``


-----------


.. eql:type:: sys::OutputFormat

    .. versionadded:: 6.0

    :eql:type:`Enum <enum>` indicating the possible output formats in a binary
    protocol.

    Possible values are:

    * ``BINARY``
    * ``JSON``
    * ``JSON_ELEMENTS``
    * ``NONE``


----------


.. eql:function:: sys::reset_query_stats( \
        named only branch_name: OPTIONAL str = {}, \
        named only id: OPTIONAL uuid = {}, \
        named only minmax_only: OPTIONAL bool = false, \
    ) -> OPTIONAL datetime

    .. versionadded:: 6.0

    Discard selected query statistics gathered so far.

    Discard query statistics gathered so far corresponding to the specified
    ``branch_name`` and ``id``. If either of the parameters is not specified,
    the statistics that match with the other parameter will be reset. If no
    parameter is specified, it will discard all statistics. When ``minmax_only``
    is ``true``, only the values of minimum and maximum planning and execution
    time will be reset (i.e. ``min_plan_time``, ``max_plan_time``,
    ``min_exec_time`` and ``max_exec_time`` fields). The default value for
    ``minmax_only`` parameter is ``false``. This function returns the time of a
    reset. This time is saved to ``stats_reset`` or ``minmax_stats_since`` field
    of :eql:type:`sys::QueryStats` if the corresponding reset was actually
    performed.

    .. code-block:: edgeql-repl

        db> select sys::reset_query_stats();
        {'2021-01-01T00:00:00Z'}

        db> select sys::reset_query_stats(branch_name := 'my_branch');
        {'2021-01-01T00:00:00Z'}

        db> select sys::reset_query_stats(id := <uuid>'00000000-0000-0000-0000-000000000000');
        {'2021-01-01T00:00:00Z'}

        db> select sys::reset_query_stats(minmax_only := true);
        {'2021-01-01T00:00:00Z'}

        db> select sys::reset_query_stats(branch_name := 'no_such_branch');
        {}
