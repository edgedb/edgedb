.. _ref_eql_functions_uuid:

====
UUID
====

:edb-alt-title: UUID Functions


.. list-table::
    :class: funcoptable

    * - :eql:func:`uuid_generate_v1mc`
      - :eql:func-desc:`uuid_generate_v1mc`


---------


.. eql:function:: std::uuid_generate_v1mc() -> uuid

    Return a version 1 UUID.

    The algorithm uses a random multicast MAC address instead of the
    real MAC address of the computer.

    .. code-block:: edgeql-repl

        db> SELECT uuid_generate_v1mc();
        {'1893e2b6-57ce-11e8-8005-13d4be166783'}
