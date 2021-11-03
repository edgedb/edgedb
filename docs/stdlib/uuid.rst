.. _ref_std_uuid:

====
UUID
====

.. list-table::
    :class: funcoptable

    * - :eql:type:`uuid`
      - UUID type

    * - :eql:op:`= <EQ>` :eql:op:`\!= <NEQ>` :eql:op:`?= <COALEQ>`
        :eql:op:`?!= <COALNEQ>` :eql:op:`\< <LT>` :eql:op:`\> <GT>`
        :eql:op:`\<= <LTEQ>` :eql:op:`\>= <GTEQ>`
      - Comparison operators

    * - :eql:func:`uuid_generate_v1mc`
      - :eql:func-desc:`uuid_generate_v1mc`


---------


.. eql:type:: std::uuid

    Universally Unique Identifiers (UUID).

    For formal definition see RFC 4122 and ISO/IEC 9834-8:2005.

    Every :eql:type:`Object` has a globally unique property ``id``
    represented by a UUID value.


---------


.. eql:function:: std::uuid_generate_v1mc() -> uuid

    Return a version 1 UUID.

    The algorithm uses a random multicast MAC address instead of the
    real MAC address of the computer.

    .. code-block:: edgeql-repl

        db> SELECT uuid_generate_v1mc();
        {'1893e2b6-57ce-11e8-8005-13d4be166783'}
