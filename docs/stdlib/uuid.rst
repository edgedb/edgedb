.. _ref_std_uuid:

=====
UUIDs
=====

.. list-table::
    :class: funcoptable

    * - :eql:type:`uuid`
      - UUID type

    * - :eql:op:`= <eq>` :eql:op:`\!= <neq>` :eql:op:`?= <coaleq>`
        :eql:op:`?!= <coalneq>` :eql:op:`\< <lt>` :eql:op:`\> <gt>`
        :eql:op:`\<= <lteq>` :eql:op:`\>= <gteq>`
      - Comparison operators

    * - :eql:func:`uuid_generate_v1mc`
      - :eql:func-desc:`uuid_generate_v1mc`

    * - :eql:func:`uuid_generate_v4`
      - :eql:func-desc:`uuid_generate_v4`


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

    The UUID will contain 47 random bits, 60 bits representing the
    current time, and 14 bits of clock sequence that may be used to
    ensure uniqueness. The rest of the bits indicate the version of
    the UUID.

    This is the default function used to populate the ``id`` column.

    .. code-block:: edgeql-repl

        db> select uuid_generate_v1mc();
        {1893e2b6-57ce-11e8-8005-13d4be166783}


---------


.. eql:function:: std::uuid_generate_v4() -> uuid

    Return a version 4 UUID.

    The UUID is derived entirely from random numbers: it will contain
    122 random bits and 6 version bits.

    It is permitted to override the ``default`` of the ``id`` column
    with a call to this function, but this should be done with
    caution: fully random ids will be less clustered than time-based id,
    which may lead to worse index performance.

    .. code-block:: edgeql-repl

        db> select uuid_generate_v4();
        {92673afc-9c4f-42b3-8273-afe0053f0f48}
