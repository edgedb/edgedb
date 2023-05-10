.. _ref_std_sequence:

=========
Sequences
=========

.. list-table::
    :class: funcoptable

    * - :eql:type:`sequence`
      - Auto-incrementing sequence of :eql:type:`int64`.

    * - :eql:func:`sequence_next`
      - :eql:func-desc:`sequence_next`

    * - :eql:func:`sequence_reset`
      - :eql:func-desc:`sequence_reset`


----------


.. eql:type:: std::sequence

    An auto-incrementing sequence of :eql:type:`int64`.

    This type can be used to create auto-incrementing :ref:`properties
    <ref_datamodel_props>`:

    .. code-block:: sdl
        :version-lt: 3.0

        scalar type TicketNo extending sequence;

        type Ticket {
            property number -> TicketNo {
                constraint exclusive;
            }
        }

    .. code-block:: sdl

        scalar type TicketNo extending sequence;

        type Ticket {
            number: TicketNo {
                constraint exclusive;
            }
        }

    A sequence is bound to the scalar type, not to the property, so
    if multiple properties use the same sequence, they will
    share the same counter. For each distinct counter, a separate
    scalar type that is extending ``sequence`` should be used.


---------


.. eql:function:: std::sequence_next(seq: schema::ScalarType) -> int64

    Increments the given sequence to its next value and returns that value.

    See the note on :ref:`specifying your sequence
    <ref_std_specifying_sequence>` for best practices on supplying the *seq*
    parameter.

    Sequence advancement is done atomically; each concurrent session and
    transaction will receive a distinct sequence value.

    .. code-block:: edgeql-repl

       db> select sequence_next(introspect MySequence);
       {11}


---------


.. eql:function:: std::sequence_reset(seq: schema::ScalarType) -> int64
                  std::sequence_reset( \
                    seq: schema::ScalarType, val: int64) -> int64

    Resets a sequence to initial state or a given value, returning the value.

    See the note on :ref:`specifying your sequence
    <ref_std_specifying_sequence>` for best practices on supplying the *seq*
    parameter.

    The single-parameter form resets the sequence to its initial state, where
    the next :eql:func:`sequence_next` call will return the first value in
    sequence. The two-parameter form allows you to set the current value of the
    sequence. The next :eql:func:`sequence_next` call will return the value
    after the one you passed to :eql:func:`sequence_reset`.

    .. code-block:: edgeql-repl

       db> select sequence_reset(introspect MySequence);
       {1}
       db> select sequence_next(introspect MySequence);
       {1}
       db> select sequence_reset(introspect MySequence, 22);
       {22}
       db> select sequence_next(introspect MySequence);
       {23}


---------

.. _ref_std_specifying_sequence:

.. note::

    To specify the sequence to be operated on by either
    :eql:func:`sequence_next` or :eql:func:`sequence_reset`, you must pass a
    ``schema::ScalarType`` object. If the sequence argument is known ahead of
    time and does not change, we recommend passing it by using the
    :eql:op:`introspect` operator:

    .. code-block:: edgeql

        select sequence_next(introspect MySequenceType);
        # or
        select sequence_next(introspect typeof MyObj.seq_prop);

    This style of execution will ensure that the reference to a sequential
    type from a given expression is tracked properly to guarantee schema
    referential integrity.

    It doesn't work in every use case, though. If in your use case, the
    sequence type must be determined at run time via a query argument,
    you will need to query it from the ``schema::ScalarType`` set directly:

    .. code-block:: edgeql

        with
          SeqType := (
            select schema::ScalarType
            filter .name = <str>$seq_type_name
          )
        select
          sequence_next(SeqType);


.. warning::

    **Caution**

    To work efficiently in high concurrency without lock contention, a
    :eql:func:`sequence_next` execution is never rolled back, even if the
    containing transaction is aborted. This may result in gaps in the
    generated sequence. Likewise, the result of a :eql:func:`sequence_reset`
    call is not undone if the transaction is rolled back.
