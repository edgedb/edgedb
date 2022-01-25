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

    Auto-incrementing sequence of :eql:type:`int64`.

    This type can be used to create auto-incrementing :ref:`properties
    <ref_datamodel_props>`.

    .. code-block:: sdl

        scalar type TicketNo extending sequence;

        type Ticket {
            property number -> TicketNo {
                constraint exclusive;
            }
        }

    The sequence is bound to the scalar type, not to the property, so
    if multiple properties use the same ``sequence`` type they will
    share the same counter. For each distinct counter, a separate
    scalar type that is extending ``sequence`` should be used.


---------


.. eql:function:: std::sequence_next(seq: schema::ScalarType) -> int64

    Advance the sequence to its next value and return that value.

    Sequence advancement is done atomically, each concurrent session and
    transaction will receive a distinct sequence value.

    .. code-block:: edgeql-repl

       db> select sequence_next(introspect MySequence);
       {11}


---------


.. eql:function:: std::sequence_reset(seq: schema::ScalarType) -> int64
                  std::sequence_reset( \
                    seq: schema::ScalarType, val: int64) -> int64

    Reset the sequence to its initial state or the specified value.

    The single-parameter form resets the sequence to its initial state, where
    the next :eql:func:`sequence_next` call will return the first value in
    sequence.  The two-parameters form allows changing the last returned
    value of the sequence.

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

.. note::

   The sequence to be operated on by the functions above is specified
   by a ``schema::ScalarType`` object.  If the sequence argument is
   known ahead of time and does not change, the recommended way to pass
   it is to use the :eql:op:`introspect` operator:

   .. code-block:: edgeql

      select sequence_next(introspect MySequenceType);
      # or
      select sequence_next(introspect typeof MyObj.seq_prop);

   This style will ensure that a reference to a sequence type from an
   expression is tracked properly to ensure schema referential integrity.

   If, on the other hand, the operated sequence type is determined at run time
   via a query argument, it must be queried from the ``schema::ScalarType``
   set directly like so:

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
   :eql:func:`sequence_next` operation is never rolled back even if
   the containing transaction is aborted.  This may result in gaps
   in the generated sequence.  Likewise, :eql:func:`sequence_reset`
   is not undone if the transaction is rolled back.
