.. versionadded:: 6.0

.. _ref_admin_statistics_update:

==============================
administer statistics_update()
==============================

:eql-statement:

Update internal statistics about data.

.. eql:synopsis::

    administer statistics_update "("
      [<type_link_or_property> [, ...]]
    ")"


Description
-----------

Updates statistics about the contents of data in the current branch.
Subsequently, the query planner uses these statistics to help determine the
most efficient execution plans for queries.

:eql:synopsis:`<type_link_or_property>`
    If a type name or a path to a link or property are specified, that data
    will be targeted for statistics update. If omitted, all user-accessible
    data will be analyzed.


Examples
--------

Update the statistics on type ``SomeType``:

.. code-block:: edgeql

    administer statistics_update(SomeType);

Update statistics of type ``SomeType`` and the link ``OtherType.ptr``.

.. code-block:: edgeql

    administer statistics_update(SomeType, OtherType.ptr);

Update statistics on everything that is user-accessible in the database:

.. code-block:: edgeql

    administer statistics_update();
