.. versionadded:: 5.0

.. _ref_admin_vacuum:

======
Vacuum
======

:eql-statement:

Reclaim storage space.

.. eql:synopsis::

    administer vacuum "("
      [<type_link_or_property> [, ...]]
      [, full := {true | false}]
    ")"


Description
-----------

Cleans and reclaims storage by removing obsolete data.

:eql:synopsis:`<type_link_or_property>`
    If a type name or a path to a link or property are specified, that data
    will be targeted for the vacuum operation. If omitted, all user-accessible
    data will be targeted.

:eql:synopsis:`full := {true | false}`
    If set to ``true``, an exclusive lock is obtained and reclaimed space is
    returned to the operating system. If set to ``false`` or if not set, the
    command can operate alongside normal reading and writing of the database
    and reclaimed space is kept available for reuse in the database, reducing
    the rate of growth of the database.


Examples
--------

Vacuum the type ``SomeType``:

.. code-block:: edgeql

    administer vacuum(SomeType);

Vacuum the type ``SomeType`` and the link ``OtherType.ptr`` and return
reclaimed space to the operating system:

.. code-block:: edgeql

    administer vacuum(SomeType, OtherType.ptr, full := true);

Vacuum everything that is user-accessible in the database:

.. code-block:: edgeql

    administer vacuum();
