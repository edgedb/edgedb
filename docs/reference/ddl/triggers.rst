.. versionadded:: 3.0

.. _ref_eql_ddl_triggers:

========
Triggers
========

This section describes the DDL commands pertaining to
:ref:`triggers <ref_datamodel_triggers>`.


Create trigger
==============

:eql-statement:


:ref:`Define <ref_eql_sdl_triggers>` a new trigger.

.. eql:synopsis::
    :version-lt: 4.0

    {create | alter} type <type-name> "{"
      create trigger <name>
        after
        {insert | update | delete} [, ...]
        for {each | all}
        do <expr>
    "}"

.. eql:synopsis::

    {create | alter} type <type-name> "{"
      create trigger <name>
        after
        {insert | update | delete} [, ...]
        for {each | all}
        [ when (<condition>) ]
        do <expr>
    "}"


Description
-----------

The command ``create trigger`` nested under ``create type`` or ``alter type``
defines a new trigger for a given object type.

The trigger name must be distinct from that of any existing trigger
on the same type.

Parameters
----------

The options of this command are identical to the
:ref:`SDL trigger declaration <ref_eql_sdl_triggers_syntax>`.


Example
-------

Declare a trigger that inserts a ``Log`` object for each new ``User`` object:

.. code-block:: edgeql

    alter type User {
      create trigger log_insert after insert for each do (
        insert Log {
          action := 'insert',
          target_name := __new__.name
        }
      );
    };

.. versionadded:: 4.0

    Declare a trigger that inserts a ``Log`` object conditionally when an update
    query makes a change to a ``User`` object:

    .. code-block:: edgeql

        alter type User {
          create trigger log_update after update for each
          when (<json>__old__ {**} != <json>__new__ {**})
          do (
            insert Log {
              action := 'update',
              target_name := __new__.name,
              change := __old__.name ++ '->' ++ __new__.name
            }
          );
        }


Drop trigger
============

:eql-statement:


Remove a trigger.

.. eql:synopsis::

    alter type <type-name> "{"
      drop trigger <name>;
    "}"


Description
-----------

The command ``drop trigger`` inside an ``alter type`` block removes the
definition of an existing trigger on the specified type.


Parameters
----------

:eql:synopsis:`<type-name>`
    The name (optionally module-qualified) of the type being triggered on.

:eql:synopsis:`<name>`
    The name of the trigger.


Example
-------

Remove the ``log_insert`` trigger on the ``User`` type:

.. code-block:: edgeql

    alter type User {
      drop trigger log_insert;
    };


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Triggers <ref_datamodel_triggers>`
  * - :ref:`SDL > Triggers <ref_eql_sdl_triggers>`
  * - :ref:`Introspection > Triggers <ref_datamodel_introspection_triggers>`
