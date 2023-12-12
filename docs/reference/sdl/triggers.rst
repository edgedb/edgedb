.. versionadded:: 3.0

.. _ref_eql_sdl_triggers:

========
Triggers
========

This section describes the SDL declarations pertaining to
:ref:`triggers <ref_datamodel_triggers>`.


Example
-------

Declare a trigger that inserts a ``Log`` object for each new ``User`` object:

.. code-block:: sdl

    type User {
      required name: str;

      trigger log_insert after insert for each do (
        insert Log {
          action := 'insert',
          target_name := __new__.name
        }
      );
    }

.. versionadded:: 4.0

Declare a trigger that inserts a ``Log`` object conditionally when an update
query makes a change to a ``User`` object:

.. code-block:: sdl

    type User {
      required name: str;

      trigger log_update after update for each
      when (<json>__old__ {**} != <json>__new__ {**})
      do (
        insert Log {
          action := 'update',
          target_name := __new__.name,
          change := __old__.name ++ '->' ++ __new__.name
        }
      );
    }

.. _ref_eql_sdl_triggers_syntax:

Syntax
------

Define a new trigger corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_triggers>`.

.. sdl:synopsis::
    :version-lt: 4.0

    type <type-name> "{"
      trigger <name>
      after
        {insert | update | delete} [, ...]
        for {each | all}
        do <expr>
    "}"

.. sdl:synopsis::

    type <type-name> "{"
      trigger <name>
      after
        {insert | update | delete} [, ...]
        for {each | all}
        [ when (<condition>) ]
        do <expr>
    "}"


Description
-----------

This declaration defines a new trigger with the following options:

:eql:synopsis:`<type-name>`
    The name (optionally module-qualified) of the type to be triggered on.

:eql:synopsis:`<name>`
    The name of the trigger.

:eql:synopsis:`insert | update | delete [, ...]`
    The query type (or types) to trigger on. Separate multiple values with
    commas to invoke the same trigger for multiple types of queries.

:eql:synopsis:`each`
    The expression will be evaluated once per modified object. ``__new__`` and
    ``__old__`` in this context within the expression will refer to a single
    object.

:eql:synopsis:`all`
    The expression will be evaluted once for the entire query, even if multiple
    objects were modified. ``__new__`` and ``__old__`` in this context within
    the expression refer to sets of the modified objects.

.. versionadded:: 4.0

    :eql:synopsis:`when (<condition>)`
        Optionally provide a condition for the trigger. If the condition is
        met, the trigger will run. If not, the trigger is skipped.

:eql:synopsis:`<expr>`
    The expression to be evaluated when the trigger is invoked.

The trigger name must be distinct from that of any existing trigger
on the same type.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Triggers <ref_datamodel_triggers>`
  * - :ref:`DDL > Triggers <ref_eql_ddl_triggers>`
  * - :ref:`Introspection > Triggers <ref_datamodel_introspection_triggers>`
