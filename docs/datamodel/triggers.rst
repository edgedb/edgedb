.. _ref_datamodel_triggers:
.. _ref_eql_sdl_triggers:

========
Triggers
========

.. index:: trigger, after insert, after update, after delete, for each, for all,
           when, do, __new__, __old__

Triggers allow you to define an expression to be executed whenever a given
query type is run on an object type. The original query will *trigger* your
pre-defined expression to run in a transaction along with the original query.
These can be defined in your schema.


Important notes
===============

Triggers are an advanced feature and have some caveats that
you should be aware of.

Consider using mutation rewrites
--------------------------------

Triggers cannot be used to *modify* the object that set off the trigger,
although they can be used with :eql:func:`assert` to do *validation* on
that object. If you need to modify the object, you can use :ref:`mutation
rewrites <ref_datamodel_mutation_rewrites>`.

Unified trigger query execution
-------------------------------

All queries within triggers, along with the initial triggering query, are
compiled into a single combined SQL query under the hood. Keep this in mind
when designing triggers that modify existing records. If multiple ``update``
queries within your triggers target the same object, only one of these
queries will ultimately be executed. To ensure all desired updates on an
object are applied, consolidate them into a single ``update`` query within
one trigger, instead of distributing them across multiple updates.

Multi-stage trigger execution
-----------------------------

In some cases, a trigger can cause another trigger to fire. When this
happens, Gel completes all the triggers fired by the initial query
before kicking off a new "stage" of triggers. In the second stage, any
triggers fired by the initial stage of triggers will fire. Gel will
continue adding trigger stages until all triggers are complete.

The exception to this is when triggers would cause a loop or would cause
the same trigger to be run in two different stages. These triggers will
generate an error.

Data visibility
---------------

Any query in your trigger will return the state of the database *after* the
triggering query. If this query's results include the object that flipped
the trigger, the results will contain that object in the same state as
``__new__``.


Example: audit log
==================

Here's an example that creates a simple **audit log** type so that we can keep
track of what's happening to our users in a database. First, we will create a
``Log`` type:

.. code-block:: sdl

   type Log {
     action: str;
     timestamp: datetime {
       default := datetime_current();
     }
     target_name: str;
     change: str;
   }

With the ``Log`` type in place, we can write some triggers that will
automatically create ``Log`` objects for any insert, update, or delete queries
on the ``Person`` type:

.. code-block:: sdl

   type Person {
     required name: str;

     trigger log_insert after insert for each do (
       insert Log {
         action := 'insert',
         target_name := __new__.name
       }
     );

     trigger log_update after update for each do (
       insert Log {
         action := 'update',
         target_name := __new__.name,
         change := __old__.name ++ '->' ++ __new__.name
       }
     );

     trigger log_delete after delete for each do (
       insert Log {
         action := 'delete',
         target_name := __old__.name
       }
     );
   }

In a trigger's expression, we have access to the ``__old__`` and/or ``__new__``
variables which capture the object before and after the query. Triggers on
``update`` can use both variables. Triggers on ``delete`` can use ``__old__``.
Triggers on ``insert`` can use ``__new__``.

Now, whenever we run a query, we get a log entry as well:

.. code-block:: edgeql-repl

   db> insert Person {name := 'Jonathan Harker'};
   {default::Person {id: b4d4e7e6-bd19-11ed-8363-1737d8d4c3c3}}
   db> select Log {action, timestamp, target_name, change};
   {
     default::Log {
       action: 'insert',
       timestamp: <datetime>'2023-03-07T18:56:02.403817Z',
       target_name: 'Jonathan Harker',
       change: {}
     }
   }
   db> update Person filter .name = 'Jonathan Harker'
   ... set {name := 'Mina Murray'};
   {default::Person {id: b4d4e7e6-bd19-11ed-8363-1737d8d4c3c3}}
   db> select Log {action, timestamp, target_name, change};
   {
     default::Log {
       action: 'insert',
       timestamp: <datetime>'2023-03-07T18:56:02.403817Z',
       target_name: 'Jonathan Harker',
       change: {}
     },
     default::Log {
       action: 'update',
       timestamp: <datetime>'2023-03-07T18:56:39.520889Z',
       target_name: 'Mina Murray',
       change: 'Jonathan Harker->Mina Murray'
     },
   }
   db> delete Person filter .name = 'Mina Murray';
   {default::Person {id: b4d4e7e6-bd19-11ed-8363-1737d8d4c3c3}}
   db> select Log {action, timestamp, target_name, change};
   {
     default::Log {
       action: 'insert',
       timestamp: <datetime>'2023-03-07T18:56:02.403817Z',
       target_name: 'Jonathan Harker',
       change: {}
     },
     default::Log {
       action: 'update',
       timestamp: <datetime>'2023-03-07T18:56:39.520889Z',
       target_name: 'Mina Murray',
       change: 'Jonathan Harker->Mina Murray'
     },
     default::Log {
       action: 'delete',
       timestamp: <datetime>'2023-03-07T19:00:52.636084Z',
       target_name: 'Mina Murray',
       change: {}
     },
   }

Our audit logging works, but the update logs have a major shortcoming: they
log an update even when nothing changes. Any time an ``update`` query runs,
we get a log, even if the values are the same. We can prevent that by
using the trigger's ``when`` to run the trigger conditionally. Here's a
rework of our ``update`` logging query:

.. code-block:: sdl-invalid

  trigger log_update after update for each
  when (__old__.name != __new__.name)
  do (
    insert Log {
      action := 'update',
      target_name := __new__.name,
      change := __old__.name ++ '->' ++ __new__.name
    }
  );

If this object were more complicated and we had many properties to compare,
we could use a ``json`` cast to compare them all in one shot:

.. code-block:: sdl-invalid

  trigger log_update after update for each
  when (<json>__old__ {**} != <json>__new__ {**})
  do (
    insert Log {
      action := 'update',
      target_name := __new__.name,
      change := __old__.name ++ '->' ++ __new__.name
    }
  );

You might find that one log entry per row is too granular or too noisy for your
use case. In that case, a ``for all`` trigger may be a better fit. Here's a
schema that changes the ``Log`` type so that each object can log multiple
writes by making ``target_name`` and ``change`` :ref:`multi properties
<ref_datamodel_props_cardinality>` and switches to ``for all`` triggers:

.. code-block:: sdl-diff

      type Log {
        action: str;
        timestamp: datetime {
          default := datetime_current();
        }
    -    target_name: str;
    -    change: str;
    +    multi target_name: str;
    +    multi change: str;
      }

      type Person {
        required name: str;

    -    trigger log_insert after insert for each do (
    +    trigger log_insert after insert for all do (
          insert Log {
            action := 'insert',
            target_name := __new__.name
          }
        );

    -    trigger log_update after update for each do (
    +    trigger log_update after update for all do (
          insert Log {
            action := 'update',
            target_name := __new__.name,
            change := __old__.name ++ '->' ++ __new__.name
          }
        );

    -    trigger log_delete after delete for each do (
    +    trigger log_delete after delete for all do (
          insert Log {
            action := 'delete',
            target_name := __old__.name
          }
        );
      }

Under this new schema, each query matching the trigger gets a single ``Log``
object instead of one ``Log`` object per row:

.. code-block:: edgeql-repl

   db> for name in {'Jonathan Harker', 'Mina Murray', 'Dracula'}
   ... union (
   ...   insert Person {name := name}
   ... );
   {
     default::Person {id: 3836f9c8-d393-11ed-9638-3793d3a39133},
     default::Person {id: 38370a8a-d393-11ed-9638-d3e9b92ca408},
     default::Person {id: 38370abc-d393-11ed-9638-5390f3cbd375},
   }
   db> select Log {action, timestamp, target_name, change};
   {
     default::Log {
       action: 'insert',
       timestamp: <datetime>'2023-03-07T19:12:21.113521Z',
       target_name: {'Jonathan Harker', 'Mina Murray', 'Dracula'},
       change: {},
     },
   }
   db> for change in {
   ...   (old_name := 'Jonathan Harker', new_name := 'Jonathan'),
   ...   (old_name := 'Mina Murray', new_name := 'Mina')
   ... }
   ... union (
   ...   update Person filter .name = change.old_name set {
   ...     name := change.new_name
   ...   }
   ... );
   {
     default::Person {id: 3836f9c8-d393-11ed-9638-3793d3a39133},
     default::Person {id: 38370a8a-d393-11ed-9638-d3e9b92ca408},
   }
   db> select Log {action, timestamp, target_name, change};
   {
     default::Log {
       action: 'insert',
       timestamp: <datetime>'2023-04-05T09:21:17.514089Z',
       target_name: {'Jonathan Harker', 'Mina Murray', 'Dracula'},
       change: {},
     },
     default::Log {
       action: 'update',
       timestamp: <datetime>'2023-04-05T09:35:30.389571Z',
       target_name: {'Jonathan', 'Mina'},
       change: {'Jonathan Harker->Jonathan', 'Mina Murray->Mina'},
     },
   }

Example: validation
===================

.. index:: trigger, validate, assert

Triggers may also be used for validation by calling :eql:func:`assert` inside
the trigger. In this example, the ``Person`` type has two multi links to other
``Person`` objects named ``friends`` and ``enemies``. These two links should be
mutually exclusive, so we have written a trigger to make sure there are no
common objects linked in both.

.. code-block:: sdl

   type Person {
     required name: str;
     multi friends: Person;
     multi enemies: Person;

     trigger prohibit_frenemies after insert, update for each do (
       assert(
         not exists (__new__.friends intersect __new__.enemies),
         message := "Invalid frenemies",
       )
     )
   }

With this trigger in place, it is impossible to link the same ``Person`` as
both a friend and an enemy of any other person.

.. code-block:: edgeql-repl

   db> insert Person {name := 'Quincey Morris'};
   {default::Person {id: e4a55480-d2de-11ed-93bd-9f4224fc73af}}
   db> insert Person {name := 'Dracula'};
   {default::Person {id: e7f2cff0-d2de-11ed-93bd-279780478afb}}
   db> update Person
   ... filter .name = 'Quincey Morris'
   ... set {
   ...   enemies := (
   ...     select detached Person filter .name = 'Dracula'
   ...   )
   ... };
   {default::Person {id: e4a55480-d2de-11ed-93bd-9f4224fc73af}}
   db> update Person
   ... filter .name = 'Quincey Morris'
   ... set {
   ...   friends := (
   ...     select detached Person filter .name = 'Dracula'
   ...   )
   ... };
   gel error: GelError: Invalid frenemies


Example: logging
================

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


Declaring triggers
==================

This section describes the syntax to declare a trigger in your schema.

Syntax
------

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


.. _ref_eql_ddl_triggers:

DDL commands
============

This section describes the low-level DDL commands for creating and dropping
triggers. You typically don't need to use these commands directly, but
knowing about them is useful for reviewing migrations.


Create trigger
--------------

:eql-statement:

:ref:`Define <ref_eql_sdl_triggers>` a new trigger.

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
^^^^^^^^^^^

The command ``create trigger`` nested under ``create type`` or ``alter type``
defines a new trigger for a given object type.

The trigger name must be distinct from that of any existing trigger
on the same type.

Parameters
^^^^^^^^^^

The options of this command are identical to the
:ref:`SDL trigger declaration <ref_eql_sdl_triggers_syntax>`.

Example
^^^^^^^

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
------------

:eql-statement:

Remove a trigger.

.. eql:synopsis::

   alter type <type-name> "{"
     drop trigger <name>;
   "}"

Description
^^^^^^^^^^^

The command ``drop trigger`` inside an ``alter type`` block removes the
definition of an existing trigger on the specified type.

Parameters
^^^^^^^^^^

:eql:synopsis:`<type-name>`
   The name (optionally module-qualified) of the type being triggered on.

:eql:synopsis:`<name>`
   The name of the trigger.

Example
^^^^^^^

Remove the ``log_insert`` trigger on the ``User`` type:

.. code-block:: edgeql

   alter type User {
     drop trigger log_insert;
   };


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Introspection > Triggers <ref_datamodel_introspection_triggers>`
