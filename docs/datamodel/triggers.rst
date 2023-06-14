.. versionadded:: 3.0

.. _ref_datamodel_triggers:

========
Triggers
========

Triggers allow you to define an expression to be executed whenever a given
query type is run on an object type. The original query will *trigger* your
pre-defined expression to run in a transaction along with the original query.
These can be defined in your schema.

.. note::

    Triggers cannot be used to modify the object that set off the trigger. This
    functionality will be addressed by the upcoming :eql:gh:`mutation rewrites
    <#4937>` feature.

Here's an example that creates a simple audit log type so that we can keep
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

.. note::

    Any query in your trigger will return the state of the database *after* the
    triggering query. If this query's results include the object that flipped
    the trigger, the results will contain that object in the same state as
    ``__new__``.

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

.. note::

    In some cases, a trigger can cause another trigger to fire. When this
    happens, EdgeDB completes all the triggers fired by the initial query
    before kicking off a new "stage" of triggers. In the second stage, any
    triggers fired by the initial stage of triggers will fire. EdgeDB will
    continue adding trigger stages until all triggers are complete.

    The exception to this is when triggers would cause a loop or would cause
    the same trigger to be run in two different stages. These triggers will
    generate an error.

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
    -   target_name: str;
    -   change: str;
    +   multi target_name: str;
    +   multi change: str;
      }

      type Person {
        required name: str;

    -   trigger log_insert after insert for each do (
    +   trigger log_insert after insert for all do (
          insert Log {
            action := 'insert',
            target_name := __new__.name
          }
        );

    -   trigger log_update after update for each do (
    +   trigger log_update after update for all do (
          insert Log {
            action := 'update',
            target_name := __new__.name,
            change := __old__.name ++ '->' ++ __new__.name
          }
        );

    -   trigger log_delete after delete for each do (
    +   trigger log_delete after delete for all do (
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


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Triggers <ref_eql_sdl_triggers>`
  * - :ref:`DDL > Triggers <ref_eql_ddl_triggers>`
  * - :ref:`Introspection > Triggers <ref_datamodel_introspection_triggers>`
