.. versionadded:: 3.0

.. _ref_datamodel_triggers:

========
Triggers
========

Triggers allow you to define an expression to be executed whenever a given
query type is run on an object type. The original query will *trigger* your
pre-defined expression to run along with the original query. These can be
defined in your schema.

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
on the ``User`` type:

.. code-block:: sdl

    type User {
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

Now, whenever we run a query, we get a log entry as well:

.. lint-off

.. code-block:: edgeql-repl

    db> insert User {name := 'Jonathan Harker'};
    {default::User {id: b4d4e7e6-bd19-11ed-8363-1737d8d4c3c3}}
    db> select Log {action, timestamp, target_name, change};
    {default::Log {action: 'insert', timestamp: <datetime>'2023-03-07T18:56:02.403817Z', target_name: 'Jonathan Harker', change: {}}}
    db> update User filter .name = 'Jonathan Harker' set {name := 'Mina Murray'};
    {default::User {id: b4d4e7e6-bd19-11ed-8363-1737d8d4c3c3}}
    db> select Log {action, timestamp, target_name, change};
    {
      default::Log {action: 'insert', timestamp: <datetime>'2023-03-07T18:56:02.403817Z', target_name: 'Jonathan Harker', change: {}},
      default::Log {action: 'update', timestamp: <datetime>'2023-03-07T18:56:39.520889Z', target_name: 'Mina Murray', change: 'Jonathan Harker->Mina Murray'},
    }
    db> delete User filter .name = 'Mina Murray';
    {default::User {id: b4d4e7e6-bd19-11ed-8363-1737d8d4c3c3}}
    db> select Log {action, timestamp, target_name, change};
    {
      default::Log {action: 'insert', timestamp: <datetime>'2023-03-07T18:56:02.403817Z', target_name: 'Jonathan Harker', change: {}},
      default::Log {action: 'update', timestamp: <datetime>'2023-03-07T18:56:39.520889Z', target_name: 'Mina Murray', change: 'Jonathan Harker->Mina Murray'},
      default::Log {action: 'delete', timestamp: <datetime>'2023-03-07T19:00:52.636084Z', target_name: 'Mina Murray', change: {}},
    }

.. lint-on

.. note::

    Triggers cannot be used to modify the object that set off the trigger. This
    functionality will be addressed by the upcoming :eql:gh:`mutation rewrites
    <#4937>` feature.



.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Triggers <ref_eql_sdl_triggers>`
  * - :ref:`DDL > Triggers <ref_eql_ddl_triggers>`
  * - :ref:`Introspection > Triggers <ref_datamodel_introspection_triggers>`
