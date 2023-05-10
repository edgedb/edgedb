.. versionadded:: 3.0

.. _ref_datamodel_introspection_triggers:

=========
Triggers
=========

This section describes introspection of :ref:`triggers
<ref_datamodel_triggers>`.

Introspection of ``schema::Trigger``:

.. code-block:: edgeql-repl

    db> with module schema
    ... select ObjectType {
    ...     name,
    ...     links: {
    ...         name,
    ...     },
    ...     properties: {
    ...         name,
    ...     }
    ... } filter .name = 'schema::Trigger';
    {
      schema::ObjectType {
        name: 'schema::Trigger',
        links: {
          schema::Link {name: 'subject'},
          schema::Link {name: '__type__'},
          schema::Link {name: 'ancestors'},
          schema::Link {name: 'bases'},
          schema::Link {name: 'annotations'}
        },
        properties: {
          schema::Property {name: 'inherited_fields'},
          schema::Property {name: 'computed_fields'},
          schema::Property {name: 'builtin'},
          schema::Property {name: 'internal'},
          schema::Property {name: 'name'},
          schema::Property {name: 'id'},
          schema::Property {name: 'abstract'},
          schema::Property {name: 'is_abstract'},
          schema::Property {name: 'final'},
          schema::Property {name: 'is_final'},
          schema::Property {name: 'timing'},
          schema::Property {name: 'kinds'},
          schema::Property {name: 'scope'},
          schema::Property {name: 'expr'},
        },
      },
    }

Introspection of a trigger named ``log_insert`` on the ``User`` type:

.. lint-off

.. code-block:: edgeql-repl

    db> with module schema
    ... select Trigger {
    ...   name,
    ...   kinds,
    ...   timing,
    ...   scope,
    ...   expr,
    ...   subject: {
    ...     name
    ...   }
    ... } filter .name = 'log_insert';
    {
      schema::Trigger {
        name: 'log_insert',
        kinds: {Insert},
        timing: After,
        scope: Each,
        expr: 'insert default::Log { action := \'insert\', target_name := __new__.name }',
        subject: schema::ObjectType {name: 'default::User'},
      },
    }

.. lint-on


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Triggers <ref_datamodel_triggers>`
  * - :ref:`SDL > Triggers <ref_eql_sdl_triggers>`
  * - :ref:`DDL > Triggers <ref_eql_ddl_triggers>`
