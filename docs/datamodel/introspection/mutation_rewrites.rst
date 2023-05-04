.. versionadded:: 3.0

.. _ref_datamodel_introspection_mutation_rewrites:

=================
Mutation rewrites
=================

This section describes introspection of :ref:`mutation rewrites
<ref_datamodel_mutation_rewrites>`.

Introspection of the ``schema::Rewrite``:

.. code-block:: edgeql-repl

    db> select schema::ObjectType {
    ...   name,
    ...   links: {
    ...     name
    ...   },
    ...   properties: {
    ...     name
    ...   }
    ... } filter .name = 'schema::Rewrite';
    {
      schema::ObjectType {
        name: 'schema::Rewrite',
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
          schema::Property {name: 'kind'},
          schema::Property {name: 'expr'},
        },
      },
    }

Introspection of all properties in the ``default`` schema with a mutation
rewrite:

.. code-block:: edgeql-repl

    db> select schema::ObjectType {
    ...   name,
    ...   properties := (
    ...     select .properties {
    ...        name,
    ...        rewrites: {
    ...          kind
    ...        }
    ...     } filter exists .rewrites
    ...   )
    ... } filter .name ilike 'default::%'
    ... and exists .properties.rewrites;
    {
      schema::ObjectType {
        name: 'default::Post',
        properties: {
          schema::Property {
            name: 'created',
            rewrites: {
              schema::Rewrite {
                kind: Insert
              }
            }
          },
          schema::Property {
            name: 'modified',
            rewrites: {
              schema::Rewrite {
              kind: Insert
              },
              schema::Rewrite {
                kind: Update
              }
            }
          },
        },
      },
    }

Introspection of all rewrites, including the type of query (``kind``),
rewrite expression, and the object and property they are on:

.. code-block:: edgeql-repl

    db> select schema::Rewrite {
    ...   subject := (
    ...     select .subject {
    ...       name,
    ...       source: {
    ...         name
    ...       }
    ...     }
    ...   ),
    ...   kind,
    ...   expr
    ... };
    {
      schema::Rewrite {
        subject: schema::Property {
          name: 'created',
          source: schema::ObjectType {
            name: 'default::Post'
          }
        },
        kind: Insert,
        expr: 'std::datetime_of_statement()'
      },
      schema::Rewrite {
        subject: schema::Property {
          name: 'modified',
          source: schema::ObjectType {
            name: 'default::Post'
          }
        },
        kind: Insert,
        expr: 'std::datetime_of_statement()'
      },
      schema::Rewrite {
        subject: schema::Property {
          name: 'modified',
          source: schema::ObjectType {
            name: 'default::Post'
          }
        },
        kind: Update,
        expr: 'std::datetime_of_statement()'
      },
    }

Introspection of all rewrites on a ``default::Post`` property named
``modified``:

.. code-block:: edgeql-repl

    db> select schema::Rewrite {kind, expr}
    ... filter .subject.source.name = 'default::Post'
    ... and .subject.name = 'modified';
    {
      schema::Rewrite {
        kind: Insert,
        expr: 'std::datetime_of_statement()'
      },
      schema::Rewrite {
        kind: Update,
        expr: 'std::datetime_of_statement()'
      }
    }


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Mutation rewrites <ref_datamodel_mutation_rewrites>`
  * - :ref:`SDL > Mutation rewrites <ref_eql_sdl_mutation_rewrites>`
  * - :ref:`DDL > Mutation rewrites <ref_eql_ddl_mutation_rewrites>`

