.. _ref_eql_statements_describe:

Describe
========

:eql-statement:

``describe`` -- provide human-readable description of a schema or a
schema object

.. eql:synopsis::

    describe schema [ as {ddl | sdl | test [ verbose ]} ];

    describe <schema-type> <name> [ as {ddl | sdl | text [ verbose ]} ];

    # where <schema-type> is one of

      object
      annotation
      constraint
      function
      link
      module
      property
      scalar type
      type

Description
-----------

``describe`` generates a human-readable description of a schema object.

The output of a ``describe`` command is a :eql:type:`str` , although
it cannot be used as an expression in queries.

There are three output formats to choose from:

:eql:synopsis:`as ddl`
    Provide a valid :ref:`DDL <ref_eql_ddl>` definition.

    The :ref:`DDL <ref_eql_ddl>` generated is a complete valid
    definition of the particular schema object assuming all the other
    referenced schema objects already exist.

    This is the default format.

:eql:synopsis:`as sdl`
    Provide an :ref:`SDL <ref_eql_sdl>` definition.

    The :ref:`SDL <ref_eql_sdl>` generated is a complete valid
    definition of the particular schema object assuming all the other
    referenced schema objects already exist.

:eql:synopsis:`as text [verbose]`
    Provide a human-oriented definition.

    The human-oriented definition generated is similar to :ref:`SDL
    <ref_eql_sdl>`, but it includes all the details that are inherited
    (if any).

    The :eql:synopsis:`verbose` mode enables displaying additional
    details, such as :ref:`annotations <ref_datamodel_annotations>`
    and :ref:`constraints <ref_datamodel_constraints>`, which are
    otherwise omitted.

When the ``describe`` command is used with the :eql:synopsis:`schema`
the result is a definition of the entire database schema. Only the
:eql:synopsis:`as ddl` option is available for schema description.

The ``describe`` command can specify the type of schema object that it
should generate the description of:

:eql:synopsis:`object <name>`
    Match any module level schema object with the specified *name*.

    This is the most general use of the ``describe`` command. It does
    not match :ref:`modules <ref_datamodel_modules>` (and other
    globals that cannot be uniquely identified just by the name).

:eql:synopsis:`annotation <name>`
    Match only :ref:`annotations <ref_datamodel_annotations>` with the
    specified *name*.

:eql:synopsis:`constraint <name>`
    Match only :ref:`constraints <ref_datamodel_constraints>` with the
    specified *name*.

:eql:synopsis:`function <name>`
    Match only :ref:`functions <ref_datamodel_functions>` with the
    specified *name*.

:eql:synopsis:`link <name>`
    Match only :ref:`links <ref_datamodel_links>` with the specified *name*.

:eql:synopsis:`module <name>`
    Match only :ref:`modules <ref_datamodel_modules>` with the
    specified *name*.

:eql:synopsis:`property <name>`
    Match only :ref:`properties <ref_datamodel_props>` with the
    specified *name*.

:eql:synopsis:`scalar type <name>`
    Match only :ref:`scalar types <ref_datamodel_scalar_types>` with the
    specified *name*.

:eql:synopsis:`type <name>`
    Match only :ref:`object types <ref_datamodel_object_types>` with the
    specified *name*.


Examples
--------

Consider the following schema:

.. code-block:: sdl
    :version-lt: 3.0

    abstract type Named {
        required property name -> str {
            delegated constraint exclusive;
        }
    }

    type User extending Named {
        required property email -> str {
            annotation title := 'Contact email';
        }
    }

.. code-block:: sdl

    abstract type Named {
        required name: str {
            delegated constraint exclusive;
        }
    }

    type User extending Named {
        required email: str {
            annotation title := 'Contact email';
        }
    }

Here are some examples of a ``describe`` command:

.. code-block:: edgeql-repl

    db> describe object User;
    {
        "create type default::User extending default::Named {
        create required single property email -> std::str {
            create annotation std::title := 'Contact email';
        };
    };"
    }
    db> describe object User as sdl;
    {
        "type default::User extending default::Named {
        required single property email -> std::str {
            annotation std::title := 'Contact email';
        };
    };"
    }
    db> describe object User as text;
    {
        'type default::User extending default::Named {
        required single link __type__ -> schema::Type {
            readonly := true;
        };
        required single property email -> std::str;
        required single property id -> std::uuid {
            readonly := true;
        };
        required single property name -> std::str;
    };'
    }
    db> describe object User as text verbose;
    {
        "type default::User extending default::Named {
        required single link __type__ -> schema::Type {
            readonly := true;
        };
        required single property email -> std::str {
            annotation std::title := 'Contact email';
        };
        required single property id -> std::uuid {
            readonly := true;
            constraint std::exclusive;
        };
        required single property name -> std::str {
            constraint std::exclusive;
        };
    };"
    }
    db> describe schema;
    {
        "create module default if not exists;
    create abstract type default::Named {
        create required single property name -> std::str {
            create delegated constraint std::exclusive;
        };
    };
    create type default::User extending default::Named {
        create required single property email -> std::str {
            create annotation std::title := 'Contact email';
        };
    };"
    }

The ``describe`` command also warns you if there are standard library
matches that are masked by some user-defined object. Consider the
following schema:

.. code-block:: sdl

    module default {
        function len(v: tuple<float64, float64>) -> float64 using (
            select (v.0 ^ 2 + v.1 ^ 2) ^ 0.5
        );
    }

So within the ``default`` module the user-defined function ``len``
(computing the length of a vector) masks the built-ins:

.. code-block:: edgeql-repl

    db> describe function len as text;
    {
      'function default::len(v: tuple<std::float64, std::float64>) ->
    std::float64 using (select
        (((v.0 ^ 2) + (v.1 ^ 2)) ^ 0.5)
    );

    # The following builtins are masked by the above:

    # function std::len(array: array<anytype>) ->  std::int64 {
    #     volatility := \'Immutable\';
    #     annotation std::description := \'A polymorphic function to calculate
    a "length" of its first argument.\';
    #     using sql $$
    #     SELECT cardinality("array")::bigint
    #     $$
    # ;};
    # function std::len(bytes: std::bytes) ->  std::int64 {
    #     volatility := \'Immutable\';
    #     annotation std::description := \'A polymorphic function to calculate
    a "length" of its first argument.\';
    #     using sql $$
    #     SELECT length("bytes")::bigint
    #     $$
    # ;};
    # function std::len(str: std::str) ->  std::int64 {
    #     volatility := \'Immutable\';
    #     annotation std::description := \'A polymorphic function to calculate
    a "length" of its first argument.\';
    #     using sql $$
    #     SELECT char_length("str")::bigint
    #     $$
    # ;};',
    }
