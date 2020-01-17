.. _ref_eql_statements_describe:

DESCRIBE
========

:eql-statement:

``DESCRIBE`` -- provide human-readable description of a schema or a
schema object

.. eql:synopsis::

    DESCRIBE SCHEMA [ AS {DDL | SDL | TEXT [ VERBOSE ]} ];

    DESCRIBE <schema-type> <name> [ AS {DDL | SDL | TEXT [ VERBOSE ]} ];

    # where <schema-type> is one of

      OBJECT
      ANNOTATION
      CONSTRAINT
      FUNCTION
      LINK
      MODULE
      PROPERTY
      SCALAR TYPE
      TYPE

Description
-----------

``DESCRIBE`` generates a human-readable description of a schema object.

The output of a ``DESCRIBE`` command is a :eql:type:`str` , although
it cannot be used as an expression in queries.

There are three output formats to choose from:

:eql:synopsis:`AS DDL`
    Provide a valid :ref:`DDL <ref_eql_ddl>` definition.

    The :ref:`DDL <ref_eql_ddl>` generated is a complete valid
    definition of the particular schema object assuming all the other
    referenced schema objects already exist.

    This is the default format.

:eql:synopsis:`AS SDL`
    Provide an :ref:`SDL <ref_eql_sdl>` definition.

    The :ref:`SDL <ref_eql_sdl>` generated is a complete valid
    definition of the particular schema object assuming all the other
    referenced schema objects already exist.

:eql:synopsis:`AS TEXT [VERBOSE]`
    Provide a human-oriented definition.

    The human-oriented definition generated is similar to :ref:`SDL
    <ref_eql_sdl>`, but it includes all the details that are inherited
    (if any).

    The :eql:synopsis:`VERBOSE` mode enables displaying additional
    details, such as :ref:`annotations <ref_datamodel_annotations>`
    and :ref:`constraints <ref_datamodel_constraints>`, which are
    otherwise omitted.

When the ``DESCRIBE`` command is used with the :eql:synopsis:`SCHEMA`
the result is a definition of the entire database schema. Only the
:eql:synopsis:`AS DDL` option is available for schema description.

The ``DESCRIBE`` command can specify the type of schema object that it
should generate the description of:

:eql:synopsis:`OBJECT <name>`
    Match any module level schema object with the specified *name*.

    This is the most general use of the ``DESCRIBE`` command. It does
    not match :ref:`modules <ref_datamodel_modules>` (and other
    globals that cannot be uniquely identified just by the name).

:eql:synopsis:`ANNOTATION <name>`
    Match only :ref:`annotations <ref_datamodel_annotations>` with the
    specified *name*.

:eql:synopsis:`CONSTRAINT <name>`
    Match only :ref:`constraints <ref_datamodel_constraints>` with the
    specified *name*.

:eql:synopsis:`FUNCTION <name>`
    Match only :ref:`functions <ref_datamodel_functions>` with the
    specified *name*.

:eql:synopsis:`LINK <name>`
    Match only :ref:`links <ref_datamodel_links>` with the specified *name*.

:eql:synopsis:`MODULE <name>`
    Match only :ref:`modules <ref_datamodel_modules>` with the
    specified *name*.

:eql:synopsis:`PROPERTY <name>`
    Match only :ref:`properties <ref_datamodel_props>` with the
    specified *name*.

:eql:synopsis:`SCALAR TYPE <name>`
    Match only :ref:`scalar types <ref_datamodel_scalar_types>` with the
    specified *name*.

:eql:synopsis:`TYPE <name>`
    Match only :ref:`object types <ref_datamodel_object_types>` with the
    specified *name*.


Examples
--------

Consider the following schema:

.. code-block:: sdl

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

Here are some examples of a ``DESCRIBE`` command:

.. code-block:: edgeql-repl

    db> DESCRIBE OBJECT User;
    {
        "CREATE TYPE default::User EXTENDING default::Named {
        CREATE REQUIRED SINGLE PROPERTY email -> std::str {
            CREATE ANNOTATION std::title := 'Contact email';
        };
    };"
    }
    db> DESCRIBE OBJECT User AS SDL;
    {
        "type default::User extending default::Named {
        required single property email -> std::str {
            annotation std::title := 'Contact email';
        };
    };"
    }
    db> DESCRIBE OBJECT User AS TEXT;
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
    db> DESCRIBE OBJECT User AS TEXT VERBOSE;
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
    db> DESCRIBE SCHEMA;
    {
        "CREATE MODULE default IF NOT EXISTS;
    CREATE ABSTRACT TYPE default::Named {
        CREATE REQUIRED SINGLE PROPERTY name -> std::str {
            CREATE DELEGATED CONSTRAINT std::exclusive;
        };
    };
    CREATE TYPE default::User EXTENDING default::Named {
        CREATE REQUIRED SINGLE PROPERTY email -> std::str {
            CREATE ANNOTATION std::title := 'Contact email';
        };
    };"
    }
