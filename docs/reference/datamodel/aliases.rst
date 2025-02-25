.. _ref_datamodel_aliases:

=======
Aliases
=======

.. index:: alias, virtual type

You can think of *aliases* as a way to give schema names to arbitrary EdgeQL
expressions. You can later refer to aliases in queries and in other aliases.

Aliases are functionally equivalent to expression aliases defined in EdgeQL
statements in :ref:`with block <ref_eql_statements_with>`, but are available
to all queries using the schema and can be introspected.

Like computed properties, the aliased expression is evaluated on the fly
whenever the alias is referenced.


Scalar alias
============

.. code-block:: sdl

  # in your schema:
  alias digits := {0,1,2,3,4,5,6,7,8,9};

Later, in some query:

.. code-block:: edgeql

  select count(digits);


Object type alias
=================

The name of a given object type (e.g. ``User``) is itself a pointer to the *set
of all User objects*. After declaring the alias below, you can use ``User`` and
``UserAlias`` interchangeably:

.. code-block:: sdl

  alias UserAlias := User;

Object type alias with computeds
================================

Object type aliases can include a *shape* that declares additional computed
properties or links:

.. code-block:: sdl

  type Post {
    required title: str;
  }

  alias PostWithTrimmedTitle := Post {
    trimmed_title := str_trim(.title)
  }

Later, in some query:

.. code-block:: edgeql

  select PostWithTrimmedTitle {
    trimmed_title
  };

Arbitrary expressions
=====================

Aliases can correspond to any arbitrary EdgeQL expression, including entire
queries.

.. code-block:: sdl

  # Tuple alias
  alias Color := ("Purple", 128, 0, 128);

  # Named tuple alias
  alias GameInfo := (
    name := "Li Europan Lingues",
    country := "Iceland",
    date_published := 2023,
    creators := (
      (name := "Bob Bobson", age := 20),
      (name := "Trina Trinadóttir", age := 25),
    ),
  );

  type BlogPost {
    required title: str;
    required is_published: bool;
  }

  # Query alias
  alias PublishedPosts := (
    select BlogPost
    filter .is_published = true
  );

.. note::

  All aliases are reflected in the database's built-in :ref:`GraphQL schema
  <ref_graphql_index>`.


.. _ref_eql_sdl_aliases:
.. _ref_eql_sdl_aliases_syntax:

Defining aliases
================

Syntax
------

Define a new alias corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_aliases>`.

.. sdl:synopsis::

  alias <alias-name> := <alias-expr> ;

  alias <alias-name> "{"
      using <alias-expr>;
      [ <annotation-declarations> ]
  "}" ;

Where:

:eql:synopsis:`<alias-name>`
  The name (optionally module-qualified) of an alias to be created.

:eql:synopsis:`<alias-expr>`
  The aliased expression.  Must be a :ref:`Stable <ref_reference_volatility>`
  EdgeQL expression.

The valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
  Set alias :ref:`annotation <ref_eql_sdl_annotations>`
  to a given *value*.


.. _ref_eql_ddl_aliases:

DDL commands
============

This section describes the low-level DDL commands for creating and
dropping aliases. You typically don't need to use these commands
directly, but knowing about them is useful for reviewing migrations.

Create alias
------------

:eql-statement:
:eql-haswith:

Define a new alias in the schema.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  create alias <alias-name> := <alias-expr> ;

  [ with <with-item> [, ...] ]
  create alias <alias-name> "{"
      using <alias-expr>;
      [ create annotation <attr-name> := <attr-value>; ... ]
  "}" ;

  # where <with-item> is:

  [ <module-alias> := ] module <module-name>

Parameters
^^^^^^^^^^

Most sub-commands and options of this command are identical to the
:ref:`SDL alias declaration <ref_eql_sdl_aliases_syntax>`, with some
additional features listed below:

:eql:synopsis:`[ <module-alias> := ] module <module-name>`
  An optional list of module alias declarations to be used in the
  alias definition.

:eql:synopsis:`create annotation <annotation-name> := <value>;`
  An optional list of annotation values for the alias.
  See :eql:stmt:`create annotation` for details.

Example
^^^^^^^

Create a new alias:

.. code-block:: edgeql

  create alias Superusers := (
      select User filter User.groups.name = 'Superusers'
  );


Drop alias
----------

:eql-statement:
:eql-haswith:

Remove an alias from the schema.

.. eql:synopsis::

  [ with <with-item> [, ...] ]
  drop alias <alias-name> ;

Parameters
^^^^^^^^^^

*alias-name*
  The name (optionally qualified with a module name) of an existing
  expression alias.

Example
^^^^^^^

Remove an alias:

.. code-block:: edgeql

  drop alias SuperUsers;
