.. _ref_datamodel_aliases:

=======
Aliases
=======

.. important::

  This section assumes a basic understanding of EdgeQL. If you aren't familiar
  with it, feel free to skip this page for now.


An **alias** is a *pointer* to a set of values. This set is defined with an
arbitrary EdgeQL expression.

Like computed properties, this expression is evaluated on the fly whenever the
alias is referenced in a query. Unlike computed properties, aliases are 
defined independent of an object type; they are standalone expressions.
As such, aliases are fairly open ended. Some examples are:

**Scalar alias**

.. code-block:: sdl

  alias digits := {0,1,2,3,4,5,6,7,8,9};

**Object type alias**

The name of a given object type (e.g. ``User``) is itself a pointer to the *set
of all User objects*. After declaring the alias below, you can use ``User`` and
``UserAlias`` interchangably.

.. code-block:: sdl

  alias UserAlias := User;

**Object type alias with computeds**

Object type aliases can include a *shape* that declare additional computed
properties or links.

.. code-block:: sdl
    :version-lt: 3.0

    type Post {
      required property title -> str;
    }

    alias PostAlias := Post {
      trimmed_title := str_trim(.title)
    }

.. code-block:: sdl

    type Post {
      required title: str;
    }

    alias PostAlias := Post {
      trimmed_title := str_trim(.title)
    }

In effect, this creates a *virtual subtype* of the base type, which can be
referenced in queries just like any other type.

**Other arbitrary expressions**

Aliases can correspond to any arbitrary EdgeQL expression, including entire
queries.

.. code-block:: sdl
    :version-lt: 3.0

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
      required property title -> str;
      required property is_published -> bool;
    }

    # Query alias
    alias PublishedPosts := (
      select BlogPost
      filter .is_published = true
    );

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



.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Aliases <ref_eql_sdl_aliases>`
  * - :ref:`DDL > Aliases <ref_eql_ddl_aliases>`
  * - :ref:`Cheatsheets > Aliases <ref_cheatsheet_aliases>`
