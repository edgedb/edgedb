.. _ref_datamodel_computed:

=========
Computeds
=========

:edb-alt-title: Computed properties and links

.. important::

  This section assumes a basic understanding of EdgeQL. If you aren't familiar
  with it, feel free to skip this page for now.

Object types can contain *computed* properties and links. Computed properties
and links are not persisted in the database. Instead, they are evaluated *on
the fly* whenever that field is queried. Computed properties must be declared
with the ``property`` keyword and computed links must be declared with the
``link`` keyword in EdgeDB versions prior to 4.0.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      property name -> str;
      property all_caps_name := str_upper(__source__.name);
    }

.. code-block:: sdl
    :version-lt: 4.0

    type Person {
      name: str;
      property all_caps_name := str_upper(__source__.name);
    }

.. code-block:: sdl

    type Person {
      name: str;
      all_caps_name := str_upper(__source__.name);
    }

Computed fields are associated with an EdgeQL expression. This expression
can be an *arbitrary* EdgeQL query. This expression is evaluated whenever the
field is referenced in a query.

.. note::

  Computed fields don't need to be pre-defined in your schema; you can drop
  them into individual queries as well. They behave in exactly the same way.
  For more information, see the :ref:`EdgeQL > Select > Computeds
  <ref_eql_select_computeds>`.

.. warning::

  :ref:`Volatile and modifying <ref_reference_volatility>` expressions are not
  allowed in computed properties defined in schema. This means that, for
  example, your schema-defined computed property cannot call
  :eql:func:`datetime_current`, but it *can* call
  :eql:func:`datetime_of_transaction` or :eql:func:`datetime_of_statement`.
  This does *not* apply to computed properties outside of schema.

.. _ref_dot_notation:

Leading dot notation
--------------------

The example above used the special keyword ``__source__`` to refer to the
current object; it's analogous to ``this`` or ``self``  in many object-oriented
languages.

However, explicitly using ``__source__`` is optional here; inside the scope of
an object type declaration, you can omit it entirely and use the ``.<name>``
shorthand.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      property first_name -> str;
      property last_name -> str;
      property full_name := .first_name ++ ' ' ++ .last_name;
    }

.. code-block:: sdl
    :version-lt: 4.0

    type Person {
      first_name: str;
      last_name: str;
      property full_name := .first_name ++ ' ' ++ .last_name;
    }

.. code-block:: sdl

    type Person {
      first_name: str;
      last_name: str;
      full_name := .first_name ++ ' ' ++ .last_name;
    }

Type and cardinality inference
------------------------------

The type and cardinality of a computed field is *inferred* from the expression.
There's no need for the modifier keywords you use for non-computed fields (like
``multi`` and ``required``). However, it's common to specify them anyway; it
makes the schema more readable and acts as a sanity check: if the provided
EdgeQL expression disagrees with the modifiers, an error will be thrown the
next time you try to :ref:`create a migration <ref_intro_migrations>`.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      property first_name -> str;

      # this is invalid, because first_name is not a required property
      required property first_name_upper := str_upper(.first_name);
    }

.. code-block:: sdl
    :version-lt: 4.0

    type Person {
      first_name: str;

      # this is invalid, because first_name is not a required property
      required property first_name_upper := str_upper(.first_name);
    }

.. code-block:: sdl

    type Person {
      first_name: str;

      # this is invalid, because first_name is not a required property
      required first_name_upper := str_upper(.first_name);
    }

Common use cases
----------------

Filtering
^^^^^^^^^

If you find yourself writing the same ``filter`` expression repeatedly in
queries, consider defining a computed field that encapsulates the filter.

.. code-block:: sdl
    :version-lt: 3.0

    type Club {
      multi link members -> Person;
      multi link active_members := (
        select .members filter .is_active = true
      )
    }

    type Person {
      property name -> str;
      property is_active -> bool;
    }

.. code-block:: sdl
    :version-lt: 4.0

    type Club {
      multi members: Person;
      multi link active_members := (
        select .members filter .is_active = true
      )
    }

    type Person {
      name: str;
      is_active: bool;
    }

.. code-block:: sdl

    type Club {
      multi members: Person;
      multi active_members := (
        select .members filter .is_active = true
      )
    }

    type Person {
      name: str;
      is_active: bool;
    }

.. _ref_datamodel_links_backlinks:

Backlinks
^^^^^^^^^

Backlinks are one of the most common use cases for computed links. In EdgeDB
links are *directional*; they have a source and a target. Often it's convenient
to traverse a link in the *reverse* direction.

.. code-block:: sdl
    :version-lt: 3.0

    type BlogPost {
      property title -> str;
      link author -> User;
    }

    type User {
      property name -> str;
      multi link blog_posts := .<author[is BlogPost]
    }

.. code-block:: sdl
    :version-lt: 4.0

    type BlogPost {
      title: str;
      author: User;
    }

    type User {
      name: str;
      multi link blog_posts := .<author[is BlogPost]
    }

.. code-block:: sdl

    type BlogPost {
      title: str;
      author: User;
    }

    type User {
      name: str;
      multi blog_posts := .<author[is BlogPost]
    }

The ``User.blog_posts`` expression above uses the *backlink operator* ``.<`` in
conjunction with a *type filter* ``[is BlogPost]`` to fetch all the
``BlogPosts`` associated with a given ``User``. For details on this syntax, see
the EdgeQL docs for :ref:`Backlinks <ref_eql_paths_backlinks>`.


.. list-table::
  :class: seealso

  * - :ref:`SDL > Links <ref_eql_sdl_links>`
  * - :ref:`DDL > Links <ref_eql_ddl_links>`
  * - :ref:`SDL > Properties <ref_eql_sdl_links>`
  * - :ref:`DDL > Properties <ref_eql_ddl_links>`
