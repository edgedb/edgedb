.. _ref_datamodel_computed:

=========
Computeds
=========

:edb-alt-title: Computed properties and links

.. important::

  This section assumes a basic understanding of EdgeQL. If you aren't familiar
  with it, feel free to skip this page for now.

Object types can contain *computed* links and properties. Computed properties
and links are not persisted in the database. Instead, they are evaluated *on
the fly* whenever that field is queried.

.. code-block:: sdl

  type Person {
    property name -> str;
    property all_caps_name := str_upper(__subject__.name);
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

  Volatile functions are not allowed in computed properties defined in schema.
  This means that, for example, your schema-defined computed property cannot
  call :eql:func:`datetime_current`, but it *can* call
  :eql:func:`datetime_of_transaction` or :eql:func:`datetime_of_statement`.
  This does *not* apply to computed properties outside of schema.

.. _ref_dot_notation:

Leading dot notation
--------------------

The example above used the special keyword ``__subject__`` to refer to
the current object; it's analogous to ``this`` in many object-oriented
languages.

However, explicitly using ``__subject__`` is optional here; inside the scope of
an object type declaration, you can omit it entirely and use the ``.<name>``
shorthand.

.. code-block:: sdl

  type Person {
    property first_name -> str;
    property last_name -> str;
    property full_name := .first_name ++ ' ' ++ .last_name;
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

  type Person {
    property first_name -> str;

    # this is invalid, because first_name is not a required property
    required property first_name_upper := str_upper(.first_name);
  }

Common use cases
----------------

Filtering
^^^^^^^^^

If you find yourself writing the same ``filter`` expression repeatedly in
queries, consider defining a computed field that encapsulates the filter.

.. code-block:: sdl

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

.. _ref_datamodel_links_backlinks:

Backlinks
^^^^^^^^^

Backlinks are one of the most common use cases for computed links. In EdgeDB
links are *directional*; they have a source and a target. Often it's convenient
to traverse a link in the *reverse* direction.

.. code-block:: sdl

  type BlogPost {
    property title -> str;
    link author -> User;
  }

  type User {
    property name -> str;
    multi link blog_posts := .<author[is BlogPost]
  }

The ``User.blog_posts`` expression above uses the *backlink operator* ``.<`` in
conjunction with a *type filter* ``[is BlogPost]`` to fetch all the
``BlogPosts`` associated with a given ``User``. For details on this syntax, see
the EdgeQL docs for :ref:`Backlinks <ref_eql_paths_backlinks>`.

Created Timestamp
^^^^^^^^^^^^^^^^^

Using a computed property, you can timestamp when an object was created in your
database.

.. code-block:: sdl

  type BlogPost {
    property title -> str;
    link author -> User;
    required property created_at -> datetime {
      readonly := true;
      default := datetime_of_statement();
    }
  }

When a ``BlogPost`` is created, :eql:func:`datetime_of_statement` will be
called to supply it with a timestamp as the ``created_at`` property. You might
also consider :eql:func:`datetime_of_transaction` if that's better suited to
your use case.


.. list-table::
  :class: seealso

  * - :ref:`SDL > Links <ref_eql_sdl_links>`
  * - :ref:`DDL > Links <ref_eql_ddl_links>`
  * - :ref:`SDL > Properties <ref_eql_sdl_links>`
  * - :ref:`DDL > Properties <ref_eql_ddl_links>`
