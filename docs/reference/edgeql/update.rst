.. _ref_eql_statements_update:

Update
======

:eql-statement:
:eql-haswith:

``update`` -- update objects in a database

.. eql:synopsis::

    [ with <with-item> [, ...] ]

    update <selector-expr>

    [ filter <filter-expr> ]

    set <shape> ;

``update`` changes the values of the specified links in all objects
selected by *update-selector-expr* and, optionally, filtered by
*filter-expr*.

:eql:synopsis:`with`
    Alias declarations.

    The ``with`` clause allows specifying module aliases as well
    as expression aliases that can be referenced by the ``update``
    statement.  See :ref:`ref_eql_statements_with` for more information.

:eql:synopsis:`update <selector-expr>`
    An arbitrary expression returning a set of objects to be updated.

:eql:synopsis:`filter <filter-expr>`
    An expression of type :eql:type:`bool` used to filter the
    set of updated objects.

    :eql:synopsis:`<filter-expr>` is an expression that has a result
    of type :eql:type:`bool`.  Only objects that satisfy the filter
    expression will be updated.  See the description of the
    ``filter`` clause of the :eql:stmt:`select` statement for more
    information.

:eql:synopsis:`set <shape>`
    A shape expression with the
    new values for the links of the updated object. There are three
    possible assignment operations permitted within the ``set`` shape:

    .. eql:synopsis::

        set { <field> := <update-expr> [, ...] }

        set { <field> += <update-expr> [, ...] }

        set { <field> -= <update-expr> [, ...] }

    The most basic assignment is the ``:=``, which just sets the
    :eql:synopsis:`<field>` to the specified
    :eql:synopsis:`<update-expr>`. The ``+=`` and ``-=`` either add or
    remove the set of values specified by the
    :eql:synopsis:`<update-expr>` from the *current* value of the
    :eql:synopsis:`<field>`.

Output
~~~~~~

On successful completion, an ``update`` statement returns the
set of updated objects.


Examples
~~~~~~~~

Here are a couple of examples of the ``update`` statement with simple
assignments using ``:=``:

.. code-block:: edgeql

    # update the user with the name 'Alice Smith'
    with module example
    update User
    filter .name = 'Alice Smith'
    set {
        name := 'Alice J. Smith'
    };

    # update all users whose name is 'Bob'
    with module example
    update User
    filter .name like 'Bob%'
    set {
        name := User.name ++ '*'
    };

For usage of ``+=`` and ``-=`` consider the following ``Post`` type:

.. code-block:: sdl
    :version-lt: 3.0

    # ... Assume some User type is already defined
    type Post {
        required property title -> str;
        required property body -> str;
        # A "tags" property containing a set of strings
        multi property tags -> str;
        link author -> User;
    }

.. code-block:: sdl

    # ... Assume some User type is already defined
    type Post {
        required title: str;
        required body: str;
        # A "tags" property containing a set of strings
        multi tags: str;
        author: User;
    }

The following queries add or remove tags from some user's posts:

.. code-block:: edgeql

    with module example
    update Post
    filter .author.name = 'Alice Smith'
    set {
        # add tags
        tags += {'example', 'edgeql'}
    };

    with module example
    update Post
    filter .author.name = 'Alice Smith'
    set {
        # remove a tag, if it exist
        tags -= 'todo'
    };


The statement ``for <x> in <expr>`` allows to express certain bulk
updates more clearly. See
:ref:`ref_eql_forstatement` for more details.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Update <ref_eql_update>`
  * - :ref:`Cheatsheets > Updating data <ref_cheatsheet_update>`
