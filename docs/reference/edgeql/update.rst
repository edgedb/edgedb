.. _ref_eql_statements_update:

UPDATE
======

:eql-statement:
:eql-haswith:

``UPDATE`` -- update objects in a database

.. eql:synopsis::

    [ WITH <with-item> [, ...] ]

    UPDATE <selector-expr>

    [ FILTER <filter-expr> ]

    SET <shape> ;

``UPDATE`` changes the values of the specified links in all objects
selected by *update-selector-expr* and, optionally, filtered by
*filter-expr*.

:eql:synopsis:`WITH`
    Alias declarations.

    The ``WITH`` clause allows specifying module aliases as well
    as expression aliases that can be referenced by the ``UPDATE``
    statement.  See :ref:`ref_eql_statements_with` for more information.

:eql:synopsis:`UPDATE <selector-expr>`
    An arbitrary expression returning a set of objects to be updated.

:eql:synopsis:`FILTER <filter-expr>`
    An expression of type :eql:type:`bool` used to filter the
    set of updated objects.

    :eql:synopsis:`<filter-expr>` is an expression that has a result
    of type :eql:type:`bool`.  Only objects that satisfy the filter
    expression will be updated.  See the description of the
    ``FILTER`` clause of the :eql:stmt:`SELECT` statement for more
    information.

:eql:synopsis:`SET <shape>`
    A shape expression with the
    new values for the links of the updated object. There are three
    possible assignment operations permitted within the ``SET`` shape:

    .. eql:synopsis::

        SET { <field> := <update-expr> [, ...] }

        SET { <field> += <update-expr> [, ...] }

        SET { <field> -= <update-expr> [, ...] }

    The most basic assignment is the ``:=``, which just sets the
    :eql:synopsis:`<field>` to the specified
    :eql:synopsis:`<update-expr>`. The ``+=`` and ``-=`` either add or
    remove the set of values specified by the
    :eql:synopsis:`<update-expr>` from the *current* value of the
    :eql:synopsis:`<field>`.

Output
~~~~~~

On successful completion, an ``UPDATE`` statement returns the
set of updated objects.


Examples
~~~~~~~~

Here are a couple of examples of the ``UPDATE`` statement with simple
assignments using ``:=``:

.. code-block:: edgeql

    # update the user with the name 'Alice Smith'
    WITH MODULE example
    UPDATE User
    FILTER .name = 'Alice Smith'
    SET {
        name := 'Alice J. Smith'
    };

    # update all users whose name is 'Bob'
    WITH MODULE example
    UPDATE User
    FILTER .name LIKE 'Bob%'
    SET {
        name := User.name ++ '*'
    };

For usage of ``+=`` and ``-=`` consider the following ``Post`` type:

.. code-block:: sdl

    # ... Assume some User type is already defined
    type Post {
        required property title -> str;
        required property body -> str;
        # A "tags" property containing a set of strings
        multi property tags -> str;
        link author -> User;
    }

The following queries add or remove tags from some user's posts:

.. code-block:: edgeql

    WITH MODULE example
    UPDATE Post
    FILTER .author.name = 'Alice Smith'
    SET {
        # add tags
        tags += {'example', 'edgeql'}
    };

    WITH MODULE example
    UPDATE Post
    FILTER .author.name = 'Alice Smith'
    SET {
        # remove a tag, if it exist
        tags -= 'todo'
    };


The statement ``FOR <x> IN <expr>`` allows to express certain bulk
updates more clearly. See
:ref:`Usage of FOR statement<ref_eql_forstatement>` for more details.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`EdgeQL > Update <ref_eql_update>`
  * - :ref:`Cheatsheets > Updating data <ref_cheatsheet_update>`
