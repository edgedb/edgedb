.. _ref_datamodel_linkprops:

===============
Link properties
===============

.. index:: property, link property, linkprops, link table, relations, @


Links, like objects, can also contain **properties**. These are used to store metadata about the link. Due to how they're persisted under the hood,
link properties have a few additional constraints: they're always ``single``
and ``optional``.

Link properties require non-trivial syntax to use them, so they are considered
to be an advanced feature. In many cases, regular properties should be used
instead. To paraphrase a famous quote: "Link properties are like a parachute,
you don't need them very often, but when you do, they can be clutch."

.. note::

  In practice, link properties are best used with many-to-many relationships
  (``multi`` links without any exclusive constraints). For one-to-one,
  one-to-many, and many-to-one relationships the same data should be stored in
  object properties instead.


Declaration
===========

Let's a create a ``Person.friends`` link with a ``strength`` property
corresponding to the strength of the friendship.

.. code-block:: sdl

    type Person {
      required name: str { constraint exclusive };

      multi friends: Person {
        strength: float64;
      }
    }

Constraints
===========

Now let's ensure that the ``@strength`` property is always non-negative:

.. code-block:: sdl

    type Person {
      required name: str { constraint exclusive };

      multi friends: Person {
        strength: float64;

        constraint expression on (
          __subject__@strength >= 0
        );
      }
    }

Indexes
=======

To add an index on a link property, we have to refactor our code and define
an abstract link ``friendship`` that will contain the ``strength`` property
with an index on it:


.. code-block:: sdl

    abstract link friendship {
      strength: float64;
      index on (__subject__@strength);
    }

    type Person {
      required name: str { constraint exclusive };

      multi friends: Person {
        extending friendship;
      };
    }

Conceptualizing link properties
===============================

A way to conceptualize the difference between a regular property and
a link property is that regular properties are used to construct an object,
while link properties are used to construct the link between objects.

For example, here the ``name`` and ``email`` properties are used to construct a
``Person`` object:

.. code-block:: edgeql

  insert Person {
    name := "Jane",
    email := "jane@jane.com"
  }

Now let's insert a ``Person`` object linking it to another ``Person`` object
setting the ``@strength`` property to the link between them:

.. code-block:: edgeql

  insert Person {
    name := "Bob",
    email := "bob@bob.com",
    friends := (
      insert Person {
        name := "Jane",
        email := "jane@jane.com",
        @strength := 3.14
      }
    )
  }

So we're not using ``@strength`` to construct a particular ``Person`` object,
but to quantify a link between two ``Person`` objects.

Inserting
=========

What if we want to insert a ``Person`` object while linking it to another
``Person`` that's already in the database?

The ``@strength`` property then will be specified in the *shape* of a
``select`` subquery:

.. code-block:: edgeql

  insert Person {
    name := "Bob",
    friends := (
      select detached Person {
        @strength := 3.14
      }
      filter .name = "Alice"
    )
  }

.. note::

  We are using the :eql:op:`detached` operator to unbind the
  ``Person`` reference from the scope of the ``insert`` query.

When doing a nested insert, link properties can be directly included in the
inner ``insert`` subquery:

.. code-block:: edgeql

  insert Person {
    name := "Bob",
    friends := (
      insert Person {
        name := "Jane",
        @strength := 3.14
      }
    )
  }

Similarly, ``with`` can be used to capture an expression returning an
object type, after which a link property can be added when linking it to
another object type:

.. code-block:: edgeql

  with
    alice := (

      insert Person {
        name := "Alice"
      }
      unless conflict on .name
      else (
        select Person
        filter .name = "Alice" limit 1
      )
    )

  insert Person {
    name := "Bob",
    friends := alice {
      @strength := 3.14
    }
  };

Updating
========

.. code-block:: edgeql

  update Person
  filter .name = "Bob"
  set {
    friends += (
      select .friends {
        @strength := 3.7
      }
      filter .name = "Alice"
    )
  };

The example updates the ``@strength`` property of Bob's friends link to
Alice to 3.7.

In the context of multi links the ``+=`` operator works like an
an insert/update operator.

To update one or more links in a multi link, you can select from the current
linked objects, as the example does. Use a ``detached`` selection if you
want to insert/update a wider selection of linked objects instead.


Selecting
=========

To select a link property, you can use the ``@<>name`` syntax inside the
select *shape*. Keep in mind, that you're not selecting a property on
an object with this syntax, but rather on the link, in this case ``friends``:

.. code-block:: edgeql-repl

  gel> select Person {
  ....   name,
  ....   friends: {
  ....     name,
  ....     @strength
  ....   }
  .... };
  {
    default::Person {name: 'Alice', friends: {}},
    default::Person {
      name: 'Bob',
      friends: {
        default::Person {name: 'Alice', @strength: 3.7}
      }
    },
  }

Unions
======

A link property cannot be referenced in a set union *except* in the case of
a :ref:`for loop <ref_eql_for>`. That means this will *not* work:

.. code-block:: edgeql

    # 🚫 Does not work
    insert Movie {
      title := 'The Incredible Hulk',
      actors := {(
        select Person {
          @character_name := 'The Hulk'
        } filter .name = 'Mark Ruffalo'
      ),
      (
        select Person {
          @character_name := 'Iron Man'
        } filter .name = 'Robert Downey Jr.'
      )}
    };

That query will produce an error: ``QueryError: invalid reference to link
property in top level shape``

You can use this workaround instead:

.. code-block:: edgeql

    # ✅ Works!
    insert Movie {
      title := 'The Incredible Hulk',

      actors := assert_distinct((
        with characters := {
          ('The Hulk', 'Mark Ruffalo'),
          ('Iron Man', 'Robert Downey Jr.')
        }
        for character in characters union (
            select Person {
                @character_name := character.0
            } filter .name = character.1
        )
      ))
    };

Note that we are also required to wrap the ``actors`` query with
:eql:func:`assert_distinct` here to assure the compiler that the result set
is distinct.

With computed backlinks
=======================

Specifying link properties of a computed backlink in your shape is also
supported. If you have this schema:

.. code-block:: sdl

    type Person {
      required name: str;

      multi follows: Person {
        followed: datetime {
          default := datetime_of_statement();
        };
      };

      multi link followers := .<follows[is Person];
    }

this query will work as expected:

.. code-block:: edgeql

    select Person {
      name,

      followers: {
        name,
        @followed
      }
    };

even though ``@followed`` is a link property of ``follows`` and we are
accessing is through the computed backlink ``followers`` instead.

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Links and link properties <ref_datamodel_link_properties>`
  * - :ref:`Properties in schema <ref_eql_sdl_props>`
  * - :ref:`Properties with DDL <ref_eql_ddl_props>`
