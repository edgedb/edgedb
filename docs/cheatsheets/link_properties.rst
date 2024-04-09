.. _ref_guide_linkprops:

=====================
Using link properties
=====================

:index: property


Links can contain **properties**. These are distinct from links themselves
(which we refer to as simply "links") and are used to store metadata about a
link. Due to how they're persisted under the hood, link properties have a few
additional constraints: they're always *single* and *optional*.

In thinking about how to use link properties, keep in mind that they are link
**properties**, not link **links**. This means they can contain only primitive
data (scalars, enums, arrays, or tuples).

.. note::

  In practice, link properties are best used with many-to-many relationships
  (``multi`` links without any exclusive constraints). For one-to-one,
  one-to-many, and many-to-one relationships the same data should be stored in
  object properties instead.


Declaration
-----------

Let's a create a ``Person.friends`` link with a ``strength`` property
corresponding to the strength of the friendship.

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      required property name -> str { constraint exclusive };

      multi link friends -> Person {
        property strength -> float64;
      }
    }

.. code-block:: sdl

    type Person {
      required name: str { constraint exclusive };

      multi friends: Person {
        strength: float64;
      }
    }

Constraints
-----------

.. code-block:: sdl
    :version-lt: 3.0

    type Person {
      required property name -> str { constraint exclusive };

      multi link friends -> Person {
        property strength -> float64;
        constraint expression on (
          __subject__@strength >= 0
        );
      }
    }

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
-------

To index on a link property, you must declare an abstract link and extend it.

.. code-block:: sdl
    :version-lt: 3.0

    abstract link friendship {
      property strength -> float64;
      index on (__subject__@strength);
    }

    type Person {
      required property name -> str { constraint exclusive };
      multi link friends extending friendship -> Person;
    }

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
-------------------------------

A good way to conceptualize the difference between a regular property and
a link property for object types is that regular properties are used to
*construct* expressions that return object types, while link properties are
*appended* to expressions that return object types to qualify the link.

For example, the properties ``name`` and ``email`` are used to construct a
``Person`` object that is inserted, also returning the same ``Person`` object.

.. code-block:: edgeql

  insert Person {
    name := "Jane",
    email := "jane@jane.com"
  }

If this ``Person`` object is inserted while linking it to another ``Person``
object we are inserting, we can append a ``@strength`` property to the link.
``@strength`` is not used to construct a ``Person``, but to quantify the link.

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

Keep this in mind when reading through the following examples.

Inserting
---------

The ``@strength`` property is specified in the *shape* of the ``select``
subquery. This is only valid in a subquery *inside* an ``insert`` statement.

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
inner ``insert`` subquery. The query below creates a link to a ``Person``
object that is being inserted in the same query, along with a link property
``strength`` that has a value of 3.14.

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
  _friends := (
    insert Person {
     name := "Alice"
    } unless conflict on .name
    else (select Person filter .name = "Alice" limit 1 )
  )
  insert Person {
    name := "Bob",
    friends := _friends {
      @strength := 3.14
    }
  };

Updating
--------

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

In the context of multi links the += operator works like an an insert/update
operator.

To update one or more links in a multi link, you can select from the current
linked objects, as the example does. Use a ``detached`` selection if you
want to insert/update a wider selection of linked objects instead.


Querying
--------

.. code-block:: edgeql-repl

  edgedb> select Person {
  .......   name,
  .......   friends: {
  .......     name,
  .......     @strength
  .......   }
  ....... };
  {
    default::Person {name: 'Alice', friends: {}},
    default::Person {
      name: 'Bob',
      friends: {
        default::Person {name: 'Alice', @strength: 3.7}
      }
    },
  }

.. warning::

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
                @character_name := 'Abomination'
              } filter .name = 'Tim Roth'
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
              ('Abomination', 'Tim Roth')
            },
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

.. note::

    Specifying link properties of a computed backlink in your shape is
    supported as of EdgeDB 3.0.

    If you have this schema:

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

    this query will work as of EdgeDB 3.0:

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

    If you need link properties on backlinks in earlier versions of EdgeDB, you
    can use this workaround:

    .. code-block:: edgeql

        select Person {
          name,
          followers := .<follows[is Person] {
            name,
            followed := @followed
          }
        };

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Data Model > Links > Link properties
      <ref_datamodel_link_properties>`
  * - :ref:`SDL > Properties <ref_eql_sdl_props>`
  * - :ref:`DDL > Properties <ref_eql_ddl_props>`
  * - :ref:`Introspection > Object Types
      <ref_datamodel_introspection_object_types>`
