.. _ref_reference_shapes:

======
Shapes
======

A *shape* is a powerful syntactic construct that can be used to describe
type variants in queries, data in ``INSERT`` and ``UPDATE`` statements,
and to specify the format of statement output.

Shapes always follow an expression, and are a list of *shape elements*
enclosed in curly braces:

.. eql:synopsis::

    <expr> "{"
        <shape_element> [, ...]
    "}"


Shape element has the following syntax:

.. eql:synopsis::

    [ "[" IS <object-type> "]" ] <pointer-spec>

If an optional :eql:synopsis:`<object-type>` filter is used,
:eql:synopsis:`<pointer-spec>` will only apply to those objects in
the :eql:synopsis:`<expr>` set that are instances of
:eql:synopsis:`<object-type>`.

:eql:synopsis:`<pointer-spec>` is one of the following:

- a name of an existing link or property of a type produced
  by :eql:synopsis:`<expr>`;

- a declaration of a computed link or property in the form

  .. eql:synopsis ::

    [@]<name> := <ptrexpr>


- a *subshape* in the form

  .. eql:synopsis ::

    <pointer-name>: [ "[" IS <target-type> "]" ] "{" ... "}"`

  The :eql:synopsis:`<pointer-name>` is the name of an existing link
  or property, and :eql:synopsis:`<target-type>` is an optional object
  type that specifies the type of target objects selected or inserted,
  depending on the context.

Shaping Query Results
=====================

At the end of the day, EdgeQL has two jobs that are similar, yet distinct:

1) Express the values that we want computed.
2) Arrange the values into a particular shape that we want.

Consider the task of getting "names of users and all of the friends'
names associated with the given user" in a database defined by the
following schema:

.. code-block:: sdl

    type User {
        required property name -> str;
        multi link friends -> User;
    }

If we only concern ourselves with getting the values, then a
reasonable solution to this might be:

.. code-block:: edgeql-repl

    db> SELECT (User.name, User.friends.name ?? '');
    {
      ('Alice', 'Cameron'),
      ('Alice', 'Dana'),
      ('Billie', 'Dana'),
      ('Cameron', ''),
      ('Dana', 'Alice'),
      ('Dana', 'Billie'),
      ('Dana', 'Cameron'),
    }

This particular solution is very similar to what one might get using
SQL. It's equivalent to a table with "user name" and "friend name"
columns. It gets the job done, albeit with some redundant repeating of
"user names".

We can improve things a little and reduce the repetition by
aggregating all the friend names into an array:

.. code-block:: edgeql-repl

    db> SELECT (User.name, array_agg(User.friends.name));
    {
      ('Alice', ['Cameron', 'Dana']),
      ('Billie', ['Dana']),
      ('Cameron', []),
      ('Dana', ['Alice', 'Billie', 'Cameron']),
    }


This achieves a couple of things: it's easier to see which friends
belong to which user and we no longer need the placeholder ``''`` for
those users who don't have friends.

The recommended way to get this information in EdgeDB, however, is to
use *shapes*, because they mimic the structure of the data and the output:

.. code-block:: edgeql-repl

    db> SELECT User {
    ...     name,
    ...     friends: {
    ...         name
    ...     }
    ... };
    {
      default::User {
        name: 'Alice',
        friends: {
          default::User {name: 'Cameron'},
          default::User {name: 'Dana'},
        },
      },
      default::User {name: 'Billie', friends: {default::User {name: 'Dana'}}},
      default::User {name: 'Cameron', friends: {}},
      default::User {
        name: 'Dana',
        friends: {
          default::User {name: 'Alice'},
          default::User {name: 'Billie'},
          default::User {name: 'Cameron'},
        },
      },
    }

So far the expression for the data that we wanted was also acceptable
for structuring the output, but what if that's not the case? Let's add
a condition and only show those users who have friends with either the
letter "i" or "o" in their names:

.. code-block:: edgeql-repl

    db> SELECT User {
    ...     name,
    ...     friends: {
    ...         name
    ...     }
    ... } FILTER .friends.name ILIKE '%i%' OR .friends.name ILIKE '%o%';
    {
      default::User {
        name: 'Alice',
        friends: {
          default::User {name: 'Cameron'},
          default::User {name: 'Dana'},
        },
      },
      default::User {
        name: 'Dana',
        friends: {
          default::User {name: 'Alice'},
          default::User {name: 'Billie'},
          default::User {name: 'Cameron'},
        },
      },
    }

That ``FILTER`` is getting a bit bulky, so perhaps we can just factor
these flags out as part of the shape's computed properties:

.. code-block:: edgeql-repl

    db> SELECT User {
    ...     name,
    ...     friends: {
    ...         name
    ...     },
    ...     has_i := .friends.name ILIKE '%i%',
    ...     has_o := .friends.name ILIKE '%o%',
    ... } FILTER .has_i OR .has_o;
    {
      default::User {
        name: 'Alice',
        friends: {
          default::User {name: 'Cameron'},
          default::User {name: 'Dana'},
        },
        has_i: {false, false},
        has_o: {true, false},
      },
      default::User {
        name: 'Dana',
        friends: {
          default::User {name: 'Alice'},
          default::User {name: 'Billie'},
          default::User {name: 'Cameron'},
        },
        has_i: {true, true, false},
        has_o: {false, false, true},
      },
    }

It looks like this refactoring came at the cost of putting extra
things into the output. In this case we don't want our intermediate
calculations to actually show up in the output, so what can we do? In
EdgeDB the output structure is determined *only* by the expression
appearing in the top-level :eql:stmt:`SELECT`. This means that we can
move our intermediate calculations into the :eql:kw:`WITH <WITH>`
block:

.. code-block:: edgeql-repl

    db> WITH U := (
    ...     SELECT User {
    ...         has_i := .friends.name ILIKE '%i%',
    ...         has_o := .friends.name ILIKE '%o%',
    ...     }
    ... )
    ... SELECT U {
    ...     name,
    ...     friends: {
    ...         name
    ...     },
    ... } FILTER .has_i OR .has_o;
    {
      default::User {
        name: 'Alice',
        friends: {
          default::User {name: 'Cameron'},
          default::User {name: 'Dana'},
        },
      },
      default::User {
        name: 'Dana',
        friends: {
          default::User {name: 'Alice'},
          default::User {name: 'Billie'},
          default::User {name: 'Cameron'},
        },
      },
    }

This way we can use ``has_i`` and ``has_o`` in our query without
leaking them into the output.

General Shaping Rules
=====================

In EdgeDB typically all shapes appearing in the top-level
:eql:stmt:`SELECT` should be reflected in the output. This also applies
to shapes no matter where and how they are nested. Aside from
other shapes, this includes nesting in arrays:

.. code-block:: edgeql-repl

    db> SELECT array_agg(User {name});
    {
      [
        default::User {name: 'Alice'},
        default::User {name: 'Billie'},
        default::User {name: 'Cameron'},
        default::User {name: 'Dana'},
      ],
    }

... or tuples:

.. code-block:: edgeql-repl

    db> SELECT enumerate(User {name});
    {
      (0, default::User {name: 'Alice'}),
      (1, default::User {name: 'Billie'}),
      (2, default::User {name: 'Cameron'}),
      (3, default::User {name: 'Dana'}),
    }

You can safely access a tuple element and expect the output shape to
be intact:

.. code-block:: edgeql-repl

    db> SELECT enumerate(User{name}).1;
    {
      default::User {name: 'Alice'},
      default::User {name: 'Billie'},
      default::User {name: 'Cameron'},
      default::User {name: 'Dana'},
    }

Accessing array elements or working with slices also preserves output
shape and is analogous to using ``OFFSET`` and ``LIMIT`` when working
with sets:

.. code-block:: edgeql-repl

    db> SELECT array_agg(User {name})[2];
    {default::User {name: 'Cameron'}}


Losing Shapes
=============

There are some situations where shape information gets completely or
partially discarded. Any such operation also prevents the altered
shape from appearing in the output altogether.

In order for the shape to be preserved, the original expression type
must be preserved. This means that :eql:op:`UNION` can alter the shape,
because the result of a :eql:op:`UNION` is a :eql:op:`union type
<TYPEOR>`. So you can still refer to the common properties, but not to
the properties that appeared in the shape.

As mentioned above, since :eql:op:`UNION` potentially alters the
expression shape it never preserves output shape, even when the
underlying type wasn't altered:

.. code-block:: edgeql-repl

    db> SELECT User{name} UNION User{name};
    {
      default::User {id: 7769045a-27bf-11ec-94ea-3f6c0ae59eb3},
      default::User {id: 7b42ed20-27bf-11ec-94ea-7700ec77834e},
      default::User {id: 7fcedbc4-27bf-11ec-94ea-73dcb6f297a4},
      default::User {id: 82f52646-27bf-11ec-94ea-3718ffb8dd15},
      default::User {id: 7769045a-27bf-11ec-94ea-3f6c0ae59eb3},
      default::User {id: 7b42ed20-27bf-11ec-94ea-7700ec77834e},
      default::User {id: 7fcedbc4-27bf-11ec-94ea-73dcb6f297a4},
      default::User {id: 82f52646-27bf-11ec-94ea-3718ffb8dd15},
    }

Listing several items inside a set ``{ ... }`` functions identically
to a :eql:op:`UNION` and so will also produce a union type and remove
shape from output.

Another subtle way for a type union to remove the shape from the output
is by the :eql:op:`?? <COALESCE>` and the :eql:op:`IF..ELSE` operators. Both
of them determine the result type as the union of the left and right
operands:

.. code-block:: edgeql-repl

    db> SELECT <User>{} ?? User {name};
    {
      default::User {id: 7769045a-27bf-11ec-94ea-3f6c0ae59eb3},
      default::User {id: 7b42ed20-27bf-11ec-94ea-7700ec77834e},
      default::User {id: 7fcedbc4-27bf-11ec-94ea-73dcb6f297a4},
      default::User {id: 82f52646-27bf-11ec-94ea-3718ffb8dd15},
    }

Shapes survive array creation (either via :eql:func:`array_agg` or by
using ``[ ... ]``), but they follow the same rules as for :eql:op:`UNION`
for array :eql:op:`concatenation <ARRAYPLUS>`. Basically the element type
of the resulting array must be a union type and thus all shape
information is lost:

.. code-block:: edgeql-repl

    db> SELECT array_agg(User{name}) ++ array_agg(User{name});
    {
      [
        default::User {id: 7769045a-27bf-11ec-94ea-3f6c0ae59eb3},
        default::User {id: 7b42ed20-27bf-11ec-94ea-7700ec77834e},
        default::User {id: 7fcedbc4-27bf-11ec-94ea-73dcb6f297a4},
        default::User {id: 82f52646-27bf-11ec-94ea-3718ffb8dd15},
        default::User {id: 7769045a-27bf-11ec-94ea-3f6c0ae59eb3},
        default::User {id: 7b42ed20-27bf-11ec-94ea-7700ec77834e},
        default::User {id: 7fcedbc4-27bf-11ec-94ea-73dcb6f297a4},
        default::User {id: 82f52646-27bf-11ec-94ea-3718ffb8dd15},
      ],
    }

.. note::

    The :eql:stmt:`FOR` statement preserves the shape given inside the
    ``UNION`` clause, effectively applying the shape to its entire
    result.
