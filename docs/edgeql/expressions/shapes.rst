:orphan:

.. _ref_eql_expr_shapes:

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

- a declaration of a computable link or property in the form

  .. eql:synopsis ::

    [@]<name> := <ptrexpr>


- a *subshape* in the form

  .. eql:synopsis ::

    <pointer-name>: [ "[" IS <target-type> "]" ] "{" ... "}"`

  The :eql:synopsis:`<pointer-name>` is the name of an existing link
  or property, and :eql:synopsis:`<target-type>` is an optional object
  type that specifies the type of target objects selected or inserted,
  depending on the context.


.. _ref_eql_expr_shapes_update:

Shapes in UPDATE
================

A shape in an ``UPDATE`` statement is used to specify how links and properties
of an object are updated.

.. code-block:: edgeql

    UPDATE Issue
    FILTER .name = 'Issue #1'
    SET
    # Update shape follows
    {
        name := 'Issue #1 (important)',
        comments := Issue.comments UNION (INSERT Comment {
                        body := 'Issue #1 updated'
                    })
    };

The above statement updates the ``name`` property and adds a ``comments`` link
to a new comment for a given ``Issue`` object.

See :ref:`ref_eql_statements_update` for more information on the use of
shapes in ``UPDATE`` statements.


Shapes in Queries
=================

A shape in a ``SELECT`` clause (or the ``UNION`` clause of a
``FOR`` statement) determines the output format for the objects in a set
computed by an expression annotated by the shape.

For example, the below query returns a set of ``Issue`` objects and includes
a ``number`` and an associated owner ``User`` object, which in turn includes
the ``name`` and the ``email`` for that user.

.. code-block:: edgeql-repl

    db> SELECT
    ...     Issue {
    ...         number,
    ...         owner: {  # sub-shape, selects Issue.owner objects
    ...            name,
    ...            email
    ...         }
    ...     };

    {
        'number': 1,
        'owner': {
            'name': 'Alice',
            'email': 'alice@example.com'
        }
    }


Cardinality
+++++++++++

Typically the cardinality of an expression can be statically
determined from the individual parts. Sometimes it is necessary to
specify the cardinality explicitly. For example, when using
computables in shapes it may be desirable to specify the cardinality
of the computable because it affects serialization.

.. code-block:: edgeql

    WITH
        MODULE example
    SELECT User {
        name,
        multi nicknames := (SELECT 'Foo')
    };

Cardinality is normally statically inferred from the query, so
overruling this inference may only be done to *relax* the cardinality,
so it is not valid to specify the ``single`` qualifier for a computable
expression that may return multiple items.


Link Properties
+++++++++++++++

A query could use a shape to create an alias to a real link. In this
case, the link properties on that link are preserved on the aliased
link as well. Consider the following schema:

.. code-block:: sdl

    type User {
        required property name -> str;
        multi link friends -> User {
            property since -> datetime;
        }
    }


Suppose that for a certain query the link ``friends`` needs to be
renamed into ``associates`` without changing the underlying schema. A
shape annotation can be used to provide an alias for the link:

.. code-block:: edgeql

    WITH
        MODULE example,
        SpecialUser := (
            SELECT User {
                associates := User.friends
            }
        )
    SELECT SpecialUser {
        name,
        associates: {
            name,
            @since
        }
    };

When a simple path is used as the definition of a computable link,
that has the effect of aliasing the underlying link and thus
preserving any link properties as well. For a path that has more than
one step, it is always the *last* step that is aliased.
