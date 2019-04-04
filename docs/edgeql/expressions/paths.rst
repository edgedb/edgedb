.. _ref_eql_expr_paths:

=====
Paths
=====

A *path expression* (or simply a *path*) represents a set of values that are
reachable when traversing a given sequence of links or properties from some
source set.

The result of a path expression is:

a) if a path *does not* end with a property reference, then it represents a
   unique set of objects reachable from the set at the root of the path;

b) if a path *does* end with a property reference, then it represents a
   list of property values for every element in the unique set of
   objects reachable from the set at the root of the path.

The syntactic form of a path is:

.. eql:synopsis::

    <expression> <path-step> [ <path-step> ... ]

Here :eql:synopsis:`<expression>` is any expression and
:eql:synopsis:`<path-step>` is:

.. eql:synopsis::

    <step-direction> <pointer-name> [ <step-target-filter> ]

:eql:synopsis:`<step-direction>` is one of the following:

- ``.`` or ``.>`` for an outgoing link reference
- ``.<`` for an incoming link reference
- ``@`` for a link property reference

:eql:synopsis:`<pointer-name>` must be a valid link or link
property name.

:eql:synopsis:`<step-target-filter>` is an optional filter that
narrows which :eql:synopsis:`<type>` of objects should be
included in the result.  It has the following syntax:

.. eql:synopsis::

   "[" IS type "]"

.. _ref_eql_expr_paths_is:

The example below shows a path that represents the names of all friends
of all ``User`` objects in the database.

.. code-block:: edgeql

    SELECT User.friends.name;

And this represents all users who are owners of at least one ``Issue``:

.. code-block:: edgeql

    SELECT Issue.<owners[IS User];

And this represents a set of all dates on which users became friends,
if ``since`` is defined as a link property on the ``User.friends`` link:

.. code-block:: edgeql

    SELECT User.friends@since;

.. note::

    Properties cannot refer to objects, so a reference to an object
    property or a link property will always be the last step in a path.
