.. _ref_eql_expr_paths:

=====
Paths
=====

.. _ref_eql_expr_index_path:

A *path expression* (or simply a *path*) represents a set of values that are
reachable when traversing a given sequence of links or properties from some
source set.

Consider the following schema:

.. code-block:: sdl

    type User {
        required property name -> str;
        multi link friends -> User {
            property since -> cal::local_date;
        }
    }

    abstract type Owned {
        required link owner -> User;
    }

    type Issue extending Owned {
        required property title -> str;
    }

The example below shows a path that represents the names of all friends
of all ``User`` objects in the database.

.. code-block:: edgeql

    SELECT User.friends.name;

And this represents all sources of the ``owner`` links that have a
``User`` as target:

.. code-block:: edgeql

    SELECT User.<owner;

By default *backlinks* don't infer any type information beyond the
fact that it's an :eql:type:`Object`. To ensure that this path
specifically reaches ``Issue`` a :eql:op:`type intersection <ISINTERSECT>`
operator must be used:

.. code-block:: edgeql

    SELECT User.<owner[IS Issue];

The following represents a set of all dates on which users became
friends, if ``since`` is defined as a link property on the
``User.friends`` link:

.. code-block:: edgeql

    SELECT User.friends@since;

.. note::

    Properties cannot refer to objects, so a reference to an object
    property or a link property will always be the last step in a path.


For a full syntax definition, see the :ref:`Path reference documentation <ref_reference_paths>`.
