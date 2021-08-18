.. _ref_datamodel_object_types_free:

============
Free Objects
============

It is also possible to package data into a *free object*.
*Free objects* are meant to be transient and used either to more
efficiently store some intermediate results in a query or for
re-shaping the output. The advantage of using *free objects* over
:eql:type:`tuples <tuple>` is that it is easier to package data that
potentially contains empty sets as links or properties of the
*free object*. The underlying type of a *free object* is
``std::FreeObject``.

Consider the following query:

.. code-block:: edgeql

    WITH U := (SELECT User FILTER .name LIKE '%user%')
    SELECT {
        matches := U {name},
        total := count(U),
        total_users := count(User),
    };

The ``matches`` are potentially ``{}``, yet the query will always
return a single *free object* with ``resutls``, ``total``, and
``total_users``. To achieve the same using a :eql:type:`named tuple
<tuple>`, the query would have to be modified like this:

.. code-block:: edgeql

    WITH U := (SELECT User FILTER .name LIKE '%user%')
    SELECT (
        matches := array_agg(U {name}),
        total := count(U),
        total_users := count(User),
    );

Without the :eql:func:`array_agg` the above query would return ``{}``
instead of the named tuple if no ``matches`` are found.


See Also
--------

Object type
:ref:`SDL <ref_eql_sdl_object_types>`,
:ref:`DDL <ref_eql_ddl_object_types>`,
and :ref:`introspection <ref_eql_introspection_object_types>`.
