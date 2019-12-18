.. _ref_datamodel_computables:

===========
Computables
===========

:ref:`Links <ref_datamodel_links>` and :ref:`properties <ref_datamodel_props>`
may be declared as *computable*.

The values of computable properties and links are not persisted in the
database and are computed using the specified expression every time they
are referenced in a query.  The type of the property or link is determined
from the expression.  Other than that computables behave exactly like their
non-computable counterparts.

Links and properties have a *source* and one or more *targets*.  The
*source* is always the object on which the link is defined. *Targets*
are either the objects to which the link points or the property
values.  The computable expressions define one or more *targets* and
can refer to the *source* as ``__source__``.

Computables are useful in the situations where there is a frequent need for
some value that is derived from the values of existing properties and links.

For example, here we define the ``User`` type to contain the
``fullname`` computable property that is derived from user's first and
last name:

.. code-block:: sdl

    type User {
        required property firstname -> str;
        required property lastname -> str;
        property fullname :=
            (__source__.firstname ++ ' ' ++
             __source__.lastname);
    }

If the computable expression is simple (i.e. not a subquery), shortcut
paths may also be used instead of explicit references to ``__source__``:

.. code-block:: sdl

    type User {
        required property firstname -> str;
        required property lastname -> str;
        property fullname := (
            .firstname ++ ' ' ++ .lastname);
    }

Computables are also often used in :ref:`aliases <ref_datamodel_aliases>`.
For example, using the ``User`` from the above example, a ``UserAlias``
can be defined with a ``lastname_first`` computable which lists the
full name in the format which is often used in formal alphabetized
lists:

.. code-block:: sdl

    alias UserAlias := User {
        lastname_first := (
            .lastname ++ ', ' ++ .firstname)
    }

Computables can be used in :ref:`shapes <ref_eql_expr_shapes>`, too:

.. code-block:: edgeql

    SELECT User {
        lastname_first := (
            .lastname ++ ', ' ++ .firstname)
    };


See Also
--------

Computable
:ref:`link SDL <ref_eql_sdl_links>`,
:ref:`link DDL <ref_eql_ddl_links>`,
:ref:`property SDL <ref_eql_sdl_links>`,
and :ref:`property DDL <ref_eql_ddl_links>`.
