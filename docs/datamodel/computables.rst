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

Computables are useful in the situations where there is a frequent need for
some value that is derived from the values of existing properties and links.

For example:

.. code-block:: eschema

    type User {
        required property firstname -> str;
        required property lastname -> str;
        property fullname :=
            (__source__.firstname + ' ' +
             __source__.lastname);
     }

Here we define the ``User`` type to contain the ``fullname`` computable
property that is derived from user's first and last name.
