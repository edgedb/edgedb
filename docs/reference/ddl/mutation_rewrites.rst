.. versionadded:: 3.0

.. _ref_eql_ddl_mutation_rewrites:

=================
Mutation Rewrites
=================

This section describes the DDL commands pertaining to
:ref:`mutation rewrites <ref_datamodel_mutation_rewrites>`.


Create rewrite
==============

:eql-statement:


:ref:`Define <ref_eql_sdl_mutation_rewrites>` a new mutation rewrite.

When creating a new property or link:

.. eql:synopsis::

    {create | alter} type <type-name> "{"
      create { property | link } <prop-or-link-name> -> <type> "{"
        create rewrite {insert | update} [, ...]
          using <expr>
      "}" ;
    "}" ;

When altering an existing property or link:

.. eql:synopsis::

    {create | alter} type <type-name> "{"
      alter { property | link } <prop-or-link-name> "{"
        create rewrite {insert | update} [, ...]
          using <expr>
      "}" ;
    "}" ;


Description
-----------

The command ``create rewrite`` nested under ``create type`` or ``alter type``
and then under ``create property/link`` or ``alter property/link`` defines a
new mutation rewrite for the given property or link on the given object.


Parameters
----------

:eql:synopsis:`<type-name>`
    The name (optionally module-qualified) of the type containing the rewrite.

:eql:synopsis:`<prop-or-link-name>`
    The name (optionally module-qualified) of the property or link being
    rewritten.

:eql:synopsis:`insert | update [, ...]`
    The query type (or types) that are rewritten. Separate multiple values with
    commas to invoke the same rewrite for multiple types of queries.


Examples
--------

Declare two mutation rewrites on new properties: one that sets a ``created``
property when a new object is inserted and one that sets a ``modified``
property on each update:

.. code-block:: edgeql

    alter type User {
      create property created -> datetime {
        create rewrite insert using (datetime_of_statement());
      };
      create property modified -> datetime {
        create rewrite update using (datetime_of_statement());
      };
    };


Drop rewrite
============

:eql-statement:


Remove a mutation rewrite.

.. eql:synopsis::

    alter type <type-name> "{"
      alter property <prop-or-link-name> "{"
        drop rewrite {insert | update} ;
      "}" ;
    "}" ;


Description
-----------

The command ``drop rewrite`` inside an ``alter type`` block and further inside
an ``alter property`` block removes the definition of an existing mutation
rewrite on the specified property or link of the specified type.


Parameters
----------

:eql:synopsis:`<type-name>`
    The name (optionally module-qualified) of the type containing the rewrite.

:eql:synopsis:`<prop-or-link-name>`
    The name (optionally module-qualified) of the property or link being
    rewritten.

:eql:synopsis:`insert | update [, ...]`
    The query type (or types) that are rewritten. Separate multiple values with
    commas to invoke the same rewrite for multiple types of queries.


Example
-------

Remove the ``insert`` rewrite of the ``created`` property on the ``User`` type:

.. code-block:: edgeql

    alter type User {
      alter property created {
        drop rewrite insert;
      };
    };


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Mutation rewrites  <ref_datamodel_mutation_rewrites>`
  * - :ref:`SDL > Mutation rewrites <ref_eql_sdl_mutation_rewrites>`
  * - :ref:`Introspection > Mutation rewrites
      <ref_datamodel_introspection_mutation_rewrites>`
