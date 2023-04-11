.. versionadded:: 3.0

.. _ref_eql_sdl_mutation_rewrites:

=================
Mutation rewrites
=================

This section describes the SDL declarations pertaining to
:ref:`mutation rewrites <ref_datamodel_mutation_rewrites>`.


Example
-------

Declare two mutation rewrites: one that sets a ``created`` property when a new
object is inserted and one that sets a ``modified`` property on each update:

.. code-block:: sdl

    type User {
      created: datetime {
        rewrite insert using (datetime_of_statement());
      }
      modified: datetime {
        rewrite update using (datetime_of_statement());
      }
    };

.. _ref_eql_sdl_mutation_rewrites_syntax:

Syntax
------

Define a new mutation rewrite corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_mutation_rewrites>`.

.. sdl:synopsis::

    rewrite {insert | update} [, ...]
      using <expr>

Mutation rewrites must be defined inside a property or link block.


Description
-----------

This declaration defines a new trigger with the following options:

:eql:synopsis:`insert | update [, ...]`
    The query type (or types) the rewrite runs on. Separate multiple values
    with commas to invoke the same rewrite for multiple types of queries.

:eql:synopsis:`<expr>`
    The expression to be evaluated to produce the new value of the property.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Mutation rewrites <ref_datamodel_mutation_rewrites>`
  * - :ref:`DDL > Mutation rewrites <ref_eql_ddl_mutation_rewrites>`
  * - :ref:`Introspection > Mutation rewrites
      <ref_datamodel_introspection_mutation_rewrites>`
