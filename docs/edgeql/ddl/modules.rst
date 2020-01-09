.. _ref_eql_ddl_modules:

=======
Modules
=======

This section describes the DDL commands pertaining to
:ref:`modules <ref_datamodel_modules>`.


CREATE MODULE
=============

:eql-statement:

Create a new module.

.. eql:synopsis::

    CREATE MODULE <name> ;

There's a :ref:`corresponding SDL declaration <ref_eql_sdl_modules>`
for a module, although in SDL a module declaration is likely to also
include that module's content.

Description
-----------

``CREATE MODULE`` defines a new module for the current database.
The name of the new module must be distinct from any existing module
in the current database.

Examples
--------

Create a new module:

.. code-block:: edgeql

    CREATE MODULE payments;


DROP MODULE
===========

:eql-statement:


Remove a module.

.. eql:synopsis::

    DROP MODULE <name> ;


Description
-----------

``DROP MODULE`` removes an existing module from the current database.
All schema items and data contained in the module is removed as well.


Examples
--------

Remove a module:

.. code-block:: edgeql

    DROP MODULE payments;
