.. _ref_eql_ddl_modules:

=======
Modules
=======

This section describes the DDL commands pertaining to
:ref:`modules <ref_datamodel_modules>`.


Create module
=============

:eql-statement:

Create a new module.

.. eql:synopsis::

    create module <name> [ if not exists ];

There's a :ref:`corresponding SDL declaration <ref_eql_sdl_modules>`
for a module, although in SDL a module declaration is likely to also
include that module's content.

.. versionadded:: 3.0

    You may also create a nested module.

    .. eql:synopsis::

        create module <parent-name>::<name> [ if not exists ];

Description
-----------

The command ``create module`` defines a new module for the current
:versionreplace:`database;5.0:branch`. The name of the new module must be
distinct from any existing module in the current
:versionreplace:`database;5.0:branch`. Unlike :ref:`SDL module declaration
<ref_eql_sdl_modules>` the ``create module`` command does not have
sub-commands, as module contents are created separately.

Parameters
----------

:eql:synopsis:`if not exists`
    Normally creating a module that already exists is an error, but
    with this flag the command will succeed. It is useful for scripts
    that add something to a module or if the module is missing the
    module is created as well.

Examples
--------

Create a new module:

.. code-block:: edgeql

    create module payments;

.. versionadded:: 3.0

    Create a new nested module:

    .. code-block:: edgeql

        create module payments::currencies;


Drop module
===========

:eql-statement:


Remove a module.

.. eql:synopsis::

    drop module <name> ;


Description
-----------

The command ``drop module`` removes an existing empty module from the
current :versionreplace:`database;5.0:branch`. If the module contains any
schema items, this command will fail.


Examples
--------

Remove a module:

.. code-block:: edgeql

    drop module payments;
