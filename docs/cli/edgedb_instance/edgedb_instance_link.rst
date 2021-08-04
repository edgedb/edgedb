.. _ref_cli_edgedb_instance_link:

=====================
edgedb ionstance link
=====================

Authenticate a connection to a remote EdgeDB instance and assign an
instance name to simplify future connections.

.. cli:synopsis::

    edgedb instance link [OPTIONS] <name>


Description
===========

``edgedb instance link`` is a terminal command used to bind a set of
connection credentials to an instance name. This is typically used as
a way to simplify connecting to remote EdgeDB database instances.
Usually there's no need to do this for local instances as
:ref:`ref_cli_edgedb_project_init` will already set up a named
instance.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``restore`` command
    restores the backup file into the database it is connected to.

:cli:synopsis:`<name>`
    Specifies a new instance name to associate with the connection
    options. If not present, the interactive mode will ask for the
    name.

:cli:synopsis:`--non-interactive`
    Run in non-interactive mode (accepting all defaults).
