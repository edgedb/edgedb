.. _ref_cli_edgedb_alter_role:


=================
edgedb alter-role
=================

Alter an existing :eql:stmt:`role <ALTER ROLE>`.

.. cli:synopsis::

    edgedb [<connection-option>...] alter-role [OPTIONS] <name>


Description
===========

``edgedb alter-role`` is a terminal command equivalent to
:eql:stmt:`ALTER ROLE`.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The
    ``create-superuser-role`` command runs in the database it is
    connected to.

:cli:synopsis:`<name>`
    The name of the role.

:cli:synopsis:`--password`
    Set the password for role (read separately from the terminal).

:cli:synopsis:`--password-from-stdin`
    Set the password for role, read from the stdin.
