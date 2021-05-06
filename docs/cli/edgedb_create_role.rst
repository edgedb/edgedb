.. _ref_cli_edgedb_create_role:


============================
edgedb create-superuser-role
============================

Create a new :eql:stmt:`role <CREATE ROLE>` (currently only superuser
roles are supported).

.. cli:synopsis::

    edgedb [<connection-option>...] create-superuser-role [OPTIONS] <name>


Description
===========

``edgedb create-superuser-role`` is a terminal command equivalent to
:eql:stmt:`CREATE ROLE`.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The
    ``create-superuser-role`` command runs in the database it is
    connected to.

:cli:synopsis:`<name>`
    The name of the new role.

:cli:synopsis:`--password`
    Set the password for role (read separately from the terminal).

:cli:synopsis:`--password-from-stdin`
    Set the password for role, read from the stdin.
