.. _ref_cli_edgedb_drop_role:


=================
edgedb drop-role
=================

Delete an existing :eql:stmt:`role <DROP ROLE>`.

.. cli:synopsis::

    edgedb [<connection-option>...] drop-role <name>


Description
===========

``edgedb drop-role`` is a terminal command equivalent to
:eql:stmt:`DROP ROLE`.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The
    ``create-superuser-role`` command runs in the database it is
    connected to.

:cli:synopsis:`<name>`
    The name of the role.
