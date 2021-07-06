.. _ref_cli_edgedb_describe_object:


======================
edgedb describe object
======================

Describe a named schema object.

.. cli:synopsis::

    edgedb [<connection-option>...] describe object [--verbose] <name>


Description
===========

``edgedb describe`` is a terminal command equivalent to
:eql:stmt:`DESCRIBE OBJECT <DESCRIBE>` introspection command.


Options
=======

:cli:synopsis:`<connection-option>`
    See the :ref:`ref_cli_edgedb_connopts`.  The ``describe`` command
    runs in the database it is connected to.

:cli:synopsis:`--verbose`
    This is equivalent to running :eql:stmt:`DESCRIBE OBJECT ... AS
    TEXT VERBOSE <DESCRIBE>` command, which enables displaying
    additional details, such as annotations and constraints, which are
    otherwise omitted.

:cli:synopsis:`<name>`
    Name of the schema object to describe.
