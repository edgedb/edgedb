.. _ref_cli_edgedb_describe_object:


======================
edgedb describe object
======================

Describe a named schema object.

.. cli:synopsis::

    edgedb describe object [<options>] <name>


Description
===========

``edgedb describe`` is a terminal command equivalent to
:eql:stmt:`DESCRIBE OBJECT <DESCRIBE>` introspection command.


Options
=======

The ``describe`` command runs in the database it is connected to. For
specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.

:cli:synopsis:`--verbose`
    This is equivalent to running :eql:stmt:`DESCRIBE OBJECT ... AS
    TEXT VERBOSE <DESCRIBE>` command, which enables displaying
    additional details, such as annotations and constraints, which are
    otherwise omitted.

:cli:synopsis:`<name>`
    Name of the schema object to describe.
