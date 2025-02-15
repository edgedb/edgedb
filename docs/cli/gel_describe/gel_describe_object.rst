.. _ref_cli_gel_describe_object:


===================
gel describe object
===================

Describe a named schema object.

.. cli:synopsis::

    gel describe object [<options>] <name>


Description
===========

:gelcmd:`describe` is a terminal command equivalent to
:eql:stmt:`describe object <describe>` introspection command.


Options
=======

The ``describe`` command runs in the database it is connected to. For
specifying the connection target see :ref:`connection options
<ref_cli_gel_connopts>`.

:cli:synopsis:`--verbose`
    This is equivalent to running :eql:stmt:`describe object ... as
    text verbose <describe>` command, which enables displaying
    additional details, such as annotations and constraints, which are
    otherwise omitted.

:cli:synopsis:`<name>`
    Name of the schema object to describe.
