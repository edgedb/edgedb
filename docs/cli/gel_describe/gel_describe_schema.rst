.. _ref_cli_gel_describe_schema:


===================
gel describe schema
===================

Give an :ref:`SDL <ref_eql_sdl>` description of the schema of the
database specified by the connection options.

.. cli:synopsis::

    gel describe schema [<options>]


Description
===========

:gelcmd:`describe schema` is a terminal command equivalent to
:eql:stmt:`describe schema as sdl <describe>` introspection command.


Options
=======

The ``describe`` command runs in the database it is connected to. For
specifying the connection target see :ref:`connection options
<ref_cli_gel_connopts>`.
