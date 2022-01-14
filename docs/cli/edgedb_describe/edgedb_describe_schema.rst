.. _ref_cli_edgedb_describe_schema:


======================
edgedb describe schema
======================

Give an :ref:`SDL <ref_eql_sdl>` description of the schema of the
database specified by the connection options.

.. cli:synopsis::

    edgedb describe schema [<options>]


Description
===========

``edgedb describe schema`` is a terminal command equivalent to
:eql:stmt:`describe schema as sdl <describe>` introspection command.


Options
=======

The ``describe`` command runs in the database it is connected to. For
specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.
