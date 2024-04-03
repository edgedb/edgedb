.. _ref_cli_edgedb_list:


===========
edgedb list
===========

List matching database objects by name and type.

.. cli:synopsis::

    edgedb list <type> [<options>] <pattern>


Description
===========

The ``edgedb list`` group of commands contains tools for listing
database objects by matching name or type. The sub-commands are
organized by the type of the objects listed.

Types
=====

:cli:synopsis:`edgedb list aliases`
    Display list of aliases defined in the schema.

:cli:synopsis:`edgedb list casts`
    Display list of casts defined in the schema.

.. TODO: Add `edgedb list branches` once the command is added. https://github.com/edgedb/edgedb-cli/issues/1275

:cli:synopsis:`edgedb list databases`
    Display list of databases in the server instance.

:cli:synopsis:`edgedb list indexes`
    Display list of indexes defined in the schema.

:cli:synopsis:`edgedb list modules`
    Display list of modules defined in the schema.

:cli:synopsis:`edgedb list roles`
    Display list of roles in the server instance.

:cli:synopsis:`edgedb list scalars`
    Display list of scalar types defined in the schema.

:cli:synopsis:`edgedb list types`
    Display list of object types defined in the schema.

Options
=======

The ``list`` command runs in the database it is connected to. For
specifying the connection target see :ref:`connection options
<ref_cli_edgedb_connopts>`.

:cli:synopsis:`-c, --case-sensitive`
    Indicates that the pattern should be treated in a case-sensitive
    manner.

:cli:synopsis:`-s, --system`
    Indicates that built-in and objects should be included in the list.

:cli:synopsis:`-v, --verbose`
    Include more details in the output.

:cli:synopsis:`<pattern>`
    The pattern that the name should match. If omitted all objects of
    a particular type will be listed.
