.. _ref_eql_with:

WITH blocks
===========


Any short name is ultimately resolved to some fully-qualified name in the following manner:

1) Look for a match to the short name in the current module (typically
``default``, but it can be changed).
2) Look for a match to the short name in the ``std`` module.

Normally the current module is called ``default``, which is
automatically created in any new database. It is possible to override
the current module globally on the session level with a ``SET MODULE
my_module`` :ref:`command <ref_eql_statements_session_set_alias>`. It
is also possible to override the current module on per-query basis
using ``WITH MODULE my_module`` :ref:`clause <ref_eql_with>`.
