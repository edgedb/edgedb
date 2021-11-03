.. _ref_datamodel_functions:

=========
Functions
=========


Functions are ways to transform one set of data into another.

User-defined Functions
----------------------

It is also possible to define custom functions. For example, consider
a function that adds an exclamation mark ``'!'`` at the end of the
string:

.. code-block:: sdl

    function exclamation(word: str) -> str
        using (word ++ '!');

This function accepts a :eql:type:`str` as an argument and produces a
:eql:type:`str` as output as well.

.. code-block:: edgeql-repl

    test> select exclamation({'Hello', 'World'});
    {'Hello!', 'World!'}


See Also
--------

Function
:ref:`SDL <ref_eql_sdl_functions>`,
:ref:`DDL <ref_eql_ddl_functions>`,
and :ref:`introspection <ref_eql_introspection_functions>`.



