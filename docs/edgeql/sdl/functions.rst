.. _ref_eql_sdl_functions:

=========
Functions
=========

This section describes the SDL declarations pertaining to
:ref:`functions <ref_datamodel_functions>`.


Example
-------

Declare a custom function that concatenates the length of a string to
the end of the that string:

.. code-block:: sdl

    function foo(s: str) -> str
        using EdgeQL $$
            SELECT s ++ <str>len(a)
        $$;


Syntax
------

Define a new function corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_functions>`.

.. sdl:synopsis::

    function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    using <language> <functionbody> ;

    function <name> ([ <argspec> ] [, ... ]) -> <returnspec>
    "{"
        session_only := {true | false} ;
        [ <annotation-declarations> ]
        using <language> <functionbody> ;
    "}" ;

    # where <argspec> is:

    [ <argkind> ] <argname>: [ <typequal> ] <argtype> [ = <default> ]

    # <argkind> is:

    [ { variadic | named only } ]

    # <typequal> is:

    [ { set of | optional } ]

    # and <returnspec> is:

    [ <typequal> ] <rettype>


Description
-----------

The core of the declaration is identical to :eql:stmt:`CREATE FUNCTION`,
while the valid SDL sub-declarations are listed below:

:sdl:synopsis:`<annotation-declarations>`
    Set function :ref:`annotation <ref_eql_sdl_annotations>`
    to a given *value*.
