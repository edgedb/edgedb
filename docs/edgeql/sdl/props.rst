.. _ref_eql_sdl_props:

==========
Properties
==========

This section describes the SDL declarations pertaining to
:ref:`properties <ref_datamodel_props>`.

Define a new property corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_props>`.

.. sdl:synopsis::


    # Concrete property form used inside type declaration:
    [ required ] [{single | multi}] property <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{" <subcommand>; [...] "}" ] ;

    # Computable property form used inside type declaration:
    [ required ] [{single | multi}] property <name> := <expression>;

    # Abstract property form:
    abstract property [<module>::]<name> [extending <base> [, ...]]
    [ "{" <subcommand>; [...] "}" ]
