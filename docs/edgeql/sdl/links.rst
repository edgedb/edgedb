.. _ref_eql_sdl_links:

=====
Links
=====

This section describes the SDL declarations pertaining to
:ref:`links <ref_datamodel_links>`.

Define a new link corresponding to the :ref:`more explicit DDL
commands <ref_eql_ddl_links>`.

.. sdl:synopsis::

    # Concrete link form used inside type declaration:
    [ required ] [{single | multi}] link <name>
      [ extending <base> [, ...] ] -> <type>
      [ "{" <subcommand>; [...] "}" ] ;

    # Computable link form used inside type declaration:
    [ required ] [{single | multi}] link <name> := <expression>;

    # Abstract link form:
    abstract link [<module>::]<name> [extending <base> [, ...]]
    [ "{" <subcommand>; [...] "}" ]

