:orphan:

.. _glossary:

========
Glossary
========

.. NOTE: Please keep the entries sorted alphabetically

.. glossary::

   DDL
      Data Definition Language. DDL is a type of database-specific
      syntax used to define the structuring of schemas. Common DDL
      statements include ``CREATE``, ``DROP``, and ``ALTER``.

   link
      Link items define a specific relationship between two object types. Link
      instances relate one object to one or more different objects.

      More on links in :ref:`Data Model <ref_datamodel_links>` and
      :ref:`Cookbook <ref_cookbook_links>`.

   set reference
      An identifier that represents a set of values. It can be the name of an
      object type or an *expression alias* (defined in a statement :ref:`WITH
      block <ref_eql_with>` or in the schema via an :ref:`alias declaration
      <ref_eql_sdl_aliases>`. or a qualified schema name).

   simple path
      A path which begins with a :term:`set reference`.
