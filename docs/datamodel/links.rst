.. _ref_datamodel_links:

=====
Links
=====

:index: link

Link items define a specific relationship between two object types.  Link
instances relate one *object* to one or more different objects.

There are two kinds of link item declarations: *abstract links*, and
*concrete links*.  Abstract links are defined on module level and are
not tied to any particular object type. Typically this is done to set
some :ref:`attributes <ref_datamodel_attributes>`, define
:ref:`link properties <ref_datamodel_props>`, or setup :ref:`constraints
<ref_datamodel_constraints>`.  Concrete links are defined on specific object
types.  For more information and examples refer to
:ref:`this section <ref_eql_sdl_links>`.

Links are directional and have a *source* and one or more *targets*.
The number of targets as specified by keywords :ref:`required
<ref_eql_ddl_links_syntax>`, :ref:`single <ref_eql_ddl_links_syntax>`,
and :ref:`multi <ref_eql_ddl_links_syntax>`.  It is also possible to
restrict how many source objects can link to the same target via
:eql:constraint:`exclusive` constraint.

Links also have a policy of handling link target *deletion*. There are
4 possible *actions* that can be taken when this happens:

- ``RESTRICT`` - any attempt to delete the target object immediately
  raises an exception;
- ``DELETE SOURCE`` - when the target of a link is deleted, the source
  is also deleted;
- ``ALLOW`` - the target object is deleted and is removed from the
  set of the link targets;
- ``DEFERRED RESTRICT`` - any attempt to delete the target object
  raises an exception at the end of the transaction, unless by
  that time this object is no longer in the set of link targets.

This :ref:`section <ref_eql_ddl_links_syntax>` covers the syntax of
how to set these policies in more detail.
