.. _ref_datamodel_future:

===============
Future Behavior
===============

Any time that we add new functionality to EdgeDB we strive to do it in the
least disruptive way possible. Deprecation warnings, documentation and guides
can help make these transitions smoother, but sometimes the changes are just
too big, especially if they affect already existing functionality. It is often
inconvenient dealing with these changes at the same time as upgrading to a new
major version of EdgeDB. To help with this transition we introduce
:ref:`future <ref_eql_sdl_future>` specification.

The purpose of this specification is to provide a way to try out and ease into
an upcoming feature before a major release. Sometimes enabling future behavior
is necessary to fix some current issues. Other times enabling future behavior
can simply provide a way to test out the feature before it gets released, to
make sure that the current project codebase is compatible and well-behaved. It
provides a longer timeframe for adopting a new feature and for catching bugs
that arise from the change in behavior.

The ``future`` specification is intended to help with transitions between
major releases. Once a feature is released this specification is no longer
necessary to enable that feature and it will be removed from the schema during
the upgrade process.

Once some behavior is available as a ``future`` all new :ref:`projects
<ref_intro_projects>` enable this behavior by default when initializing an
empty database. It is possible to explicitly disable the ``future`` feature by
removing it from the schema, but it is not recommended unless the feature is
causing some issues which cannot be fixed otherwise. Existing projects don't
change their behavior by default, the ``future`` specification needs to be
added to the schema by the developer in order to gain early access to it.

At the moment there is only one ``future`` available:

- ``nonrecursive_access_policies``: makes access policies :ref:`non-recursive
  <ref_datamodel_access_policies_nonrecursive>` and simplifies policy
  interactions.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Future Behavior <ref_eql_sdl_future>`
  * - :eql:stmt:`DDL > CREATE FUTURE <create future>`
  * - :eql:stmt:`DDL > DROP FUTURE <drop future>`
