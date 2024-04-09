.. _ref_eql_ddl_future:

===============
Future Behavior
===============

This section describes the DDL commands pertaining to
:ref:`future <ref_datamodel_future>`.


Create future
=============

:eql-statement:

Enable a particular future behavior for the current schema.

.. eql:synopsis::

    create future <FutureBehavior> ";"

There's a :ref:`corresponding SDL declaration <ref_eql_sdl_future>`
for enabling a future behavior, which is the recommended way of doing this.

Description
-----------

The command ``create future`` enables the specified future behavior for
the current :versionreplace:`database;5.0:branch`.

Examples
--------

Enable simpler non-recursive access policy behavior :ref:`non-recursive access
policy <ref_datamodel_access_policies_nonrecursive>` for the current schema:

.. code-block:: edgeql

    create future nonrecursive_access_policies;


drop future
===========

:eql-statement:


Stop importing future behavior prior to the EdgeDB version in which it appears.

.. eql:synopsis::

    drop future <FutureBehavior> ";"


Description
-----------

The command ``drop future`` disables a currently active future behavior for the
current :versionreplace:`database;5.0:branch`. However, this is only possible
for versions of EdgeDB when the behavior in question is not officially
introduced. Once a particular behavior is introduced as the standard behavior
in an EdgeDB release, it cannot be disabled. Running this command will simply
denote that no special action is needed to enable it in this case.


Examples
--------

Disable simpler non-recursive access policy behavior :ref:`non-recursive
access policy <ref_datamodel_access_policies_nonrecursive>` for the current
schema. This will make access policy restrictions apply to the expressions
defining other access policies:

.. code-block:: edgeql

    drop future nonrecursive_access_policies;


Once EdgeDB 3.0 is released there is no more need for enabling non-recursive
access policy behavior anymore. So the above command will simply indicate that
the database no longer does anything non-standard.
