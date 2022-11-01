.. _ref_eql_sdl_future:

===============
Future Behavior
===============

This section describes the SDL commands pertaining to
:ref:`future <ref_datamodel_future>`.


Syntax
------

Declare that the current schema enables a particular future behavior.

.. sdl:synopsis::

    using future <FutureBehavior> ";"


Description
-----------

Future behavior declaration must be outside any :ref:`module block
<ref_eql_sdl_modules>` since this behavior affects the entire database and not
a specific module.


Examples
--------

Enable simpler non-recursive access policy behavior :ref:`non-recursive access
policy <ref_datamodel_access_policies_nonrecursive>` for the current schema:

.. code-block:: sdl

    using extension nonrecursive_access_policies;
