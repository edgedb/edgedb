.. _ref_reference_paths:

=====
Paths
=====


A *path expression* (or simply a *path*) represents a set of values that are
reachable when traversing a given sequence of links or properties from some
source set.

The result of a path expression depends on whether it terminates with a link or
property reference.

a) if a path *does not* end with a property reference, then it represents a
   unique set of objects reachable from the set at the root of the path;

b) if a path *does* end with a property reference, then it represents a
   list of property values for every element in the unique set of
   objects reachable from the set at the root of the path.

The syntactic form of a path is:

.. eql:synopsis::

    <expression> <path-step> [ <path-step> ... ]

    # where <path-step> is:
      <step-direction> <pointer-name>

The individual path components are:

:eql:synopsis:`<expression>`
    Any valid expression.

:eql:synopsis:`<step-direction>`
    It can be one of the following:

    - ``.`` for an outgoing link reference
    - ``.<`` for an incoming or :ref:`backlink <ref_datamodel_links>`
      reference
    - ``@`` for a link property reference

:eql:synopsis:`<pointer-name>`
    This must be a valid link or link property name.

