.. _ref_datamodel_links:

=====
Links
=====

:index: link

Link items define a specific relationship between two object types.  Link
instances relate one *object* to one or more different objects.

There are two kinds of link item declarations: *abstract links*,
and *concrete links*.  Abstract links are defined on module level and are not
tied to any particular object type.  Concrete links are defined on specific
object types.


Definition
==========

Abstract Links
--------------

An *abstract link* may be defined in EdgeDB Schema using the ``abstract link``
declaration:

.. sdl:synopsis::

    abstract link <link_name> [ extending [(] <parent-link> [, ...] [)]]:
        [ <property-declarations> ]
        [ <attribute-declarations> ]

Parameters:

:sdl:synopsis:`<link-name>`
    Specifies the name of the link item.  Customarily, link names
    are lowercase, with words separated by underscores as necessary for
    readability.

:sdl:synopsis:`extending <parent-link> [, ...]`
    If specified, declares the *parents* of the link item.

    Use of ``extending`` creates a persistent schema relationship
    between this link and its parents.  Schema modifications
    to the parent(s) propagate to the child.

    If the same *property* name exists in more than one parent, or
    is explicitly defined in the new link and at least one parent,
    then the data types of the property targets must be *compatible*.
    If there is no conflict, the link properties are merged to form a
    single property in the new link item.

:sdl:synopsis:`<property-declarations>`
    :ref:`Property <ref_datamodel_props>` declarations.

:sdl:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_datamodel_attributes>` declarations.


Abstract links can also be defined using the :eql:stmt:`CREATE LINK`
EdgeQL command.


.. _ref_datamodel_links_concrete:

Concrete Links
--------------

:index: cardinality

A *concrete link* may be defined in EdgeDB Schema using the ``link``
declaration in the context of a ``type`` declaration:

.. sdl:synopsis::

    type <TypeName>:
        [required] [inherited] [{multi | single}] link <link-name> \
            [ extending ( <parent-link> [, ...] )] -> <type>:
            [ expr := <computable-expr> ]
            [ default := <default-expr> ]
            [ readonly := {true | false} ]
            [ <property-declarations> ]
            [ <attribute-declarations> ]
            [ <constraint-declarations> ]
            [ on target delete { restrict |
                                 allow |
                                 delete source |
                                 deferred restrict } ]

    # shorthand form for computable link declaration:

    type <TypeName>:
        [inherited] [{multi | single}] link <link-name> := <computable-expr>


Parameters:

:sdl:synopsis:`required`
    If specified, the link is considered *required* for the parent
    object type.  It is an error for an object to have a required
    link resolve to an empty value.  Child links **always** inherit
    the *required* attribute, i.e it is not possible to make a
    required link non-required by extending it.

:sdl:synopsis:`inherited`
    This qualifier must be specified if the link is *inherited* from
    one or more parent object types.

:sdl:synopsis:`multi`
    Specifies that there may be more than one instance of this link
    in an object, in other words, ``Object.link`` may resolve to a set
    of a size greater than one.

:sdl:synopsis:`single`
    Specifies that there may be at most *one* instance of this link
    in an object, in other words, ``Object.link`` may resolve to a set
    of a size not greater than one.  ``single`` is assumed if nether
    ``multi`` nor ``single`` qualifier is specified.

:sdl:synopsis:`extending <parent-link> [, ...]`
    If specified, declares the *parents* of the link item.

    Use of ``extending`` creates a persistent schema relationship
    between this link and its parents.  Schema modifications
    to the parent(s) propagate to the child.

    If the same *property* name exists in more than one parent, or
    is explicitly defined in the new link and at least one parent,
    then the data types of the property targets must be *compatible*.
    If there is no conflict, the link properties are merged to form a
    single property in the new link item.

:sdl:synopsis:`readonly`
    If specified, the link is considered *read-only*.  Modifications
    of this link are prohibited once an object is created.

:sdl:synopsis:`default`
    Specifies the default value for the link as an EdgeQL expression.
    The default value is used in an ``INSERT`` statement if an explicit
    value for this link is not specified.

:sdl:synopsis:`<computable-expr>`
    If specified, designates this link as a *computable link*
    (see :ref:`Computables <ref_datamodel_computables>`).  A computable
    link cannot be *required* or *readonly* (the latter is implied and
    always true).  There is a shorthand form using the ``:=`` syntax,
    as shown in the synopsis above.

:sdl:synopsis:`<property-declarations>`
    :ref:`Property <ref_datamodel_props>` declarations.

:sdl:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_datamodel_attributes>` declarations.

:sdl:synopsis:`<constraint-declarations>`
    :ref:`Constraint <ref_datamodel_constraints>` declarations.

:sdl:synopsis:`on target delete`
    On target delete options cover the situation when the target
    object of a link is deleted without explicitly updating the link.

:sdl:synopsis:`restrict`
    Prohibit deleting the link target as long as the source object exists.
    This is the default behavior.

:sdl:synopsis:`allow`
    Allow dropping the connection between the source and target when
    the target is deleted.

:sdl:synopsis:`delete source`
    Delete the source object if any link target is deleted. This means
    that for ``multi`` links the source object will be deleted
    if even one of the link targets is deleted (e.g. automatically
    dissolving a team when all team members are critical and one has
    been deleted).

:sdl:synopsis:`deferred restrict`
    Same as ``restrict``, but the check is performed at the end of
    transaction instead of immediately.


Concrete links can also be defined using the
:eql:stmt:`CREATE LINK` EdgeQL command.
