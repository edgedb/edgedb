.. _ref_datamodel_props:

==========
Properties
==========

:ref:`Object types <ref_datamodel_object_types>` and
:ref:`links <ref_datamodel_links>` can contain *properties*: a name-value
collection of primitive data associated with the given object or link
instance.

Every property is declared to have a specific
:ref:`scalar type <ref_datamodel_scalar_types>` or a
:ref:`collection type <ref_datamodel_collection_types>`.

There are two kinds of property item declarations: *abstract properties*,
and *concrete properties*.  Abstract properties are defined on module level
and are not tied to any particular object type or link.  Concrete properties
are defined on specific object types.


Definition
==========

Abstract Properties
-------------------

An *abstract property* may be defined in EdgeDB Schema using the
``abstract property`` declaration:

.. eschema:synopsis::

    abstract property <prop-name> [ extending [(] <parent-prop> [, ...] [)]]:
        [ <attribute-declarations> ]

Parameters:

:eschema:synopsis:`<prop-name>`
    Specifies the name of the property item.  Customarily, property names
    are lowercase, with words separated by underscores as necessary for
    readability.

:eschema:synopsis:`extending <parent-prop> [, ...]`
    If specified, declares the *parents* of the property item.

    Use of ``extending`` creates a persistent schema relationship
    between this property and its parents.  Schema modifications
    to the parent(s) propagate to the child.

:eschema:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_datamodel_attributes>` declarations.


Abstract links can also be defined using the
:eql:stmt:`CREATE PROPERTY` EdgeQL command.


.. _ref_datamodel_props_concrete:

Concrete Properties
-------------------

A *concrete property* may be defined in EdgeDB Schema using the ``property``
declaration in the context of a ``type`` or ``abstract link`` declaration:

.. eschema:synopsis::

    type <TypeName>:
        [required] [inherited] [{multi|single}] property \
            <prop-name> [ extending ( <parent-prop> [, ...] ) ] -> <type>:
            [ expr := <computable-expr> ]
            [ default := <default-expr> ]
            [ readonly := {true | false} ]
            [ <attribute-declarations> ]
            [ <constraint-declarations> ]

    # shorthand form for computable property declaration:

    type <TypeName>:
        [inherited] [{multi|single}] property <prop-name> := <computable-expr>

    # link property declaration:

    abstract link <link-name>:
        [inherited] property <prop-name> \
            [ extending ( <parent-prop> [, ...] )] -> <type>:
            [ expr := <computable-expr> ]
            [ default := <default-expr> ]
            [ readonly := {true | false} ]
            [ <attribute-declarations> ]

    # shorthand form for computable link property declaration:

    abstract link <link-name>:
        [inherited] property <prop_name> := <computable-expr>


Parameters:

:eschema:synopsis:`required`
    If specified, the property is considered *required* for the
    parent object type.  It is an error for an object to have a required
    property resolve to an empty value.  Child properties **always**
    inherit the *required* attribute, i.e it is not possible to
    make a required property non-required by extending it.

    .. note::

        Link properties cannot be ``required``.

:eschema:synopsis:`inherited`
    This qualifier must be specified if the property is *inherited* from
    one or more parent object types or links.

:eschema:synopsis:`multi`
    Specifies that there may be more than one instance of this property
    in an object, in other words, ``Object.property`` may resolve to a set
    of a size greater than one.

:eschema:synopsis:`single`
    Specifies that there may be at most *one* instance of this property
    in an object, in other words, ``Object.property`` may resolve to a set
    of a size not greater than one.  ``single`` is assumed if nether
    ``multi`` nor ``single`` qualifier is specified.

    .. note::

        Link properties are always ``single``.

:eschema:synopsis:`extending <parent-prop> [, ...]`
    If specified, declares the *parents* of the property item.

    Use of ``extending`` creates a persistent schema relationship
    between this property and its parents.  Schema modifications
    to the parent(s) propagate to the child.

:eschema:synopsis:`readonly`
    If specified, the property is considered *read-only*.  Modifications
    of this property are prohibited once an object or link is created.

:eschema:synopsis:`default`
    Specifies the default value for the property as an EdgeQL expression.
    The default value is used in an ``INSERT`` statement if an explicit
    value for this property is not specified.

:eschema:synopsis:`<computable-expr>`
    If specified, designates this property as a *computable property*
    (see :ref:`Computables <ref_datamodel_computables>`).  A computable
    property cannot be *required* or *readonly* (the latter is implied and
    always true).  There is a shorthand form using the ``:=`` syntax,
    as shown in the synopsis above.

:eschema:synopsis:`<attribute-declarations>`
    :ref:`Schema attribute <ref_datamodel_attributes>` declarations.

:eschema:synopsis:`<constraint-declarations>`
    :ref:`Constraint <ref_datamodel_constraints>` declarations.


Concrete properties can also be defined using the
:eql:stmt:`CREATE PROPERTY` EdgeQL command.
