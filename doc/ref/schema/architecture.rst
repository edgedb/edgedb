.. _ref_edgeql_architecture:

Schema architecture
-------------------

EdgeDB data schema is defined by a set of declarative documents.

Here's how the concepts mentioned in the overview can be defined in a
EdgeDB schema:

.. code-block:: eschema

    concept City:
        required link name to str
        required link country to Country:
            mapping := '*1'  # any given city only belongs to one country,
                             # but many cities can belong to the same
                             # country

    concept Country:
        required link name to str
        required link capital to City:
            mapping := '11'  # there can only be one capital per country
                             # and vice versa


EdgeDB schemas can define the following fundamental elements:
``atom``, ``link``, ``link property``, ``concept``, ``constraint``,
``action``, and ``event``. Any combination of these can be defined in
a given module.

Some elements are implicitly defined by being used in other
declarations. For example using a ``link`` in a ``concept``
declaration (like in the sample above) implicitly defines a ``link``
element with the specified name in the current module.

.. todo::

    Need to describe "generalized" and "specialized" notions. They are
    kinda like metaclass and class in the sense that:

    generalized [link] -> specialized [link] -> [link] instance

    are like

    metaclass -> class -> object

    But maybe we need better terminology.

    This is important when talking about definitions because certain
    things can only be defined on specialized elements.


Atoms
~~~~~

Atoms are one of the two EdgeDB node types. From schema standpoint
atoms are final and non-divisible, i.e. they never have outgoing
relationships. Most of the time atoms hold simple scalar values.
Fundamentally, atoms are the only elements that actually hold data (as
opposed to metadata), all other elements merely define relationship
semantics and behaviour. All atoms are usually either directly or
indirectly derived from one of the predefined fundamental types; it is
also possible to define custom fundamental types.

The following is the list of fundamental atoms defined in module
``std``:

* ``bool`` -- boolean data type
* ``bytes`` -- raw bytes data type
* ``date`` -- date data type
* ``datetime`` -- date and time data type
* ``decimal`` -- arbitrary-precision fixed-point decimal number
* ``float`` -- IEEE 754 floating point number
* ``int`` -- 8-byte integer data type
* ``json`` -- JSON data type
* ``sequence`` -- sequence datatype
* ``str`` -- text data type
* ``time`` -- time data type
* ``timedelta`` -- time interval data type
* ``uuid`` -- UUID data type


.. _ref_schema_architechture_concepts:

Concepts
~~~~~~~~

*Concepts* define *entity classes*. Every concept is always a
derivative from ``std::Object`` and always has the ``std::id`` (which
can be referenced by its short name ``id``) link pointing to a
``uuid`` atom. This means that each and every concept instance
(*entity*) has a universally-unique identifier. Concepts can define an
arbitrary number of links to other concepts or atoms.

.. code-block:: eschema

    concept City:
        required link name to str
        required link country to Country:
            mapping := '*1'  # any given city only belongs to one country,
                             # but many cities can belong to the same
                             # country


In the example above concept ``City`` defines two links: ``name`` as a
link to a string atom (links to atoms are called *atomic links*) and
``country`` as a link to the ``Country`` concept.

Each such link definition creates a new Link element specifically for
the (source, link, target) triple. Such link element implicitly
derives from a common *generic* link element with the same name.
Generic link elements define common behaviour and properties of the
link family and can either be defined explicitly as a separate ``link``
declaration of the schema, or implicitly, if no such declaration exists.


Links and Link Properties
~~~~~~~~~~~~~~~~~~~~~~~~~

Links signify explicit relationship between two nodes. Links are used
to bind concepts to concepts or atoms. Links have a standard hierarchy
whereby all *specialized* links derive from a single *generic* link of
the same name. Thus, if two different concepts within the same module
each define the ``name`` link, this will create three elements: a
generic ``name`` link and two specialized links derived from it for
each concept.

Generic links can themselves define a list of *link properties*, which
are the same to links as links are to concepts, except that link
properties can only target atoms.

Collectively links and link properties are called *pointers*, while
elements that can host pointers -- concepts and links -- are called
*source nodes*.

So, the general element relationship diagram looks like this:

.. aafig::
    :aspect: 60
    :scale: 150

     +---------+                    +----------------+
     |         |                    |                |
     | concept +------+{link}+------> concept / atom |
     |         |          +         |                |
     +---------+          |         +----------------+
                   {link property}
                          |
                          |
                       +--v---+
                       |      |
                       | atom |
                       |      |
                       +------+

Generic pointers can be defined explicitly in the corresponding sections.

.. code-block:: eschema

  link property assigned_on:
      title := "Link Assignment Timestamp"

  link name:
      title := "Name"
      link property assigned_on to datetime


*Link properties* are meant to qualify the kind of relationship the
``link`` denotes, but they are not part of the identity of this
relationship. This means that regardless of presence of *link
properties* there can only be at most one *link* of specific name
between any two entities.

A typical use case for link properties involves annotating things like
ranking of some set of common objects by several different subjects.
The ``rank`` doesn't make sense as either part of the ``User`` or
``Post``, in the example below, because it really depends on both of
them.

.. code-block:: eschema

    link favorites:
        link property rank to int

    concept Post:
        required link body to str
        required link owner to User

    concept User extending std::Named:
        link favorites to Post:
            mapping := '**'


Constraints
~~~~~~~~~~~

It is possible to add constraints to the definitions. There are some
built-in constraints that are available to be used without having to
define them first. It is also possible to create custom constraints if
necessary.


Built-in Constraints
********************

Atoms, links, link properties, and concepts can optionally define a
list of *constraints*, such as maximum length or a list of allowed
values. Constraints provide a mechanism for restricting the values of
atoms, links, or link properties to some desired range. For example,
an atom denoting a two-letter state code can be defined as:

.. code-block:: eschema

    atom state_code_t extending str:
        constraint minlength(2)
        constraint maxlength(2)

Below is a list of built-in constraint types:

- ``enum``: <array> --
  the value of the atom must be one of the specified values

- ``max``: <value> --
  specifies the maximum allowed value of the atom, the atom must be orderable

- ``maxlength``: <number> --
  restricts maximum length of textual representation of atom value in
  characters

- ``maxexclusive``: <value> --
  specifies the maximum allowed value, excluding the value itself, of the
  atom, the atom must be orderable

- ``min``: <value> --
  specifies the minimum allowed value of the atom, the atom must be orderable

- ``minlength``: <number> --
  restricts minimum length of textual representation of atom value in
  characters

- ``minexclusive``: <value> --
  specifies the minimum allowed value, excluding the value itself, of the
  atom, the atom must be orderable

- ``regexp``: <regular expression string> --
  specifies the regular expression that must match on a textual representation
  of atom value

- ``unique`` --
  the value of an atom must be unique


Custom Constraints
******************

It is possible to define custom constraints using EdgeQL expressions.
For example, suppose we need to define some atom to always take even
values:

.. code-block:: eschema

    constraint must_be_even:
        # {__subject__} is a special placeholder to refer what the
        # constraint is actually applied to
        expr := __subject__ % 2 = 0
        # when used in the errmessage, "subject" will be substituted
        # with the name of the atom or link the constraint has been
        # applied to
        errmessage := '{__subject__} value must be even.'

    atom foo_t extending int:
        constraint must_be_even

Custom constraints can refer to multiple links or link properties. In
that case the constraint would be defined on the concept or link,
respectively.

For more information on how custom constraints can be defined see
`Constraint Inheritance`_.


.. _ref_schema_architechture_inheritance:

Inheritance
~~~~~~~~~~~

All elements of EdgeDB schema form inheritance hierarchies. All,
except atoms, support multiple inheritance. This is an extremely
important aspect of EdgeDB data architecture that distinguishes it
from the majority of the contemporary databases. EdgeDB schema
primarily describes what attributes, links and properties an object
has, rather than behavior (there's nothing quite like the notion of
class methods used in OOP). This means that inheritance only affects
what something *is* (see
:ref:`IS operator in EdgeQL<ref_edgeql_types>`) and what attributes,
links and properties an object has. This makes multiple inheritance
easier to understand and use. In fact, many of the usage patterns for
multiple inheritance are the same as for *mixins* in OOP.

The full-fledged inheritance mechanism forms an additional dimension
of element relationships. All elements in the schema either directly
or indirectly derive from corresponding base elements:

* concepts derive from ``std::Object``
* atoms derive from one of the basic types
* links derive from ``std::link``
* link properties derive from ``std::link_property``
* constraints derive from ``std::constraint``

Each element can specify its parents with the ``extending`` field in the
schema.


Atom Inheritance
****************

Atoms are the only elements that do not support multiple inheritance
due to their nature of being "non-divisible", and also "non-composable".
The usual reason to extend atoms is to add constraints. Note that it
is never possible to relax constraints through atom inheritance. When
inheriting from a parent atom, a child atom can only add more
constraints.

Consider the following schema:

.. code-block:: eschema

    # define some additional constraints
    constraint must_be_even:
        expr := __subject__ % 2 = 0
        errmesage := 'Stable versions must be even.'

    constraint must_be_odd:
        expr := __subject__ % 2 = 1
        errmesage := 'Unstable versions must be odd.'

    # define atoms that will be used for version numbers
    atom ver_t extending int:
        constraint min(0)

    atom stable_ver_t extending ver_t:
        constraint must_be_even

    atom unstable_ver_t extending ver_t:
        constraint must_be_odd

    concept Project:
        required link major_version to ver_t
        required link minor_stable_version to stable_ver_t
        required link minor_unstable_version to unstable_ver_t

All of the atoms defined above have ``constraint`` as part of their
definition. A ``ver_t`` is defined to be an integer ≥ 0 by using a
built-in constraint ``min``. Since ``stable_ver_t`` and
``unstable_ver_t`` both inherit from ``ver_t``, they also must satisfy
the constraint of their parent. This means that ``stable_ver_t`` must
both be ≥ 0 and even, whereas ``unstable_ver_t`` must be ≥ 0 and odd.

.. note::

    When defining custom atoms throughout this documentation ``_t`` is
    appended to the name as a matter of convention. It stands for
    "type" and is meant to make it easier to distinguish custom atomic
    types from everything else.


Concept Inheritance
*******************

Concept inheritance can be compared to class inheritance. Every
*object* in EdgeDB is the concrete instance of a *concept* (much like
objects and classes relationship in OOP). The two major use-cases for
concept inheritance are representing *is-a* hierarchies and *mixins*.

For representing hierarchies sometimes no new links are added to the
concepts, but the type itself is carrying some meaning. For example,
consider a system that has ``Person`` and ``Employee`` concepts. An
``Employee`` is definitely also a ``Person``, so there are features
(and therefore DB queries) that are common to anyone who is a
``Person``. Yet, there may be some things only relevant to
``Employees``. This can more naturally be expresses via inheritance,
rather than through introducing a special "person_type" or
"is_employee" link (which would be a typical relational DB solution).

.. code-block:: eschema

    concept Person:
        required link name to str

    concept Employee extending Person

With the above schema it's possible to write a simple query looking
for a specific ``Person`` (including ``Employee``) or a specific
``Employee``:

.. code-block:: eql

    # looking for any Person named Alice Smith
    SELECT Person FILTER Person.name = 'Alice Smith';

    # looking for an Employee named Bob Johnson
    SELECT Employee FILTER Employee.name = 'Bob Johnson';

An example of using concept inheritance as a mixin pattern would be a
back-end for a bug-tracking system:

.. code-block:: eschema

    abstract concept Authored:
        required link author to User

    abstract concept Titled:
        required link title to str

    abstract concept Text:
        required link body to str

    abstract concept Commentable:
        link comments to Comment:
            mapping := '1*'

    abstract concept Timestamped:
        required link timetamp to datetime:
            default := SELECT datetime::current_datetime()
            # the timestap will be automatically set to the current
            # time if it is not specified at the point of comment
            # creation

    # specific concepts that will be instantiated
    concept User:
        required link name to str

    concept Issue extending (Authored, Titled, Text, Commentable,
                             Timestamped):
        required link status to str

    concept Comment extending Authored, Text, Timestamped

    concept Discussion extending (Authored, Titled, Text, Commentable,
                                  Timestamped)

    concept EmailTemplate extending Titled, Text

By using multiple inheritance it's possible to create a bunch of
concepts that share some common traits. Mixins make it easier to keep
consistent names for the same object properties, that in turn makes it
easier to write more generic and reusable code making use of those
properties. It's also easier to apply certain improvements
consistently, such as maybe realizing that all ``Timestamped`` objects
actually need two links ``created`` and ``modified`` (see
:ref:`Schema evolution<ref_schema_evolution>` for how to apply changes
to the existing schema).


Link Inheritance
****************

Link inheritance is similar to concept and atom inheritance.

Much like concepts links have an *is-a* hierarchy that can be defined
via inheritance and used in queries:

.. code-block:: eschema

    abstract link relatives:
        title := "Relatives"

    abstract link descendants extending relatives
    abstract link ancestors extending relatives

    link children extending descendants
    link grandchildren extending descendants
    link parents extending ancestors

    concept Person:
        required link name to str

        link children to Person:
            mapping := '**'

        link grandchildren to Person:
            mapping := '**'

        link parents to Person:
            mapping := '**'

With the above schema the following queries make use of the link
inheritance:

.. code-block:: eql

    # Select all grandchildren names
    SELECT Person.grandchildren.name FILTER Person.name = 'John Ham';

    # Select all descendants' names
    SELECT Person.descendants.name FILTER Person.name = 'John Ham';

    # Select all relatives' names
    SELECT Person.relatives.name FILTER Person.name = 'John Ham';


So, even though ``Person`` defines only concrete relationship links,
we can exploit inheritance to use implicit relationships.

Much like concepts use a mixin inheritance pattern to inherit links,
links can use the same pattern to inherit link properties.


Link Property Inheritance
*************************

Link Property inheritance works just like concept inheritance (with
the exception that there is no parallel to inheriting links on
concepts).


Constraint Inheritance
**********************

When constraints are defined (as opposed to being used in other
definitions such as those of links or atoms), they can also make use
of inheritance. The aspects of *is-a* hierarchy and *mixin* usage
pattern are similar to what has already been described in the case of
concept inheritance. Constraints also make use of overriding their
attributes to change what the constraint applies to. For example,
consider ``maxlength`` and ``minlength`` constraints:

.. code-block:: eschema

    # abstract constraint cannot be applied directly, but must be
    # inherited from, typically used as a mixin
    abstract constraint length on (len(<str>__subject__)):
        errmessage := 'Invalid {__subject__}'

    constraint max(any):
        expr := __subject__ <= $0
        errmessage := 'Maximum allowed value for {__subject__} is {$0}.'

    constraint min(any):
        expr := __subject__ >= $0
        errmessage := 'Minimum allowed value for {__subject__} is {$0}.'

    constraint maxlength(any) extending max, length:
        errmessage := '{__subject__} must be no longer than {$0} characters.'

    constraint minlength(any) extending min, length:
        errmessage := '{__subject__} must be no shorter than {$0} characters.'

Every constraint in the example above overrides the ``errmessage`` to
better correspond to its intended meaning. Additionally, ``length``
constraint overrides ``subject`` attribute, which basically determines
what the expression defined in ``expr`` actually operates on. By
default the ``subject`` is whatever the constraint is attached to
(typically, it's an *atom*, *link* or *link property*).

In principle, it's possible to construct a custom constraint to
process a string containing distance measured in meters or kilometers:

.. code-block:: eschema

    # assume that "max" and "min" are already defined

    # define an abstract constraint to covert a str distance into a
    # number
    abstract constraint distance on (
        <float>__subject__[:-2] * 1000 IF __subject__[:-2] = 'km' ELSE
        <float>__subject__[:-1]  # assuming suffix 'm'
    )

    constraint maxldistance(any) extending max, distance:
        errmessage := '{__subject__} must be no longer than {$0} meters.'

    constraint minldistance(any) extending min, distance:
        errmessage := '{__subject__} must be no shorter than {$0} meters.'


Schema composition
~~~~~~~~~~~~~~~~~~

In large applications, the schema will usually be split into several
:ref:`modules<ref_schema_evolution_modules>`. A *schema module*
defines the effective namespace for elements it defines. Schema
modules can import other modules to use schema elements they define.
This makes it very easy and natural to separate and group common
schema elements into modules for re-use. EdgeDB core provides a
default module: ``std`` which is always implicitly imported.

Since both the City and Country have a name, we can inherit them from
an abstract ``std::NamedObject``:

.. code-block:: eschema

    concept City extending NamedObject:
        link country to Country:
            mapping := '*1'

    concept Country extending NamedObject:
        link capital to City:
            mapping := '11'

``std::NamedObject`` is defined as *abstract*, thus it cannot be
instantiated, and exists solely for the purposes of being inherited
from.

Together, multiple inheritance, schema modules and namespace
separation form a very powerful schema re-use framework.
