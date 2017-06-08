.. _ref_edgeql_architecture:

Schema architecture
-------------------

EdgeDB data schema is defined by a set of documents similar to YAML in
structure.

Here's how the concepts mentioned in the overview can be defined in a
EdgeDB schema:

.. code-block:: eschema

    concept City:
        required link name to str
        required link country to Country:
            mapping: *1  # any given city only belongs to one country,
                         # but many cities can belong to the same
                         # country

    concept Country:
        required link name to str
        required link capital to City:
            mapping: 11  # there can only be one capital per country
                         # and vice versa


EdgeDB schemas can define the following fundamental elements:
``atom``, ``link``, ``linkproperty``, ``concept``, ``constraint``,
``action``, and ``event``. Any combination of these can be defined in
a given module. An empty module is also a valid module.

Some elements are implicitly defined by being used in other
declarations. For example using a ``link`` in a ``concept``
declaration (like in the sample above) implicitly defines a ``link``
element with the specified name in the current module.


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

The following is the list of fundamental atoms defined in
``edgedb.lang.schema.types``:

* ``str`` -- text data type
* ``int`` -- 8-byte integer data type
* ``float`` -- IEEE 754 floating point number
* ``decimal`` -- arbitrary-precision fixed-point decimal number
* ``bool`` -- boolean data type
* ``uuid`` -- UUID data type
* ``datetime`` -- date and time data type
* ``time`` -- time data type
* ``timedelta`` -- time interval data type
* ``sequence`` -- sequence datatype


Constraints
~~~~~~~~~~~

It is possible to add constraints to the definitions. There are some built-in constraints that are available to be used without having to define them first. It is also possible to create custom constraints if necessary.

Built-in Constraints
********************

Atoms can optionally define a list of *constraints*, such as maximum
length or a list of allowed values, which form the shape of atom
values *domain*. For example, an atom denoting state code can be
defined as:

.. code-block:: eschema

    atom state_code_t extends str:
        constraint minlength: 2
        constraint maxlength: 2


Below is a list of built-in constraint types:

- ``maxlength``: <number> --
  restricts maximum length of textual representation of atom value in characters

- ``minlength``: <number> --
  restricts minimum length of textual representation of atom value in characters

- ``max``: <value> --
  specifies the maximum allowed value of the atom, the atom must be orderable

- ``maxexclusive``: <value> --
  specifies the maximum allowed value, excluding the value itself, of the
  atom, the atom must be orderable

- ``min``: <value> --
  specifies the minimum allowed value of the atom, the atom must be orderable

- ``minexclusive``: <value> --
  specifies the minimum allowed value, excluding the value itself, of the
  atom, the atom must be orderable

- ``regexp``: <regular expression> --
  specifies the regular expression that must match on a textual representation
  of atom value

- ``enum``: <sequence> --
  the value of the atom must be one of the specified values

- ``unique`` --
  the value of an atom must be unique

Custom Constraints
******************

It is possible to define custom constraints using EdgeQL expressions.
For example, suppose we need to define some atom to always take even
values:

.. code-block:: eschema

    constraint must_be_even:
        expr:= subject % 2 = 0

    atom foo extends int:
        constraint must_be_even


.. _ref_schema_architechture_concepts:

Concepts
~~~~~~~~

*Concepts* define *entity classes*. Every concept is always a
derivative from ``std::Object`` and always has the ``std::uuid`` link
pointing to a ``uuid`` atom. This means that each and every concept
instance (*entity*) has a universally-unique identifier. Concepts can
define an arbitrary number of links to other concepts or atoms.

.. code-block:: eschema

    concept City:
        required link name to str
        required link country to Country:
            mapping: *1  # any given city only belongs to one country,
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
the same name. Thus, if two different concepts each define the
``name`` link, this will create three elements: a generic ``name``
link and two specialized links derived from it for each concept.

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

  linkproperty assigned_on:
      title: "Link Assignment Timestamp"

  link name:
      title: "Name"
      linkproperty assigned_on to datetime


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
        linkproperty rank to int

    concept Post:
        required link body to str
        required link owner to User

    concept User extends std::Named:
        link favorites to Post:
            mapping: **


.. _ref_schema_architechture_inheritance:

Inheritance
~~~~~~~~~~~

All four element classes of EdgeDB schema form inheritance
hierarchies. All elements, except atoms, support multiple inheritance.
This is an extremely important aspect of EdgeDB data architecture that
distinguishes it from the majority of the contemporary solutions.
There's an important difference between OO classes and EdgeDB schema
classes: schema classes have no methods. This means that inheritance
only affects what something *is* (see
:ref:`IS operator in EdgeQL<ref_edgeql_types>`) and what attributes,
links and properties an object has. This makes multiple inheritance a
much simpler concept to understand and use. In fact, many of the usage
patterns for multiple inheritance are the same as for *mixins* in OOP.

The full-fledged inheritance mechanism forms an additional dimension
of element relationships. All elements in the schema either directly
or indirectly derive from corresponding base elements:

* concepts derive from ``std::Object``
* atoms derive from one of the basic types
* links derive from ``std::link``
* link properties derive from ``std::link_property``

Each element can specify its parents with the "extends" field in the schema.


Atom Inheritance
****************

Atoms are the only elements that do not support multiple inheritance
due to their nature of being "non-divisible", and also "non-
composable". The usual reason to extend atoms is to add constraints.
Note that it is never possible to relax constraints through
inheritance, child atoms must have either equal or stricter
constraints.

.. code-block:: eschema

    atom state_code_t extends str:
        constraint minlength: 2
        constraint maxlength: 2

    concept Address:
        link state_code to state_code_t


Concept Inheritance
*******************

Concept inheritance can be compared to class inheritance. Every
*object* in EdgeDB is the concrete instance of a *concept* (much like
objects and classes relationship in OOP). The two major use-cases for
concept inheritance are representing *is-a* hierarchies and *mixins*.

For representing hierarchies sometimes no new links are added tot he
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

    concept Employee extends Person

With the above schema it's possible to write a simple query looking
for a specific ``Person`` (including ``Employee``) or a specific
``Employee``:

.. code-block:: eql

    # looking for any Person named Alice Smith
    SELECT Person FILTER Person.name = 'Alice Smith'

    # looking for an Employee named Bob Johnson
    SELECT Employee FILTER Employee.name = 'Bob Johnson'

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
            mapping: 1*

    abstract concept Timestamped:
        required link timetamp to datetime:
            default := SELECT datetime::current_datetime()
            # the timestap will be automatically set to the current
            # time if it is not specified at the point of comment
            # creation

    # specific concepts that will be instantiated
    concept User:
        required link name to str

    concept Issue extends (Authored, Titled, Text, Commentable,
                           Timestamped):
        required link status to str

    concept Comment extends Authored, Text, Timestamped

    concept Discussion extends (Authored, Titled, Text, Commentable,
                                Timestamped)

    concept EmailTemplate extends Titled, Text

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

Link inheritance is similar to concept inheritance.

.. code-block:: eschema

    abstract link relatives:
        title: "Relatives"

    abstract link descendants extends relatives
    abstract link ancestors extends relatives

    link children extends descendants
    link grandchildren extends descendants
    link parents extends ancestors

    concept Person:
        required link name to str

        link children to Person:
            mapping: **

        link grandchildren to Person:
            mapping: **

        link parents to Person:
            mapping: **


With the above schema:

.. code-block:: eql

    # Select all grandchildren names
    SELECT Person.grandchildren.name FILTER Person.name = 'John Ham';

    # Select all descendants' names
    SELECT Person.descendants.name FILTER Person.name = 'John Ham';

    # Select all relatives' names
    SELECT Person.relatives.name FILTER Person.name = 'John Ham'


So, even though ``Person`` defines only concrete relationship links,
we can exploit inheritance to use implicit relationships.


Link Property Inheritance
*************************

Link Property inheritance works just like link inheritance.


Schema composition
~~~~~~~~~~~~~~~~~~

In large applications, the schema will usually be split into several
files. All such documents within the same directory are considered to
be part of the same *schema module*. A *schema module* defines the
effective namespace for elements it defines. Schema modules can import
other modules to use schema elements they define. This makes it very
easy and natural to separate and group common schema elements into
modules for re-use. EdgeDB core provides a default module: ``std``
which is always implicitly imported.

Since both the City and Country have a name, we can inherit them from
an abstract ``std::NamedObject``:

.. code-block:: eschema

    concept City extends NamedObject:
        link country to Country:
            mapping: *1

    concept Country extends NamedObject:
        link capital to City:
            mapping: 11

``std::NamedObject`` is defined as *abstract*, thus it cannot be
instantiated, and exists solely for the purposes of being inherited
from.

Together, multiple inheritance, schema modules and namespace
separation form a very powerful schema re-use framework.
