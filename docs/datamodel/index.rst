.. eql:section-intro-page:: dna

==========
Data Model
==========

In this section you'll find an overview of the fundamental types, objects,
and concepts for EdgeDB, a relational database with strongly typed schema.
Here's a quick overview of the links below:

Types in EdgeDB include your own **Object Types** (e.g. *User*) and
**Abstract Types** for other types to extend (e.g. *HasEmailAddress* for
*User* and others can inherit), plus **Scalar Types** with single values
(str, int64, etc.) and **Collection Types** like **arrays** and **tuples**
for multiple values.

Start putting your **Object Types** together with **properties** and
**links**. Then build on them with items like **annotations** (readable
notes for others), **constraints** to set limits (e.g. maximum length,
minimum value, or even create your own), **indexes** for faster querying,
and **computables** to use expressions to define properties or links
(e.g. *property email := .user_name ++ '@' ++ .provider_name*).

**Expression Aliases** let you use existing types under new names to build
on them without touching the original - both in your schema or on the fly
inside a query. You can also create your own **functions**, strongly typed
along with everything else in EdgeDB. All this goes into the schema under
one or more **modules** (namespaces).


.. toctree::
    :maxdepth: 3

    overview
    objects
    scalars/index
    colltypes
    abstract
    functions
    links
    props
    computables
    indexes
    constraints
    aliases
    annotations
    modules
    databases
