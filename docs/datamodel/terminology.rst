.. _ref_datamodel_terminology:

===========
Terminology
===========

.. important::

  Below is an overview of EdgeDB's terminology. Use it as a roadmap, but don't
  worry if it doesn't all make sense immediately. The following pages go into
  detail on each concept below.

An EdgeDB **instance** is a collection of databases that store their data in
a shared directory and are managed by a running EdgeDB process. You can create,
start, stop, and destroy instances on your local computer with the :ref:`EdgeDB
CLI <ref_cli_overview>`. Instances listen for incoming queries on a connection
port.

.. _ref_datamodel_databases:

Each instance can contain several **databases**, each with a unique name. At
the time of creation, all instances contain a single database called
``edgedb``. This is the default database; all incoming queries are executed
against it unless otherwise specified.

Each database can contain several **modules**, each with a unique name. Modules
can be used to organize large schemas into logical units. In SDL, ``module``
blocks are used to define types inside a particular module.

.. code-block:: sdl

  module default {
    # declare types here
  }

  module another_module {
    # more types here
  }

.. important::

  Some module names (``std``, ``math``, ``cal``, ``schema``, ``sys``, ``cfg``)
  are reserved by EdgeDB and contain pre-defined types, utility functions, and
  operators. It's common to define an application's entire schema inside a
  single module called ``default``.

Schemas are predominantly composed of **object types**, the EdgeDB equivalent
of SQL tables. Object types contain **properties** and **links**. Both
properties and links are associated with a unique name (e.g.
``first_name``, ``friends``, etc) and a cardinality, which can be either
**single** (the default) or **multi**.

Properties correspond to either a **scalar type** (e.g. ``str``, ``int64``) or
a **collection type** (an array or a tuple). They can be augmented with
**constraints**, **annotations**, and **default values**. They can also be
marked as **readonly**.

Links represent relationships between object types. Like properties, they can
be constrained, annotated, associated default values, and marked as readonly.
They can optionally contain **link properties**.

Links and properties can also be **computed**. Computed links and properties
are not physically stored in the database, but they can used in queries just
like non-computed ones. The value will be computed as needed.

Object types can be augmented with constraints, annotations, and **indexes**
(which speed up certain queries).

You can also define custom **functions**.
