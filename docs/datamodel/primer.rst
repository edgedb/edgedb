Primer
------

This section is indended as a rapid-fire overview of SDL syntax so you can hit
the ground running with EdgeDB. Refer to the linked pages for more in-depth
documentation.

Define object types
^^^^^^^^^^^^^^^^^^^

Object types contain **properties** and **links** to other
object types. They are analogous to tables in SQL.

.. code-block:: sdl

  type Movie {
    property title -> str;  # optional by default
  }

See :ref:`Schema > Object types <ref_std_object_types>`.

Define properties
^^^^^^^^^^^^^^^^^

.. code-block:: sdl

  type Movie {
    required property title -> str;       # required
    property release_year -> int64;       # optional
  }

See :ref:`Schema > Properties <ref_datamodel_props>`.

Define constraints
^^^^^^^^^^^^^^^^^^

.. code-block:: sdl

  type Movie {
    required property title -> str {
      constraint unique;
      constraint min_len_value(8);
      constraint regexp(r'^[A-Za-z0-9 ]+$');
    }
  }

See :ref:`Schema > Constraints <ref_datamodel_constraints>`.

Define indexes
^^^^^^^^^^^^^^

.. code-block:: sdl

  type Movie {
    required property title -> str;
    required property release_year -> int64;

    index on (.title);                 # simple index
    index on (.title, .release_year);  # compound index
    index on (str_upper(.title));      # computed index
  }

The ``id`` property, all links, and all properties with ``exclusive``
constraints are automatically indexed.

See :ref:`Schema > Indexes <ref_datamodel_indexes>`.

Define computed properties
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sdl

  type Movie {
    required property title -> str;
    property uppercase_title := str_upper(.title);
  }

See :ref:`Schema > Computeds <ref_datamodel_computed>`.

Define links
^^^^^^^^^^^^

.. code-block:: sdl

  type Movie {
    required property title -> str;
    link director -> Person;
  }

  type Person {
    required property name -> str;
  }

Use the ``required`` and ``multi`` keywords to specify the cardinality of the
relation.

.. code-block:: sdl

  type Movie {
    required property title -> str;

    link cinematographer -> Person;             # zero or one
    required link director -> Person            # exactly one
    multi link writers -> Person;               # zero or more
    required multi link actors -> Person;       # one or more
  }

  type Person {
    required property name -> str;
  }

To define a one-to-one relation, use an ``exclusive`` constraint.

.. code-block:: sdl

  type Movie {
    required property title -> str;
    required link stats -> MovieStats {
      constraint exclusive;
    };
  }

  type MovieStats {
    required property budget -> int64;
    required property box_office -> int64;
  }

See :ref:`Schema > Links <ref_datamodel_links>`.

Define computed links
^^^^^^^^^^^^^^^^^^^^^

Links can be computed. The example below defines a backlink.

.. code-block:: sdl

  type Movie {
    required property title -> str;
    multi link actors -> Person;
  }

  type Person {
    required property name -> str;
    link acted_in := .<actors[is Movie]
  }

See :ref:`Schema > Computeds > Backlinks <ref_datamodel_links_backlinks>`.

Define schema mixins
^^^^^^^^^^^^^^^^^^^^

.. code-block:: sdl

  abstract type Content {
    required property title -> str;
  }

  type Movie extending Content {
    required property release_year -> int64;
  }

  type TVShow extending Content {
    required property num_seasons -> int64;
  }

See :ref:`Schema > Object types > Inheritance
<ref_datamodel_objects_inheritance>`.

Define polymorphic links
^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: sdl

  abstract type Content {
    required property title -> str;
  }

  type Movie extending Content {
    required property release_year -> int64;
  }

  type TVShow extending Content {
    required property num_seasons -> int64;
  }

  type Franchise {
    required property name -> str;
    multi link entries -> Content;
  }

See :ref:`Schema > Links > Polymorphism
<ref_datamodel_link_polymorphic>`.

