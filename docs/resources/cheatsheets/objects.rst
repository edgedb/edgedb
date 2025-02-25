.. _ref_cheatsheet_object_types:

Object types
============


Define an abstract type:

.. code-block:: sdl

    abstract type HasImage {
        # just a URL to the image
        required image: str;
        index on (.image);
    }


----------


Define a type extending from the abstract:

.. code-block:: sdl

    type User extending HasImage {
        required name: str {
            # Ensure unique name for each User.
            constraint exclusive;
        }
    }


----------


Define a type with constraints and defaults for properties:

.. code-block:: sdl

    type Review {
        required body: str;
        required rating: int64 {
            constraint min_value(0);
            constraint max_value(5);
        }
        required flag: bool {
            default := False;
        }

        required author: User;
        required movie: Movie;

        required creation_time: datetime {
            default := datetime_current();
        }
    }


----------


Define a type with a property that is computed from the combination of
the other properties:

.. code-block:: sdl

    type Person extending HasImage {
        required first_name: str {
            default := '';
        }
        required middle_name: str {
            default := '';
        }
        required last_name: str;
        full_name :=
            (
                (
                    (.first_name ++ ' ')
                    if .first_name != '' else
                    ''
                ) ++
                (
                    (.middle_name ++ ' ')
                    if .middle_name != '' else
                    ''
                ) ++
                .last_name
            );
        bio: str;
    }



----------


Define an abstract links:

.. code-block:: sdl

    abstract link crew {
        # Provide a way to specify some "natural"
        # ordering, as relevant to the movie. This
        # may be order of importance, appearance, etc.
        list_order: int64;
    }

    abstract link directors {
        extending crew;
    };

    abstract link actors {
        extending crew;
    };


----------


Define a type using abstract links and a computed property that
aggregates values from another linked type:

.. code-block:: sdl

    type Movie extending HasImage {
        required title: str;
        required year: int64;

        # Add an index for accessing movies by title and year,
        # separately and in combination.
        index on (.title);
        index on (.year);
        index on ((.title, .year));

        description: str;

        multi directors: Person {
            extending crew;
        };
        multi actors: Person {
            extending crew
        };

        avg_rating := math::mean(.<movie[is Review].rating);
    }



----------


Define an :eql:type:`auto-incrementing <sequence>` scalar type and an
object type using it as a property:

.. code-block:: sdl

    scalar type TicketNo extending sequence;

    type Ticket {
        number: TicketNo {
            constraint exclusive;
        }
    }

.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Object types <ref_datamodel_object_types>`
  * - :ref:`SDL > Object types <ref_eql_sdl_object_types>`
  * - :ref:`DDL > Object types <ref_eql_ddl_object_types>`
  * - :ref:`Introspection > Object types
      <ref_datamodel_introspection_object_types>`
