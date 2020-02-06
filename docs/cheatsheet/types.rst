.. _ref_cheatsheet_types:

Types
=====

.. note::

    Types are commonly defined by :ref:`migrations
    <ref_cheatsheet_migrations>` using :ref:`SDL <ref_eql_sdl>`.

Define an abstract type:

.. code-block:: sdl

    abstract type HasImage {
        # just a URL to the image
        required property image -> str;
        index on (__subject__.image);
    }

Define a type extending from the abstract:

.. code-block:: sdl

    type User extending HasImage {
        required property name -> str;
    }

Define a type with constraints and defaults for properties:

.. code-block:: sdl

    type Review {
        required property body -> str;
        required property rating -> int64 {
            constraint min_value(0);
            constraint max_value(5);
        }
        required property flag -> bool {
            default := False;
        }

        required link author -> User;
        required link movie -> Movie;

        required property creation_time -> datetime {
            default := datetime_current();
        }
    }

Define a type with a property that is computed from the combination of
the other properties:

.. code-block:: sdl

    type Person extending HasImage {
        required property first_name -> str {
            default := '';
        }
        required property middle_name -> str {
            default := '';
        }
        required property last_name -> str;
        property full_name :=
            (
                (
                    (.first_name ++ ' ')
                    IF .first_name != '' ELSE
                    ''
                ) ++
                (
                    (.middle_name ++ ' ')
                    IF .middle_name != '' ELSE
                    ''
                ) ++
                .last_name
            );
        property bio -> str;
    }


Define an abstract links:

.. code-block:: sdl

    abstract link crew {
        # Provide a way to specify some "natural"
        # ordering, as relevant to the movie. This
        # may be order of importance, appearance, etc.
        property list_order -> int64;
    }

    abstract link directors extending crew;

    abstract link actors extending crew;


Define a type using abstract links and a computable property that
aggregates values from another linked type:

.. code-block:: sdl

    type Movie extending HasImage {
        required property title -> str;
        required property year -> int64;
        property description -> str;

        multi link directors extending crew -> Person;
        multi link actors extending crew -> Person;

        property avg_rating := math::mean(.<movie[IS Review].rating);
    }
