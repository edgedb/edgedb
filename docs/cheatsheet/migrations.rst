.. _ref_cheatsheet_migrations:

Migrations
==========

Migrate to a new schema using SDL:

.. code-block:: sdl

    module default {
        abstract type HasImage {
            # just a URL to the image
            required property image -> str;
            index on (.image);
        }

        type User extending HasImage {
            required property name -> str {
                # Ensure unique name for each User.
                constraint exclusive;
            }
        }

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

        abstract link crew {
            # Provide a way to specify some "natural"
            # ordering, as relevant to the movie. This
            # may be order of importance, appearance, etc.
            property list_order -> int64;
        }

        abstract link directors extending crew;

        abstract link actors extending crew;

        type Movie extending HasImage {
            required property title -> str;
            required property year -> int64;
            # Add an index for accessing movies by title and year,
            # separately and in combination.
            index on (.title);
            index on (.year);
            index on ((.title, .year));
            property description -> str;
            multi link directors extending crew -> Person;
            multi link actors extending crew -> Person;
            property avg_rating :=
                math::mean(.<movie[IS Review].rating);
        }

        type Label {
            annotation description :=
                'Special label to stick on reviews';
            required property comments -> str;
            link review -> Review {
                annotation description :=
                    'This review needs some attention';
            };
        }

        alias ReviewAlias := Review {
            # It will already have all the Review
            # properties and links.
            author_name := .author.name,
            movie_title := .movie.title,
        };

        alias MovieAlias := Movie {
            # A computed link for accessing all the
            # reviews for this movie.
            reviews := .<movie[IS Review]
        };
    };

Find the ``dbschema`` directory created by
:ref:`ref_cli_edgedb_project_init`, then put the above schema in the
``dbschema/default.esdl`` (you can also rename the file to anything
you like).

Then create a new migration using :ref:`ref_cli_edgedb_migration_create`.

Apply the migration using :ref:`ref_cli_edgedb_migrate`.
