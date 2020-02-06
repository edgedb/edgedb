.. _ref_cheatsheet_migrations:

Migrations
==========

Migrate to a new schema using SDL (must be done inside a transaction):

.. code-block:: edgeql-repl

    db> START TRANSACTION;
    START TRANSACTION
    db> CREATE MIGRATION m TO {
    ...     module default {
    ...         abstract type HasImage {
    ...             # just a URL to the image
    ...             required property image -> str;
    ...             index on (__subject__.image);
    ...         }
    ...         type User extending HasImage {
    ...             required property name -> str;
    ...         }
    ...         type Review {
    ...             required property body -> str;
    ...             required property rating -> int64 {
    ...                 constraint min_value(0);
    ...                 constraint max_value(5);
    ...             }
    ...             required property flag -> bool {
    ...                 default := False;
    ...             }
    ...             required link author -> User;
    ...             required link movie -> Movie;
    ...             required property creation_time -> datetime {
    ...                 default := datetime_current();
    ...             }
    ...         }
    ...         type Person extending HasImage {
    ...             required property first_name -> str {
    ...                 default := '';
    ...             }
    ...             required property middle_name -> str {
    ...                 default := '';
    ...             }
    ...             required property last_name -> str;
    ...             property full_name :=
    ...                 (
    ...                     (
    ...                         (.first_name ++ ' ')
    ...                         IF .first_name != '' ELSE
    ...                         ''
    ...                     ) ++
    ...                     (
    ...                         (.middle_name ++ ' ')
    ...                         IF .middle_name != '' ELSE
    ...                         ''
    ...                     ) ++
    ...                     .last_name
    ...                 );
    ...             property bio -> str;
    ...         }
    ...         abstract link crew {
    ...             # Provide a way to specify some "natural"
    ...             # ordering, as relevant to the movie. This
    ...             # may be order of importance, appearance, etc.
    ...             property list_order -> int64;
    ...         }
    ...         abstract link directors extending crew;
    ...         abstract link actors extending crew;
    ...         type Movie extending HasImage {
    ...             required property title -> str;
    ...             required property year -> int64;
    ...             property description -> str;
    ...             multi link directors extending crew -> Person;
    ...             multi link actors extending crew -> Person;
    ...             property avg_rating :=
    ...                 math::mean(.<movie[IS Review].rating);
    ...         }
    ...         type Label {
    ...             annotation description :=
    ...                 'Special label to stick on reviews';
    ...             required property comments -> str;
    ...             link review -> Review {
    ...                 annotation description :=
    ...                     'This review needs some attention';
    ...             };
    ...         }
    ...         alias ReviewAlias := Review {
    ...             # It will already have all the Review
    ...             # properties and links.
    ...             author_name := .author.name,
    ...             movie_title := .movie.title,
    ...         };
    ...         alias MovieAlias := Movie {
    ...             # A computable link for accessing all the
    ...             # reviews for this movie.
    ...             reviews := .<movie[IS Review]
    ...         };
    ...     }
    ... };
    CREATE MIGRATION
    db> COMMIT MIGRATION m;
    COMMIT MIGRATION
    db> COMMIT;
    COMMIT TRANSACTION
