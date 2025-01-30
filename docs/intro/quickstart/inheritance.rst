.. _ref_quickstart_inheritance:

========================
Adding Shared Properties
========================

.. edb:split-section::

  One common pattern in applications is to add shared properties to the schema that are used by multiple modules. For example, we might want to add a ``created_at`` and ``updated_at`` property to every object in our schema. We can do this by adding an abstract type and using it as a mixin for our other object types.

  .. code-block:: sdl-diff
    :caption: dbschema/default.gel

      module default {
        single optional global access_token: str;
        single optional global current_user := (
          select AccessToken filter .token = access_token
        ).user;

    +   abstract type Timestamped {
    +     required created_at: datetime {
    +       default := datetime_of_statement();
    +     };
    +     required updated_at: datetime {
    +       default := datetime_of_statement();
    +     };
    +   }
    +
    -   type User {
    +   type User extends Timestamped {
          required name: str;

          tokens := (select .<user[is AccessToken]);
        }

    -   type AccessToken {
    +   type AccessToken extends Timestamped {
          required user: User;
          required token: str {
            constraint exclusive;
          };
        }

    -   type Deck {
    +   type Deck extends Timestamped {
          required name: str;
          description: str;

          creator: User;

          cards := (select .<deck[is Card] order by .order);

          access policy creator_has_full_access
            allow all
            using (
              .creator ?= global current_user
            );
        };

    -   type Card {
    +   type Card extends Timestamped {
          required order: int64;
          required front: str;
          required back: str;

          required deck: Deck;

          access policy deck_creator_has_full_access
            allow all
            using (
              .deck.creator ?= global current_user
            );
        }
      }

.. edb:split-section::

  This will require that we make a manual migration since we will need to backfill the ``created_at`` and ``updated_at`` properties for all existing objects. We will just set the value to be the current wall time since we do not have a meaningful way to backfill the values for existing objects.

  .. code-block:: sh

    $ npx gel migration create
    fill_expr> datetime_of_statement()

    $ npx gel migrate

.. edb:split-section::

  Now when we look at the data in the UI, we will see the new properties on each of our object types.

