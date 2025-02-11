.. _ref_quickstart_inheritance:

========================
Adding Shared Properties
========================

.. edb:split-section::

  One common pattern in applications is to add shared properties to the schema that are used by multiple objects. For example, you might want to add a ``created_at`` and ``updated_at`` property to every object in your schema. You can do this by adding an abstract type and using it as a mixin for your other object types.

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
    +   type User extending Timestamped {
          required name: str;
        }

    -   type AccessToken {
    +   type AccessToken extending Timestamped {
          required user: User;
          required token: str {
            constraint exclusive;
          };
        }

    -   type Deck {
    +   type Deck extending Timestamped {
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
    +   type Card extending Timestamped {
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

  Since you don't have historical data for when these objects were actually created or modified, the migration will fall back to the default values set in the ``Timestamped`` type.

  .. code-block:: sh

    $ npx gel migration create
    did you create object type 'default::Timestamped'? [y,n,l,c,b,s,q,?]
    > y
    did you alter object type 'default::AccessToken'? [y,n,l,c,b,s,q,?]
    > y
    did you alter object type 'default::User'? [y,n,l,c,b,s,q,?]
    > y
    did you alter object type 'default::Card'? [y,n,l,c,b,s,q,?]
    > y
    did you alter object type 'default::Deck'? [y,n,l,c,b,s,q,?]
    > y
    Created /home/strinh/projects/flashcards/dbschema/migrations/00004-m1d2m5n.edgeql, id: m1d2m5n5ajkalyijrxdliioyginonqbtfzihvwdfdmfwodunszstya

    $ npx gel migrate
    Applying m1d2m5n5ajkalyijrxdliioyginonqbtfzihvwdfdmfwodunszstya (00004-m1d2m5n.edgeql)
    ... parsed
    ... applied

.. edb:split-section::

  Now when you look at the data in the UI, you will see the new properties on each of your object types.

  .. image:: https://placehold.co/600x400?text=Show+timestamped+properties
