.. _ref_guide_data_migrations_postgres:

========
Postgres
========

In this guide, we show how to move your data from Postgres to EdgeDB. However,
most of the approaches covered here should be applicable to other SQL
databases as well.

As an example we'll use an app that allowed users to chat. The main features
of this app revolve around posting and responding to messages. Once the data
is moved, it is then possible to use :ref:`EdgeQL <ref_edgeql>` instead of SQL
to fetch the desired data in the app's API calls. Here we'll mainly focus on
the data structures used for that, how to reflect them into EdgeDB, and how to
script moving the data across to the EdgeDB database.


Schema modeling
---------------

Let's start from an overview of the SQL tables we have:

.. code-block::

    social=> \d
                  List of relations
     Schema |       Name       |   Type   | Owner
    --------+------------------+----------+-------
     public | badges           | table    | myapp
     public | bookmarks        | table    | myapp
     public | bookmarks_id_seq | sequence | myapp
     public | posts            | table    | myapp
     public | posts_id_seq     | sequence | myapp
     public | statuses         | table    | myapp
     public | statuses_id_seq  | sequence | myapp
     public | threads          | table    | myapp
     public | threads_id_seq   | sequence | myapp
     public | users            | table    | myapp
     public | users_id_seq     | sequence | myapp
    (11 rows)

The ``users`` and ``posts`` tables store the bulk of the content, whereas the
``badges``, ``bookmarks``, ``statuses``, and ``threads`` tables store some
metadata that helps us tie everything together. When importing data, we want
to start with tables that are standalone and don't reference anything else and
then move on to more complex data. In this case, ``badges`` and ``statuses``
are such tables, so let's look at them:

.. lint-off

.. code-block::

    social=> \d badges
                    Table "public.badges"
       Column    | Type | Collation | Nullable | Default
    -------------+------+-----------+----------+---------
     name        | text |           | not null |
     description | text |           | not null |
    Indexes:
        "badges_pkey" PRIMARY KEY, btree (name)
    Referenced by:
        TABLE "users" CONSTRAINT "users_badge_name_fkey" FOREIGN KEY (badge_name) REFERENCES badges(name)

    social=> \d statuses
                                Table "public.statuses"
     Column |  Type   | Collation | Nullable |               Default
    --------+---------+-----------+----------+--------------------------------------
     id     | integer |           | not null | nextval('statuses_id_seq'::regclass)
     title  | text    |           | not null |
    Indexes:
        "statuses_pkey" PRIMARY KEY, btree (id)
        "statuses_title_key" UNIQUE CONSTRAINT, btree (title)
    Referenced by:
        TABLE "users" CONSTRAINT "users_status_id_fkey" FOREIGN KEY (status_id) REFERENCES statuses(id)

.. lint-on

The ``badges`` table uses the ``name`` of the badge as a primary key as
opposed to a separate ``id``. In order to reflect that properly in EdgeDB, we
would have to add an :eql:constraint:`exclusive` constraint to this property.
Meanwhile ``not null`` makes the property ``required``, leaving us with a type
like this:

.. code-block:: sql

    type Badge {
        required name: str {
            constraint exclusive;
        }
        required description: str;
    }

The ``statuses`` table has a dedicated ``id`` column in addition to ``title``.
However, the automatic ``id`` in EdgeDB is a :eql:type:`uuid`, whereas in our
original dataset it is an ``integer``. Let's assume that for this table we
never actually use the ``id`` in our code, relying instead on the fact that
``title`` is ``UNIQUE`` and serves as a much more descriptive identifier. We
can use the unique title to correctly map the data during our migration
without the need to also copy the old ``id``. This leaves us with the
following type:

.. code-block:: sql

    type Status {
        required title: str {
            constraint exclusive;
        }
    }

Next, we can look at the ``users`` table:

.. lint-off

.. code-block::

    social=> \d users
                                     Table "public.users"
         Column      |  Type   | Collation | Nullable |              Default
    -----------------+---------+-----------+----------+-----------------------------------
     id              | integer |           | not null | nextval('users_id_seq'::regclass)
     name            | text    |           | not null |
     email           | text    |           | not null |
     password        | text    |           | not null |
     client_settings | jsonb   |           |          |
     badge_name      | text    |           |          |
     status_id       | integer |           |          |
    Indexes:
        "users_pkey" PRIMARY KEY, btree (id)
        "users_email_key" UNIQUE CONSTRAINT, btree (email)
        "users_name_key" UNIQUE CONSTRAINT, btree (name)
    Foreign-key constraints:
        "users_badge_name_fkey" FOREIGN KEY (badge_name) REFERENCES badges(name)
        "users_status_id_fkey" FOREIGN KEY (status_id) REFERENCES statuses(id)
    Referenced by:
        TABLE "bookmarks" CONSTRAINT "bookmarks_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(id)
        TABLE "posts" CONSTRAINT "posts_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(id)

.. lint-on

The ``users`` table, like ``statuses``, has an ``id`` column, which is not a
:eql:type:`uuid`. Instead of omitting the ``id`` data, we'll record it as
``app_id`` in EdgeDB to facilitate the transition. We may still want to
eventually drop it in favor of the built-in ``id`` from EdgeDB, but we need it
for now. Incidentally, even if the ``id`` was specified as a ``uuid`` value
the recommended process is to record it as ``app_id`` as opposed to try and
replicate it as the main object ``id``. It is, however, also possible to bring
it over as the main ``id`` by adjusting certain :ref:`client connection
settings <ref_std_cfg_client_connections>`. The column ``client_settings``
would become a :eql:type:`json` property. The columns ``badge_name`` and
``status_id`` reference ``badges`` and ``statuses`` respectively and will
become *links* in EdgeDB instead of *properties*, even though a property would
more closely mirror how they are stored in Postgres:

.. code-block:: sql

    type User {
        required app_id: int32 {
            # It was unique originally, so this should be preserved.
            constraint exclusive;
        }
        required name: str {
            constraint exclusive;
        }
        required email: str {
            constraint exclusive;
        }
        required password: str;
        client_settings: json;

        # Both badge and status are optional.
        badge: Badge;
        status: Status;
    }

The next table to consider is ``threads``, which provides a way to group posts
by referring to it:

.. lint-off

.. code-block::

    social=> \d threads
                                Table "public.threads"
     Column |  Type   | Collation | Nullable |               Default
    --------+---------+-----------+----------+-------------------------------------
     id     | integer |           | not null | nextval('threads_id_seq'::regclass)
     title  | text    |           |          |
    Indexes:
        "threads_pkey" PRIMARY KEY, btree (id)
    Referenced by:
        TABLE "posts" CONSTRAINT "posts_thread_id_fkey" FOREIGN KEY (thread_id) REFERENCES threads(id)

.. lint-on

This table has very simple structure that we've seen before, so we can model
it similarly to our ``Status`` type in the new database. However, just like we
did for the ``users`` table, we may want to preserve the original ``id`` as
``app_id``:

.. code-block:: sql

    type Thread {
        required app_id: int32 {
            constraint exclusive;
        }
        title: str;
    }

Then we look at the ``posts`` table:

.. lint-off

.. code-block::

    social=> \d posts
                                            Table "public.posts"
        Column     |           Type           | Collation | Nullable |              Default
    ---------------+--------------------------+-----------+----------+-----------------------------------
     id            | integer                  |           | not null | nextval('posts_id_seq'::regclass)
     body          | text                     |           | not null |
     creation_time | timestamp with time zone |           | not null |
     edited_time   | timestamp with time zone |           |          |
     user_id       | integer                  |           | not null |
     thread_id     | integer                  |           |          |
     reply_to_id   | integer                  |           |          |
    Indexes:
        "posts_pkey" PRIMARY KEY, btree (id)
    Foreign-key constraints:
        "posts_reply_to_id_fkey" FOREIGN KEY (reply_to_id) REFERENCES posts(id)
        "posts_thread_id_fkey" FOREIGN KEY (thread_id) REFERENCES threads(id)
        "posts_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(id)
    Referenced by:
        TABLE "bookmarks" CONSTRAINT "bookmarks_post_id_fkey" FOREIGN KEY (post_id) REFERENCES posts(id)
        TABLE "posts" CONSTRAINT "posts_reply_to_id_fkey" FOREIGN KEY (reply_to_id) REFERENCES posts(id)

.. lint-on

The ``posts`` table also has an ``id`` that we want to keep around, at least
during the transition. We have a couple of columns using a ``timestamp with
time zone`` value, so they'll become :eql:type:`datetime` properties in
EdgeDB. The ``user_id``, ``thread_id``, and ``reply_to_id`` columns will
become *links* to ``User``, ``Thread``, and ``Post`` respectively:

.. code-block:: sql

    type Post {
        required app_id: int32 {
            constraint exclusive;
        }
        required body: str {
            constraint exclusive;
        }
        required creation_time: datetime {
            # We might as well provide a default here so we don't have
            # to pass it all the time when making a new post.
            default := datetime_current();
        }
        edited_time: datetime;

        required user: User;
        thread: Thread;
        reply_to: Post;
    }

Finally, we get to ``bookmarks``, which refers to both ``users`` and
``posts``:

.. lint-off

.. code-block::

    social=> \d bookmarks
                                 Table "public.bookmarks"
     Column  |  Type   | Collation | Nullable |                Default
    ---------+---------+-----------+----------+---------------------------------------
     id      | integer |           | not null | nextval('bookmarks_id_seq'::regclass)
     user_id | integer |           | not null |
     post_id | integer |           | not null |
     note    | text    |           |          |
    Indexes:
        "bookmarks_pkey" PRIMARY KEY, btree (id)
    Foreign-key constraints:
        "bookmarks_post_id_fkey" FOREIGN KEY (post_id) REFERENCES posts(id)
        "bookmarks_user_id_fkey" FOREIGN KEY (user_id) REFERENCES users(id)

.. lint-on

This is expressing a many-to-many relationship between ``users`` and
``posts``, which we can model as a *multi link*. We can then declare the
``note`` as a *link property*. Since we're likely to want to fetch bookmarks
given a particular user, it makes sense to place the link on the ``User``
type. In the end we end up with a schema that looks something like this:

.. code-block:: sql

    type Badge {
        required name: str {
            constraint exclusive;
        }
        required description: str;
    }

    type Status {
        required title: str {
            constraint exclusive;
        }
    }

    type User {
        required app_id: int32 {
            constraint exclusive;
        }
        required name: str {
            constraint exclusive;
        }
        required email: str {
            constraint exclusive;
        }
        required password: str;
        client_settings: json;

        badge: Badge;
        status: Status;

        # Multi link to the Post objects
        multi bookmark: Post {
            note: str;
        }
    }

    type Thread {
        required app_id: int32 {
            constraint exclusive;
        }
        title: str;

        # Let's add a computed link back to posts.
        posts := .<thread[is Post];
    }

    type Post {
        required app_id: int32 {
            constraint exclusive;
        }
        required body: str {
            constraint exclusive;
        }
        required creation_time: datetime {
            default := datetime_current();
        }
        edited_time: datetime;

        required user: User;
        thread: Thread;
        reply_to: Post;
    }


Copying the data
----------------

Now that we have a schema, we can use :ref:`ref_cli_edgedb_project_init` to
set up our new EdgeDB database. A new schema migration is added via
:ref:`ref_cli_edgedb_migration_create` and then :ref:`edgedb migrate
<ref_cli_edgedb_migration_apply>` applies the schema changes to the database.
After the schema migration, we'll still need to copy the existing data from
Postgres. JSON is a pretty good intermediate format for this operation. EdgeDB
can cast data from :eql:type:`json` to all of the built-in scalar types, so we
should be able to use a JSON dump with minimal additional processing when
importing all the data.

We will dump ``badges`` and ``statuses`` first:

.. lint-off

.. code-block::

    social=> SELECT ROW_TO_JSON(t) FROM badges AS t;
                                    row_to_json
    ---------------------------------------------------------------------------
     {"name":"admin","description":"Superuser who can do anything"}
     {"name":"moderator","description":"User who can edit other user's posts"}
    (2 rows)

    social=> SELECT ROW_TO_JSON(t) FROM statuses AS t;
            row_to_json
    ----------------------------
     {"id":1,"title":"happy"}
     {"id":2,"title":"sad"}
     {"id":3,"title":"excited"}
     {"id":4,"title":"mad"}
    (4 rows)

.. lint-on

These tables can be dumped directly to a file using a ``COPY ... TO
<filename>`` command. We can then read the files and use a simple loop to
import the data into EdgeDB.

.. note::

    When Postgres dumps the JSON data as text files there will be a known
    gotcha causing all the backslashes used to escape characters inside JSON
    string values to be doubled. This is because the *text* format causes
    backslashes themselves to be escaped. This needs to be accounted for when
    reading the resulting files. Accomplishing this through the Python client
    library would look like the following:

.. code-block:: python

    for line in open('badges.json'):
        client.query('''
            with data := to_json(<str>$line)
            insert Badge {
                name := <str>data['name'],
                description := <str>data['description'],
            }
        ''',
        line=line.replace(r'\\', '\\'))

    for line in open('statuses.json'):
        client.query('''
            with data := to_json(<str>$line)
            insert Status {
                title := <str>data['title'],
            }
        ''',
        line=line.replace(r'\\', '\\'))

The ``threads`` table can likewise be dumped directly as JSON with the only
minor difference being that we want to change the ``id`` to ``app_id`` when we
move the data to EdgeDB:

.. code-block:: python

    for line in open('threads.json'):
        client.query('''
            with data := to_json(<str>$line)
            insert Thread {
                app_id := <int32>data['id'],
                title := <str>data['title'],
            }
        ''',
        line=line.replace(r'\\', '\\'))

To copy the ``users`` table, we may want to use a more complex ``SELECT`` that
joins the statuses so we can match them by their unique names:

.. code-block::

    SELECT ROW_TO_JSON(t)
    FROM (
      SELECT
        users.id, name, email, password, client_settings,
        badge_name, statuses.title
      FROM users
      LEFT JOIN statuses ON status_id = statuses.id
    ) AS t;

When we run our import script, we can convert the ``badge_name`` and
``status`` JSON values into the corresponding objects by using sub-queries:

.. code-block:: python

    for line in open('users.json'):
        client.query('''
            with data := to_json(<str>$line)
            insert User {
                app_id := <int32>data['id'],
                name := <str>data['name'],
                email := <str>data['email'],
                password := <str>data['password'],
                client_settings := data['client_settings'],
                badge := (
                    select Badge filter .name = <str>data['badge_name']
                ),
                status := (
                    select Status filter .title = <str>data['status']
                ),
            }
        ''',
        line=line.replace(r'\\', '\\'))

The ``posts`` table can be dumped as JSON directly, but we'll need to write
sub-queries in the import script to correctly link ``Post`` objects. In order
to make this simpler, we can order the original data by ``creation_time`` so
we know any ``Post`` object that is referenced by the ``reply_to_id`` has
already been re-created in EdgeDB.

.. code-block:: python

    for line in open('posts.json'):
        client.query('''
            with data := to_json(<str>$line)
            insert Post {
                app_id := <int32>data['id'],
                body := <str>data['body'],
                creation_time := <datetime>data['creation_time'],
                edited_time := <datetime>data['edited_time'],
                user := (
                    select User filter .app_id = <int32>data['user_id']
                ),
                thread := (
                    select Thread filter .app_id = <int32>data['thread_id']
                ),
                reply_to := (
                    select detached Post
                    filter .app_id = <int32>data['reply_to_id']
                ),
            }
        ''',
        line=line.replace(r'\\', '\\'))

Finally, we can deal with the bookmarks since we've imported both the users
and the posts. The ``bookmarks`` table can be dumped as JSON directly, and
then we can write appropriate ``update`` query to add this data to EdgeDB:

.. code-block:: python

    for line in open('bookmarks.json'):
        client.query('''
            with data := to_json(<str>$line)
            update User
            filter .app_id = <int32>data['user_id']
            set {
                bookmark += (
                    select Post {
                        @note := <str>data['note']
                    }
                    filter .app_id = <int32>data['post_id']
                ),
            }
        ''',
        line=line)

We use ``+=`` in our ``update`` query to add data incrementally. This way we
don't need to further organize the bookmarks when importing. This approach
also mimics how the bookmarks might be created in the app going forward.

After all the import scripts, we end up with data that looks something like
this:

.. lint-off

.. code-block:: edgeql-repl

    local:db> select User {
    .........   name,
    .........   email,
    .........   status: {title},
    .........   badge: {name},
    .........   bookmark: {
    .........     @note,
    .........     body,
    .........     user: {name}
    .........   },
    ......... } filter .name = 'Cameron';
    {
      default::User {
        name: 'Cameron',
        email: 'cameron@edgedb.com',
        status: {},
        badge: default::Badge {name: 'admin'},
        bookmark: {
          default::Post {
            body: 'Hey everyone! How\'s your day going?',
            user: default::User {name: 'Alice'},
            @note: 'rendering glitch',
          },
          default::Post {
            body: 'Funny you ask, Alice. I actually work at EdgeDB!',
            user: default::User {name: 'Dana'},
            @note: 'follow-up',
          },
          default::Post {
            body: 'Pineapple on pizza? No way! It\'s a crime against taste buds.',
            user: default::User {name: 'Billie'},
            @note: {},
          },
        },
      },
    }

.. lint-on