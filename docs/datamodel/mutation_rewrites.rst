.. versionadded:: 3.0

.. _ref_datamodel_mutation_rewrites:

=================
Mutation rewrites
=================

.. edb:youtube-embed:: ImgMfb_jCJQ?end=41

Mutation rewrites allow you to intercept database mutations (i.e.,
:ref:`inserts <ref_eql_insert>` and/or :ref:`updates <ref_eql_update>`) and set
the value of a property or link to the result of an expression you define. They
can be defined in your schema.

Mutation rewrites are complementary to :ref:`triggers
<ref_datamodel_triggers>`. While triggers are unable to modify the triggering
object, mutation rewrites are built for that purpose.

Here's an example of a mutation rewrite that updates a property of a ``Post``
type to reflect the time of the most recent modification:

.. code-block:: sdl

    type Post {
      required title: str;
      required body: str;
      modified: datetime {
        rewrite insert, update using (datetime_of_statement())
      }
    }


Every time a ``Post`` is updated, the mutation rewrite will be triggered,
updating the ``modified`` property:

.. code-block:: edgeql-repl

    db> insert Post {
    ...   title := 'One wierd trick to fix all your spelling errors'
    ... };
    {default::Post {id: 19e024dc-d3b5-11ed-968c-37f5d0159e5f}}
    db> select Post {title, modified};
    {
      default::Post {
        title: 'One wierd trick to fix all your spelling errors',
        modified: <datetime>'2023-04-05T13:23:49.488335Z',
      },
    }
    db> update Post
    ... filter .id = <uuid>'19e024dc-d3b5-11ed-968c-37f5d0159e5f'
    ... set {title := 'One weird trick to fix all your spelling errors'};
    {default::Post {id: 19e024dc-d3b5-11ed-968c-37f5d0159e5f}}
    db> select Post {title, modified};
    {
      default::Post {
        title: 'One weird trick to fix all your spelling errors',
        modified: <datetime>'2023-04-05T13:25:04.119641Z',
      },
    }

In some cases, you will want different rewrites depending on the type of query.
Here, we will add an ``insert`` rewrite and an ``update`` rewrite:

.. code-block:: sdl

    type Post {
      required title: str;
      required body: str;
      created: datetime {
        rewrite insert using (datetime_of_statement())
      }
      modified: datetime {
        rewrite update using (datetime_of_statement())
      }
    }

With this schema, inserts will set the ``Post`` object's ``created`` property
while updates will set the ``modified`` property:

.. code-block:: edgeql-repl

    db> insert Post {
    ...   title := 'One wierd trick to fix all your spelling errors'
    ... };
    {default::Post {id: 19e024dc-d3b5-11ed-968c-37f5d0159e5f}}
    db> select Post {title, created, modified};
    {
      default::Post {
        title: 'One wierd trick to fix all your spelling errors',
        created: <datetime>'2023-04-05T13:23:49.488335Z',
        modified: {},
      },
    }
    db> update Post
    ... filter .id = <uuid>'19e024dc-d3b5-11ed-968c-37f5d0159e5f'
    ... set {title := 'One weird trick to fix all your spelling errors'};
    {default::Post {id: 19e024dc-d3b5-11ed-968c-37f5d0159e5f}}
    db> select Post {title, created, modified};
    {
      default::Post {
        title: 'One weird trick to fix all your spelling errors',
        created: <datetime>'2023-04-05T13:23:49.488335Z',
        modified: <datetime>'2023-04-05T13:25:04.119641Z',
      },
    }

.. note::

    Each property may have a single ``insert`` and a single ``update`` mutation
    rewrite rule, or they may have a single rule that covers both.

Available variables
===================

Inside the rewrite rule's expression, you have access to a few special values:

* ``__subject__`` refers to the object type with the new property and link
  values
* ``__specified__`` is a named tuple with a key for each property or link in
  the type and a boolean value indicating whether this value was explicitly set
  in the mutation
* ``__old__`` refers to the object type with the previous property and link
  values (available for update-only mutation rewrites)

Here are some examples of the special values in use. Maybe your blog hosts
articles about particularly controversial topics. You could use ``__subject__``
to enforce a "cooling off" period before publishing a blog post:

.. code-block:: sdl

    type Post {
      required title: str;
      required body: str;
      publish_time: datetime {
        rewrite insert, update using (
          __subject__.publish_time ?? datetime_of_statement() +
          cal::to_relative_duration(days := 10)
        )
      }
    }

Here we take the post's ``publish_time`` if set or the time the statement is
executed and add 10 days to it. That should give our authors time to consider
if they want to make any changes before a post goes live.

You can omit ``__subject__`` in many cases and achieve the same thing:

.. code-block:: sdl-diff

      type Post {
        required title: str;
        required body: str;
        publish_time: datetime {
          rewrite insert, update using (
    -       __subject__.publish_time ?? datetime_of_statement() +
    +       .publish_time ?? datetime_of_statement() +
            cal::to_relative_duration(days := 10)
          )
        }
      }

but only if the path prefix has not changed. In the following schema, for
example, the ``__subject__`` in the rewrite rule is required, because in the
context of the nested ``select`` query, the leading dot resolves from the
``User`` path:

.. code-block:: sdl

    type Post {
      required title: str;
      required body: str;
      author_email: str;
      author_name: str {
        rewrite insert, update using (
          (select User {name} filter .email = __subject__.author_email).name
        )
      }
    }
    type User {
      name: str;
      email: str;
    }

.. note::

    Learn more about how this works in our documentation on :ref:`path
    resolution <ref_eql_path_resolution>`.

Using ``__specified__``, we can determine which fields were specified in the
mutation. This would allow us to track when a single property was last modified
as in the ``title_modified`` property in this schema:

.. code-block:: sdl

    type Post {
      required title: str;
      required body: str;
      title_modified: datetime {
        rewrite update using (
          datetime_of_statement()
          if __specified__.title
          else __old__.title_modified
        )
      }
    }

``__specified__.title`` will be ``true`` if that value was set as part of the
update, and this rewrite mutation rule will update ``title_modified`` to
``datetime_of_statement()`` in that case.

Another way you might use this is to set a default value but allow overriding:

.. code-block:: sdl

    type Post {
      required title: str;
      required body: str;
      modified: datetime {
        rewrite update using (
          datetime_of_statement()
          if not __specified__.modified
          else .modified
        )
      }
    }

Here, we rewrite ``modified`` on updates to ``datetime_of_statment()`` unless
``modified`` was set in the update. In that case, we allow the specified value
to be set. This is different from a :ref:`default
<ref_datamodel_props_default_values>` value because the rewrite happens on each
update whereas a default value is applied only on insert of a new object.

One shortcoming in using ``__specified__`` to decide whether to update the
``modified`` property is that we still don't know whether the value changed —
only that it was specified in the query. It's possible the value specified was
the same as the existing value. You'd need to check the value itself to decide
if it has changed.

This is easy enough for a single value, but what if you want a global
``modified`` property that is updated only if any of the properties or links
were changed? That could get cumbersome quickly for an object of any
complexity.

Instead, you might try casting ``__subject__`` and ``__old__`` to ``json`` and
comparing them:

.. code-block:: sdl

    type Post {
      required title: str;
      required body: str;
      modified: datetime {
        rewrite update using (
          datetime_of_statement()
          if <json>__subject__ {**} != <json>__old__ {**}
          else __old__.modified
        )
      }
    }

Lastly, if we want to add an ``author`` property that can be set for each write
and keep a history of all the authors, we can do this with the help of
``__old__``:

.. code-block:: sdl

    type Post {
      required title: str;
      required body: str;
      author: str;
      all_authors: array<str> {
        default := <array<str>>[];
        rewrite update using (
          __old__.all_authors
          ++ [__subject__.author]
        );
      }
    }

On insert, our ``all_authors`` property will get initialized to an empty array
of strings. We will rewrite updates to concatenate that array with an array
containing the new author value.


Mutation rewrite as cached computed
===================================

Mutation rewrites can be used to effectively create a cached computed value as
demonstrated with the ``byline`` property in this schema:

.. code-block:: sdl

    type Post {
      required title: str;
      required body: str;
      author: str;
      created: datetime {
        rewrite insert using (datetime_of_statement())
      }
      byline: str {
        rewrite insert, update using (
          'by ' ++
          __subject__.author ++
          ' on ' ++
          to_str(__subject__.created, 'Mon DD, YYYY')
        )
      }
    }

The ``byline`` property will be updated on each insert or update, but the value
will not need to be calculated at read time like a proper :ref:`computed
property <ref_datamodel_computed>`.


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`SDL > Mutation rewrites <ref_eql_sdl_mutation_rewrites>`
  * - :ref:`DDL > Mutation rewrites <ref_eql_ddl_mutation_rewrites>`
  * - :ref:`Introspection > Mutation rewrites
      <ref_datamodel_introspection_mutation_rewrites>`
