<p align="center">
  <a href="https://edgedb.com"><img width="160px" src="logo.svg"></a>
</p>

[![Build Status](https://github.com/edgedb/edgedb/workflows/Tests/badge.svg?event=push&branch=master)](https://github.com/edgedb/edgedb/) [![Join the community on Spectrum](https://img.shields.io/badge/join%20the%20community-on%20spectrum-blueviolet)](https://spectrum.chat/edgedb)


What is EdgeDB?
===============

EdgeDB is an **open-source** object-relational database built on top of
PostgreSQL.  The goal of EdgeDB is to *empower* its users to build safe
and efficient software with less effort.

EdgeDB features:

- strict, strongly typed schema;
- powerful and expressive query language;
- rich standard library;
- built-in support for schema migrations;
- native GraphQL support.

Check out the [blog](https://edgedb.com/blog/edgedb-a-new-beginning)
[posts](https://edgedb.com/blog/edgedb-1-0-alpha-1) for more examples and
the philosophy behind EdgeDB.


Modern Type-safe Schema
-----------------------

The data schema in EdgeDB is a clean high-level representation of a conceptual
data model:

```
type User {
    required property name -> str;
}

type Person {
    required property first_name -> str;
    required property last_name -> str;
}

type Review {
    required property body -> str;
    required property rating -> int64 {
        constraint min_value(0);
        constraint max_value(5);
    }

    required link author -> User;
    required link movie -> Movie;

    required property creation_time -> local_datetime;
}

type Movie {
    required property title -> str;
    required property year -> int64;
    required property description -> str;

    multi link directors -> Person;
    multi link cast -> Person;

    property avg_rating := math::mean(.<movie[IS Review].rating);
}
```

EdgeDB has a rich library of datatypes and functions.


EdgeQL
------

EdgeQL is the query language of EdgeDB. It is efficient, intuitive, and easy
to learn.

EdgeQL supports fetching object hierarchies with arbitrary level of nesting,
filtering, sorting and aggregation:

```
SELECT User {
    id,
    name,
    image,
    latest_reviews := (
        WITH UserReviews := User.<author
        SELECT UserReviews {
            id,
            body,
            rating,
            movie: {
                id,
                title,
                avg_rating,
            }
        }
        ORDER BY .creation_time DESC
        LIMIT 10
    )
}
FILTER .id = <uuid>$id
```


Status
======

EdgeDB is currently in alpha. See our
[Issues](https://github.com/edgedb/edgedb/issues) for a list of features
planned or in development.


Getting Started
===============

Please refer to the [Tutorial](https://edgedb.com/docs/tutorial/index) section
of the documentation on how to install and run EdgeDB.


Documentation
=============

The EdgeDB documentation can be found at
[edgedb.com/docs](https://edgedb.com/docs).


Building From Source
====================

Please follow the instructions outlined
[in the documentation](https://edgedb.com/docs/internals/dev).


License
=======

The code in this repository is developed and distributed under the
Apache 2.0 license.  See [LICENSE](LICENSE) for details.
