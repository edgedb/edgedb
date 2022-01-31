<p align="center">
  <a href="https://www.edgedb.com">
    <img src="https://i.imgur.com/H2Jio0X.png">
  </a>
</p>

<div align="center">
  <h1>EdgeDB</h1>
  <a href="https://github.com/edgedb/edgedb" rel="nofollow"><img src="https://img.shields.io/github/stars/edgedb/edgedb" alt="Stars"></a>
  <a href="https://github.com/edgedb/edgedb/actions"><img src="https://github.com/edgedb/edgedb/workflows/Tests/badge.svg?event=push&branch=master" /></a>
  <a href="https://github.com/edgedb/edgedb/blob/master/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue" /></a>
  <br />
  <br />
  <a href="https://www.edgedb.com/docs/guides/quickstart">Quickstart</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://www.edgedb.com">Website</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://www.edgedb.com/docs">Docs</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://www.edgedb.com/tutorial">Playground</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://www.edgedb.com/blog">Blog</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://discord.gg/umUueND6ag">Discord</a>
  <span>&nbsp;&nbsp;•&nbsp;&nbsp;</span>
  <a href="https://twitter.com/edgedatabase">Twitter</a>
  <br />
  <hr />
</div>

EdgeDB is an open-source **object-relational database** designed to bring the best parts of relational databases, graph databases, ORMs, and GraphQL into a new kind of database.

### EdgeDB 1.0 is coming

The first stable release of EdgeDB is finally here. On February 10th, 2022, EdgeDB 1.0 will be released after 14 pre-releases and 4 years of active development. Join us for the live launch event!

<a href="https://lu.ma/edgedb" rel="nofollow">
  <img
    style="max-width: 450px;"
    src="https://www.edgedb.com/blog/edgedb_day_register.png"
    alt="Register for EdgeDB Launch Day"
  />
</a>

[View the full event page →](https://lu.ma/edgedb)

## The core principles

EdgeDB is a whole new abstraction for databases, designed with a few core
principles in mind.

### Types, not tables. 🧩

Schema is the foundation of your application. It should be something you can read, write, and understand.

Forget foreign keys; tabular data modeling is a relic of an older age, and it [isn't compatible](https://en.wikipedia.org/wiki/Object%E2%80%93relational_impedance_mismatch) with modern languages. Instaed, EdgeDB thinks about schema the same way you do: as **object types** containing **properties** connected by **links**.

```
type Character {
  required property name -> str;
}

type Movie {
  required property title -> str;
  multi link characters -> Character;
}
```

This example is intentionally simple; EdgeDB supports everything you'd expect from your database—indexes, constraints, computed properties—plus some shiny new stuff too: link properties, world-class JSON support, and schema mixins. Read the [schema docs](https://www.edgedb.com/docs/datamodel/index) for details.

### Objects, not rows. ❄️

EdgeDB's super-powered query language EdgeQL is designed as a ground-up redesign of SQL that aims to it in power and surpass it in elegance, brevity, and expressiveness.

Based in set theory, EdgeQL features a comprehensive standard library, composable syntax, and painless deep querying...bye, bye, JOINs.

```
select Movie {
  title,
  characters: {
    name
  }
}
filter .title = "The Matrix"
```

One of its core design goals of EdgeQL is _composability_; it should be possible to use one EdgeQL query as an expression inside another. This property makes thinks like _subqueries_ and _nested mutations_ a breeze.

```
insert Movie {
  title := "The Matrix Resurrections",
  characters := (
    select Character
    filter .name in {'Neo', 'Trinity', 'Niobe'}
  )
}
```

There's a lot more to EdgeQL: computed properties, polymorphic queries, `with` blocks, transactions; read the [EdgeQL docs](https://www.edgedb.com/docs/edgeql/index) for details.

### More than a mapper. 🦋

While EdgeDB solves the same problems as ORM libraries, it's so much more. It's a full-fledged database with a formally-defined query language, migrations system, suite of client libraries in different langauges, command-line tool, and—soon—cloud hosting service. The goal is to rethink every aspect of how developers manage, maintain, migrate, and query their database.

Here's a taste-test of EdgeDB: you can install our CLI, spin up an instance, and open an interactive shell with just three commands (Linux/macOS)

```
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh
$ edgedb project init
$ edgedb
```

## Get started

To start learning about EdgeDB, check out the following resources:

- If you're just starting out, go through 10-minute [Quickstart guide](https://www.edgedb.com/docs/guides/quickstart). It's the fastest way to get up and running.
- For a structured deep-dive into the EdgeQL query language, try the web-based [interactive tutorial](https://www.edgedb.com/tutorial)— no need to install anything.
- For the most comprehensive walkthrough of EdgeDB concepts, check out our illustrated e-book `Easy EdgeDB </easy-edgedb>`\_. It's designed to walk a total beginner through EdgeDB, from the basics through advanced concepts.
- Or jump straight into the docs for [schema modeling](https://www.edgedb.com/docs/datamodel/index) or [EdgeQL](https://www.edgedb.com/docs/edgeql/index)!

## Contributing

PRs are always welcome! To get started, follow [this guide](https://www.edgedb.com/docs/internals/dev) to build EdgeDB from source on your local machine.

## License

The code in this repository is developed and distributed under the
Apache 2.0 license. See [LICENSE](LICENSE) for details.
