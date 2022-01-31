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

</div>

<br />
<br />

## EdgeDB 1.0 is almost here! 👀

The first stable release of EdgeDB is finally here. On February 10th, 2022, EdgeDB 1.0 will be released after 14 pre-releases and 4 years of active development. Join us for the live launch event!

<a href="https://lu.ma/edgedb" rel="nofollow">
  <img
    width="450px"
    src="https://www.edgedb.com/blog/edgedb_day_register.png"
    alt="Register for EdgeDB Launch Day"
  />
</a>

[View the full event page →](https://lu.ma/edgedb)

<br />
<br />

<br/>
<div align="center">
  <h2>What is EdgeDB?</h2>
  <p>
    EdgeDB is a new kind of database that takes the
    <br/>
    best parts of relational databases, graph databases,
    <br/>
    and ORMs. We call it a graph-relational database. It
    <br/>
    was designed with a few key principles in mind.</p>
</div>

<br/>

<br/>
<div align="center">
  <h3>🧩 Types, not tables 🧩</h3>
</div>
<br/>

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

<!-- ### Objects, not rows. ❄️ -->

<br/>
<div align="center">
  <h3>❄️ Objects, not rows ❄️</h3>
</div>
<br/>

EdgeDB's super-powered query language EdgeQL is designed as a ground-up redesign of SQL that aims to match it in power and surpass it in elegance, brevity, and expressiveness.

EdgeQL queries produce rich, structured objects, not flat lists of rows. Deeply fetching related objects is painless...bye, bye, JOINs.

```
select Movie {
  title,
  characters: {
    name
  }
}
filter .title = "The Matrix"
```

EdgeQL queries are also _composable_; you can use one EdgeQL query as an expression inside another. This property makes thinks like _subqueries_ and _nested mutations_ a breeze.

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

<br/>
<div align="center">
  <h3>🦋 More than a mapper 🦋</h3>
</div>
<br/>

<!-- ### More than a mapper. 🦋 -->

While EdgeDB solves the same problems as ORM libraries, it's so much more. It's a full-fledged database with a formally-defined query language, migrations system, suite of client libraries in different langauges, command-line tool, and—soon—cloud hosting service. The goal is to rethink every aspect of how developers manage, maintain, migrate, and query their database.

Here's a taste-test of EdgeDB: you can install our CLI, spin up an instance, and open an interactive shell with just three commands (Linux/macOS)

```
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh
$ edgedb project init
$ edgedb
```

<br />

## Get started

To start learning about EdgeDB, check out the following resources:

- **[The quickstart](https://www.edgedb.com/docs/guides/quickstart)**. If you're just starting out, the 10-minute quickstart guide is the fastest way to get up and running.
- **[The interactive tutorial](https://www.edgedb.com/tutorial)**. For a structured deep-dive into the EdgeQL query language, try the web-based tutorial— no need to install anything.
- **[The e-book](https://www.edgedb.com/easy-edgedb)**. For the most comprehensive walkthrough of EdgeDB concepts, check out our illustrated e-book [Easy EdgeDB](https://www.edgedb.com/easy-edgedb). It's designed to walk a total beginner through EdgeDB in its entirety, from the basics through advanced concepts.
- **The docs.** Jump straight into the docs for [schema modeling](https://www.edgedb.com/docs/datamodel/index) or [EdgeQL](https://www.edgedb.com/docs/edgeql/index)!

<br />

## Contributing

PRs are always welcome! To get started, follow [this guide](https://www.edgedb.com/docs/internals/dev) to build EdgeDB from source on your local machine.

[File an issue 👉](https://github.com/edgedb/edgedb/issues/new/choose)
<br />
[Start a Discussion 👉](https://github.com/edgedb/edgedb/discussions/new)
<br />
[Join the discord 👉](https://discord.gg/umUueND6ag)

<br />

## License

The code in this repository is developed and distributed under the
Apache 2.0 license. See [LICENSE](LICENSE) for details.
