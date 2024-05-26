<p align="center">
  <a href="https://www.edgedb.com">
    <img src="https://www.edgedb.com/github_banner.png">
  </a>
</p>

<div align="center">
  <h1>EdgeDB</h1>
  <a href="https://github.com/edgedb/edgedb" rel="nofollow">
    <img src="https://img.shields.io/github/stars/edgedb/edgedb" alt="Stars">
  </a>
  <a href="https://github.com/edgedb/edgedb/actions">
    <img src="https://github.com/edgedb/edgedb/workflows/Tests/badge.svg?event=push&branch=master" />
  </a>
  <a href="https://github.com/edgedb/edgedb/blob/master/LICENSE">
    <img alt="license" src="https://img.shields.io/badge/license-Apache%202.0-blue" />
  </a>
  <a href="https://discord.gg/umUueND6ag">
    <img alt="discord" src="https://img.shields.io/discord/841451783728529451?color=5865F2&label=discord&logo=discord&logoColor=8a9095">
  </a>
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

<br/>
<div align="center">
  <h2>What is EdgeDB?</h2>
  <p style="max-width: 450px;">
    EdgeDB is a new kind of database
    <br/>
    that takes the best parts of
    <br/>
    relational databases, graph
    <br/>
    databases, and ORMs. We call it
    <br/>a <b>graph-relational database</b>.
  </p>
</div>

<br/>

<br/>
<div align="center">
  <h3>🧩 Types, not tables 🧩</h3>
</div>
<br/>

Schema is the foundation of your application. It should be something you can
read, write, and understand.

Forget foreign keys; tabular data modeling is a relic of an older age, and it
[isn't compatible](https://en.wikipedia.org/wiki/Object%E2%80%93relational_impedance_mismatch)
with modern languages. Instead, EdgeDB thinks about schema the same way you do:
as **object types** containing **properties** connected by **links**.

```esdl
type Person {
  required name: str;
}

type Movie {
  required title: str;
  multi actors: Person;
}
```

This example is intentionally simple, but EdgeDB supports everything you'd
expect from your database: a strict type system, indexes, constraints, computed
properties, stored procedures...the list goes on. Plus it gives you some shiny
new features too: link properties, schema mixins, and best-in-class JSON
support. Read the [schema docs](https://www.edgedb.com/docs/datamodel/index)
for details.

<!-- ### Objects, not rows. ❄️ -->

<br/>
<div align="center">
  <h3>🌳 Objects, not rows 🌳</h3>
</div>
<br/>

EdgeDB's super-powered query language EdgeQL is designed as a ground-up
redesign of SQL. EdgeQL queries produce rich, structured objects, not flat
lists of rows. Deeply fetching related objects is painless...bye, bye, JOINs.

```esdl
select Movie {
  title,
  actors: {
    name
  }
}
filter .title = "The Matrix"
```

EdgeQL queries are also _composable_; you can use one EdgeQL query as an
expression inside another. This property makes things like _subqueries_ and
_nested mutations_ a breeze.

```esdl
insert Movie {
  title := "The Matrix Resurrections",
  actors := (
    select Person
    filter .name in {
      'Keanu Reeves',
      'Carrie-Anne Moss',
      'Laurence Fishburne'
    }
  )
}
```

There's a lot more to EdgeQL: a comprehensive standard library, computed
properties, polymorphic queries, `with` blocks, transactions, and much more.
Read the [EdgeQL docs](https://www.edgedb.com/docs/edgeql/index) for the full
picture.

<br/>
<div align="center">
  <h3>🦋 More than a mapper 🦋</h3>
</div>
<br/>

While EdgeDB solves the same problems as ORM libraries, it's so much more. It's
a full-fledged database with a
[powerful and elegant query language](https://www.edgedb.com/docs/edgeql/index), a
[migrations system](https://www.edgedb.com/docs/guides/migrations/index), a
[suite of client libraries](https://www.edgedb.com/docs/clients/index) in
different languages, a
[command line tool](https://www.edgedb.com/docs/cli/index), and—coming soon—a
cloud hosting platform. The goal is to rethink every aspect of how developers
model, migrate, manage, and query their database.

Here's a taste-test of EdgeDB's next-level developer experience: you can
install our CLI, spin up an instance, and open an interactive EdgeQL shell with
just three commands.

```
$ curl --proto '=https' --tlsv1.2 -sSf https://sh.edgedb.com | sh
$ edgedb project init
$ edgedb
edgedb> select "Hello world!"
```

Windows users: use this Powershell command to install the CLI.

```
PS> iwr https://ps1.edgedb.com -useb | iex
```

<br />

## Get started

To start learning about EdgeDB, check out the following resources:

- **[The quickstart](https://www.edgedb.com/docs/guides/quickstart)**. If
  you're just starting out, the 10-minute quickstart guide is the fastest way
  to get up and running.
- **[EdgeDB Cloud 🌤️](https://www.edgedb.com/cloud)**. The best
  most effortless way to host your EdgeDB database in the cloud.
- **[The interactive tutorial](https://www.edgedb.com/tutorial)**. For a
  structured deep-dive into the EdgeQL query language, try the web-based
  tutorial— no need to install anything.
- **[The e-book](https://www.edgedb.com/easy-edgedb)**. For the most
  comprehensive walkthrough of EdgeDB concepts, check out our illustrated
  e-book [Easy EdgeDB](https://www.edgedb.com/easy-edgedb). It's designed to
  walk a total beginner through EdgeDB in its entirety, from the basics through
  advanced concepts.
- **The docs.** Jump straight into the docs for
  [schema modeling](https://www.edgedb.com/docs/datamodel/index) or
  [EdgeQL](https://www.edgedb.com/docs/edgeql/index)!

<br />

## Contributing

PRs are always welcome! To get started, follow
[this guide](https://www.edgedb.com/docs/internals/dev) to build EdgeDB from
source on your local machine.

[File an issue 👉](https://github.com/edgedb/edgedb/issues/new/choose)
<br />
[Start a Discussion 👉](https://github.com/edgedb/edgedb/discussions/new)
<br />
[Join the discord 👉](https://discord.gg/umUueND6ag)

<br />

## License

The code in this repository is developed and distributed under the
Apache 2.0 license. See [LICENSE](LICENSE) for details.
