.. _edgedb_client_connection:

Connection
----------

There are a couple ways to provide connection information to a client
library.

- [Recommended] Use projects. This is the recommended approach for developing
  applications with EdgeDB locally. Once the project is initialized, all client
  libraries will auto-discover the project-linked instance, no need for
  environment variables or hard-coded credentials. Follow the :ref:`Using
  projects <ref_guide_using_projects>` guide to get started.

- Pass the :ref:`name of a local instance
  <ref_reference_connection_instance_name>`
  to the client creation function:
  ``edgedb.createClient`` in JS, ``edgedb.create_client()`` in Python, and
  ``edgedb.CreateClient`` in Go.

  You can create new instances manually with :ref:`the CLI
  <ref_cli_edgedb_instance_create>`.

- Pass a DSN (connection URL) to the client creation function. This is the
  recommended approach when connecting to a remote instance. A DSN is a
  connection URL of the form ``edgedb://user:pass@host:port/database``. For a
  guide to DSNs, see the :ref:`DSN Specification <ref_dsn>`.

These are the most common ways to connect to an instance, however EdgeDB
supports several other options for advanced use cases. For a complete reference
on connection configuration, see :ref:`Reference > Connection Parameters
<ref_reference_connection>`.
