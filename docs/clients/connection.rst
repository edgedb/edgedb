.. _edgedb_client_connection:

==========
Connection
==========

There are several ways to provide connection information to a client
library.

- Use **projects**. This is the recommended approach for *local
  development*. Once the project is initialized, all client libraries that are
  running inside the project directory can auto-discover the project-linked
  instance, no need for environment variables or hard-coded credentials.
  Follow the :ref:`Using projects <ref_guide_using_projects>` guide to get
  started.

- Set the ``EDGEDB_DSN`` environment variable to a valid DSN (connection
  string). This is the recommended approach in *production*. A DSN is a
  connection URL of the form ``edgedb://user:pass@host:port/branch``. For a
  guide to DSNs, see the :ref:`DSN Specification <ref_dsn>`.

- Set the ``EDGEDB_INSTANCE`` environment variable to a :ref:`name
  <ref_reference_connection_instance_name>` of a local instance, remote linked
  instance, or an EdgeDB Cloud instance. (:ref:`More info on EdgeDB Cloud
  connection details below. <edgedb_client_connection_cloud>`) You can create
  new instances manually with the :ref:`edgedb instance create
  <ref_cli_edgedb_instance_create>` command.

- Explicitly pass a DSN or :ref:`instance name
  <ref_reference_connection_instance_name>`
  into the client creation function:
  ``edgedb.createClient`` in JS, ``edgedb.create_client()`` in Python, and
  ``edgedb.CreateClient`` in Go.

  .. code-block:: typescript

    const client = edgedb.createClient({
      dsn: "edgedb://..."
    });

  Only use this approach in development; it isn't recommended to include
  sensitive information hard-coded in your production source code. Use
  environment variables instead. Different languages, frameworks, cloud hosting
  providers, and container-based workflows each provide various mechanisms for
  setting environment variables.

These are the most common ways to connect to an instance, however EdgeDB
supports several other options for advanced use cases. For a complete reference
on connection configuration, see :ref:`Reference > Connection Parameters
<ref_reference_connection>`.

.. _edgedb_client_connection_cloud:

EdgeDB Cloud
============

To provide client connection information for an EdgeDB Cloud instance, set the
``EDGEDB_INSTANCE`` variable to the instance name
(``<org-name>/<instance-name>`` where ``<instance-name>`` is the name you set
when you created the EdgeDB Cloud instance) and the ``EDGEDB_SECRET_KEY``
variable to your secret key which can be created in the EdgeDB Cloud UI or by
running :ref:`ref_cli_edgedb_cloud_secretkey_create` via the CLI.
