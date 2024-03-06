.. _ref_cli_edgedb_info:


===========
edgedb info
===========

Display information about the EdgeDB installation. Currently this command
displays the filesystem paths used by EdgeDB.

.. cli:synopsis::

	edgedb info [<options>]


.. _ref_cli_edgedb_paths:

Paths
-----

EdgeDB uses several directories, each storing different kinds of information.
The exact path to these directories is determined by your operating system.
Throughout the documentation, these paths are referred to as "EdgeDB config
directory", "EdgeDB data directory", etc.

- **Config**: contains auto-generated credentials for all local instances and
  project metadata.
- **Data**: contains the *contents* of all local EdgeDB instances.
- **CLI Binary**: contains the CLI binary, if installed.
- **Service**: the home for running processes/daemons.
- **Cache**: a catchall for logs and various caches.

Options
=======

:cli:synopsis:`--get <path-name>`
    Return only a single path. ``<path-name>`` can be any of ``config-dir``,
    ``cache-dir``, ``data-dir``, or ``service-dir``.
