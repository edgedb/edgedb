.. _ref_cli_gel_info:


========
gel info
========

Display information about the |Gel| installation. Currently this command
displays the filesystem paths used by Gel.

.. cli:synopsis::

	gel info [<options>]


.. _ref_cli_gel_paths:

Paths
-----

|Gel| uses several directories, each storing different kinds of information.
The exact path to these directories is determined by your operating system.
Throughout the documentation, these paths are referred to as "Gel config
directory", "Gel data directory", etc.

- **Config**: contains auto-generated credentials for all local instances and
  project metadata.
- **Data**: contains the *contents* of all local Gel instances.
- **CLI Binary**: contains the CLI binary, if installed.
- **Service**: the home for running processes/daemons.
- **Cache**: a catchall for logs and various caches.

Options
=======

:cli:synopsis:`--get <path-name>`
    Return only a single path. ``<path-name>`` can be any of ``config-dir``,
    ``cache-dir``, ``data-dir``, or ``service-dir``.
