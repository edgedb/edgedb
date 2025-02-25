.. _ref_cli_gel_network:

=============
Network usage
=============

Generally command-line tools connect only to the database host with a few
exceptions:

1. When the command-line tool starts, it checks if its version is up to
   date. :ref:`Details <ref_cli_gel_version_check>`
2. The :ref:`ref_cli_gel_server` family of commands and
   :ref:`ref_cli_gel_cli_upgrade` discover package versions and
   docker images and also invoke package managers and the docker
   engine to do :ref:`index updates and related data.
   <ref_cli_gel_net_server>`
3. The CLI communicates with the |Gel| Cloud API to provide easy access to
   your Gel Cloud instances.


.. _ref_cli_gel_version_check:

Version Check
=============

Version check checks the current version of command-line tool by fetching
``https://packages.geldata.com/.jsonindexes/*.json``.

Here is how such a request looks like::

    GET /archive/.jsonindexes/linux-x86_64.json HTTP/1.1
    host: packages.geldata.com
    content-length: 0
    user-agent: gel

The ``User-Agent`` header only specifies that request is done by
``gel`` command-line tool (without version number). The platform,
architecture and whether nightly is used can be devised from the URL of
the query.

Latest version number is cached for the random duration from 16 to 32
hours (this randomization is done both for spreading the load and for
better anonymizing the data). A failure is cached for the random
duration from 6 to 12 hours.


Disabling Version Check
=======================

To disable version check do one of two things:

1. Use ``--no-cli-update-check`` command-line parameter to disable just
   for this invocation
2. Export :gelenv:`RUN_VERSION_CHECK=never` in the environment.

.. XXX: edgedb::version_check=debug

To verify that check is skipped and no network access is being done
logging facility can be used::

   $ export RUST_LOG=edgedb::version_check=debug
   $ gel --no-cli-update-check
   [..snip..] Skipping version check due to --no-cli-update-check
   gel>
   $ GEL_RUN_VERSION_CHECK=never gel
   [..snip..] Skipping version check due to GEL_RUN_VERSION_CHECK=never
   gel>


.. _ref_cli_gel_net_server:

"gel server" and "gel self upgrade"
===================================

Generally these commands do requests with exactly the headers
like :ref:`version check <ref_cli_gel_version_check>`.

Data sources for the commands directly:

1. Package indexes and packages at ``https://packages.geldata.com``
2. Docker image index at ``https://registry.hub.docker.com``

Data sources that can be used indirectly:

1. Docker engine may fetch indexes and images. Currently the only
   images used are at Docker Hub. More specifically
   are ``gel/*`` and ``busybox`` (Docker's official image).
2. Package managers (currently ``apt-get``, ``yum``) can fetch indexes
   and install packages from ``https://packages.geldata.com``. And
   as we use generic commands (e.g. ``apt-get update``) and system
   dependencies, package manager can fetch package indexes and package
   data from any sources listed in repositories configured in the
   system.

To avoid reaching these hosts, avoid using: :gelcmd:`server` and
:gelcmd:`self upgrade` subcommands. These commands only simplify
installation and maintenance of the installations. All |Gel| features
are available without using them.
