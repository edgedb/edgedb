.. _ref_cli_edgedb_instance_credentials:


===========================
edgedb instance credentials
===========================

Display instance credentials.

.. cli:synopsis::

     edgedb instance credentials [options] [connection-options]


Description
===========

``edgedb instance credentials`` is a terminal command for displaying the
credentials of an EdgeDB instance.


Options
=======

:cli:synopsis:`--json`
    Output in JSON format. In addition to formatting the credentials as JSON,
    this option also includes the password in cleartext and the TLS
    certificates.

:cli:synopsis:`--insecure-dsn`
    Output a DSN with password in cleartext.

Connection Options
==================

By default, the ``edgedb.toml`` connection is used.

:cli:synopsis:`<connection-options>`
    See :ref:`connection options <ref_cli_edgedb_connopts>`.
