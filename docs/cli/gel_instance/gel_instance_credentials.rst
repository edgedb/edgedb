.. _ref_cli_gel_instance_credentials:


========================
gel instance credentials
========================

Display instance credentials.

.. cli:synopsis::

     gel instance credentials [options] [connection-options]


Description
===========

:gelcmd:`instance credentials` is a terminal command for displaying the
credentials of a |Gel| instance.


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

By default, the |gel.toml| connection is used.

:cli:synopsis:`<connection-options>`
    See :ref:`connection options <ref_cli_gel_connopts>`.
