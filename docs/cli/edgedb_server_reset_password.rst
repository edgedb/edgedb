.. _ref_cli_edgedb_server_reset_password:


============================
edgedb server reset-password
============================

Reset password for a user in the EdgeDB instance.

.. cli:synopsis::

     edgedb server reset-password [OPTIONS] <name>


Description
===========

``edgedb server reset-password`` is a terminal command for resetting
or updating the password for a user of an EdgeDB instance.


Options
=======

:cli:synopsis:`<name>`
    The name of the EdgeDB instance.

:cli:synopsis:`--user=<user>`
    User to change password for. Defaults to the user in the
    credentials file.

:cli:synopsis:`--password`
    Read the password from the terminal rather than generating a new one.

:cli:synopsis:`--password-from-stdin`
    Read the password from stdin rather than generating a new one.

:cli:synopsis:`--save-credentials`
    Save new user and password into a credentials file. By default
    credentials file is updated only if user name matches.

:cli:synopsis:`--no-save-credentials`
    Do not save generated password into a credentials file even if
    user name matches.

:cli:synopsis:`--quiet`
    Do not print any messages, only indicate success by exit status.
