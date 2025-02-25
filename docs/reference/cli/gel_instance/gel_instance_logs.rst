.. _ref_cli_gel_instance_logs:


=================
gel instance logs
=================

Show instance logs.

.. cli:synopsis::

     gel instance logs [<options>] <name>


Description
===========

:gelcmd:`instance logs` is a terminal command for displaying the logs
for a given |Gel| instance.

.. note::

    The :gelcmd:`instance logs` command is not intended for use with
    self-hosted instances.


Options
=======

:cli:synopsis:`<name>`
    The name of the |Gel| instance.

:cli:synopsis:`-n, --tail=<tail>`
    Number of the most recent lines to show.

:cli:synopsis:`-f, --follow`
    Show log's tail and the continue watching for the new entries.
