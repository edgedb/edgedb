.. _ref_cli_gel_instance_start:


==================
gel instance start
==================

Start a |Gel| instance.

.. cli:synopsis::

     gel instance start [--foreground] <name>


Description
===========

:gelcmd:`instance start` is a terminal command for starting a new
|Gel| instance.

.. note::

    The :gelcmd:`instance start` command is not intended for use with
    self-hosted instances.


Options
=======

:cli:synopsis:`<name>`
    The |Gel| instance name.

:cli:synopsis:`--foreground`
    Start the instance in the foreground rather than using systemd to
    manage the process (note you might need to stop non-foreground
    instance first).
