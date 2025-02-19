.. _ref_cli_gel_instance_stop:


=================
gel instance stop
=================

Stop a |Gel| instance.

.. cli:synopsis::

     gel instance stop <name>


Description
===========

:gelcmd:`instance stop` is a terminal command for stopping a running
|Gel| instance. This is a necessary step before
:ref:`destroying <ref_cli_gel_instance_destroy>` an instance.

.. note::

    The :gelcmd:`instance stop` command is not intended for use with
    self-hosted instances.


Options
=======

:cli:synopsis:`<name>`
    The |Gel| instance name.
