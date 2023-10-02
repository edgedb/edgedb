.. _ref_cli_edgedb_instance_stop:


====================
edgedb instance stop
====================

Stop an EdgeDB instance.

.. cli:synopsis::

     edgedb instance stop <name>


Description
===========

``edgedb instance stop`` is a terminal command for stopping a running
EdgeDB instance. This is a necessary step before
:ref:`destroying <ref_cli_edgedb_instance_destroy>` an instance.

.. note::

    The ``edgedb instance stop`` command is not intended for use with
    self-hosted instances.


Options
=======

:cli:synopsis:`<name>`
    The EdgeDB instance name.
