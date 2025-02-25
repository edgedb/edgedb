.. _ref_cli_gel_instance:

============
gel instance
============

The :gelcmd:`instance` group of commands contains all sorts of tools
for managing |Gel| instances.

.. note::

    Most commands in the :gelcmd:`instance` command group are not intended to
    manage self-hosted instances. See individual commands for more details.

.. toctree::
    :maxdepth: 3
    :hidden:

    gel_instance_create
    gel_instance_credentials
    gel_instance_destroy
    gel_instance_link
    gel_instance_list
    gel_instance_logs
    gel_instance_start
    gel_instance_status
    gel_instance_stop
    gel_instance_reset_password
    gel_instance_restart
    gel_instance_revert
    gel_instance_unlink
    gel_instance_upgrade

.. list-table::
    :class: funcoptable

    * - :ref:`ref_cli_gel_instance_create`
      - Initialize a new server instance
    * - :ref:`ref_cli_gel_instance_credentials`
      - Display instance credentials
    * - :ref:`ref_cli_gel_instance_destroy`
      - Destroy a server instance and remove the data stored
    * - :ref:`ref_cli_gel_instance_link`
      - Link a remote instance
    * - :ref:`ref_cli_gel_instance_list`
      - Show all instances
    * - :ref:`ref_cli_gel_instance_logs`
      - Show logs of an instance
    * - :ref:`ref_cli_gel_instance_start`
      - Start an instance
    * - :ref:`ref_cli_gel_instance_status`
      - Show statuses of all or of a matching instance
    * - :ref:`ref_cli_gel_instance_stop`
      - Stop an instance
    * - :ref:`ref_cli_gel_instance_reset_auth`
      - Reset password for a user in the instance
    * - :ref:`ref_cli_gel_instance_restart`
      - Restart an instance
    * - :ref:`ref_cli_gel_instance_revert`
      - Revert a major instance upgrade
    * - :ref:`ref_cli_gel_instance_unlink`
      - Unlink a remote instance
    * - :ref:`ref_cli_gel_instance_upgrade`
      - Upgrade installations and instances
