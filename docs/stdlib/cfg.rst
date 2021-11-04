.. _ref_std_cfg:

======
Config
======

The ``cfg`` module contains a set of types and scalars used for configuring
EdgeDB.


.. list-table::
  :class: funcoptable

  * - **Type**
    - **Description**
  * - :eql:type:`cfg::Config`
    - The base type for all configuration objects. The properties of this type
      define the set of configuruation settings supported by EdgeDB.
  * - :eql:type:`cfg::Auth`
    - An object type representing an authentication profile.
  * - :eql:type:`cfg::AuthMethod`
    - An abstract object type representing a method of authentication
  * - :eql:type:`cfg::Trust`
    - A subclass of ``AuthMethod`` indicating an "always trust" policy (no
      authentication).
  * - :eql:type:`cfg::SCRAM`
    - A subclass of ``AuthMethod`` indicating password-based authentication.
  * - :eql:type:`cfg::memory`
    - A scalar type for storing a quantity of memory storage.


----------


.. eql:type:: cfg::Config

  An abstract type representing the configuration of an instance or database.

  The properties of this object type represent the set of configuration
  options supported by EdgeDB.


  .. list-table::

    * - **Setting**
      - **Type**
      - **Default**
    * - ``client_idle_timeout``
      - ``required std::int16``
      - ``30 seconds``
    * - ``listen_port``
      - ``required std::int16``
      - ``5656``
    * - ``listen_addresses``
      - ``multi std::str``
      - N/A
    * - ``auth``
      - ``multi cfg::Auth``
      - N/A
    * - ``allow_dml_in_functions``
      - ``std::bool``
      - N/A
    * - ``shared_buffers``
      - ``std::str``
      - ``'-1'``
    * - ``query_work_mem``
      - ``std::str``
      - ``'-1'``
    * - ``effective_cache_size``
      - ``std::str``
      - ``'-1'``
    * - ``effective_io_concurrency``
      - ``std::str``
      - ``'50'``
    * - ``default_statistics_target``
      - ``std::str``
      - ``'100'``



----------


.. eql:type:: cfg::Auth

  An object type designed to specify a client authentication profile.

  Below are the properties of the ``Auth`` class.

  :eql:synopsis:`priority (int64)`
      The priority of the authentication rule.  The lower this number,
      the higher the priority.

  :eql:synopsis:`user (SET OF str)`
      The name(s) of the database role(s) this rule applies to.  If set to
      ``'*'``, then it applies to all roles.

  :eql:synopsis:`method (cfg::AuthMethod)`
      The name of the authentication method type. Expects an instance of
      :eql:type:`cfg::AuthMethod`;  Valid values are:
      ``Trust`` for no authentication and ``SCRAM`` for SCRAM-SHA-256
      password authentication.

  :eql:synopsis:`comment`
      An optional comment for the authentication rule.


---------

.. eql:type:: cfg::AuthMethod

  An abstract object class that represents an authentication method.

  It currently has two concrete subclasses, each of which represent an
  available authentication method: :eql:type:`cfg::Trust` and
  :eql:type:`cfg::SCRAM`.

-------

.. eql:type:: cfg::Trust

  The ``cfg::Trust`` indicates an "always-trust" policy.

  When active, it disables password-based authentication.

  .. code-block:: edgeql-repl

    edgedb> CONFIGURE INSTANCE INSERT
    .......   Auth {priority := 0, method := (INSERT Trust)};
    OK: CONFIGURE INSTANCE

-------

.. eql:type:: cfg::SCRAM

  The ``cfg::SCRAM`` indicates password-based authentication.

  This policy is implemented via ``SCRAM-SHA-256``.

  .. code-block:: edgeql-repl

    edgedb> CONFIGURE INSTANCE INSERT
    .......   Auth {priority := 0, method := (INSERT Scram)};
    OK: CONFIGURE INSTANCE


-------

.. eql:type:: cfg::memory

  A scalar type representing a quantity of memory storage.

  As with ``uuid``, ``datetime``, and several other types, ``cfg::memory``
  values are declared by casting from an appropriately formatted string.

  .. code-block:: edgeql-repl

    db> select <cfg::memory>'1B'; # 1 byte
    {<cfg::memory>'1B'}
    db> select <cfg::memory>'5KiB'; # 5 kibibytes
    {<cfg::memory>'5KiB'}
    db> select <cfg::memory>'128MiB'; # 128 mebibytes
    {<cfg::memory>'128MiB'}

  The numerical component of the value must be a non-negative integer; the
  units must be one of ``B|KiB|MiB|GiB|TiB|PiB``. We're using the explicit
  ``KiB`` unit notation (1024 bytes) instead of ``kB`` (which is ambiguous,
  and may mean 1000 or 1024 bytes).
