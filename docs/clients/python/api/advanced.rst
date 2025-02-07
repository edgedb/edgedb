.. _edgedb-python-advanced:

==============
Advanced Usage
==============

.. py:currentmodule:: edgedb


.. _edgedb-python-transaction-options:

Transaction Options
===================

Transactions can be customized with different options:

.. py:class:: TransactionOptions(isolation=IsolationLevel.Serializable, readonly=False, deferrable=False)

    :param IsolationLevel isolation: transaction isolation level
    :param bool readonly: if true the transaction will be readonly
    :param bool deferrable: if true the transaction will be deferrable

    .. py:method:: defaults()
        :classmethod:

        Returns the default :py:class:`TransactionOptions`.

.. py:class:: IsolationLevel

    Isolation level for transaction

    .. py:attribute:: Serializable

        Serializable isolation level

    .. py:attribute:: RepeatableRead

        Repeatable read isolation level (supported in read-only transactions)

:py:class:`TransactionOptions` can be set on :py:class:`~edgedb.Client` or
:py:class:`~edgedb.AsyncIOClient` using one of these methods:

* :py:meth:`edgedb.Client.with_transaction_options`
* :py:meth:`edgedb.AsyncIOClient.with_transaction_options`

These methods return a "shallow copy" of the current client object with modified
transaction options. Both ``self`` and the returned object can be used, but
different transaction options will applied respectively.

Transaction options are used by the future calls to the method
:py:meth:`edgedb.Client.transaction` or :py:meth:`edgedb.AsyncIOClient.transaction`.


.. _edgedb-python-retry-options:

Retry Options
=============

Individual EdgeQL commands or whole transaction blocks are automatically retried on
retryable errors. By default, edgedb-python will try at most 3 times, with an
exponential backoff time interval starting from 100ms, plus a random hash under 100ms.

Retry rules can be granularly customized with different retry options:

.. py:class:: RetryOptions(attempts, backoff=default_backoff)

    :param int attempts: the default number of attempts
    :param Callable[[int], Union[float, int]] backoff: the default backoff function

    .. py:method:: with_rule(condition, attempts=None, backoff=None)

        Adds a backoff rule for a particular condition

        :param RetryCondition condition: condition that will trigger this rule
        :param int attempts: number of times to retry
        :param Callable[[int], Union[float, int]] backoff:
          function taking the current attempt number and returning the number
          of seconds to wait before the next attempt

    .. py:method:: defaults()
        :classmethod:

        Returns the default :py:class:`RetryOptions`.

.. py:class:: RetryCondition

    Specific condition to retry on for fine-grained control

    .. py:attribute:: TransactionConflict

        Triggered when a TransactionConflictError occurs.

    .. py:attribute:: NetworkError

        Triggered when a ClientError occurs.

:py:class:`RetryOptions` can be set on :py:class:`~edgedb.Client` or
:py:class:`~edgedb.AsyncIOClient` using one of these methods:

* :py:meth:`edgedb.Client.with_retry_options`
* :py:meth:`edgedb.AsyncIOClient.with_retry_options`

These methods return a "shallow copy" of the current client object with modified
retry options. Both ``self`` and the returned object can be used, but different
retry options will applied respectively.


.. _edgedb-python-state:

State
=====

State is an execution context that affects the execution of EdgeQL commands in
different ways: default module, module aliases, session config and global values.

.. py:class:: State(default_module=None, module_aliases={}, config={}, globals_={})

    :type default_module: str or None
    :param default_module:
        The *default module* that the future commands will be executed with.
        ``None`` means the default *default module* on the server-side,
        which is usually just ``default``.

    :param dict[str, str] module_aliases:
        Module aliases mapping of alias -> target module.

    :param dict[str, object] config:
        Non system-level config settings mapping of config name -> config value.

        For available configuration parameters refer to the
        :ref:`Config documentation <ref_std_cfg>`.

    :param dict[str, object] globals_:
        Global values mapping of global name -> global value.

        .. note::
            The global name can be either a qualified name like
            ``my_mod::glob2``, or a simple name under the default module.
            Simple names will be prefixed with the default module, while module
            aliases in qualified names - if any - will be resolved into actual
            module names.

    .. py:method:: with_default_module(module=None)

        Returns a new :py:class:`State` copy with adjusted default module.

        .. note::
            This will not affect the globals that are already stored in this
            state using simple names, because their names were resolved before
            this call to ``with_default_module()``, which affects only the
            future calls to the :py:meth:`with_globals` method.

        This is equivalent to using the ``set module`` command, or using the
        ``reset module`` command when giving ``None``.

        :type module: str or None
        :param module:
            Adjust the *default module*. If ``module`` is ``None``, the
            *default module* will be reset to default.

    .. py:method:: with_module_aliases(aliases_dict=None, /, **aliases)

        Returns a new :py:class:`State` copy with adjusted module aliases.

        .. note::
            This will not affect the globals that are already stored in this
            state using module aliases, because their names were resolved
            before this call to ``with_module_aliases()``, which affects only
            the future calls to the :py:meth:`with_globals` method.

        This is equivalent to using the ``set alias`` command.

        :type aliases_dict: dict[str, str] or None
        :param aliases_dict:
            Adjust the module aliases by merging with the given alias -> target
            module mapping. This is an optional positional-only argument.

        :param dict[str, str] aliases:
            Adjust the module aliases by merging with the given alias -> target
            module mapping, after applying ``aliases_dict`` if set.

    .. py:method:: without_module_aliases(*aliases)

        Returns a new :py:class:`State` copy without specified module aliases.

        .. note::
            This will not affect the globals that are already stored in this
            state using module aliases, because their names were resolved
            before this call to ``without_module_aliases()``, which affects
            only the future calls to the :py:meth:`with_globals` method.

        This is equivalent to using the ``reset alias`` command.

        :param tuple[str] aliases:
            Adjust the module aliases by dropping the specified aliases if they
            were set, no errors will be raised if they weren't.

            If no aliases were given, all module aliases will be dropped.

    .. py:method:: with_config(config_dict=None, /, **config)

        Returns a new :py:class:`State` copy with adjusted session config.

        This is equivalent to using the ``configure session set`` command.

        :type config_dict: dict[str, object] or None
        :param config_dict:
            Adjust the config settings by merging with the given config name ->
            config value mapping. This is an optional positional-only argument.

        :param dict[str, object] config:
            Adjust the config settings by merging with the given config name ->
            config value mapping, after applying ``config_dict`` if set.

    .. py:method:: without_config(*config_names)

        Returns a new :py:class:`State` copy without specified session config.

        This is equivalent to using the ``configure session reset`` command.

        :param tuple[str] config_names:
            Adjust the config settings by resetting the specified config to
            default if they were set, no errors will be raised if they weren't.

            If no names were given, all session config will be reset.

    .. py:method:: with_globals(globals_dict=None, /, **globals_)

        Returns a new :py:class:`State` copy with adjusted global values.

        .. note::
            The globals are stored with their names resolved into the actual
            fully-qualified names using the current default module and module
            aliases set on this state.

        This is equivalent to using the ``set global`` command.

        :type globals_dict: dict[str, object] or None
        :param globals_dict:
            Adjust the global values by merging with the given global name ->
            global value mapping. This is an optional positional-only argument.

        :param dict[str, object] globals_:
            Adjust the global values by merging with the given global name ->
            global value mapping, after applying ``globals_dict`` if set.

    .. py:method:: without_globals(*global_names)

        Returns a new :py:class:`State` copy without specified globals.

        This is equivalent to using the ``reset global`` command.

        :param tuple[str] global_names:
            Adjust the globals by resetting the specified globals to default if
            they were set, no errors will be raised if they weren't.

            If no names were given, all globals will be reset.

:py:class:`State` can be set on :py:class:`~edgedb.Client` or
:py:class:`~edgedb.AsyncIOClient` using one of these methods:

* :py:meth:`edgedb.Client.with_state`
* :py:meth:`edgedb.AsyncIOClient.with_state`

These methods return a "shallow copy" of the current client object with
modified state, affecting all future commands executed using the returned copy.
Both ``self`` and the returned object can be used, but different state will
applied respectively.

Alternatively, shortcuts are available on client objects:

* :py:meth:`edgedb.Client.with_default_module`
* :py:meth:`edgedb.Client.with_module_aliases`
* :py:meth:`edgedb.Client.without_module_aliases`
* :py:meth:`edgedb.Client.with_config`
* :py:meth:`edgedb.Client.without_config`
* :py:meth:`edgedb.Client.with_globals`
* :py:meth:`edgedb.Client.without_globals`
* :py:meth:`edgedb.AsyncIOClient.with_default_module`
* :py:meth:`edgedb.AsyncIOClient.with_module_aliases`
* :py:meth:`edgedb.AsyncIOClient.without_module_aliases`
* :py:meth:`edgedb.AsyncIOClient.with_config`
* :py:meth:`edgedb.AsyncIOClient.without_config`
* :py:meth:`edgedb.AsyncIOClient.with_globals`
* :py:meth:`edgedb.AsyncIOClient.without_globals`

They work the same way as ``with_state``, and adjusts the corresponding state
values.
