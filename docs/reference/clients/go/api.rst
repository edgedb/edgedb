
API
===


*type* Client
-------------

Client is a connection pool and is safe for concurrent use.


.. code-block:: go

    type Client = gel.Client


*type* Error
------------

Error is the error type returned from gel.


.. code-block:: go

    type Error = gel.Error


*type* ErrorCategory
--------------------

ErrorCategory values represent Gel's error types.


.. code-block:: go

    type ErrorCategory = gel.ErrorCategory


*type* ErrorTag
---------------

ErrorTag is the argument type to Error.HasTag().


.. code-block:: go

    type ErrorTag = gel.ErrorTag


*type* Executor
---------------

Executor is a common interface between \*Client and \*Tx,
that can run queries on an Gel database.


.. code-block:: go

    type Executor = gel.Executor


*type* IsolationLevel
---------------------

IsolationLevel documentation can be found here
`docs/reference/edgeql/tx_start#parameters <https://www.gel.com/docs/reference/edgeql/tx_start#parameters>`_


.. code-block:: go

    type IsolationLevel = gel.IsolationLevel


*type* ModuleAlias
------------------

ModuleAlias is an alias name and module name pair.


.. code-block:: go

    type ModuleAlias = gel.ModuleAlias


*type* Options
--------------

Options for connecting to a |Gel| server


.. code-block:: go

    type Options = gel.Options


*type* RetryBackoff
-------------------

RetryBackoff returns the duration to wait after the nth attempt
before making the next attempt when retrying a transaction.


.. code-block:: go

    type RetryBackoff = gel.RetryBackoff


*type* RetryCondition
---------------------

RetryCondition represents scenarios that can cause a transaction
run in Tx() methods to be retried.


.. code-block:: go

    type RetryCondition = gel.RetryCondition


*type* RetryOptions
-------------------

RetryOptions configures how Tx() retries failed transactions.  Use
NewRetryOptions to get a default RetryOptions value instead of creating one
yourself.


.. code-block:: go

    type RetryOptions = gel.RetryOptions


*type* RetryRule
----------------

RetryRule determines how transactions should be retried when run in Tx()
methods. See Client.Tx() for details.


.. code-block:: go

    type RetryRule = gel.RetryRule


*type* TLSOptions
-----------------

TLSOptions contains the parameters needed to configure TLS on |Gel|
server connections.


.. code-block:: go

    type TLSOptions = gel.TLSOptions


*type* TLSSecurityMode
----------------------

TLSSecurityMode specifies how strict TLS validation is.


.. code-block:: go

    type TLSSecurityMode = gel.TLSSecurityMode


*type* Tx
---------

Tx is a transaction. Use Client.Tx() to get a transaction.


.. code-block:: go

    type Tx = gel.Tx


*type* TxBlock
--------------

TxBlock is work to be done in a transaction.


.. code-block:: go

    type TxBlock = gel.TxBlock


*type* TxOptions
----------------

TxOptions configures how transactions behave.


.. code-block:: go

    type TxOptions = gel.TxOptions


*type* WarningHandler
---------------------

WarningHandler takes a slice of gel.Error that represent warnings and
optionally returns an error. This can be used to log warnings, increment
metrics, promote warnings to errors by returning them etc.


.. code-block:: go

    type WarningHandler = gel.WarningHandler
