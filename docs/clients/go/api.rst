
API
===


*type* Client
-------------

Client is a connection pool and is safe for concurrent use.


.. code-block:: go

    type Client = edgedb.Client


*type* Error
------------

Error is the error type returned from edgedb.


.. code-block:: go

    type Error = edgedb.Error


*type* ErrorCategory
--------------------

ErrorCategory values represent EdgeDB's error types.


.. code-block:: go

    type ErrorCategory = edgedb.ErrorCategory


*type* ErrorTag
---------------

ErrorTag is the argument type to Error.HasTag().


.. code-block:: go

    type ErrorTag = edgedb.ErrorTag


*type* Executor
---------------

Executor is a common interface between \*Client and \*Tx,
that can run queries on an EdgeDB database.


.. code-block:: go

    type Executor = edgedb.Executor


*type* IsolationLevel
---------------------

IsolationLevel documentation can be found here
`docs/reference/edgeql/tx_start#parameters <https://www.edgedb.com/docs/reference/edgeql/tx_start#parameters>`_


.. code-block:: go

    type IsolationLevel = edgedb.IsolationLevel


*type* ModuleAlias
------------------

ModuleAlias is an alias name and module name pair.


.. code-block:: go

    type ModuleAlias = edgedb.ModuleAlias


*type* Options
--------------

Options for connecting to an EdgeDB server


.. code-block:: go

    type Options = edgedb.Options


*type* RetryBackoff
-------------------

RetryBackoff returns the duration to wait after the nth attempt
before making the next attempt when retrying a transaction.


.. code-block:: go

    type RetryBackoff = edgedb.RetryBackoff


*type* RetryCondition
---------------------

RetryCondition represents scenarios that can cause a transaction
run in Tx() methods to be retried.


.. code-block:: go

    type RetryCondition = edgedb.RetryCondition


*type* RetryOptions
-------------------

RetryOptions configures how Tx() retries failed transactions.  Use
NewRetryOptions to get a default RetryOptions value instead of creating one
yourself.


.. code-block:: go

    type RetryOptions = edgedb.RetryOptions


*type* RetryRule
----------------

RetryRule determines how transactions should be retried when run in Tx()
methods. See Client.Tx() for details.


.. code-block:: go

    type RetryRule = edgedb.RetryRule


*type* TLSOptions
-----------------

TLSOptions contains the parameters needed to configure TLS on EdgeDB
server connections.


.. code-block:: go

    type TLSOptions = edgedb.TLSOptions


*type* TLSSecurityMode
----------------------

TLSSecurityMode specifies how strict TLS validation is.


.. code-block:: go

    type TLSSecurityMode = edgedb.TLSSecurityMode


*type* Tx
---------

Tx is a transaction. Use Client.Tx() to get a transaction.


.. code-block:: go

    type Tx = edgedb.Tx


*type* TxBlock
--------------

TxBlock is work to be done in a transaction.


.. code-block:: go

    type TxBlock = edgedb.TxBlock


*type* TxOptions
----------------

TxOptions configures how transactions behave.


.. code-block:: go

    type TxOptions = edgedb.TxOptions


*type* WarningHandler
---------------------

WarningHandler takes a slice of edgedb.Error that represent warnings and
optionally returns an error. This can be used to log warnings, increment
metrics, promote warnings to errors by returning them etc.


.. code-block:: go

    type WarningHandler = edgedb.WarningHandler