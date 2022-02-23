.. _ref_protocol_errors:

======
Errors
======

Errors inheritance
==================

Each error in EdgeDB consists of a code, a name, and optionally tags. Errors
in EdgeDB can inherit from other errors. This is denoted by matching code
prefixes. For example, ``TransactionConflictError`` (``0x_05_03_01_00``) is
the parent error for ``TransactionSerializationError`` (``0x_05_03_01_01``)
and ``TransactionDeadlockError`` (``0x_05_03_01_02``). The matching prefix
here is ``0x_05_03_01``.

When the EdgeDB client expects a more general error and EdgeDB returns a more
specific error that inherits from the general error, the check in the client
must take this into account. This can be expressed by the ``binary and``
operation or ``&`` opeator in most programming languages:

.. code-block::

  (expected_error_code & server_error_code) == expected_error_code


Note that although it is not explicitly stated in the ``edb/api/errors.txt``
file, each inherited error must contain all tags of the parent error. Given
that, ``TransactionSerializationError`` and ``TransactionDeadlockError``, for
example, must contain the ``SHOULD_RETRY`` tag that is defined for
``TransactionConflictError``.


.. _ref_protocol_error_codes:

Error codes
===========

Error codes and names as specified in ``edb/api/errors.txt``:

.. raw:: text
    :file: errors.txt
