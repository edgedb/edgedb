=====================
EdgeQL Specification
=====================

:Author: [Your Name]
:Date: [Current Date]

Introduction
------------

[Insert introduction to EdgeQL here]

Types
-----

In EdgeQL, there are several built-in types that can be used to represent different kinds of data. These include:

- ``str``: A string of characters
- ``int``: An integer value
- ``float``: A floating-point number
- ``bool``: A Boolean value (either ``true`` or ``false``)
- ``datetime``: A date and time value
- ``uuid``: A universally unique identifier

Values
------

In EdgeQL, values can be represented using literal syntax. For example, a string value can be represented using quotes, an integer value can be represented using a numeric literal, and a Boolean value can be represented using the keywords ``true`` or ``false``. For example:


Expressions
-----------

In EdgeQL, expressions are used to represent operations and computations that involve values. These can include arithmetic operations, comparison operations, and logical operations. For example:

Type Checking
-------------

In EdgeQL, type checking is performed at compile time to ensure that expressions and operations are used with the correct types of values. For example, adding a string value to an integer value would result in a type error. Type checking can help catch these errors before the code is executed.

Evaluation
----------

In EdgeQL, expressions are evaluated at runtime to produce a result. For example, an arithmetic expression like ``1 + 2`` would be evaluated to produce the value ``3``. Evaluation can involve type coercion, where values are automatically converted to a compatible type before an operation is performed.

Conclusion
----------

[Insert conclusion to EdgeQL specification here]