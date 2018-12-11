.. _ref_eql_operators_conditional:

===========
Conditional
===========


IF..ELSE
========

.. eql:operator:: IF..ELSE: A IF C ELSE B

    :optype A: SET OF anytype
    :optype C: bool
    :optype B: SET OF anytype
    :resulttype: SET OF anytype

    :index: if else ifelse elif ternary

    Conditionally provide one or the other result.

    IF *C* is ``true``, then the value of the ``IF..ELSE`` expression
    is the value of *A*, if *C* is ``false``, the result is the value of
    *B*.

    ``IF..ELSE`` expressions can be chained when checking multiple conditions
    is necessary:

    .. code-block:: edgeql

        SELECT 'Apple' IF Fruit IS Apple ELSE
               'Banana' IF Fruit IS Banana ELSE
               'Orange' IF Fruit IS Orange ELSE
               'Other';


