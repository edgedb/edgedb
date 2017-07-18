.. _ref_edgeql_builtins:


Built-ins
=========

EdgeQL has a number of built-in functions in the ``std`` module. Like
everything else in ``std`` module it is not necessary to specify the
module name to refer to these functions.


Polymorphic functions
---------------------

.. function:: len(any) -> int

    Polymorphic function that can take ``str``, ``bytes`` or any
    ``array`` as input.

    Return the number of characters in a ``str``, or the number of
    bytes in ``bytes``, or the number of elements in an ``array``.

    .. code-block:: eql

        SELECT len('foo');
        # returns 3

        SELECT len([2, 5, 7]);
        # also returns 3


Array functions
---------------

.. _ref_edgeql_builtins_array_agg:

.. function:: array_agg(any) -> any

    Return the array made from all of the input set elements. The
    ordering of the input set will be preserved if specified.

    .. code-block:: pseudo-eql

        SELECT array_agg({2, 3, 5});
        # returns [2, 3, 5]

        SELECT array_agg(User.name ORDER BY User.name);
        # returns a string array containing all User names sorted
        # alphabetically

.. function:: array_contains(array<any>, any) -> bool

    Return true if the array contains the specified value.

    .. code-block:: eql

        SELECT array_contains([2, 3, 5], 2);
        # returns TRUE

        SELECT array_contains(['foo', 'bar'], 'baz');
        # returns FALSE

.. function:: array_enumerate(array<any>) -> SET OF tuple<any, int>

    Return a set of tuples where the first element is an array value
    and the second element is the index of that value for all values
    in the array.

    .. code-block:: eql

        SELECT array_enumerate([2, 3, 5]);
        # returns {(3, 1), (2, 0), (5, 2)}

    .. note::

        Notice that the ordering of the returned set is not
        guaranteed.


.. function:: array_unpack(array<any>) -> any

    Return array elements as a set.

    .. code-block:: eql

        SELECT array_unpack([2, 3, 5]);
        # returns {3, 2, 5}

    .. note::

        Notice that the ordering of the returned set is not
        guaranteed.


String functions
----------------

    .. TODO::

        This whole section will need more explanation and details with
        rules, flags, etc.

.. function:: lower(str) -> str

    Return a copy of the string where all the characters are converted
    to lowercase.

    .. code-block:: eql

        SELECT lower('Some Fancy Title');
        # returns 'some fancy title'


.. function:: re_match(str, str) -> SET OF array<str>

    Given an input string and a regular expression string find the
    first match for the regular expression within the string. Return
    the set of all matches, each match represented by an
    ``array<str>`` of matched groups.

.. function:: re_match_all(str, str) -> SET OF array<str>

    Given an input string and a regular expression string repeatedly
    match the regular expression within the string. Return the set of
    all matches, each match represented by an ``array<str>`` of
    matched groups.

.. function:: re_test(str, str) -> bool

    Given an input string and a regular expression string test whether
    there is a match for the regular expression within the string.
    Return ``TRUE`` if there is a match, ``FALSE`` otherwise.


Set aggregate functions
-----------------------

.. function:: count(any) -> int

    Return the number of elements in a set.

    .. code-block:: pseudo-eql

        SELECT count({2, 3, 5});
        # returns 3

        SELECT count(User);
        # returns the number of User objects in the DB

.. function:: sum(number) -> number

    Return the sum of the set of numbers. The numbers have to be
    either ``int`` or ``float``.

    .. code-block:: eql

        SELECT sum({2, 3, 5});
        # returns 10

        SELECT sum({0.2, 0.3, 0.5});
        # returns 1.0

Here's a list of aggregate functions covered in other sections:
:ref:`array_agg<ref_edgeql_builtins_array_agg>`.


Date/time functions
-------------------

.. function:: current_date() -> date

    Return the current server date.

.. function:: current_datetime() -> datetime

    Return the current server date and time.

.. function:: current_time() -> time

    Return the current server time.


Random/UUID functions
---------------------

.. function:: random() -> float

    Return a pseudo-random number in the range `[0, 1)`.

.. function:: uuid_generate_v1mc() -> uuid

    Return a version 1 UUID using a random multicast MAC address
    instead of the real MAC address of the computer.
