Introspection
=============

EdgeQL keeps a records of all of the classes declared in the *schema*.
It is possible to access these via ``__class__`` attribute of any
object. It is also possible to access them directly via the built-in
special module ``schema``.

.. code-block:: eql

    # the following two are equivalent, provided there's at least
    # one Text object in the DB
    SELECT example::Text.__class__ LIMIT 1;

    SELECT schema::Concept
    FILTER schema::Concept.name = 'example::Text';

There are various built-in attributes that can be queried directly by
their names in introspection queries: ``name``, ``is_abstract``,
``is_derived``. Generally any attribute that appears in the schema
definition can also be queried via ``attributes`` link. The
``Attribute`` will have a ``name`` and the particular value will be
store as a *link property* ``value``. The caveat is that all values in
generic attributes are stored as their string representations.

Various schema entities are represented by their own concepts in the
``schema`` module such as ``Atom``, ``Concept``, ``Link``,
``LinkProperty``, etc. A full list can be retrieved by getting all the
``schema::Class`` objects.

.. code-block:: eql

    # get all the classes defined in the 'example' module
    WITH MODULE schema
    SELECT Class.name
    FILTER Class.name ~ '^example::\w+$'
    ORDER BY Class.name;

``Concept`` has ``links`` that are represented by a set of ``Link``
objects. The actual link targets can be accessed by the *link*
```target``` on the actual ``Link`` object. Additionally, the mapping
for each of the links can be retrieved via ``attributes``, using the
attribute name ``stdattrs::mapping`` and ``@value`` to get the mapping
value.

.. code-block:: eql

    # get all 'example' concepts with their links
    WITH MODULE schema
    SELECT Concept {
        name,
        links: {
            name,
            target: {
                name
            },
            attributes: {
                name,
                @value
            } FILTER
                Concept.links.attributes.name = 'stdattr::mapping'
        }
    }
    FILTER Concept.name LIKE 'example::%'
    ORDER BY Concept.name;
