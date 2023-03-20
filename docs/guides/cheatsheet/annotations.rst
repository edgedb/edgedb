.. _ref_cheatsheet_annotations:

Declaring annotations
=====================

Use annotations to add descriptions to types and links:

.. code-block:: sdl
    :version-lt: 3.0

    type Label {
        annotation description :=
            'Special label to stick on reviews';
        required property comments -> str;
        link review -> Review {
            annotation description :=
                'This review needs some attention';
        };
    }

.. code-block:: sdl

    type Label {
        annotation description :=
            'Special label to stick on reviews';
        required comments: str;
        review: Review {
            annotation description :=
                'This review needs some attention';
        };
    }


----------


Retrieving the annotations can be done via an introspection query:

.. code-block:: edgeql-repl

    db> with module schema
    ... select ObjectType {
    ...     name,
    ...     annotations: {name, @value},
    ...     links: {name, annotations: {name, @value}}
    ... }
    ... filter .name = 'default::Label';
    {
        Object {
            name: 'default::Label',
            annotations: {
                Object {
                    name: 'std::description',
                    @value: 'Special label to stick on reviews'
                }
            },
            links: {
                Object {
                    name: 'review',
                    annotations: {
                        Object {
                            name: 'std::description',
                            @value: 'Special label to stick on reviews'
                        }
                    }
                },
                Object { name: '__type__', annotations: {} }
            }
        }
    }


----------


Alternatively, the annotations can be viewed by the following REPL
command:

.. code-block:: edgeql-repl

    db> \d+ Label
    type default::Label {
        annotation std::description := 'Special label to stick on reviews';
        required single link __type__ -> schema::Type {
            readonly := true;
        };
        single link review -> default::Review {
            annotation std::description := 'Special label to stick on reviews';
        };
        required single property comments -> std::str;
        required single property id -> std::uuid {
            readonly := true;
            constraint std::exclusive;
        };
    };


.. list-table::
  :class: seealso

  * - **See also**
  * - :ref:`Schema > Annotations <ref_datamodel_annotations>`
  * - :ref:`SDL > Annotations <ref_eql_sdl_annotations>`
  * - :ref:`DDL > Annotations <ref_eql_ddl_annotations>`
  * - :ref:`Introspection > Object types <ref_eql_introspection_object_types>`
