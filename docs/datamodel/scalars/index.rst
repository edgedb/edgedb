.. _ref_datamodel_scalar_types:

============
Scalar Types
============

*Scalar types* are primitive individual types.  Scalar type instances
hold a single value, called a *scalar value*.

The standard EdgeDB scalar types are:

- :ref:`Numeric types <ref_datamodel_scalars_numeric>`:

  * :eql:type:`int16`
  * :eql:type:`int32`
  * :eql:type:`int64`
  * :eql:type:`float32`
  * :eql:type:`float64`
  * :eql:type:`bigint`
  * :eql:type:`decimal`

- :ref:`String type <ref_datamodel_scalars_str>`

- :ref:`Boolean type <ref_datamodel_scalars_bool>`

- :ref:`Date and Time types <ref_datamodel_scalars_datetime>`:

  * :eql:type:`datetime`
  * :eql:type:`local_datetime`
  * :eql:type:`local_date`
  * :eql:type:`local_time`
  * :eql:type:`duration`

- :ref:`UUID type <ref_datamodel_scalars_uuid>`

- :ref:`JSON type <ref_datamodel_scalars_json>`

- :ref:`Enum types <ref_datamodel_scalars_enum>`

See also scalar type
:ref:`SDL <ref_eql_sdl_scalars>`,
:ref:`DDL <ref_eql_ddl_scalars>`,
:ref:`introspection <ref_eql_introspection_scalar_types>`,
and :ref:`functions and operators <ref_eql_funcops>`.

.. toctree::
    :maxdepth: 3
    :hidden:

    numeric
    str
    bool
    datetime
    json
    enum
    bytes
    uuid
