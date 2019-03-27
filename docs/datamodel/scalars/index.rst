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
  * :eql:type:`decimal`

- :ref:`String type <ref_datamodel_scalars_str>`

- :ref:`Boolean type <ref_datamodel_scalars_bool>`

- :ref:`Date and Time types <ref_datamodel_scalars_datetime>`:

  * :eql:type:`datetime`
  * :eql:type:`naive_datetime`
  * :eql:type:`naive_date`
  * :eql:type:`naive_time`
  * :eql:type:`timedelta`

- :ref:`UUID type <ref_datamodel_scalars_uuid>`

- :ref:`JSON type <ref_datamodel_scalars_json>`


.. toctree::
    :maxdepth: 3
    :hidden:

    numeric
    str
    bool
    datetime
    bytes
    sequence
    uuid
    json
