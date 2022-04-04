.. _ref_graphql_limitations:

=================
Known limitations
=================

- Due to the differences between EdgeQL and GraphQL syntax
  :eql:type:`enum <std::enum>` types which have values that cannot be
  represented as GraphQL identifiers (e.g. ```N/A``` or ```NOT
  APPLICABLE```) cannot be properly reflected into GraphQL enums.

- EdgeDB :eql:type:`tuples <std::tuple>` are reflected as either
  ``List`` or ``Object`` depending on whether they are unnamed or
  named, respectively. However, inserting or updating tuples is
  not yet supported.
  
- :ref:`Link properties<ref_datamodel_link_properties>` are not reflected, as
  GraphQL has no such concept. 

- Every non-abstract EdgeDB object type is simultaneously an interface
  and an object in terms of the GraphQL type system, which means that for
  every one object type name two names are needed in reflected
  GraphQL. This potentially results in name clashes if the convention
  of using camel-case names for user types is not followed in EdgeDB.
