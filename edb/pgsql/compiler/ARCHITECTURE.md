# Architecture of PG compiler

RelVar = Relation variable. Basically an instance of a relation within a query.

PathVar = Reference to a column, as seen from within the declaring query.

OutputVar = Reference to a column, that can be used from outside of declaring
query.

## Recursive column injection

When as IR set is compiled, it may not be known which properties of that object
will be needed downstream. To avoid fetching, computing and possibly
materializing too much data, sets are compiled in two steps:

1. Compile general structure of the query. In this process every IR set
   will be bound to some SQL select statement.

2. Inject columns into this tree. This is mainly done in
   `pathctx.get_path_var`, which:
   - finds which RelVar provides source aspect for this path
     (see `pathctx._find_rel_rvar`)
   - determines what is the OutputVar of this path within the RelVar
     (see `pathctx.get_path_output`). This recursively calls `get_path_var`.
   - when an actual table is encountered, a plain ColRef to it's columns is
     returned.

## Overlays

Postgres has a limitation where effects of any DML are not visible in the same
query.

For example:

```
WITH insert_result AS (INSERT INTO my_table(a) VALUES (1) RETURNING a)
SELECT a FROM my_table, insert_result
```

In this query, `my_table` will not contain the inserted value. Obvious
solution is use `insert_result` only and not rely on `my_table` anymore.

This is the gist of what overlays accomplish. They define a new relation that
should be used instead of the base table when the compiler wants to pull data
for some path_id.

Overlay also allows specifying operation that needs to be applied when
constructing the rel var: union, exclude, replace.
For example, union is used after INSERTing, exclude when DELETING.

Overlays are also used for access policies and rewrites.

## Misc

Most references to database objects are prepared by `common.get_backend_name`.
