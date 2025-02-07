.. _edgedb-go-intro:

================
EdgeDB Go Driver
================


.. toctree::
   :maxdepth: 3
   :hidden:

   api
   types
   codegen



Package edgedb is the official Go driver for `EdgeDB <https://www.edgedb.com>`_. Additionally,
`edgeql-go <https://pkg.go.dev/github.com/edgedb/edgedb-go/cmd/edgeql-go>`_ is a code generator that
generates go functions from edgeql files.

Typical client usage looks like this:

.. code-block:: go

    package main
    
    import (
        "context"
        "log"
    
        "github.com/edgedb/edgedb-go"
    )
    
    func main() {
        ctx := context.Background()
        client, err := edgedb.CreateClient(ctx, edgedb.Options{})
        if err != nil {
            log.Fatal(err)
        }
        defer client.Close()
    
        var (
            age   int64 = 21
            users []struct {
                ID   edgedb.UUID `edgedb:"id"`
                Name string      `edgedb:"name"`
            }
        )
    
        query := "SELECT User{name} FILTER .age = <int64>$0"
        err = client.Query(ctx, query, &users, age)
        ...
    }
    
We recommend using environment variables for connection parameters. See the
`client connection docs <https://www.edgedb.com/docs/clients/connection>`_ for more information.

You may also connect to a database using a DSN:

.. code-block:: go

    url := "edgedb://edgedb@localhost/edgedb"
    client, err := edgedb.CreateClientDSN(ctx, url, opts)
    
Or you can use Option fields.

.. code-block:: go

    opts := edgedb.Options{
        Database:    "edgedb",
        User:        "edgedb",
        Concurrency: 4,
    }
    
    client, err := edgedb.CreateClient(ctx, opts)
    

Errors
------

edgedb never returns underlying errors directly.
If you are checking for things like context expiration
use errors.Is() or errors.As().

.. code-block:: go

    err := client.Query(...)
    if errors.Is(err, context.Canceled) { ... }
    
Most errors returned by the edgedb package will satisfy the edgedb.Error
interface which has methods for introspecting.

.. code-block:: go

    err := client.Query(...)
    
    var edbErr edgedb.Error
    if errors.As(err, &edbErr) && edbErr.Category(edgedb.NoDataError){
        ...
    }
    

Datatypes
---------

The following list shows the marshal/unmarshal
mapping between EdgeDB types and go types:

.. code-block:: go

    EdgeDB                   Go
    ---------                ---------
    Set                      []anytype
    array<anytype>           []anytype
    tuple                    struct
    named tuple              struct
    Object                   struct
    bool                     bool, edgedb.OptionalBool
    bytes                    []byte, edgedb.OptionalBytes
    str                      string, edgedb.OptionalStr
    anyenum                  string, edgedb.OptionalStr
    datetime                 time.Time, edgedb.OptionalDateTime
    cal::local_datetime      edgedb.LocalDateTime,
                             edgedb.OptionalLocalDateTime
    cal::local_date          edgedb.LocalDate, edgedb.OptionalLocalDate
    cal::local_time          edgedb.LocalTime, edgedb.OptionalLocalTime
    duration                 edgedb.Duration, edgedb.OptionalDuration
    cal::relative_duration   edgedb.RelativeDuration,
                             edgedb.OptionalRelativeDuration
    float32                  float32, edgedb.OptionalFloat32
    float64                  float64, edgedb.OptionalFloat64
    int16                    int16, edgedb.OptionalFloat16
    int32                    int32, edgedb.OptionalInt16
    int64                    int64, edgedb.OptionalInt64
    uuid                     edgedb.UUID, edgedb.OptionalUUID
    json                     []byte, edgedb.OptionalBytes
    bigint                   *big.Int, edgedb.OptionalBigInt
    
    decimal                  user defined (see Custom Marshalers)
    
Note that EdgeDB's std::duration type is represented in int64 microseconds
while go's time.Duration type is int64 nanoseconds. It is incorrect to cast
one directly to the other.

Shape fields that are not required must use optional types for receiving
query results. The edgedb.Optional struct can be embedded to make structs
optional.

.. code-block:: go

    type User struct {
        edgedb.Optional
        Email string `edgedb:"email"`
    }
    
    var result User
    err := client.QuerySingle(ctx, `SELECT User { email } LIMIT 0`, $result)
    fmt.Println(result.Missing())
    // Output: true
    
    err := client.QuerySingle(ctx, `SELECT User { email } LIMIT 1`, $result)
    fmt.Println(result.Missing())
    // Output: false
    
Not all types listed above are valid query parameters.  To pass a slice of
scalar values use array in your query. EdgeDB doesn't currently support
using sets as parameters.

.. code-block:: go

    query := `select User filter .id in array_unpack(<array<uuid>>$1)`
    client.QuerySingle(ctx, query, $user, []edgedb.UUID{...})
    
Nested structures are also not directly allowed but you can use `json <https://www.edgedb.com/docs/edgeql/insert#bulk-inserts>`_
instead.

By default EdgeDB will ignore embedded structs when marshaling/unmarshaling.
To treat an embedded struct's fields as part of the parent struct's fields,
tag the embedded struct with \`edgedb:"$inline"\`.

.. code-block:: go

    type Object struct {
        ID edgedb.UUID
    }
    
    type User struct {
        Object `edgedb:"$inline"`
        Name string
    }
    

Custom Marshalers
-----------------

Interfaces for user defined marshaler/unmarshalers  are documented in the
internal/marshal package.



Usage Example
-------------

.. code-block:: go
    
    package edgedb_test
    
    import (
        "context"
        "fmt"
        "log"
        "time"
    
        edgedb "github.com/edgedb/edgedb-go"
    )
    
    type User struct {
        ID   edgedb.UUID `edgedb:"id"`
        Name string      `edgedb:"name"`
        DOB  time.Time   `edgedb:"dob"`
    }
    
    func Example() {
        opts := edgedb.Options{Concurrency: 4}
        ctx := context.Background()
        db, err := edgedb.CreateClientDSN(ctx, "edgedb://edgedb@localhost/test", opts)
        if err != nil {
            log.Fatal(err)
        }
        defer db.Close()
    
        // create a user object type.
        err = db.Execute(ctx, `
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE PROPERTY dob -> datetime;
            }
        `)
        if err != nil {
            log.Fatal(err)
        }
    
        // Insert a new user.
        var inserted struct{ id edgedb.UUID }
        err = db.QuerySingle(ctx, `
            INSERT User {
                name := <str>$0,
                dob := <datetime>$1
            }
        `, &inserted, "Bob", time.Date(1984, 3, 1, 0, 0, 0, 0, time.UTC))
        if err != nil {
            log.Fatal(err)
        }
    
        // Select users.
        var users []User
        args := map[string]interface{}{"name": "Bob"}
        query := "SELECT User {name, dob} FILTER .name = <str>$name"
        err = db.Query(ctx, query, &users, args)
        if err != nil {
            log.Fatal(err)
        }
    
        fmt.Println(users)
    }
    
