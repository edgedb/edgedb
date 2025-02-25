.. _gel-go-intro:

=============
Gel Go Driver
=============


.. toctree::
   :maxdepth: 3
   :hidden:

   api
   types
   codegen


Typical client usage looks like this:

.. code-block:: go

    package main

    import (
        "context"
        "log"

        "github.com/geldata/gel-go"
    )

    func main() {
        ctx := context.Background()
        client, err := gel.CreateClient(ctx, gel.Options{})
        if err != nil {
            log.Fatal(err)
        }
        defer client.Close()

        var (
            age   int64 = 21
            users []struct {
                ID   gel.UUID    `gel:"id"`
                Name string      `gel:"name"`
            }
        )

        query := "SELECT User{name} FILTER .age = <int64>$0"
        err = client.Query(ctx, query, &users, age)
        ...
    }

We recommend using environment variables for connection parameters. See the
`client connection docs <https://www.geldata.com/docs/clients/connection>`_ for more information.

You may also connect to a database using a DSN:

.. code-block:: go

    url := "gel://admin@localhost/gel"
    client, err := gel.CreateClientDSN(ctx, url, opts)

Or you can use Option fields.

.. code-block:: go

    opts := gel.Options{
        Database:    "gel",
        User:        "gel",
        Concurrency: 4,
    }

    client, err := gel.CreateClient(ctx, opts)


Errors
------

``gel`` never returns underlying errors directly.
If you are checking for things like context expiration
use errors.Is() or errors.As().

.. code-block:: go

    err := client.Query(...)
    if errors.Is(err, context.Canceled) { ... }

Most errors returned by the gel package will satisfy the gel.Error
interface which has methods for introspecting.

.. code-block:: go

    err := client.Query(...)

    var edbErr gel.Error
    if errors.As(err, &edbErr) && edbErr.Category(gel.NoDataError){
        ...
    }


Datatypes
---------

The following list shows the marshal/unmarshal
mapping between |Gel| types and go types:

.. code-block:: go

    Gel                      Go
    ---------                ---------
    Set                      []anytype
    array<anytype>           []anytype
    tuple                    struct
    named tuple              struct
    Object                   struct
    bool                     bool, gel.OptionalBool
    bytes                    []byte, gel.OptionalBytes
    str                      string, gel.OptionalStr
    anyenum                  string, gel.OptionalStr
    datetime                 time.Time, gel.OptionalDateTime
    cal::local_datetime      gel.LocalDateTime,
                             gel.OptionalLocalDateTime
    cal::local_date          gel.LocalDate, gel.OptionalLocalDate
    cal::local_time          gel.LocalTime, gel.OptionalLocalTime
    duration                 gel.Duration, gel.OptionalDuration
    cal::relative_duration   gel.RelativeDuration,
                             gel.OptionalRelativeDuration
    float32                  float32, gel.OptionalFloat32
    float64                  float64, gel.OptionalFloat64
    int16                    int16, gel.OptionalFloat16
    int32                    int32, gel.OptionalInt16
    int64                    int64, gel.OptionalInt64
    uuid                     gel.UUID, gel.OptionalUUID
    json                     []byte, gel.OptionalBytes
    bigint                   *big.Int, gel.OptionalBigInt

    decimal                  user defined (see Custom Marshalers)

Note that Gel's std::duration type is represented in int64 microseconds
while go's time.Duration type is int64 nanoseconds. It is incorrect to cast
one directly to the other.

Shape fields that are not required must use optional types for receiving
query results. The gel.Optional struct can be embedded to make structs
optional.

.. code-block:: go

    type User struct {
        gel.Optional
        Email string `gel:"email"`
    }

    var result User
    err := client.QuerySingle(ctx, `SELECT User { email } LIMIT 0`, $result)
    fmt.Println(result.Missing())
    // Output: true

    err := client.QuerySingle(ctx, `SELECT User { email } LIMIT 1`, $result)
    fmt.Println(result.Missing())
    // Output: false

Not all types listed above are valid query parameters.  To pass a slice of
scalar values use array in your query. |Gel| doesn't currently support
using sets as parameters.

.. code-block:: go

    query := `select User filter .id in array_unpack(<array<uuid>>$1)`
    client.QuerySingle(ctx, query, $user, []gel.UUID{...})

Nested structures are also not directly allowed but you can use `json <https://www.gel.com/docs/edgeql/insert#bulk-inserts>`_
instead.

By default |Gel| will ignore embedded structs when marshaling/unmarshaling.
To treat an embedded struct's fields as part of the parent struct's fields,
tag the embedded struct with \`gel:"$inline"\`.

.. code-block:: go

    type Object struct {
        ID gel.UUID
    }

    type User struct {
        Object `gel:"$inline"`
        Name string
    }


Custom Marshalers
-----------------

Interfaces for user defined marshaler/unmarshalers  are documented in the
internal/marshal package.



Usage Example
-------------

.. code-block:: go

    package gel_test

    import (
        "context"
        "fmt"
        "log"
        "time"

        gel "github.com/gel/gel-go"
    )

    type User struct {
        ID   gel.UUID `gel:"id"`
        Name string      `gel:"name"`
        DOB  time.Time   `gel:"dob"`
    }

    func Example() {
        opts := gel.Options{Concurrency: 4}
        ctx := context.Background()
        db, err := gel.CreateClientDSN(ctx, "gel://gel@localhost/test", opts)
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
        var inserted struct{ id gel.UUID }
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

