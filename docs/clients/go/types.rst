Datatypes
=========


*type* DateDuration
-------------------

DateDuration represents the elapsed time between two dates in a fuzzy human
way.


.. code-block:: go

    type DateDuration struct {
        // contains filtered or unexported fields
    }


*function* NewDateDuration
..........................

.. code-block:: go

    func NewDateDuration(months int32, days int32) DateDuration

NewDateDuration returns a new DateDuration




*method* MarshalText
....................

.. code-block:: go

    func (dd DateDuration) MarshalText() ([]byte, error)

MarshalText returns dd marshaled as text.




*method* String
...............

.. code-block:: go

    func (dd DateDuration) String() string




*method* UnmarshalText
......................

.. code-block:: go

    func (dd *DateDuration) UnmarshalText(b []byte) error

UnmarshalText unmarshals bytes into \*dd.




*type* Duration
---------------

Duration represents the elapsed time between two instants
as an int64 microsecond count.


.. code-block:: go

    type Duration int64


*function* DurationFromNanoseconds
..................................

.. code-block:: go

    func DurationFromNanoseconds(d time.Duration) Duration

DurationFromNanoseconds creates a Duration represented as microseconds
from a `time.Duration <https://pkg.go.dev/time>`_ represented as nanoseconds.




*function* ParseDuration
........................

.. code-block:: go

    func ParseDuration(s string) (Duration, error)

ParseDuration parses an |Gel| duration string.




*method* AsNanoseconds
......................

.. code-block:: go

    func (d Duration) AsNanoseconds() (time.Duration, error)

AsNanoseconds returns `time.Duration <https://pkg.go.dev/time>`_ represented as nanoseconds,
after transforming from Duration microsecond representation.
Returns an error if the Duration is too long and would cause an overflow of
the internal int64 representation.




*method* String
...............

.. code-block:: go

    func (d Duration) String() string




*type* LocalDate
----------------

LocalDate is a date without a time zone.
`docs/stdlib/datetime#type::cal::local_date <https://www.geldata.com/docs/stdlib/datetime#type::cal::local_date>`_


.. code-block:: go

    type LocalDate struct {
        // contains filtered or unexported fields
    }


*function* NewLocalDate
.......................

.. code-block:: go

    func NewLocalDate(year int, month time.Month, day int) LocalDate

NewLocalDate returns a new LocalDate




*method* MarshalText
....................

.. code-block:: go

    func (d LocalDate) MarshalText() ([]byte, error)

MarshalText returns d marshaled as text.




*method* String
...............

.. code-block:: go

    func (d LocalDate) String() string




*method* UnmarshalText
......................

.. code-block:: go

    func (d *LocalDate) UnmarshalText(b []byte) error

UnmarshalText unmarshals bytes into \*d.




*type* LocalDateTime
--------------------

LocalDateTime is a date and time without timezone.
`docs/stdlib/datetime#type::cal::local_datetime <https://www.geldata.com/docs/stdlib/datetime#type::cal::local_datetime>`_


.. code-block:: go

    type LocalDateTime struct {
        // contains filtered or unexported fields
    }


*function* NewLocalDateTime
...........................

.. code-block:: go

    func NewLocalDateTime(
        year int, month time.Month, day, hour, minute, second, microsecond int,
    ) LocalDateTime

NewLocalDateTime returns a new LocalDateTime




*method* MarshalText
....................

.. code-block:: go

    func (dt LocalDateTime) MarshalText() ([]byte, error)

MarshalText returns dt marshaled as text.




*method* String
...............

.. code-block:: go

    func (dt LocalDateTime) String() string




*method* UnmarshalText
......................

.. code-block:: go

    func (dt *LocalDateTime) UnmarshalText(b []byte) error

UnmarshalText unmarshals bytes into \*dt.




*type* LocalTime
----------------

LocalTime is a time without a time zone.
`docs/stdlib/datetime#type::cal::local_time <https://www.geldata.com/docs/stdlib/datetime#type::cal::local_time>`_


.. code-block:: go

    type LocalTime struct {
        // contains filtered or unexported fields
    }


*function* NewLocalTime
.......................

.. code-block:: go

    func NewLocalTime(hour, minute, second, microsecond int) LocalTime

NewLocalTime returns a new LocalTime




*method* MarshalText
....................

.. code-block:: go

    func (t LocalTime) MarshalText() ([]byte, error)

MarshalText returns t marshaled as text.




*method* String
...............

.. code-block:: go

    func (t LocalTime) String() string




*method* UnmarshalText
......................

.. code-block:: go

    func (t *LocalTime) UnmarshalText(b []byte) error

UnmarshalText unmarshals bytes into \*t.




*type* Memory
-------------

Memory represents memory in bytes.


.. code-block:: go

    type Memory int64


*method* MarshalText
....................

.. code-block:: go

    func (m Memory) MarshalText() ([]byte, error)

MarshalText returns m marshaled as text.




*method* String
...............

.. code-block:: go

    func (m Memory) String() string




*method* UnmarshalText
......................

.. code-block:: go

    func (m *Memory) UnmarshalText(b []byte) error

UnmarshalText unmarshals bytes into \*m.




*type* MultiRangeDateTime
-------------------------

MultiRangeDateTime is a type alias for a slice of RangeDateTime values.


.. code-block:: go

    type MultiRangeDateTime = []RangeDateTime


*type* MultiRangeFloat32
------------------------

MultiRangeFloat32 is a type alias for a slice of RangeFloat32 values.


.. code-block:: go

    type MultiRangeFloat32 = []RangeFloat32


*type* MultiRangeFloat64
------------------------

MultiRangeFloat64 is a type alias for a slice of RangeFloat64 values.


.. code-block:: go

    type MultiRangeFloat64 = []RangeFloat64


*type* MultiRangeInt32
----------------------

MultiRangeInt32 is a type alias for a slice of RangeInt32 values.


.. code-block:: go

    type MultiRangeInt32 = []RangeInt32


*type* MultiRangeInt64
----------------------

MultiRangeInt64 is a type alias for a slice of RangeInt64 values.


.. code-block:: go

    type MultiRangeInt64 = []RangeInt64


*type* MultiRangeLocalDate
--------------------------

MultiRangeLocalDate is a type alias for a slice of
RangeLocalDate values.


.. code-block:: go

    type MultiRangeLocalDate = []RangeLocalDate


*type* MultiRangeLocalDateTime
------------------------------

MultiRangeLocalDateTime is a type alias for a slice of
RangeLocalDateTime values.


.. code-block:: go

    type MultiRangeLocalDateTime = []RangeLocalDateTime


*type* Optional
---------------

Optional represents a shape field that is not required.
Optional is embedded in structs to make them optional. For example:

.. code-block:: go

    type User struct {
        gel.Optional
        Name string `gel:"name"`
    }


.. code-block:: go

    type Optional struct {
        // contains filtered or unexported fields
    }


*method* Missing
................

.. code-block:: go

    func (o *Optional) Missing() bool

Missing returns true if the value is missing.




*method* SetMissing
...................

.. code-block:: go

    func (o *Optional) SetMissing(missing bool)

SetMissing sets the structs missing status. true means missing and false
means present.




*method* Unset
..............

.. code-block:: go

    func (o *Optional) Unset()

Unset marks the value as missing




*type* OptionalBigInt
---------------------

OptionalBigInt is an optional \*big.Int. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalBigInt struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalBigInt
............................

.. code-block:: go

    func NewOptionalBigInt(v *big.Int) OptionalBigInt

NewOptionalBigInt is a convenience function for creating an OptionalBigInt
with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalBigInt) Get() (*big.Int, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalBigInt) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalBigInt) Set(val *big.Int)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalBigInt) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalBigInt) Unset()

Unset marks the value as missing.




*type* OptionalBool
-------------------

OptionalBool is an optional bool. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalBool struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalBool
..........................

.. code-block:: go

    func NewOptionalBool(v bool) OptionalBool

NewOptionalBool is a convenience function for creating an OptionalBool with
its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalBool) Get() (bool, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalBool) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalBool) Set(val bool)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalBool) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalBool) Unset()

Unset marks the value as missing.




*type* OptionalBytes
--------------------

OptionalBytes is an optional []byte. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalBytes struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalBytes
...........................

.. code-block:: go

    func NewOptionalBytes(v []byte) OptionalBytes

NewOptionalBytes is a convenience function for creating an OptionalBytes
with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalBytes) Get() ([]byte, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalBytes) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalBytes) Set(val []byte)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalBytes) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalBytes) Unset()

Unset marks the value as missing.




*type* OptionalDateDuration
---------------------------

OptionalDateDuration is an optional DateDuration. Optional types
must be used for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalDateDuration struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalDateDuration
..................................

.. code-block:: go

    func NewOptionalDateDuration(v DateDuration) OptionalDateDuration

NewOptionalDateDuration is a convenience function for creating an
OptionalDateDuration with its value set to v.




*method* Get
............

.. code-block:: go

    func (o *OptionalDateDuration) Get() (DateDuration, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalDateDuration) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalDateDuration) Set(val DateDuration)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalDateDuration) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalDateDuration) Unset()

Unset marks the value as missing.




*type* OptionalDateTime
-----------------------

OptionalDateTime is an optional time.Time.  Optional types must be used for
out parameters when a shape field is not required.


.. code-block:: go

    type OptionalDateTime struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalDateTime
..............................

.. code-block:: go

    func NewOptionalDateTime(v time.Time) OptionalDateTime

NewOptionalDateTime is a convenience function for creating an
OptionalDateTime with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalDateTime) Get() (time.Time, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalDateTime) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalDateTime) Set(val time.Time)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalDateTime) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalDateTime) Unset()

Unset marks the value as missing.




*type* OptionalDuration
-----------------------

OptionalDuration is an optional Duration. Optional types must be used for
out parameters when a shape field is not required.


.. code-block:: go

    type OptionalDuration struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalDuration
..............................

.. code-block:: go

    func NewOptionalDuration(v Duration) OptionalDuration

NewOptionalDuration is a convenience function for creating an
OptionalDuration with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalDuration) Get() (Duration, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalDuration) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalDuration) Set(val Duration)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalDuration) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalDuration) Unset()

Unset marks the value as missing.




*type* OptionalFloat32
----------------------

OptionalFloat32 is an optional float32. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalFloat32 struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalFloat32
.............................

.. code-block:: go

    func NewOptionalFloat32(v float32) OptionalFloat32

NewOptionalFloat32 is a convenience function for creating an OptionalFloat32
with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalFloat32) Get() (float32, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalFloat32) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalFloat32) Set(val float32)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalFloat32) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalFloat32) Unset()

Unset marks the value as missing.




*type* OptionalFloat64
----------------------

OptionalFloat64 is an optional float64. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalFloat64 struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalFloat64
.............................

.. code-block:: go

    func NewOptionalFloat64(v float64) OptionalFloat64

NewOptionalFloat64 is a convenience function for creating an OptionalFloat64
with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalFloat64) Get() (float64, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalFloat64) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalFloat64) Set(val float64)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalFloat64) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalFloat64) Unset()

Unset marks the value as missing.




*type* OptionalInt16
--------------------

OptionalInt16 is an optional int16. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalInt16 struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalInt16
...........................

.. code-block:: go

    func NewOptionalInt16(v int16) OptionalInt16

NewOptionalInt16 is a convenience function for creating an OptionalInt16
with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalInt16) Get() (int16, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalInt16) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalInt16) Set(val int16)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalInt16) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalInt16) Unset()

Unset marks the value as missing.




*type* OptionalInt32
--------------------

OptionalInt32 is an optional int32. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalInt32 struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalInt32
...........................

.. code-block:: go

    func NewOptionalInt32(v int32) OptionalInt32

NewOptionalInt32 is a convenience function for creating an OptionalInt32
with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalInt32) Get() (int32, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalInt32) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalInt32) Set(val int32)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalInt32) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalInt32) Unset()

Unset marks the value as missing.




*type* OptionalInt64
--------------------

OptionalInt64 is an optional int64. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalInt64 struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalInt64
...........................

.. code-block:: go

    func NewOptionalInt64(v int64) OptionalInt64

NewOptionalInt64 is a convenience function for creating an OptionalInt64
with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalInt64) Get() (int64, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalInt64) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalInt64) Set(val int64) *OptionalInt64

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalInt64) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalInt64) Unset() *OptionalInt64

Unset marks the value as missing.




*type* OptionalLocalDate
------------------------

OptionalLocalDate is an optional LocalDate. Optional types must be used for
out parameters when a shape field is not required.


.. code-block:: go

    type OptionalLocalDate struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalLocalDate
...............................

.. code-block:: go

    func NewOptionalLocalDate(v LocalDate) OptionalLocalDate

NewOptionalLocalDate is a convenience function for creating an
OptionalLocalDate with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalLocalDate) Get() (LocalDate, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalLocalDate) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalLocalDate) Set(val LocalDate)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalLocalDate) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalLocalDate) Unset()

Unset marks the value as missing.




*type* OptionalLocalDateTime
----------------------------

OptionalLocalDateTime is an optional LocalDateTime. Optional types must be
used for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalLocalDateTime struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalLocalDateTime
...................................

.. code-block:: go

    func NewOptionalLocalDateTime(v LocalDateTime) OptionalLocalDateTime

NewOptionalLocalDateTime is a convenience function for creating an
OptionalLocalDateTime with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalLocalDateTime) Get() (LocalDateTime, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalLocalDateTime) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalLocalDateTime) Set(val LocalDateTime)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalLocalDateTime) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalLocalDateTime) Unset()

Unset marks the value as missing.




*type* OptionalLocalTime
------------------------

OptionalLocalTime is an optional LocalTime. Optional types must be used for
out parameters when a shape field is not required.


.. code-block:: go

    type OptionalLocalTime struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalLocalTime
...............................

.. code-block:: go

    func NewOptionalLocalTime(v LocalTime) OptionalLocalTime

NewOptionalLocalTime is a convenience function for creating an
OptionalLocalTime with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalLocalTime) Get() (LocalTime, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalLocalTime) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalLocalTime) Set(val LocalTime)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalLocalTime) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalLocalTime) Unset()

Unset marks the value as missing.




*type* OptionalMemory
---------------------

OptionalMemory is an optional Memory. Optional types must be used for
out parameters when a shape field is not required.


.. code-block:: go

    type OptionalMemory struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalMemory
............................

.. code-block:: go

    func NewOptionalMemory(v Memory) OptionalMemory

NewOptionalMemory is a convenience function for creating an
OptionalMemory with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalMemory) Get() (Memory, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalMemory) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalMemory) Set(val Memory)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalMemory) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalMemory) Unset()

Unset marks the value as missing.




*type* OptionalRangeDateTime
----------------------------

OptionalRangeDateTime is an optional RangeDateTime. Optional
types must be used for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalRangeDateTime struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalRangeDateTime
...................................

.. code-block:: go

    func NewOptionalRangeDateTime(v RangeDateTime) OptionalRangeDateTime

NewOptionalRangeDateTime is a convenience function for creating an
OptionalRangeDateTime with its value set to v.




*method* Get
............

.. code-block:: go

    func (o *OptionalRangeDateTime) Get() (RangeDateTime, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o *OptionalRangeDateTime) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalRangeDateTime) Set(val RangeDateTime)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalRangeDateTime) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalRangeDateTime) Unset()

Unset marks the value as missing.




*type* OptionalRangeFloat32
---------------------------

OptionalRangeFloat32 is an optional RangeFloat32. Optional
types must be used for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalRangeFloat32 struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalRangeFloat32
..................................

.. code-block:: go

    func NewOptionalRangeFloat32(v RangeFloat32) OptionalRangeFloat32

NewOptionalRangeFloat32 is a convenience function for creating an
OptionalRangeFloat32 with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalRangeFloat32) Get() (RangeFloat32, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalRangeFloat32) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalRangeFloat32) Set(val RangeFloat32)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalRangeFloat32) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalRangeFloat32) Unset()

Unset marks the value as missing.




*type* OptionalRangeFloat64
---------------------------

OptionalRangeFloat64 is an optional RangeFloat64. Optional
types must be used for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalRangeFloat64 struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalRangeFloat64
..................................

.. code-block:: go

    func NewOptionalRangeFloat64(v RangeFloat64) OptionalRangeFloat64

NewOptionalRangeFloat64 is a convenience function for creating an
OptionalRangeFloat64 with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalRangeFloat64) Get() (RangeFloat64, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalRangeFloat64) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalRangeFloat64) Set(val RangeFloat64)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalRangeFloat64) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalRangeFloat64) Unset()

Unset marks the value as missing.




*type* OptionalRangeInt32
-------------------------

OptionalRangeInt32 is an optional RangeInt32. Optional types must be used
for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalRangeInt32 struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalRangeInt32
................................

.. code-block:: go

    func NewOptionalRangeInt32(v RangeInt32) OptionalRangeInt32

NewOptionalRangeInt32 is a convenience function for creating an
OptionalRangeInt32 with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalRangeInt32) Get() (RangeInt32, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalRangeInt32) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalRangeInt32) Set(val RangeInt32)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalRangeInt32) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalRangeInt32) Unset()

Unset marks the value as missing.




*type* OptionalRangeInt64
-------------------------

OptionalRangeInt64 is an optional RangeInt64. Optional
types must be used for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalRangeInt64 struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalRangeInt64
................................

.. code-block:: go

    func NewOptionalRangeInt64(v RangeInt64) OptionalRangeInt64

NewOptionalRangeInt64 is a convenience function for creating an
OptionalRangeInt64 with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalRangeInt64) Get() (RangeInt64, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalRangeInt64) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalRangeInt64) Set(val RangeInt64)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalRangeInt64) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalRangeInt64) Unset()

Unset marks the value as missing.




*type* OptionalRangeLocalDate
-----------------------------

OptionalRangeLocalDate is an optional RangeLocalDate. Optional types must be
used for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalRangeLocalDate struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalRangeLocalDate
....................................

.. code-block:: go

    func NewOptionalRangeLocalDate(v RangeLocalDate) OptionalRangeLocalDate

NewOptionalRangeLocalDate is a convenience function for creating an
OptionalRangeLocalDate with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalRangeLocalDate) Get() (RangeLocalDate, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalRangeLocalDate) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalRangeLocalDate) Set(val RangeLocalDate)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalRangeLocalDate) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalRangeLocalDate) Unset()

Unset marks the value as missing.




*type* OptionalRangeLocalDateTime
---------------------------------

OptionalRangeLocalDateTime is an optional RangeLocalDateTime. Optional
types must be used for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalRangeLocalDateTime struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalRangeLocalDateTime
........................................

.. code-block:: go

    func NewOptionalRangeLocalDateTime(
        v RangeLocalDateTime,
    ) OptionalRangeLocalDateTime

NewOptionalRangeLocalDateTime is a convenience function for creating an
OptionalRangeLocalDateTime with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalRangeLocalDateTime) Get() (RangeLocalDateTime, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalRangeLocalDateTime) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalRangeLocalDateTime) Set(val RangeLocalDateTime)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalRangeLocalDateTime) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalRangeLocalDateTime) Unset()

Unset marks the value as missing.




*type* OptionalRelativeDuration
-------------------------------

OptionalRelativeDuration is an optional RelativeDuration. Optional types
must be used for out parameters when a shape field is not required.


.. code-block:: go

    type OptionalRelativeDuration struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalRelativeDuration
......................................

.. code-block:: go

    func NewOptionalRelativeDuration(v RelativeDuration) OptionalRelativeDuration

NewOptionalRelativeDuration is a convenience function for creating an
OptionalRelativeDuration with its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalRelativeDuration) Get() (RelativeDuration, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalRelativeDuration) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalRelativeDuration) Set(val RelativeDuration)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalRelativeDuration) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalRelativeDuration) Unset()

Unset marks the value as missing.




*type* OptionalStr
------------------

OptionalStr is an optional string. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalStr struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalStr
.........................

.. code-block:: go

    func NewOptionalStr(v string) OptionalStr

NewOptionalStr is a convenience function for creating an OptionalStr with
its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalStr) Get() (string, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalStr) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalStr) Set(val string)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalStr) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o.




*method* Unset
..............

.. code-block:: go

    func (o *OptionalStr) Unset()

Unset marks the value as missing.




*type* OptionalUUID
-------------------

OptionalUUID is an optional UUID. Optional types must be used for out
parameters when a shape field is not required.


.. code-block:: go

    type OptionalUUID struct {
        // contains filtered or unexported fields
    }


*function* NewOptionalUUID
..........................

.. code-block:: go

    func NewOptionalUUID(v UUID) OptionalUUID

NewOptionalUUID is a convenience function for creating an OptionalUUID with
its value set to v.




*method* Get
............

.. code-block:: go

    func (o OptionalUUID) Get() (UUID, bool)

Get returns the value and a boolean indicating if the value is present.




*method* MarshalJSON
....................

.. code-block:: go

    func (o OptionalUUID) MarshalJSON() ([]byte, error)

MarshalJSON returns o marshaled as json.




*method* Set
............

.. code-block:: go

    func (o *OptionalUUID) Set(val UUID)

Set sets the value.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (o *OptionalUUID) UnmarshalJSON(bytes []byte) error

UnmarshalJSON unmarshals bytes into \*o




*method* Unset
..............

.. code-block:: go

    func (o *OptionalUUID) Unset()

Unset marks the value as missing.




*type* RangeDateTime
--------------------

RangeDateTime is an interval of time.Time values.


.. code-block:: go

    type RangeDateTime struct {
        // contains filtered or unexported fields
    }


*function* NewRangeDateTime
...........................

.. code-block:: go

    func NewRangeDateTime(
        lower, upper OptionalDateTime,
        incLower, incUpper bool,
    ) RangeDateTime

NewRangeDateTime creates a new RangeDateTime value.




*method* Empty
..............

.. code-block:: go

    func (r RangeDateTime) Empty() bool

Empty returns true if the range is empty.




*method* IncLower
.................

.. code-block:: go

    func (r RangeDateTime) IncLower() bool

IncLower returns true if the lower bound is inclusive.




*method* IncUpper
.................

.. code-block:: go

    func (r RangeDateTime) IncUpper() bool

IncUpper returns true if the upper bound is inclusive.




*method* Lower
..............

.. code-block:: go

    func (r RangeDateTime) Lower() OptionalDateTime

Lower returns the lower bound.




*method* MarshalJSON
....................

.. code-block:: go

    func (r RangeDateTime) MarshalJSON() ([]byte, error)

MarshalJSON returns r marshaled as json.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (r *RangeDateTime) UnmarshalJSON(data []byte) error

UnmarshalJSON unmarshals bytes into \*r.




*method* Upper
..............

.. code-block:: go

    func (r RangeDateTime) Upper() OptionalDateTime

Upper returns the upper bound.




*type* RangeFloat32
-------------------

RangeFloat32 is an interval of float32 values.


.. code-block:: go

    type RangeFloat32 struct {
        // contains filtered or unexported fields
    }


*function* NewRangeFloat32
..........................

.. code-block:: go

    func NewRangeFloat32(
        lower, upper OptionalFloat32,
        incLower, incUpper bool,
    ) RangeFloat32

NewRangeFloat32 creates a new RangeFloat32 value.




*method* Empty
..............

.. code-block:: go

    func (r RangeFloat32) Empty() bool

Empty returns true if the range is empty.




*method* IncLower
.................

.. code-block:: go

    func (r RangeFloat32) IncLower() bool

IncLower returns true if the lower bound is inclusive.




*method* IncUpper
.................

.. code-block:: go

    func (r RangeFloat32) IncUpper() bool

IncUpper returns true if the upper bound is inclusive.




*method* Lower
..............

.. code-block:: go

    func (r RangeFloat32) Lower() OptionalFloat32

Lower returns the lower bound.




*method* MarshalJSON
....................

.. code-block:: go

    func (r RangeFloat32) MarshalJSON() ([]byte, error)

MarshalJSON returns r marshaled as json.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (r *RangeFloat32) UnmarshalJSON(data []byte) error

UnmarshalJSON unmarshals bytes into \*r.




*method* Upper
..............

.. code-block:: go

    func (r RangeFloat32) Upper() OptionalFloat32

Upper returns the upper bound.




*type* RangeFloat64
-------------------

RangeFloat64 is an interval of float64 values.


.. code-block:: go

    type RangeFloat64 struct {
        // contains filtered or unexported fields
    }


*function* NewRangeFloat64
..........................

.. code-block:: go

    func NewRangeFloat64(
        lower, upper OptionalFloat64,
        incLower, incUpper bool,
    ) RangeFloat64

NewRangeFloat64 creates a new RangeFloat64 value.




*method* Empty
..............

.. code-block:: go

    func (r RangeFloat64) Empty() bool

Empty returns true if the range is empty.




*method* IncLower
.................

.. code-block:: go

    func (r RangeFloat64) IncLower() bool

IncLower returns true if the lower bound is inclusive.




*method* IncUpper
.................

.. code-block:: go

    func (r RangeFloat64) IncUpper() bool

IncUpper returns true if the upper bound is inclusive.




*method* Lower
..............

.. code-block:: go

    func (r RangeFloat64) Lower() OptionalFloat64

Lower returns the lower bound.




*method* MarshalJSON
....................

.. code-block:: go

    func (r RangeFloat64) MarshalJSON() ([]byte, error)

MarshalJSON returns r marshaled as json.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (r *RangeFloat64) UnmarshalJSON(data []byte) error

UnmarshalJSON unmarshals bytes into \*r.




*method* Upper
..............

.. code-block:: go

    func (r RangeFloat64) Upper() OptionalFloat64

Upper returns the upper bound.




*type* RangeInt32
-----------------

RangeInt32 is an interval of int32 values.


.. code-block:: go

    type RangeInt32 struct {
        // contains filtered or unexported fields
    }


*function* NewRangeInt32
........................

.. code-block:: go

    func NewRangeInt32(
        lower, upper OptionalInt32,
        incLower, incUpper bool,
    ) RangeInt32

NewRangeInt32 creates a new RangeInt32 value.




*method* Empty
..............

.. code-block:: go

    func (r RangeInt32) Empty() bool

Empty returns true if the range is empty.




*method* IncLower
.................

.. code-block:: go

    func (r RangeInt32) IncLower() bool

IncLower returns true if the lower bound is inclusive.




*method* IncUpper
.................

.. code-block:: go

    func (r RangeInt32) IncUpper() bool

IncUpper returns true if the upper bound is inclusive.




*method* Lower
..............

.. code-block:: go

    func (r RangeInt32) Lower() OptionalInt32

Lower returns the lower bound.




*method* MarshalJSON
....................

.. code-block:: go

    func (r RangeInt32) MarshalJSON() ([]byte, error)

MarshalJSON returns r marshaled as json.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (r *RangeInt32) UnmarshalJSON(data []byte) error

UnmarshalJSON unmarshals bytes into \*r.




*method* Upper
..............

.. code-block:: go

    func (r RangeInt32) Upper() OptionalInt32

Upper returns the upper bound.




*type* RangeInt64
-----------------

RangeInt64 is an interval of int64 values.


.. code-block:: go

    type RangeInt64 struct {
        // contains filtered or unexported fields
    }


*function* NewRangeInt64
........................

.. code-block:: go

    func NewRangeInt64(
        lower, upper OptionalInt64,
        incLower, incUpper bool,
    ) RangeInt64

NewRangeInt64 creates a new RangeInt64 value.




*method* Empty
..............

.. code-block:: go

    func (r RangeInt64) Empty() bool

Empty returns true if the range is empty.




*method* IncLower
.................

.. code-block:: go

    func (r RangeInt64) IncLower() bool

IncLower returns true if the lower bound is inclusive.




*method* IncUpper
.................

.. code-block:: go

    func (r RangeInt64) IncUpper() bool

IncUpper returns true if the upper bound is inclusive.




*method* Lower
..............

.. code-block:: go

    func (r RangeInt64) Lower() OptionalInt64

Lower returns the lower bound.




*method* MarshalJSON
....................

.. code-block:: go

    func (r RangeInt64) MarshalJSON() ([]byte, error)

MarshalJSON returns r marshaled as json.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (r *RangeInt64) UnmarshalJSON(data []byte) error

UnmarshalJSON unmarshals bytes into \*r.




*method* Upper
..............

.. code-block:: go

    func (r RangeInt64) Upper() OptionalInt64

Upper returns the upper bound.




*type* RangeLocalDate
---------------------

RangeLocalDate is an interval of LocalDate values.


.. code-block:: go

    type RangeLocalDate struct {
        // contains filtered or unexported fields
    }


*function* NewRangeLocalDate
............................

.. code-block:: go

    func NewRangeLocalDate(
        lower, upper OptionalLocalDate,
        incLower, incUpper bool,
    ) RangeLocalDate

NewRangeLocalDate creates a new RangeLocalDate value.




*method* Empty
..............

.. code-block:: go

    func (r RangeLocalDate) Empty() bool

Empty returns true if the range is empty.




*method* IncLower
.................

.. code-block:: go

    func (r RangeLocalDate) IncLower() bool

IncLower returns true if the lower bound is inclusive.




*method* IncUpper
.................

.. code-block:: go

    func (r RangeLocalDate) IncUpper() bool

IncUpper returns true if the upper bound is inclusive.




*method* Lower
..............

.. code-block:: go

    func (r RangeLocalDate) Lower() OptionalLocalDate

Lower returns the lower bound.




*method* MarshalJSON
....................

.. code-block:: go

    func (r RangeLocalDate) MarshalJSON() ([]byte, error)

MarshalJSON returns r marshaled as json.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (r *RangeLocalDate) UnmarshalJSON(data []byte) error

UnmarshalJSON unmarshals bytes into \*r.




*method* Upper
..............

.. code-block:: go

    func (r RangeLocalDate) Upper() OptionalLocalDate

Upper returns the upper bound.




*type* RangeLocalDateTime
-------------------------

RangeLocalDateTime is an interval of LocalDateTime values.


.. code-block:: go

    type RangeLocalDateTime struct {
        // contains filtered or unexported fields
    }


*function* NewRangeLocalDateTime
................................

.. code-block:: go

    func NewRangeLocalDateTime(
        lower, upper OptionalLocalDateTime,
        incLower, incUpper bool,
    ) RangeLocalDateTime

NewRangeLocalDateTime creates a new RangeLocalDateTime value.




*method* Empty
..............

.. code-block:: go

    func (r RangeLocalDateTime) Empty() bool

Empty returns true if the range is empty.




*method* IncLower
.................

.. code-block:: go

    func (r RangeLocalDateTime) IncLower() bool

IncLower returns true if the lower bound is inclusive.




*method* IncUpper
.................

.. code-block:: go

    func (r RangeLocalDateTime) IncUpper() bool

IncUpper returns true if the upper bound is inclusive.




*method* Lower
..............

.. code-block:: go

    func (r RangeLocalDateTime) Lower() OptionalLocalDateTime

Lower returns the lower bound.




*method* MarshalJSON
....................

.. code-block:: go

    func (r RangeLocalDateTime) MarshalJSON() ([]byte, error)

MarshalJSON returns r marshaled as json.




*method* UnmarshalJSON
......................

.. code-block:: go

    func (r *RangeLocalDateTime) UnmarshalJSON(data []byte) error

UnmarshalJSON unmarshals bytes into \*r.




*method* Upper
..............

.. code-block:: go

    func (r RangeLocalDateTime) Upper() OptionalLocalDateTime

Upper returns the upper bound.




*type* RelativeDuration
-----------------------

RelativeDuration represents the elapsed time between two instants in a fuzzy
human way.


.. code-block:: go

    type RelativeDuration struct {
        // contains filtered or unexported fields
    }


*function* NewRelativeDuration
..............................

.. code-block:: go

    func NewRelativeDuration(
        months, days int32,
        microseconds int64,
    ) RelativeDuration

NewRelativeDuration returns a new RelativeDuration




*method* MarshalText
....................

.. code-block:: go

    func (rd RelativeDuration) MarshalText() ([]byte, error)

MarshalText returns rd marshaled as text.




*method* String
...............

.. code-block:: go

    func (rd RelativeDuration) String() string




*method* UnmarshalText
......................

.. code-block:: go

    func (rd *RelativeDuration) UnmarshalText(b []byte) error

UnmarshalText unmarshals bytes into \*rd.




*type* UUID
-----------

UUID is a universally unique identifier
`docs/stdlib/uuid <https://www.geldata.com/docs/stdlib/uuid>`_


.. code-block:: go

    type UUID [16]byte


*function* ParseUUID
....................

.. code-block:: go

    func ParseUUID(s string) (UUID, error)

ParseUUID parses s into a UUID or returns an error.




*method* MarshalText
....................

.. code-block:: go

    func (id UUID) MarshalText() ([]byte, error)

MarshalText returns the id as a byte string.




*method* String
...............

.. code-block:: go

    func (id UUID) String() string




*method* UnmarshalText
......................

.. code-block:: go

    func (id *UUID) UnmarshalText(b []byte) error

UnmarshalText unmarshals the id from a string.

