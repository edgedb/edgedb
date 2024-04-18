

CREATE FUNCTION
std::`_[_]`(array: array<anytype>, idx: std::int64) -> anytype
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[_]`(array: json, idx: std::int64) -> json
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[_:]`(array: array<anytype>, idx: std::int64) -> array<anytype>
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[_:]`(array: json, idx: std::int64) -> json
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[:_]`(array: array<anytype>, idx: std::int64) -> array<anytype>
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[:_]`(array: json, idx: std::int64) -> json
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[_:_]`(array: array<anytype>, idx_start: std::int64, idx_end: std::int64) -> array<anytype>
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[_:_]`(array: json, idx_start: std::int64, idx_end: std::int64) -> json
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[_]`(s: std::str, idx: std::int64) -> std::str
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[_:]`(s: std::str, idx: std::int64) -> std::str
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[:_]`(s: std::str , idx: std::int64) -> std::str
{
    USING SQL EXPRESSION;
};

CREATE FUNCTION
std::`_[_:_]`(s: std::str, idx_start: std::int64, idx_end: std::int64) -> std::str
{
    USING SQL EXPRESSION;
};