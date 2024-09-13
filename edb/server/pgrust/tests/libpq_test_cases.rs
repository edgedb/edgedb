// Copyright (c) 2021-2023, PostgreSQL Global Development Group

// These testcases were extracted from 001_uri.pl.

mod test_util;

test_case!(full_uri, "postgresql://uri-user:secret@host:12345/db", output={
    "user": "uri-user",
    "password": "secret",
    "dbname": "db",
    "host": "host",
    "port": "12345"
}, no_env=no_env);

test_case!(user_host_port_db, "postgresql://uri-user@host:12345/db", output={
    "user": "uri-user",
    "dbname": "db",
    "host": "host",
    "port": "12345"
}, no_env=no_env);

test_case!(user_host_db, "postgresql://uri-user@host/db", output={
    "user": "uri-user",
    "dbname": "db",
    "host": "host"
}, no_env=no_env);

test_case!(host_port_db, "postgresql://host:12345/db", output={
    "dbname": "db",
    "host": "host",
    "port": "12345"
}, no_env=no_env);

test_case!(host_db, "postgresql://host/db", output={
    "dbname": "db",
    "host": "host"
}, no_env=no_env);

test_case!(user_host_port, "postgresql://uri-user@host:12345/", output={
    "user": "uri-user",
    "host": "host",
    "port": "12345"
}, no_env=no_env);

test_case!(user_host, "postgresql://uri-user@host/", output={
    "user": "uri-user",
    "host": "host"
}, no_env=no_env);

test_case!(user_only, "postgresql://uri-user@", output={
    "user": "uri-user"
}, no_env=no_env);

test_case!(host_port, "postgresql://host:12345/", output={
    "host": "host",
    "port": "12345"
}, no_env=no_env);

test_case!(host_port_no_slash, "postgresql://host:12345", output={
    "host": "host",
    "port": "12345"
}, no_env=no_env);

test_case!(host_only, "postgresql://host/", output={
    "host": "host"
}, no_env=no_env);

test_case!(host_no_slash, "postgresql://host", output={
    "host": "host"
}, no_env=no_env);

test_case!(empty_uri, "postgresql://", output = {}, no_env = no_env);

test_case!(hostaddr_only, "postgresql://?hostaddr=127.0.0.1", output={
    "hostaddr": "127.0.0.1"
}, no_env=no_env);

test_case!(host_and_hostaddr, "postgresql://example.com?hostaddr=63.1.2.4", output={
    "host": "example.com",
    "hostaddr": "63.1.2.4"
}, no_env=no_env);

test_case!(percent_encoded_host, "postgresql://%68ost/", output={
    "host": "host"
}, no_env=no_env);

test_case!(query_user, "postgresql://host/db?user=uri-user", output={
    "user": "uri-user",
    "dbname": "db",
    "host": "host"
}, no_env=no_env);

test_case!(query_user_port, "postgresql://host/db?user=uri-user&port=12345", output={
    "user": "uri-user",
    "dbname": "db",
    "host": "host",
    "port": "12345"
}, no_env=no_env);

test_case!(query_percent_encoded_user, "postgresql://host/db?u%73er=someotheruser&port=12345", output={
    "user": "someotheruser",
    "dbname": "db",
    "host": "host",
    "port": "12345"
}, no_env=no_env);

test_case!(
    invalid_percent_encoded_uzer,
    "postgresql://host/db?u%7aer=someotheruser&port=12345",
    output = {"uzer": "someotheruser", "host": "host", "dbname": "db", "port": "12345"},
    expect_libpq_mismatch = "Our library allows arbitrary params",
    no_env = no_env
);

test_case!(query_user_with_port, "postgresql://host:12345?user=uri-user", output={
    "user": "uri-user",
    "host": "host",
    "port": "12345"
}, no_env=no_env);

test_case!(query_user_with_host, "postgresql://host?user=uri-user", output={
    "user": "uri-user",
    "host": "host"
}, no_env=no_env);

test_case!(empty_query, "postgresql://host?", output={
    "host": "host"
}, no_env=no_env);

test_case!(ipv6_host_port_db, "postgresql://[::1]:12345/db", output={
    "dbname": "db",
    "host": "::1",
    "port": "12345"
}, no_env=no_env);

test_case!(ipv6_host_db, "postgresql://[::1]/db", output={
    "dbname": "db",
    "host": "::1"
}, no_env=no_env);

test_case!(ipv6_host_full, "postgresql://[2001:db8::1234]/", output={
    "host": "2001:db8::1234"
}, no_env=no_env);

test_case!(
    invalid_ipv6_host,
    "postgresql://[200z:db8::1234]/",
    error = "",
    expect_libpq_mismatch = "Invalid hosts are caught early",
    no_env = no_env
);

test_case!(ipv6_host_only, "postgresql://[::1]", output={
    "host": "::1"
}, no_env=no_env);

test_case!(postgres_empty, "postgres://", output = {}, no_env = no_env);

test_case!(postgres_root, "postgres:///", output = {}, no_env = no_env);

test_case!(postgres_db_only, "postgres:///db", output={
    "dbname": "db"
}, no_env=no_env);

test_case!(postgres_user_db, "postgres://uri-user@/db", output={
    "user": "uri-user",
    "dbname": "db"
}, no_env=no_env);

test_case!(postgres_socket_dir, "postgres://?host=/path/to/socket/dir", output={
    "host": "/path/to/socket/dir"
}, no_env=no_env);

test_case!(
    invalid_query_param,
    "postgresql://host?uzer=",
    output = {
        "uzer": "",
        "host": "host",
    },
    expect_libpq_mismatch = "Arbitrary query params are supported",
    no_env = no_env
);

test_case!(
    invalid_scheme,
    "postgre://",
    error = "missing \"=\" after \"postgre://\" in connection info string",
    no_env = no_env
);

test_case!(unclosed_ipv6, "postgres://[::1", error="end of string reached when looking for matching \"]\" in IPv6 host address in URI: \"postgres://[::1", no_env=no_env);

test_case!(
    empty_ipv6,
    "postgres://[]",
    error = "IPv6 host address may not be empty in URI: \"postgres://[]\"",
    no_env = no_env
);

test_case!(invalid_ipv6_end, "postgres://[::1]z", error="unexpected character \"z\" at position 17 in URI (expected \":\" or \"/\"): \"postgres://[::1]z\"", no_env=no_env);

test_case!(
    missing_query_value,
    "postgresql://host?zzz",
    error = "missing key/value separator \"=\" in URI query parameter: \"zzz\"",
    no_env = no_env
);

test_case!(
    multiple_missing_values,
    "postgresql://host?value1&value2",
    error = "missing key/value separator \"=\" in URI query parameter: \"value1\"",
    no_env = no_env
);

test_case!(
    extra_equals,
    "postgresql://host?key=key=value",
    error = "",
    no_env = no_env
);

test_case!(
    invalid_percent_encoding,
    "postgres://host?dbname=%XXfoo",
    error = "invalid percent-encoded token: \"%XXfoo\"",
    no_env = no_env
);

test_case!(
    null_in_percent_encoding,
    "postgresql://a%00b",
    error = "forbidden value %00 in percent-encoded value: \"a%00b\"",
    no_env = no_env
);

test_case!(
    invalid_percent_encoding_zz,
    "postgresql://%zz",
    error = "invalid percent-encoded token: \"%zz\"",
    no_env = no_env
);

test_case!(
    incomplete_percent_encoding_1,
    "postgresql://%1",
    error = "invalid percent-encoded token: \"%1\"",
    no_env = no_env
);

test_case!(
    incomplete_percent_encoding_empty,
    "postgresql://%",
    error = "invalid percent-encoded token: \"%\"",
    no_env = no_env
);

test_case!(empty_user, "postgres://@host", output={
    "host": "host"
}, no_env=no_env);

test_case!(empty_port, "postgres://host:/", output={
    "host": "host"
}, no_env=no_env);

test_case!(port_only, "postgres://:12345/", output={
    "port": "12345"
}, no_env=no_env);

test_case!(user_query_host, "postgres://otheruser@?host=/no/such/directory", output={
    "user": "otheruser",
    "host": "/no/such/directory"
}, no_env=no_env);

test_case!(user_query_host_with_slash, "postgres://otheruser@/?host=/no/such/directory", output={
    "user": "otheruser",
    "host": "/no/such/directory"
}, no_env=no_env);

test_case!(user_port_query_host, "postgres://otheruser@:12345?host=/no/such/socket/path", output={
    "user": "otheruser",
    "host": "/no/such/socket/path",
    "port": "12345"
}, no_env=no_env);

test_case!(user_port_db_query_host, "postgres://otheruser@:12345/db?host=/path/to/socket", output={
    "user": "otheruser",
    "dbname": "db",
    "host": "/path/to/socket",
    "port": "12345"
}, no_env=no_env);

test_case!(port_db_query_host, "postgres://:12345/db?host=/path/to/socket", output={
    "dbname": "db",
    "host": "/path/to/socket",
    "port": "12345"
}, no_env=no_env);

test_case!(port_query_host, "postgres://:12345?host=/path/to/socket", output={
    "host": "/path/to/socket",
    "port": "12345"
}, no_env=no_env);

test_case!(percent_encoded_path, "postgres://%2Fvar%2Flib%2Fpostgresql/dbname", output={
    "dbname": "dbname",
    "host": "/var/lib/postgresql"
}, no_env=no_env);

test_case!(sslmode_disable, "postgresql://host?sslmode=disable", output={
    "host": "host",
    "sslmode": "disable"
}, no_env=no_env);

// This one is challenging to test because of the sslmode defaults
// test_case!(sslmode_prefer, "postgresql://host?sslmode=prefer", output={
//     "host": "host",
//     "sslmode": "prefer"
// }, no_env=no_env);

// Intentional difference from libpq: this is what they do (from 001_uri.pl):
//
// "Usually the default sslmode is 'prefer' (for libraries with SSL) or
// 'disable' (for those without). This default changes to 'verify-full' if
// the system CA store is in use.""
test_case!(sslmode_verify_full, "postgresql://host?sslmode=verify-full", output={
    "host": "host",
    "sslmode": "verify-full"
}, no_env=no_env);
