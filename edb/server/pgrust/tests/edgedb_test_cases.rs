mod test_util;

test_case!(all_env_default_ssl, "postgresql://host:123/testdb", env={
    "PGUSER": "user",
    "PGDATABASE": "testdb",
    "PGPASSWORD": "passw",
    "PGHOST": "host",
    "PGPORT": "123",
    "PGCONNECT_TIMEOUT": "8"
}, output={
    "user": "user",
    "password": "passw",
    "dbname": "testdb",
    "host": "host",
    "port": "123",
    "sslmode": "prefer",
    "connect_timeout": "8"
});

test_case!(dsn_override_env, "postgres://user2:passw2@host2:456/db2?connect_timeout=6", env={
    "PGUSER": "user",
    "PGDATABASE": "testdb",
    "PGPASSWORD": "passw",
    "PGHOST": "host",
    "PGPORT": "123",
    "PGCONNECT_TIMEOUT": "8"
}, output={
    "user": "user2",
    "password": "passw2",
    "dbname": "db2",
    "host": "host2",
    "port": "456",
    "connect_timeout": "6"
});

test_case!(dsn_override_env_ssl, "postgres://user2:passw2@host2:456/db2?sslmode=disable", env={
    "PGUSER": "user",
    "PGDATABASE": "testdb",
    "PGPASSWORD": "passw",
    "PGHOST": "host",
    "PGPORT": "123",
    "PGSSLMODE": "allow"
}, output={
    "user": "user2",
    "password": "passw2",
    "dbname": "db2",
    "host": "host2",
    "port": "456",
    "sslmode": "disable",
});

test_case!(dsn_overrides_env_partially, "postgres://user3:123123@localhost:5555/abcdef", env={
    "PGUSER": "user",
    "PGDATABASE": "testdb",
    "PGPASSWORD": "passw",
    "PGHOST": "host",
    "PGPORT": "123",
    "PGSSLMODE": "allow"
}, output={
    "user": "user3",
    "password": "123123",
    "dbname": "abcdef",
    "host": "localhost",
    "port": "5555",
    "sslmode": "allow"
});

test_case!(dsn_override_env_ssl_prefer, "postgres://user2:passw2@host2:456/db2?sslmode=disable", env={
    "PGUSER": "user",
    "PGDATABASE": "testdb",
    "PGPASSWORD": "passw",
    "PGHOST": "host",
    "PGPORT": "123",
    "PGSSLMODE": "prefer"
}, output={
    "user": "user2",
    "password": "passw2",
    "dbname": "db2",
    "host": "host2",
    "port": "456",
    "sslmode": "disable",
});

test_case!(dsn_overrides_env_partially_ssl_prefer, "postgres://user3:123123@localhost:5555/abcdef", env={
    "PGUSER": "user",
    "PGDATABASE": "testdb",
    "PGPASSWORD": "passw",
    "PGHOST": "host",
    "PGPORT": "123",
    "PGSSLMODE": "prefer"
}, output={
    "user": "user3",
    "password": "123123",
    "dbname": "abcdef",
    "host": "localhost",
    "port": "5555",
    "sslmode": "prefer"
});

test_case!(dsn_only, "postgres://user3:123123@localhost:5555/abcdef", output={
    "user": "user3",
    "password": "123123",
    "dbname": "abcdef",
    "host": "localhost",
    "port": "5555"
});

test_case!(dsn_only_multi_host, "postgresql://user@host1,host2/db", output={
    "user": "user",
    "dbname": "db",
    "host": "host1,host2",
    "port": "5432,5432"
});

test_case!(dsn_only_multi_host_and_port, "postgresql://user@host1:1111,host2:2222/db", output={
    "user": "user",
    "dbname": "db",
    "host": "host1,host2",
    "port": "1111,2222"
});

test_case!(params_multi_host_dsn_env_mix, "postgresql://host1,host2/db", env={
    "PGUSER": "foo"
}, output={
    "user": "foo",
    "dbname": "db",
    "host": "host1,host2",
    "port": "5432,5432"
});

test_case!(dsn_settings_override_and_ssl, "postgresql://me:ask@127.0.0.1:888/db?param=sss&param=123&host=testhost&user=testuser&port=2222&dbname=testdb&sslmode=require", output={
    "user": "testuser",
    "password": "ask",
    "dbname": "testdb",
    "host": "testhost",
    "port": "2222",
    "sslmode": "require",
    "param": "123"
}, expect_libpq_mismatch="Extra params are unsupported");

test_case!(multiple_settings, "postgresql://me:ask@127.0.0.1:888/db?param=sss&param=123&host=testhost&user=testuser&port=2222&dbname=testdb&sslmode=verify_full&aa=bb", output={
    "user": "testuser",
    "password": "ask",
    "dbname": "testdb",
    "host": "testhost",
    "port": "2222",
    "sslmode": "verify-full",
    "aa": "bb",
    "param": "123"
}, expect_libpq_mismatch="Extra params are unsupported");

test_case!(dsn_only_unix, "postgresql:///dbname?host=/unix_sock/test&user=spam", output={
    "user": "spam",
    "dbname": "dbname",
    "host": "/unix_sock/test",
    "port": "5432"
});

test_case!(dsn_only_quoted, "postgresql://us%40r:p%40ss@h%40st1,h%40st2:543%33/d%62", output={
    "user": "us@r",
    "password": "p@ss",
    "dbname": "db",
    "host": "h@st1,h@st2",
    "port": "5432,5433"
});

test_case!(dsn_only_unquoted_host, "postgresql://user:p@ss@host/db", output={
    "user": "user",
    "password": "p",
    "dbname": "db",
    "host": "ss@host",
    "port": "5432"
});

test_case!(dsn_only_quoted_params, "postgresql:///d%62?user=us%40r&host=h%40st&port=543%33", output={
    "user": "us@r",
    "dbname": "db",
    "host": "h@st",
    "port": "5433"
});

test_case!(dsn_ipv6_multi_host, "postgresql://user@[2001:db8::1234%25eth0],[::1]/db", output={
    "user": "user",
    "dbname": "db",
    "host": "2001:db8::1234%eth0,::1",
    "port": "5432,5432"
});

test_case!(dsn_ipv6_multi_host_port, "postgresql://user@[2001:db8::1234]:1111,[::1]:2222/db", output={
    "user": "user",
    "dbname": "db",
    "host": "2001:db8::1234,::1",
    "port": "1111,2222"
});

test_case!(dsn_ipv6_multi_host_query_part, "postgresql:///db?user=user&host=2001:db8::1234,::1", output={
    "user": "user",
    "dbname": "db",
    "host": "2001:db8::1234,::1",
    "port": "5432,5432"
});

test_case!(
    dsn_only_illegal_protocol,
    "pq:///dbname?host=/unix_sock/test&user=spam",
    error = "Invalid DSN.*"
);

test_case!(env_ports_mismatch_dsn_multi_hosts, "postgresql://host1,host2,host3/db", env={ "PGPORT": "111,222" }, error="Unexpected number of ports.*", expect_libpq_mismatch="Port count check doesn't happen in parse");

test_case!(dsn_only_quoted_unix_host_port_in_params, "postgres://user@?port=56226&host=%2Ftmp", output={
    "user": "user",
    "dbname": "user",
    "host": "/tmp",
    "port": "56226",
    "sslmode": "disable",
});

test_case!(dsn_only_cloudsql, "postgres:///db?host=/cloudsql/project:region:instance-name&user=spam", output={
    "user": "spam",
    "dbname": "db",
    "host": "/cloudsql/project:region:instance-name",
    "port": "5432"
});

test_case!(connect_timeout_neg8, "postgres://spam@127.0.0.1:5432/postgres?connect_timeout=-8", output={
    "user": "spam",
    "dbname": "postgres",
    "host": "127.0.0.1",
    "port": "5432"
});

test_case!(connect_timeout_neg1, "postgres://spam@127.0.0.1:5432/postgres?connect_timeout=-1", output={
    "user": "spam",
    "dbname": "postgres",
    "host": "127.0.0.1",
    "port": "5432"
});

test_case!(connect_timeout_0, "postgres://spam@127.0.0.1:5432/postgres?connect_timeout=0", output={
    "user": "spam",
    "dbname": "postgres",
    "host": "127.0.0.1",
    "port": "5432"
});

test_case!(connect_timeout_1, "postgres://spam@127.0.0.1:5432/postgres?connect_timeout=1", output={
    "user": "spam",
    "dbname": "postgres",
    "host": "127.0.0.1",
    "port": "5432",
    "connect_timeout": "2"
});

test_case!(connect_timeout_2, "postgres://spam@127.0.0.1:5432/postgres?connect_timeout=2", output={
    "user": "spam",
    "dbname": "postgres",
    "host": "127.0.0.1",
    "port": "5432",
    "connect_timeout": "2"
});

test_case!(connect_timeout_3, "postgres://spam@127.0.0.1:5432/postgres?connect_timeout=3", output={
    "user": "spam",
    "dbname": "postgres",
    "host": "127.0.0.1",
    "port": "5432",
    "connect_timeout": "3"
});

// We intentially don't pass these tests

test_case!(dsn_combines_env_multi_host, "postgresql:///db", env={
    "PGHOST": "host1:1111,host2:2222",
    "PGUSER": "foo"
}, error="", expect_libpq_mismatch="libpq parses hostnames with colons");

test_case!(dsn_only_cloudsql_unix_and_tcp, "postgres:///db?host=127.0.0.1:5432,/cloudsql/project:region:instance-name,localhost:5433&user=spam", error="", expect_libpq_mismatch="libpq parses hostnames with colons");

test_case!(
    dsn_multi_host_combines_env,
    "postgresql:///db?host=host1:1111,host2:2222",
    error = "",
    expect_libpq_mismatch = "libpq parses hostnames with colons"
);
