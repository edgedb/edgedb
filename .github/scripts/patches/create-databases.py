# Create databases on the older edgedb version

import edgedb
import os
import subprocess

version = os.getenv('EDGEDB_VERSION')
cmd = [
    f'edgedb-server-{version}/bin/edgedb-server', '-D' 'test-dir',
    '--testmode', '--security', 'insecure_dev_mode', '--port', '10000',
]
proc = subprocess.Popen(cmd)

try:
    db = edgedb.create_client(
        host='localhost', port=10000, tls_security='insecure'
    )
    for name in [
        'json', 'functions', 'expressions', 'casts', 'policies', 'vector',
        'scope', 'httpextauth',
    ]:
        db.execute(f'create database {name};')

    # For the scope database, let's actually migrate to it.  This
    # will test that the migrations can still work after the upgrade.
    db2 = edgedb.create_client(
        host='localhost', port=10000, tls_security='insecure', database='scope'
    )
    with open("tests/schemas/cards.esdl") as f:
        body = f.read()
    db2.execute(f'''
        START MIGRATION TO {{
            module default {{
                {body}
            }}
        }};
        POPULATE MIGRATION;
        COMMIT MIGRATION;
    ''')

    # Put something in the query cache
    db2.query(r'''
        SELECT User {
            name,
            id
        }
        ORDER BY User.name;
    ''')

    db2.close()

    # Compile a query from the CLI.
    # (At one point, having a cached query with proto version 1 caused
    # trouble...)
    cli_base = [
        'edgedb',
        'query',
        '-H',
        'localhost',
        '-P',
        '10000',
        '-b',
        'json',
        '--tls-security',
        'insecure',
    ]
    subprocess.run(
        [*cli_base, 'select 1+1'],
        check=True,
    )

    # For the httpextauth database, create the proper extensions, so
    # that patching of the auth extension in place can get tested.
    db2 = edgedb.create_client(
        host='localhost', port=10000, tls_security='insecure',
        database='httpextauth'
    )
    db2.execute(f'''
        create extension pgcrypto;
        create extension auth;
    ''')
    db2.close()

finally:
    proc.terminate()
    proc.wait()
